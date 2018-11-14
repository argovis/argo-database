# -*- coding: utf-8 -*-
import pymongo
import os
import re
import glob
import logging
import numpy as np
from datetime import datetime
from netCDF4 import Dataset
import bson.errors
import pdb
from netCDFToDoc import netCDFToDoc
from openArgoNC import openArgoNcFile

class argoDatabase(object):
    def __init__(self,
                 dbName,
                 collectionName, 
                 replaceProfile=False,
                 qcThreshold='1', 
                 dbDumpThreshold=10000,
                 removeExisting=True, 
                 testMode=False):
        logging.debug('initializing ArgoDatabase')
        self.init_database(dbName)
        self.init_profiles_collection(collectionName)
        self.dbName = dbName
        self.home_dir = os.getcwd()
        self.replaceProfile = replaceProfile
        self.url = 'ftp://ftp.ifremer.fr/ifremer/argo/dac/'
        self.qcThreshold = qcThreshold
        self.dbDumpThreshold = dbDumpThreshold
        self.removeExisting = removeExisting
        self.argoFlagsWriter = openArgoNcFile() # used for adding additional flags
        self.testMode = testMode # used for testing documents outside database
        self.documents = []

    def init_database(self, dbName):
        logging.debug('initializing init_database')
        dbUrl = 'mongodb://localhost:27017/'
        client = pymongo.MongoClient(dbUrl)
        self.db = client[dbName]
    
    @staticmethod
    def create_collection(dbName, collectionName='profiles'):
        dbUrl = 'mongodb://localhost:27017/'
        client = pymongo.MongoClient(dbUrl)
        db = client[dbName]
        return db[collectionName]    

    def init_profiles_collection(self, collectionName):
        try:
            self.profiles_coll = self.db[collectionName]
            self.profiles_coll.create_index([('date', pymongo.DESCENDING)])
            self.profiles_coll.create_index([('platform_number', pymongo.DESCENDING)])
            self.profiles_coll.create_index([('cycle_number', pymongo.DESCENDING)])
            self.profiles_coll.create_index([('dac', pymongo.DESCENDING)])
            self.profiles_coll.create_index([('geoLocation', pymongo.GEOSPHERE)])
            self.profiles_coll.create_index([('containsBGC', pymongo.DESCENDING)])
        except:
            logging.warning('not able to get collections or set indexes')
    
    @staticmethod
    def get_file_names_to_add(localDir, howToAdd='all', files=[], dacs=[]):

        reBR = re.compile(r'^(?!.*BR\d{1,})') # ignore characters starting with BR followed by a digit
        if howToAdd=='by_dac_profiles':
            logging.debug('adding dac profiles from path: {0}'.format(localDir))
            for dac in dacs:
                logging.debug('On dac: {0}'.format(dac))
                files = files+glob.glob(os.path.join(localDir, dac, '**', 'profiles', '*.nc'))     
        elif howToAdd=='profile_list':
            logging.debug('adding profiles in provided list')
        elif howToAdd=='profiles':
            logging.debug('adding profiles individually')
            files = files+glob.glob(os.path.join(localDir, '**', '**', 'profiles', '*.nc'))
            files = list(filter(reBR.search, files))
        else:
            logging.warning('howToAdd not recognized. not going to do anything.')
            return
        return files        

    def add_locally(self, localDir, files):

        if self.removeExisting and not self.testMode: # Removes profiles on list before adding list (redundant...but requested)
            self.remove_profiles(files)
            
        coll = self.create_collection(self.dbName)

        logging.warning('Attempting to add: {}'.format(len(files)))
        documents = []
        for fileName in files:
            logging.info('on file: {0}'.format(fileName))
            dacName = fileName.split('/')[-4]
            root_grp = Dataset(fileName, "r", format="NETCDF4")
            remotePath = self.url + os.path.relpath(fileName, localDir)
            variables = root_grp.variables
            
            #current method of creating dac
            doc = self.make_profile_doc(variables, dacName, remotePath, fileName)
            if isinstance(doc, dict):
                doc = self.add_flags(doc, fileName)
                documents.append(doc)
                if self.testMode:
                    self.documents.append(doc)
            if len(documents) >= self.dbDumpThreshold and not self.testMode:
                logging.debug('dumping data to database')
                self.add_many_profiles(documents, fileName, coll)
                documents = []
        logging.debug('all files have been read. dumping remaining documents to database')
        if len(documents) == 1 and not self.testMode:
            self.add_single_profile(documents[0], fileName, coll)
        elif len(documents) > 1 and not self.testMode:
            self.add_many_profiles(documents, fileName, coll)

    def add_flags(self, doc, filename):
        try:
            self.argoFlagsWriter.create_profile_data_if_exists(filename)
            flagDoc = self.argoFlagsWriter.get_profile_data()
        except TypeError as err:
            logging.warning('Type error: {}'.format(err))
            logging.warning('could not retrieve flags for: {}'.format(filename))
            return doc            
        except:
            logging.warning('unknown error for: {}. could not retrieve flags'.format(filename))
            return doc
        doc['VERTICAL_SAMPLING_SCHEME'] = flagDoc['VERTICAL_SAMPLING_SCHEME']
        doc['STATION_PARAMETERS_inMongoDB'] = flagDoc['STATION_PARAMETERS_inMongoDB']
        doc['BASIN'] = flagDoc['BASIN']
        
        return doc
        
    def remove_profiles(self, files):
        #get profile ids
        idList = []
        for fileName in files:
            profileName = fileName.split('/')[-1]
            profileName = profileName[1:-3]
            profileCycle = profileName.split('_')
            cycle = profileName.split('_')[1].lstrip('0')
            profileName = profileCycle[0] + '_' + cycle
            idList.append(profileName)
        #remove all profiles at once
        logging.debug('removing profiles before reintroducing')
        logging.debug('number of profiles to be deleted: {}'.format(len(idList)))
        countBefore = self.profiles_coll.find({}).count()
        self.profiles_coll.delete_many({'_id': {'$in': idList}})
        countAfter = self.profiles_coll.find({}).count()
        delta = countBefore - countAfter
        self.profiles_coll.find({}).count()
        logging.debug('number of profiles removed: {}'.format(delta))

    @staticmethod
    def format_param(param):
        """
        Param is an fixed array of characters. format_param attempts to convert this array to
        a string.
        """
        if type(param) == np.ndarray:
            formatted_param = ''.join([(x.astype(str)) for x in param])
        elif type(param) == np.ma.core.MaskedArray:
            try:
                formatted_param = ''.join([(x.astype(str)) for x in param.data])
            except NotImplementedError:
                logging.debug('NotImplementedError param is not expected type')
            except AttributeError:  # sometimes platform_num is an array...
                logging.debug('attribute error: param is not expected type')
                logging.debug('type: {}'.format(type(param)))
            except:
                logging.debug('type: {}'.format(type(param)))
        else:
            logging.error(' check type: {}'.format(type(param)))
            pass
        return formatted_param.strip(' ')

    def make_profile_doc(self, variables, dacName, remotePath, fileName):
        """
        Retrieves some meta information and counts number of profile measurements.
        Sometimes a profile will contain more than one measurement in variables field.
        Only the first is added.
        """

        cycles = variables['CYCLE_NUMBER'][:][:]
        list_of_dup_inds = [np.where(a == cycles)[0] for a in np.unique(cycles)]
        for array in list_of_dup_inds:
            if len(array) > 2:
                logging.info('duplicate cycle numbers found...')

        platformNumber = self.format_param(variables['PLATFORM_NUMBER'][0])
        stationParameters = list(map(lambda param: self.format_param(param), variables['STATION_PARAMETERS'][0]))

        refDateArray = variables['REFERENCE_DATE_TIME'][:]
        refStr = ''.join([x.astype(str) for x in refDateArray])
        refDate = datetime.strptime(refStr, '%Y%m%d%H%M%S')
        idx = 0 #stometimes there are two profiles. The second profile is ignored.
        qcThreshold='1'
        try:
            p2D = netCDFToDoc(variables, dacName, refDate, remotePath, stationParameters, platformNumber, idx, qcThreshold)
            doc = p2D.get_profile_doc()
            return doc
        except AttributeError as err:
            logging.warning('Profile: {0} enountered AttributeError. \n Reason: {1}'.format(fileName, err.args))
        except TypeError as err:
            logging.warning('Profile: {0} enountered TypeError. \n Reason: {1}'.format(fileName, err.args))
        except ValueError as err:
            logging.warning('Profile: {0} enountered ValueError. \n Reason: {1}'.format(fileName, err.args))
        except UnboundLocalError as err:
            if 'no valid measurements.' in err.args[0]:
                logging.warning('Profile: {0} has no valid measurements. not going to add'.format(fileName))
            else:
                logging.warning('Profile: {0} encountered UnboundLocalError. \n Reason: {1}'.format(fileName, err.args))

    def add_single_profile(self, doc, file_name, coll, attempt=0):
        if self.replaceProfile:
            try:
                coll.replace_one({'_id': doc['_id']}, doc, upsert=True)
            except pymongo.errors.WriteError:
                logging.warning('check the following id '
                                'for filename : {0}'.format(doc['_id'], file_name))
            except bson.errors.InvalidDocument as err:
                logging.warning('bson error {1} for: {0}'.format(doc['_id'], err))
            except TypeError:
                logging.warning('Type error while inserting one document.')
        else:
            try:
                coll.insert_one(doc)
            except pymongo.errors.DuplicateKeyError:
                logging.error('duplicate key: {0}'.format(doc['_id']))
                logging.error('not going to add')
    
            except pymongo.errors.WriteError:
                logging.warning('check the following id '
                                'for filename : {0}'.format(doc['_id'], file_name))
            except bson.errors.InvalidDocument as err:
                logging.warning('bson error {1} for: {0}'.format(doc['_id'], err))
            except TypeError:
                logging.warning('Type error while inserting one document.')

    def add_many_profiles(self, documents, file_name, coll):
        try:
            coll.insert_many(documents, ordered=False)
        except pymongo.errors.BulkWriteError as bwe:
            writeErrors = bwe.details['writeErrors']
            problem_idx = []
            for we in writeErrors:
                problem_idx.append(we['index'])
            trouble_list = [documents[i] for i in problem_idx]
            logging.warning('bulk write failed for: {0}'.format(file_name))
            logging.warning('adding the failed documents one at a time.')
            for doc in trouble_list:
                self.add_single_profile(doc, file_name, coll)
        except bson.errors.InvalidDocument:
            logging.warning('bson error')
            for doc in documents:
                self.add_single_profile(doc, file_name, coll)
        except TypeError:
            nonDictDocs = [doc for doc in documents if not isinstance(doc, dict)]
            logging.warning('Type error during insert_many method. Check documents.')
            logging.warning('number of non dictionary items in documents: {}'.format(len(nonDictDocs)))
        
        
def getOutput():
    mySystem = os.uname().nodename
    if mySystem == 'carby':
        OUTPUT_DIR = os.path.join('/storage', 'ifremer')
    elif mySystem == 'kadavu.ucsd.edu':
        OUTPUT_DIR = os.path.join('/home', 'tylertucker', 'ifremer')
    elif mySystem == 'ciLab':
        OUTPUT_DIR = os.path.join('/home', 'gstudent4', 'Desktop', 'ifremer')
    else:
        print('pc not found. assuming default')
        OUTPUT_DIR = os.path.join('/storage', 'ifremer')
    return OUTPUT_DIR
