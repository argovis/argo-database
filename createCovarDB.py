# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
Created on Mon Nov 19 15:08:59 2018

@author: tyler
"""

import json
import pdb
import os
import glob
import pymongo
import pandas as pd

def get_feature_collection(file):
    with open(file) as dataFile:
        data = dataFile.read()
        featureColl = data.strip('var dataset = ')
        featureColl = json.loads(featureColl)
    return featureColl

def create_collection(collName, dbName):
    dbUrl = 'mongodb://localhost:27017/'
    client = pymongo.MongoClient(dbUrl)
    db = client[dbName]
    coll = db[collName]
    return coll

def format_features(features):
    formattedFeatures = []
    for feature in features:
        doc = feature
        coords = feature['geometry']['coordinates']
        doc['geometry']['coordinates'] = [coords[1], coords[0]]
        formattedFeatures.append(doc)
    return formattedFeatures

def create_covar_docs(localDir, forcastDays, forTest):
    files = glob.glob(os.path.join(localDir, '*.js'))
    docs = []
    if forTest:
        files = files[0:10]
    for file in files:
        doc = {}
        
        lat = float(file.strip('.js').split('_')[-3])
        lng = float(file.strip('.js').split('_')[-1])
        _id = str(lng)+'_'+ str(lat) + '_' + str(forcastDays)
        doc['_id'] = _idgi
        doc['forcastDays'] = float(forcastDays)
        doc['geoLocation'] = {'type': 'Point', 'coordinates': [lng, lat]}
        featureColl = get_feature_collection(file)
        features = format_features(featureColl['features'])
        doc['features'] = features
        docs.append(doc)
    return docs

def add_docs_to_database(docs, coll):
    for doc in docs:
        try:
            coll.insert(doc)
        except Exception as err:
            pdb.set_trace()
            doc    
    coll.create_index([('geoLocation', pymongo.GEOSPHERE)])

def main_add(localDir, coll, forcastDays, forTest=False):
    docs = create_covar_docs(localDir, forcastDays, forTest)
    print('number of docs: {}'.format(len(docs)))
    if not forTest:
        add_docs_to_database(docs, coll)
    else:
        add_docs_to_database(docs, coll)

if __name__ == '__main__':

    dbName = 'argo2'
    sLocalDir = './60_day'
    collName = 'covars'
    lLocalDir = './140_day'
    coll = create_collection(collName, dbName)
    coll.drop()
    main_add(sLocalDir, coll, forcastDays=60)
    main_add(lLocalDir, coll, forcastDays=140)

    # add for testing purposes
    testDbName = 'argo-express-test'
    testColl = create_collection(collName, testDbName)
    testColl.drop()
    main_add(sLocalDir, testColl, forcastDays=60, forTest=True)
    main_add(lLocalDir, testColl, forcastDays=140, forTest=True)

        
