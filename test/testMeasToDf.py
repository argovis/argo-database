# -*- coding: utf-8 -*-
"""
Created on Thu Nov 29 15:55:28 2018

@author: tyler
"""

import os
import sys
import pdb
import re
from numpy import isnan
sys.path.append('..')
sys.path.append('../add-profiles/')
from argoDatabase import argoDatabase
import addFunctions as af
import unittest
import warnings
import pandas as pd
from numpy import warnings as npwarnings
from argoDBClass import argoDBClass

#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

class measToDfTest(argoDBClass):

    def test_noisy_platform(self):
        #  check platform with noisy bgc meas
        platform = ['3901498']
        df = self.df
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['platform'].isin(platform)].head()
        self.ad.addToDb = False
        files = df.file.tolist()
        self.ad.add_locally(self.OUTPUTDIR, files)
        for doc in self.ad.documents:
            bgcDf = pd.DataFrame(doc['bgcMeas'])
            self.assertEqual(bgcDf.dropna(axis=1, how='all').shape[1], bgcDf.shape[1], 'should not contain empty columns')
    
    def test_all_bad_temp(self):
        #  nc with bad qc on temp should not be created
        profiles = ['6900287_8']
        df = self.df
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['_id'].isin(profiles)]
        self.ad.addToDb = False
        files = df.file.tolist()
        self.ad.add_locally(self.OUTPUTDIR, files)
        self.assertEqual(len(self.ad.documents), 0, "profile 6900287_8 should not be added")

    def test_missing_rows(self):
        #  profile with missing psal at the end
        _id = '4900421_16'
        profiles = [_id]
        df = self.df
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['_id'].isin(profiles)]
        self.ad.addToDb = True
        files = df.file.tolist()
        self.ad.add_locally(self.OUTPUTDIR, files)
        coll = self.ad.create_collection()
        doc = coll.find_one({'_id': _id})
        lastPsal = doc['measurements'][-1]['psal']
        self.assertTrue(isnan(lastPsal), 'last psal should be nan')   

if __name__ == '__main__':
    unittest.main()
