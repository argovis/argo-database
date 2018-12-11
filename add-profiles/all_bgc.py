#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import sys
sys.path.append('..')
from argoDatabase import argoDatabase, getOutput
from multiprocessing import cpu_count
from addFunctions import format_logger, run_parallel_process
import warnings

from numpy import warnings as npwarnings
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

dbName = 'argo'
npes = cpu_count()
if __name__ == '__main__':
    format_logger('bgc.log', level=logging.DEBUG)
    logging.warning('Start of log file')
    dacs = ['aoml', 'coriolis', 'nmdis', 'kordi', 'meds', 'kma', 'bodc', 'csio', 'incois', 'jma', 'csiro']
    ncFileDir = getOutput()
    ad = argoDatabase(dbName, 'profiles',
                      replaceProfile=True,
                      qcThreshold='1', 
                      dbDumpThreshold=1000,
                      removeExisting=True,
                      testMode=False)

    allFiles = ad.get_file_names_to_add(ncFileDir, dacs=dacs)
    files = []
    for file in allFiles:
        fileName = file.split('/')[-1]
        if fileName.startswith('M'):
            files.append(file)
    run_parallel_process(ad, files, npes)
        
    #ad.add_locally(OUTPUTDIR, files)
    logging.warning('End of log file')