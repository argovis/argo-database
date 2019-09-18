import os
import sys
sys.path.append('..')
import logging
from datetime import datetime
import multiprocessing as mp
from argoDatabase import argoDatabase
import tmpFunctions as tf
from addFunctions import format_logger, run_parallel_process
import warnings
from numpy import warnings as npwarnings
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

dbName = 'argo'
#npes = mp.cpu_count()
npes = 1

if len(sys.argv) == 3:
    minDate = datetime.strptime(sys.argv[1], '%Y-%m-%d')
    maxDate = datetime.strptime(sys.argv[2], '%Y-%m-%d')
else:
    minDate = tf.get_last_updated(filename='lastUpdated.txt')
    maxDate = datetime.today()

if __name__ == '__main__':
    ncFileDir = 'tmp'
    
    format_logger('tmp.log', level=logging.WARNING)
    basinPath = os.path.join(os.path.pardir, 'basinmask_01.nc')
    logging.warning('Start of log file')
    logging.warning('Downloading Profile Indexes')
    df = tf.get_df_from_dates_updated(minDate, maxDate)
    print(df.shape[0])
    logging.warning('Num of files downloading to tmp: {}'.format(df.shape[0]))
    tf.create_dir_of_files(df, tf.GDAC, tf.FTP, tf.tmpDir)
    logging.warning('Download complete. Now going to add to db: {}'.format(dbName))

    ad = argoDatabase(dbName,
                      'profiles',
                      replaceProfile=True,
                      qcThreshold='1', 
                      dbDumpThreshold=1000,
                      removeExisting=False,
                      basinFilename=basinPath)
    
    files = ad.get_file_names_to_add(ncFileDir)
    run_parallel_process(ad, files, ncFileDir, npes)
    logging.warning('setting date updated to: {}'.format(tf.todayDate))
    tf.write_last_updated(tf.todayDate)

    logging.warning('Cleaning up space')
    tf.clean_up_space()
    logging.warning('End of log file')