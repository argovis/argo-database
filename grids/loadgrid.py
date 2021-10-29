from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import xarray as xr
from utilities import create_collection, insert_grid_levels

####### adjust input dataset to taste #########
dataFilename = 'RG_ArgoClim_Temperature_2019.nc'
dataTimeAxis = pd.date_range(start='01-Jan-2004', end='01-Dec-2018', freq='MS')  # dates hardcoded, need to reflect file contents
####### touch nothing below this line #########

dataset_tag  = 'rg'
measurement  ='temperature' # or salinity
dbName          ='argo'
dbUrl = 'mongodb://database:27017/'
year_start_mean = 2006
year_end_mean   = 2018
dataFilePath = '/raw/RG/'
transformLongitude=True #since RG has long greater than 180: they will be changed to be -180 to 180 
#### create a total field adding mean and anomaly
level_units   ='dbar'
level_name    ='PRESSURE'
latitude_name ='LATITUDE'
longitude_name='LONGITUDE'
time_name     = 'TIME'
cellsize      = 1

######## parameters set based on variable
if measurement=='temperature':
    var_tag = 'Temp'
    units   = 'Degrees Celsius'
    var_anom_min = -5
    var_anom_max = 5

#>> RG files that need to be downloaded can be found here and should be stored in a folder for RG only files
#>>>>>> https://sio-argo.ucsd.edu/RG_Climatology.html
#>> Direct links are e.g.
#>>>>>> ftp://kakapo.ucsd.edu/pub/gilson/argo_climatology/RG_ArgoClim_Temperature_2019.nc.gz 
#>> (change Temperature to Salinity for Salinity grids)
#>>>>>> ftp://kakapo.ucsd.edu/pub/gilson/argo_climatology/RG_ArgoClim_201901_2019.nc.gz 
#>> (change 201902 for Feb 2019 and so on based on what additional months are available)

####### load the data in the main file (RG will also have other additional files yet we will deal with that later)
dataIN           = xr.open_dataset(dataFilePath+dataFilename, decode_times=False)
dataIN['total'] = dataIN['ARGO_'+measurement.upper()+'_ANOMALY']+dataIN['ARGO_'+measurement.upper()+'_MEAN']


####### rename and set the time axis
dataIN = dataIN.rename_dims({time_name: 'time'})
dataIN['time'] = dataTimeAxis
dataIN = dataIN.drop(time_name)
####### rename level, long and lat dimensions
dataIN['lon'] = dataIN[longitude_name]
dataIN['lat'] = dataIN[latitude_name]
dataIN['level'] = dataIN[level_name]
dataIN = dataIN.swap_dims({longitude_name: 'lon'})
dataIN = dataIN.swap_dims({latitude_name: 'lat'})
dataIN = dataIN.swap_dims({level_name: 'level'})
dataIN = dataIN.drop(longitude_name)
dataIN = dataIN.drop(latitude_name)
dataIN = dataIN.drop(level_name)
####### select the period to compute the mean and annual anomaly
dataIN_sel = dataIN.sel(time=dataIN.time.dt.year.isin(np.arange(year_start_mean, year_end_mean+1,1)))

# compute and add to mongodb the time mean of the data using dataIN_sel
############## compute the mean using the field 'total'
param = 'Mean'
date_Mean = datetime.strptime('9999-9', '%Y-%m') # let's assign a fake date to the mean
meanDataArray = dataIN_sel['total'].mean('time').to_dataset()
dataVariable   = measurement.upper()+'_'+param.upper()
meanDataArray = meanDataArray.rename({'total': dataVariable})

#print(meanDataArray)
############## lines to add to mongodb
collectionName = dataset_tag+var_tag+param

meanColl = create_collection(dbName=dbName, collectionName=collectionName,dbUrl=dbUrl)
meanColl.drop()
insert_grid_levels(df=meanDataArray,coll=meanColl,date=date_Mean,dataVariable=dataVariable, 
                  param=param, measurement=measurement, gridName=collectionName, 
                  units=units, level_units=level_units, cellsize=cellsize,
                   transformLongitude=transformLongitude,insertOne=True)