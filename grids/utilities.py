import numpy as np
import pandas as pd
import xarray as xr
import bson
import pymongo
import pdb
from datetime import datetime, timedelta
from dateutil.relativedelta import *
from scipy.io import loadmat
import os
import glob, logging

# check the statements "to keep eventually" and see if anything needs editing

def transform_lon(lon):
    '''
    Transforms longitude from absolute to -180 to 180 deg
    '''
    if lon >= 180:
        lon -= 360
    return lon

def init_profiles_collection(coll):
    try:
        coll.create_index([('date', pymongo.ASCENDING)])
        # to keep eventually: coll.create_index([('level', pymongo.ASCENDING)]) instead of:
        coll.create_index([('pres', pymongo.ASCENDING)])
        #
        coll.create_index([('gridName', pymongo.ASCENDING)])
        coll.create_index([('date', pymongo.ASCENDING), ('level', pymongo.ASCENDING), ('gridName', pymongo.ASCENDING)])
        coll.create_index([('data.lat', pymongo.DESCENDING)])
        coll.create_index([('data.lon', pymongo.ASCENDING)])
    except:
        logging.warning('not able to get collections or set indexes')
    return coll
                
def create_collection(dbName, collectionName,dbUrl='mongodb://localhost:27017/'):
    #dbUrl = 'mongodb://localhost:27017/'
    client = pymongo.MongoClient(dbUrl)
    db = client[dbName]
    coll = db[collectionName]
    coll = init_profiles_collection(coll)
    return coll

def insert_grid_levels_in_reg_coll(dbName,dbUrl,dataset_tag,var_tag,
                                  ilon_edges,ilat_edges,
                                  df,date,dataVariable, param, 
                                  measurement, units, level_units, 
                                  cellsize,transformLongitude,
                                  insertOne,makePlot):
    n = -1
    for i in np.arange(0,len(ilon_edges)-1,1):
        for j in np.arange(0,len(ilat_edges)-1,1):
            n        = n+1
            df_reg   = df.isel(lon=np.arange(ilon_edges[i],ilon_edges[i+1],1),lat=np.arange(ilat_edges[j],ilat_edges[j+1],1))
            collectionName_reg = dataset_tag+var_tag+param+'_'+str(n).zfill(2)

            Coll_reg = create_collection(dbName=dbName, collectionName=collectionName_reg,dbUrl=dbUrl)
            Coll_reg.drop()
            
            print(collectionName_reg)
            insert_grid_levels(df=df_reg,coll=Coll_reg,date=date,dataVariable=dataVariable, 
                  param=param, measurement=measurement, gridName=collectionName_reg, 
                  units=units, level_units=level_units, cellsize=cellsize,
                   transformLongitude=transformLongitude,insertOne=insertOne)
            


def insert_grid_levels_in_reg_doc(dbName,dbUrl,dataset_tag,var_tag,
                                  ilon_edges,ilat_edges,
                                  df,date,dataVariable, param, 
                                  measurement, units, level_units, 
                                  cellsize,transformLongitude,
                                  insertOne,makePlot):
    collectionName_reg = dataset_tag+var_tag+param #+'_'+str(n).zfill(2)

    Coll_reg = create_collection(dbName=dbName, collectionName=collectionName_reg,dbUrl=dbUrl)
    Coll_reg.drop()
            
    for i in np.arange(0,len(ilon_edges)-1,1):
        for j in np.arange(0,len(ilat_edges)-1,1):
            df_reg   = df.isel(lon=np.arange(ilon_edges[i],ilon_edges[i+1],1),lat=np.arange(ilat_edges[j],ilat_edges[j+1],1))
            
            insert_grid_levels(df=df_reg,coll=Coll_reg,date=date,dataVariable=dataVariable, 
                  param=param, measurement=measurement, gridName=collectionName_reg, 
                  units=units, level_units=level_units, cellsize=cellsize,
                   transformLongitude=transformLongitude,insertOne=insertOne)
            

                
##### add 3d variable with dimensions lon, lat, level
def insert_grid_levels(df,coll,date,dataVariable, param, 
                      measurement, gridName, units, level_units, cellsize,transformLongitude,insertOne):
    # df is <class 'xarray.core.dataset.Dataset'>
    # see example for rg as the function was created for that
    for ldx, levelDf in df.groupby('level'):
        
        date_added = datetime.now().strftime("%Y-%m-%d")# %H:%M:%S
        levelDf = levelDf.drop('level')
        
        levelDf = levelDf.to_dataframe()
        levelDf = levelDf.reset_index()
        
        if transformLongitude:
            levelDf['lon'] = levelDf['lon'].apply(lambda lon: transform_lon(lon))
        
        doc = make_doc(levelDf, date, date_added, ldx, dataVariable, param, 
                       measurement, gridName, units, level_units, cellsize)
        print('doc size: ' + str(len(bson.BSON.encode(doc))/1000000)+ ' Mb')
        if insertOne: # Use for testing
            coll.insert_one(doc)
            return
        else:
            coll.insert_one(doc)

def make_doc(df, date, date_added, Level, dataVariable, param, measurement, gridName, units, level_units, cellsize):
    '''
    Takes df and converts it into a document for mongodb
    '''
    doc = {}
    df = df.rename(index=str, columns={dataVariable: 'value'})
    dataDict = df.to_dict(orient='records')
    #print(date)
    doc['data'] = dataDict 
    doc['gridName'] = gridName
    doc['measurement'] = measurement #temperature or psal
    doc['units'] = units # degrees celsius or psu
    doc['variable'] = dataVariable # ARGO_TEMPERATURE_ANOMALY or ARGO_TEMPERATURE_MEAN or total
    doc['date'] = date
    # to keep eventually: doc['level'] = float(Level) instead of:
    doc['pres'] = float(Level)
    #
    doc['level_units'] = level_units
    doc['param'] = param # anomaly, mean, or total
    doc['cellsize'] = cellsize  #  Degree
    doc['NODATA_value'] = np.NaN
    doc['date_added'] = date_added
    return doc

# def create_grid_df(chunk,latitude_name,longitude_name):
#     df = chunk.to_dataframe()
#     df = df.reset_index()
#     df = df.rename(columns={latitude_name:'lat', longitude_name:'lon'})
#     df['lon'] = df['lon'].apply(lambda lon: transform_lon(lon))
#     return df

# def convert_longitudes(ds,longitude_name):
#     # ds is an xarray dataset
#     # Adjust lon values to make sure they are within (-180, 180)
#     ds['_longitude_adjusted'] = xr.where(
#         ds[longitude_name] > 180,
#         ds[longitude_name] - 360,
#         ds[longitude_name])
# #     # reassign the new coords to as the main lon coords
# #     # and sort DataArray using new coordinate values
# #     ds = (
# #         ds
# #         .swap_dims({longitude_name: '_longitude_adjusted'})
# #         .sel(**{'_longitude_adjusted': sorted(ds._longitude_adjusted)})
# #         )
    
#     ds = ds.drop(longitude_name)
#     ds = ds.rename({'_longitude_adjusted': longitude_name})
# #     ds[longitude_name] = ds['_longitude_adjusted']

# #     ds = ds.drop('_longitude_adjusted')
#     print(ds)
#     return ds
