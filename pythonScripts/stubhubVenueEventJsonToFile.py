#!/usr/bin/env python
# coding: utf-8

# In[163]:


# TODO get table col names from sql to make sure you append correctlt
import json
import requests
import pandas as pd
from sqlalchemy import create_engine
import urllib
import pyodbc
from pandas.io.json import json_normalize
import DO_NOT_GIT as dng
import pySqlServerFunks as sf
from datetime import datetime
import os


def writeDFtoDB(df, conStr, tblNameStr, if_existsStr, TFaddTimeStamp=False, timeStampNameStr=""):
    if TFaddTimeStamp == True and timeStampNameStr not in df.columns:
        df.insert(0, timeStampNameStr, pd.datetime.now())
        #df = df.df.insert(0, 'scrapeDateTime', pd.datetime.now())
    quoted = urllib.parse.quote_plus(conStr)
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    df.columns = [x.replace(".", "_") for x in df.columns]
    df.to_sql(tblNameStr, schema='dbo', if_exists=if_existsStr, con=engine)


def clearDir(dirRoot):
    next(os.walk('.'))[1]
    #os.replace("path/to/current/file.foo", "path/to/new/destination/for/file.foo")


def makeDir(dirRoot, addDateTimeToDirName=False, subDirDateTime=True):
    datetime_now = str(datetime.now()).replace(':', '_').replace(
        '-', '_').replace(' ', '_').replace('.', '_')
    if addDateTimeToDirName == True:
        dirRoot = dirRoot + datetime_now
    if subDirDateTime == True:
        dirRoot = dirRoot + "/" + datetime_now
    os.mkdir(dirRoot)
    return dirRoot


def getStubhubVenueEventData(venueId, appToken):
    fname = '_venueID_' + venueId
    fpath = rootFolder + '/' + fname

    resource_url = 'https://api.stubhub.com/sellers/search/events/v3'
    headers = {'Authorization': "Bearer " + appToken,
               'Accept': 'application/json',
               'Accept-Encoding': 'application/json'}
    data = {'venueId': venueId, 'rows': 500}
    venuesData = requests.get(resource_url, headers=headers, params=data)
    if('events' in venuesData.json().keys()):
        # try:
        def printToFile(x, fpath):
            # only change fpath local here
            # try:
            fpath = fpath + '_eventID_' + str(x['id'])
            if 'id' in x.keys():
                with open(fpath + '.txt', 'w') as outfile:
                    json.dump(x, outfile)
                    print("Done: " + fpath)
            # except:
            #    print("Something went wrong")
        #pd.io.json.json_normalize(venuesData.json(), record_path='events').to_json(fpath)
        #j=pd.io.json.json_normalize(venuesData.json(), record_path='events')
        # printToFile(x)
        [printToFile(x, fpath) for x in venuesData.json()['events']]
#         except:
#             print("Something went wrong")
#             print(resp)
#         finally:
#             #return pd.io.json.json_normalize(venuesData.json(), record_path='events')
#             return venuesData
    else:
        print('events not in keys')
        print(venuesData)


# In[168]:


dirR = 'C:/tempOutput/stubhubJsonDumps/Events/'
dirList = next(os.walk(dirR))[1]
[os.replace(dirR + x, dirR + 'Archive/' + x)
 for x in dirList if 'Archive' not in x]
rootFolder = makeDir('C:/tempOutput/stubhubJsonDumps/Events/',
                     addDateTimeToDirName=False, subDirDateTime=True)
# venue =  sf.getDBqueryAsDF(
#         dng.constr_mssql_subhubApi(), "select distinct(id) from tblStubhubVenue")['id']
listVenueIDs = [x for x in pd.read_csv("C:/tempOutput/venueIDs/stubhubVenueIDs.csv",
                                       header=None, squeeze=True, usecols=[0])]


# In[ ]:


venueEventData = [getStubhubVenueEventData(
    str(x),
    dng.app_key_subhubApi(),
) for x in listVenueIDs if x is not None]
#dfVenueEventData = pd.concat(venueEventData, sort=False)


# In[ ]:


#dfVenueEventData = pd.concat(venueEventData, sort=False)


# In[ ]:


# writeDFtoDB(dfVenueEventData.astype(str),
#             dng.constr_mssql_subhubApi(),
#             "tblStubhubVenueEvent",
#             "append",
#             TFaddTimeStamp=True,
#             timeStampNameStr="scrapeDate")
# print("done")


# In[171]:


#getStubhubVenueEventData('1235473', dng.app_key_subhubApi())
