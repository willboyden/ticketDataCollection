#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# remember to remove tokens before posting anywhere like github!!!!!!
# TODO: utilize num found for checks & balances/error handling/prevention
# TODO: remove modification of data in for loop and use oppropriate method


# In[36]:


import json
import requests
import base64
import pandas as pd
from pandas import DataFrame
from pandas.io.json import json_normalize
from sqlalchemy import create_engine
import urllib
import pyodbc
import DO_NOT_GIT as dng


# In[4]:


def writeDFtoDB(df, conStr, tblNameStr, if_existsStr, TFaddTimeStamp=False, timeStampNameStr=""):
    if TFaddTimeStamp == True and timeStampNameStr not in df.columns:
        df.insert(0, timeStampNameStr, pd.datetime.now())
        #df = df.df.insert(0, 'scrapeDateTime', pd.datetime.now())
    quoted = urllib.parse.quote_plus(conStr)
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    df.columns = [x.replace(".", "_") for x in df.columns]
    df.to_sql(tblNameStr, schema='dbo', if_exists=if_existsStr, con=engine)


# In[5]:


def getDBtblAsDF(conStr, tblNameStr):
    quoted = urllib.parse.quote_plus(conStr)
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    result = engine.execute('SELECT * FROM [dbo].['+tblNameStr+']')
    df = DataFrame(result.fetchall())
    df.columns = engine.execute(
        'SELECT TOP(1) * FROM [dbo].['+tblNameStr+']').keys()
    return df


# In[6]:


# get MA data
def getStateData(stateCodeStr, countryNameStr, appToken):
    resource_url = 'https://api.stubhub.com/sellers/search/locations/v3'
    data = {'state': stateCodeStr, 'rows': 500, 'country': countryNameStr}
    headers = {'Authorization': "Bearer " + appToken,
               'Accept': 'application/json',
               'Accept-Encoding': 'application/json'}
    stateData = requests.get(resource_url, headers=headers, params=data)
    return stateData


# In[ ]:


def getVenueData(stateCodeStr, appToken, cityNameStr="All"):
    resource_url = 'https://api.stubhub.com/partners/search/venues/v3/'
    if(cityNameStr != "All"):
        data = {'state': stateCodeStr, 'rows': 500,
                'country': 'US', 'city': cityNameStr}
    else:
        data = {'state': stateCodeStr, 'rows': 500, 'country': 'US'}
    headers = {'Authorization': "Bearer " + appToken,
               'Accept': 'application/json',
               'Accept-Encoding': 'application/json'}
    venuesData = requests.get(resource_url, headers=headers, params=data)
    return venuesData


# In[ ]:


def getVenueEventData(venueId, appToken):
    resource_url = 'https://api.stubhub.com/sellers/search/events/v3'
    headers = {'Authorization': "Bearer " + appToken,
               'Accept': 'application/json',
               'Accept-Encoding': 'application/json'}
    data = {'venueId': venueId, 'rows': 500}
    venuesData = requests.get(resource_url, headers=headers, params=data)
    #vd = venuesData.json()['events']
    # return vd
    if('events' in venuesData.json().keys()):
        return pd.io.json.json_normalize(venuesData.json(), record_path='events')


# In[70]:


#stateData = getStateData('MA', 'United States', dng.app_key_subhubApi())
# print(stateData.json())


# In[71]:


# sdlocals = stateData.json()['locations']
# for item in sdlocals:
#     # print(item['timeZone'])
#     item['timeZoneId'] = item['timeZone']['id']
#     item['timeZoneRawOffset'] = item['timeZone']['rawOffset']
#     item['timeZoneDisplayOffset'] = item['timeZone']['displayOffset']
#     del item['timeZone']
# dfCities = pd.DataFrame(sdlocals)
#writeDFtoDB(dfCities, constr, "tblStubhubCities")


# venueData = getVenueData("MA", dng.app_key_subhubApi())
# print(venueData.json())


# Table was originally imported to sql with address as a string
# "with addresss like
# venueDataJson = getVenueData("MA", dng.app_key_subhubApi()).json()['venues']
# for item in venueDataJson:
#     # print(item['timeZone'])
#     item['address1'] = item['address']['address1']
#     if 'address2' in item['address'].keys():
#         item['address2'] = item['address']['address2']
#     else:
#         item['address2'] = None
#     item['city'] = item['address']['city']
#     item['state'] = item['address']['state']
#     item['country'] = item['address']['country']
#     item['postalCode'] = item['address']['postalCode']
#     del item['address']
# dfVenue = pd.DataFrame(venueDataJson)
# print(dfVenue)
# writeDFtoDB(dfVenueEventData.astype(str),
#             dng.constr_mssql_subhubApi(),
#             "tblStubhubVenue",
#             "append",
#             TFaddTimeStamp=True,
#             timeStampNameStr="scrapeDate")


# In[26]:


def getVenueEventsFromApi(dfrow):
    print(dfrow)


#venueNames = [getVenueEventsFromApi(x) for x in dfVenue["name"]]


# In[ ]:


# venueEventData = [getVenueEventData(
#     x, dng.app_key_subhubApi()) for x in dfVenue["id"][0:len(dfVenue)]]
#dfVenueEventData = pd.concat(venueEventData, sort=False)


# In[ ]:


# to much time has been spent try to fix the json in python, for now it will just be handeled in sql
# dfVenueEventData = pd.concat(venueEventData, sort=False)
# writeDFtoDB(dfVenueEventData.astype(str),
#             dng.constr_mssql_subhubApi(),
#             "tblStubhubVenueEvent",
#             "append",
#             TFaddTimeStamp=True,
#             timeStampNameStr="scrapeDate")
# print("done")
