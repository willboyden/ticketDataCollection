#!/usr/bin/env python
# coding: utf-8

# In[2]:


import json
import requests
import pandas as pd
#from sqlalchemy import create_engine
#import urllib
#import pyodbc
from pandas.io.json import json_normalize
import DO_NOT_GIT as dng
import pySqlServerFunks as sf
from datetime import datetime
import os


# In[3]:


def getTicketMasterVenueEventsToJsonDump(constr, api_key, filepathRoot):
    scrape_datetime = str(datetime.now()).replace(':', '_').replace(
        '-', '_').replace(' ', '_').replace('.', '_')
    strpath_day = filepathRoot+scrape_datetime

    os.mkdir(strpath_day)
    # listVenueIDs = sf.getDBqueryAsDF(
    #     constr, "select distinct(id) from tblTicketMasterVenue")['id']
    listVenueIDs = [x for x in pd.read_csv("C:/tempOutput/venueIDs/ticketmasterVenueIDs.csv",
                                           header=None, squeeze=True, usecols=[0])]

    def writeTicketMasterVenueEventToJsonFile(venueId, api_key):
        urls = 'https://app.ticketmaster.com/discovery/v2/events?apikey=' + \
            api_key+'&venueId='+venueId+'&size=200&locale=*'
        resp = requests.get(urls)

        def writeTicketMasterEventToJsonFile(x):
            if 'id' in x.keys():
                fname = '_venueID_' + venueId + '_eventID_' + str(x['id'])
                fpath = strpath_day + '/'+fname
                with open(fpath + '.txt', 'w') as outfile:
                    json.dump(resp.json()['_embedded'], outfile)
                    print("success" + '_venueID_' +
                          venueId + '_eventID_' + str(x['id']))

        if '_embedded' in resp.json().keys():
            if 'events' in resp.json()['_embedded'].keys():
                [writeTicketMasterEventToJsonFile(x) for x in resp.json()[
                    '_embedded']['events']]
            else:
                print('events not in json()._embedded keys' + str())
        else:
            print('_embedded not in json keys' + str())

    [writeTicketMasterVenueEventToJsonFile(
        venueId, api_key) for venueId in listVenueIDs]


# In[4]:
#  dirR = 'C:/tempOutput/ticketmasterJsonDumps/Events/'
#     dirList = next(os.walk(dirR))[1]
#     [os.replace(dirR + x, dirR + 'Archive/' + x) for x in dirList if 'Archive' not in x]
# print(dng.constr_mssql_subhubApi())
getTicketMasterVenueEventsToJsonDump(dng.constr_mssql_subhubApi(),
                                     dng.app_key_ticketmasterApi(),
                                     'C:/tempOutput/ticketmasterJsonDumps/Events/')


# In[ ]:
#x = pd.read_csv("C:/tempOutput/venueIDs/ticketmasterVenueIDs.csv")


# %%
