#!/usr/bin/env python
# coding: utf-8

# In[3]:


import json
import requests
#import base64
import pandas as pd
#from pandas import DataFrame 
from sqlalchemy import create_engine
import urllib
import pyodbc
from pandas.io.json import json_normalize
import DO_NOT_GIT as dng
import pySqlServerFunks as sf
from datetime import datetime
import os


# In[ ]:


#this function is configured to get all venues in Massachuesetts 
#gets the json response from the tickmaster API and writes to file
#api limits response to 200 items per page, allowing up to the 1000th item
#for a given set of api query parameters
#seeing that there are under 1000 venues in MA this does not present much of a problem
#we just go through the pages
def printTicketmasterVenuesToJsonFile(apikey, filepathRoot):
    scrape_datetime=str(datetime.now()).replace(':', '_').replace('-', '_').replace(' ', '_').replace('.', '_')#easier programability format
    strpath_day=filepathRoot+scrape_datetime
    os.mkdir(strpath_day)
    urls = 'https://app.ticketmaster.com/discovery/v2/venues?apikey='+apikey+'&locale=*'+'&stateCode=MA'+'&size=200&page=0'
    resp = requests.get(urls)
    venues=resp.json()
    venues_total_pages=venues['page']['totalPages']
    venues_totalElements=venues['page']['totalElements']    
    l=[]
    if '_embedded' in venues.keys():
        if venues_total_pages > 0 and venues_totalElements > 0:
            for i in range(0, int(venues_total_pages)):           
                print(i)
                strpath_day_page = strpath_day + '/_' + str(i)
                #os.mkdir(strpath_day_page)
                urls = 'https://app.ticketmaster.com/discovery/v2/venues?apikey='+apikey+'&locale=*&size=200&page='+str(i)+'&stateCode=MA'
                resp = requests.get(urls)
                if '_embedded' in resp.json().keys():
                    #print(resp.json()['_embedded'])
                    with open(strpath_day_page + '_.txt', 'w') as outfile:
                        json.dump(resp.json()['_embedded'], outfile)
                        print('success with page' +str(i))
                else:
                     print('_embedded not in resp.json().keys() INNER IF')
                #print(urls)
        else:
            print('either 0 pages or 0 elements in response')
    else:
        print('_embedded not in resp.json().keys()')


# In[ ]:


#TODO make file path a command lien argument
printTicketmasterVenuesToJsonFile(dng.app_key_ticketmasterApi(), 'C:/tempOutput/ticketmasterJsonDumps/Venues/')

