#!/usr/bin/env python
# coding: utf-8

# In[10]:


import pandas as pd
from pandas import DataFrame 
from sqlalchemy import create_engine
import pyodbc
import datetime
import urllib


# In[11]:


def writeDFtoDB(df, constr, tblNameStr ,if_existsStr, TFaddTimeStamp=False, timeStampNameStr=""):
    if TFaddTimeStamp == True and timeStampNameStr not in df.columns:        
        df.insert(0, timeStampNameStr, pd.datetime.now())
        #df = df.df.insert(0, 'scrapeDateTime', pd.datetime.now())
    quoted = urllib.parse.quote_plus(constr)
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    df.columns=[x.replace(".","_") for x in df.columns]
    df.to_sql(tblNameStr, schema='dbo', if_exists=if_existsStr, con = engine)
    #result = engine.execute('SELECT * FROM [dbo].['+tblNameStr+']')
    #df = DataFrame(result.fetchall())
    #df.columns = engine.execute('SELECT * FROM [dbo].['+tblNameStr+']').keys()
    #print(df)


# In[ ]:


def getDBqueryAsDF(constr, qrystr):
    quoted = urllib.parse.quote_plus(constr)
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    result = engine.execute(qrystr)
    df = DataFrame(result.fetchall())
    df.columns = engine.execute(qrystr).keys()
    return df


# In[4]:


def getDBtblAsDF(constr, tblNameStr,):
    quoted = urllib.parse.quote_plus(constr)
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    result = engine.execute('SELECT * FROM [dbo].['+tblNameStr+']')
    df = DataFrame(result.fetchall())
    df.columns = engine.execute('SELECT TOP(1) * FROM [dbo].['+tblNameStr+']').keys()
    return df

def checkIfTableExists(constr, tblNameStr):  
    if getDBqueryAsDF(constr, "IF (EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '"
                         +tblNameStr
                         +"')) select 'true' as response Else select 'false' as respnse").response[0] == 'true':
        return True
    else:
        return False

