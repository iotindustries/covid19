#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
import time
import datetime
import schedule
import pandas as pd
import json
import requests
import copy
import sys
import sqlite3


# In[ ]:


# Get the latest covid statistics for specific country from arcgis.com via their RESP API
# 
# example url request:
# https://services1.arcgis.com/0MSEUqKaxRlEPj5g/arcgis/rest/services/ncov_cases/FeatureServer/1/query?f=json&where=Country_Region=%27Slovakia%27&returnGeometry=false&outStatistics=[{%27statisticType%27:%27sum%27,%27onStatisticField%27:%27Confirmed%27,%27outStatisticFieldName%27:%27Confirmed%27},{%27statisticType%27:%27sum%27,%27onStatisticField%27:%27Active%27,%27outStatisticFieldName%27:%27Active%27},{%27statisticType%27:%27sum%27,%27onStatisticField%27:%27Recovered%27,%27outStatisticFieldName%27:%27Recovered%27},{%27statisticType%27:%27sum%27,%27onStatisticField%27:%27Deaths%27,%27outStatisticFieldName%27:%27Deaths%27},{%27statisticType%27:%27max%27,%27onStatisticField%27:%27Last_Update%27,%27outStatisticFieldName%27:%27Last_Update%27}]&groupByFieldsForStatistics=Country_Region

def getLatestCovidData(country):
    url='https://services1.arcgis.com/0MSEUqKaxRlEPj5g/arcgis/rest/services/ncov_cases/FeatureServer/1/query'
    query = {'f':'json', 
         'where':"Country_Region='Country'",
         'returnGeometry':'false',
         'outStatistics':("[{'statisticType':'sum','onStatisticField':'Confirmed','outStatisticFieldName':'Confirmed'}," +
                          "{'statisticType':'sum','onStatisticField':'Active','outStatisticFieldName':'Active'}," +
                          "{'statisticType':'sum','onStatisticField':'Recovered','outStatisticFieldName':'Recovered'}," +
                          "{'statisticType':'sum','onStatisticField':'Deaths','outStatisticFieldName':'Deaths'}," +
                          "{'statisticType':'max','onStatisticField':'Last_Update','outStatisticFieldName':'Last_Update'}]"),
         'groupByFieldsForStatistics':'Country_Region'}
    try:
        query['where'] = "Country_Region='{}'".format(country)
        response=requests.get(url, params=query)
        if response.status_code == 200:
            return response.status_code, response.json()
        else:
            return response.status_code, ''
    except requests.exceptions.RequestException as err:
        print('getCovidData: Error while accessing REST API: ', err, file=sys.stderr, flush=True)
        return err, ''


# In[ ]:


# Create table in SQLite DB for specific country (if not exists)
def DBCreateTableIfNotExists(country):
    conn = None
    try:
        conn = sqlite3.connect('covid19.db')
        query = '''CREATE TABLE IF NOT EXISTS {} (
                id INTEGER PRIMARY KEY,
                Confirmed INTEGER NOT NULL,
                Active INTEGER NOT NULL,
                Recovered INTEGER NOT NULL,
                Deaths INTEGER NOT NULL,
                Last_Update datetime NOT NULL,
                Published datetime NOT NULL);'''.format(country)
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()

    except sqlite3.Error as error:
        print('DBCreateTable: Error while accessing DB: ', error, file=sys.stderr, flush=True)
    finally:
        if (conn):
            conn.close()


# In[ ]:


# Insert new published data into DB
def DBInsertNewData(payload):
    conn = None
    try:
        conn = sqlite3.connect('covid19.db')
        
        query = '''INSERT INTO {} (Confirmed, Active, Recovered, Deaths, Last_Update, Published)
                VALUES (?, ?, ?, ?, ?, ?)'''.format(payload['data']['Country']) # cursor.execute does not accept table name as parameter
        cursor = conn.cursor()                                          # so have to use '{}' with the format() function instead
        cursor.execute(query, (payload['data']['Confirmed'], 
                               payload['data']['Active'], 
                               payload['data']['Recovered'], 
                               payload['data']['Deaths'], 
                               payload['data']['Last_Update'], 
                               payload['published']))
        conn.commit()
        cursor.close()

    except sqlite3.Error as error:
        print('DBInsertData: Error while accessing DB: ', error, file=sys.stderr, flush=True)
    finally:
        if (conn):
            conn.close()


# In[ ]:


# return the last record for Country older than today (Last_Update < today)
def DBSelectLastRecordFromYesterday(country):
    conn = None
    try:
        conn = sqlite3.connect('covid19.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        query = 'SELECT * FROM {} where Date(Last_Update) < ? ORDER BY Last_Update DESC LIMIT 1'.format(country) # cursor.execute does not accept table name as parameter
        cursor.execute(query, [datetime.date.today()])                                                     # so have to use '{}' with the format() function instead
        row = cursor.fetchone()
        cursor.close()
        if row is None:
            return {}
        else:
            return dict(zip(row.keys(),row))

    except sqlite3.Error as error:
        print('DBSelectData: Error while accessing DB: ', error, file=sys.stderr, flush=True)
    finally:
        if (conn):
            conn.close()


# In[ ]:


# return the last record for Country older than today (Last_Update < today)
def DBSelectLastRecordFromToday(country):
    conn = None
    try:
        conn = sqlite3.connect('covid19.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        query = 'SELECT * FROM {} where Date(Last_Update) == ? ORDER BY Last_Update DESC LIMIT 1'.format(country) # cursor.execute does not accept table name as parameter
        cursor.execute(query, [datetime.date.today()])                                                     # so have to use '{}' with the format() function instead
        row = cursor.fetchone()
        cursor.close()
        if row is None:
            return {}
        else:
            return dict(zip(row.keys(),row))

    except sqlite3.Error as error:
        print('DBSelectData: Error while accessing DB: ', error, file=sys.stderr, flush=True)
    finally:
        if (conn):
            conn.close()


# In[ ]:


# Check if there is change in the received data (compared to last record from previous day)
# If yes, create and return JSON payload with updated statistics
def compareNewDataAndCreatePayload(data):
    dataChanged = False
    for features in data['features']:
        for attributes in features:
            country = features[attributes]['Country_Region']
            newData = {}
            newData['Country'] = features[attributes]['Country_Region']
            newData['Confirmed'] = features[attributes]['Confirmed']
            newData['Active'] = features[attributes]['Active']
            newData['Recovered'] = features[attributes]['Recovered']
            newData['Deaths'] = features[attributes]['Deaths']
            
            #get the last record from today for country in SQLite DB
            lastRecord = DBSelectLastRecordFromToday(country)
            #if there is no data already available from today, get it from yesterday or later
            if (lastRecord == {}):
                lastRecord = DBSelectLastRecordFromYesterday(country)
            
            lastRecordYesterday = DBSelectLastRecordFromYesterday(country)

            if ((lastRecord.get('Confirmed',0) != newData['Confirmed']) or 
                (lastRecord.get('Active',0) != newData['Active']) or 
                (lastRecord.get('Recovered',0) != newData['Recovered']) or 
                (lastRecord.get('Deaths',0) != newData['Deaths'])):

                delta = newData['Confirmed'] - lastRecordYesterday.get('Confirmed',0)
                newData['Confirmed_delta'] = '{0:+}'.format(delta)

                delta = newData['Active'] - lastRecordYesterday.get('Active',0)
                newData['Active_delta'] = '{0:+}'.format(delta)

                delta = newData['Recovered'] - lastRecordYesterday.get('Recovered',0)
                newData['Recovered_delta'] = '{0:+}'.format(delta)

                delta = newData['Deaths'] - lastRecordYesterday.get('Deaths',0)
                newData['Deaths_delta'] = '{0:+}'.format(delta)
                
                newDataUpdate = pd.to_datetime(features[attributes]['Last_Update'],unit='ms')
                newDataUpdate = newDataUpdate.tz_localize('UTC').tz_convert('Europe/Bratislava')
                newDataUpdate = newDataUpdate.tz_localize(None)
                newData['Last_Update'] = str(newDataUpdate)

                dataChanged = True
                
    if dataChanged == True:
        payload = {'data': None, 'published': None, 'timezone':'Europe/Bratislava'}
        payload['data'] = copy.deepcopy(newData)
        t = time.localtime()
        payload['published'] = time.strftime("%Y-%m-%d %H:%M:%S", t)
    else:
        payload = ''

    return payload


# In[ ]:


# could be imporoved by adding the on_publish callback and inserting the data to DB upon this event to be sure it was really published

def publishNewData(country):
    rc, response = getLatestCovidData(country)
    if rc == 200:
        payload = compareNewDataAndCreatePayload(response)
        response = ''
        if payload != '':
            try:
                topic = 'COVID19/' + country.title() #.title() makes country name always Camel case (topics are case-sensitive)
                publish.single(topic, 
                               payload = json.dumps(payload), 
                               qos = 0, 
                               retain = True, 
                               hostname='localhost', 
                               port=1883, 
                               client_id='covid19',
                               keepalive=60)
                
                print('publishNewData: New data published for ' + country.title(), flush=True)      
                DBInsertNewData(payload)
                payload.clear()
            except Exception as e:
                print('publishNewData: Error occured: publish FAILED: ' + str(e), file=sys.stderr, flush=True)
    else:
        print('publishNewData: Error occured while calling getLatestCovidData()', file=sys.stderr, flush=True)


# In[ ]:


# Initialize DB for each country
DBCreateTableIfNotExists('Slovakia')
DBCreateTableIfNotExists('Austria')
DBCreateTableIfNotExists('Czechia')
DBCreateTableIfNotExists('Hungary')
DBCreateTableIfNotExists('Poland')
DBCreateTableIfNotExists('Ukraine')


# In[ ]:


def checkAndPublishForEachCountry():
    publishNewData('Slovakia')
    publishNewData('Austria')
    publishNewData('Czechia')
    publishNewData('Hungary')
    publishNewData('Poland')
    publishNewData('Ukraine')


# In[ ]:


#schedule.every().minute.do(SendPayload)
schedule.every().hour.at(":00").do(checkAndPublishForEachCountry)

while True:
    schedule.run_pending()
    time.sleep(60)


# In[ ]:




