import pandas as pd
import numpy as np
import requests
import json
from datetime import datetime
import shutil
import os
import math
import time

def generateToken(myUsername = '',myPassword = ''):
    """Generates the token object from Tick History to pair with later calls.
    """
    requestUrl = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Authentication/RequestToken"
    requestHeaders={
        "Prefer":"respond-async",
        "Content-Type":"application/json"
    }
    requestBody={"Credentials": {"Username": myUsername,"Password": myPassword}}
    r1 = requests.post(requestUrl, json=requestBody,headers=requestHeaders)

    if r1.status_code == 200 :
        jsonResponse = json.loads(r1.text.encode('ascii', 'ignore'))
        token = jsonResponse["value"]
        print ('\tSTATUS: Authentication token (valid 24 hours):')

    else:
        print ('Replace myUserName and myPassword with valid credentials, then repeat the request')
    return token
        
def generateEkTsRequestBody(token, start, end, instruments):
    requestHeaders={
        "Prefer":"respond-async",
        "Content-Type":"application/json",
        "Authorization": "Token " + token
    }
    
    requestBody={
        "ExtractionRequest": {
            "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.ElektronTimeseriesExtractionRequest",
            "ContentFieldNames": [
                "RIC",
                "Volume",
                "Trade Date"
            ],
            "IdentifierList": {
                "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
                "InstrumentIdentifiers": []
            },
            "Condition": {
                "ReportDateRangeType": "Range",
                "QueryStartDate": start,
                "QueryEndDate": end
            }
        }
    }
    for i in instruments:
        requestBody["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append({"Identifier": i,"IdentifierType": "Ric"})
    requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractRaw'
    return requestUrl,requestHeaders,requestBody

def generateEkTsFiles(token, requestUrl,requestHeaders,requestBody, fileName = 'output.csv', filePath = ''):
    r2 = requests.post(requestUrl, json=requestBody,headers=requestHeaders)
    status_code = r2.status_code
    print ("\tSTATUS: HTTP status of the response: " + str(status_code))
    files = filePath + fileName
    if status_code == 202:    
        requestUrl = r2.headers["location"]
        print ('\tSTATUS: Begin Polling Pickup Location URL')

        requestHeaders={
            "Prefer":"respond-async",
            "Content-Type":"application/json",
            "Authorization":"Token " + token
        }
        while (status_code == 202):
            print ('\tSTATUS: 202; waiting 30 seconds to poll again (need status 200 to continue)')
            time.sleep(30)
            r3 = requests.get(requestUrl,headers=requestHeaders)
            status_code = r3.status_code

        if status_code != 200 :
            print ('ERROR: An error occurred. Try to run this cell again. If it fails, re-run the previous cell.\n')

        if status_code == 200:
            requestUrl = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/RawExtractionResults" + "('" + json.loads(r3.content)['JobId'] + "')" + "/$value"
            requestHeaders={
                "Prefer":"respond-async",
                "Content-Type":"text/plain",
                "Accept-Encoding":"gzip",
                "X-Direct-Download":"true",
                "Authorization": "token " + token
            }
            returnData = requests.get(requestUrl,headers=requestHeaders,stream=True)
            if type(returnData.content) is bytes:
                with open(files, 'wb') as fd:
                    fd.write(returnData.content)
            else:
                print('return object did not resolve to bytes')
            print('Successfully downloaded file')
    elif status_code == 200:
        requestUrl = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/RawExtractionResults" + "('" + json.loads(r2.content)['JobId'] + "')" + "/$value"
        requestHeaders={
            "Prefer":"respond-async",
            "Content-Type":"text/plain",
            "Accept-Encoding":"gzip",
            "X-Direct-Download":"true",
            "Authorization": "token " + token
        }
        returnData = requests.get(requestUrl,headers=requestHeaders,stream=True)
        if type(returnData.content) is bytes:
            with open(files, 'wb') as fd:
                fd.write(returnData.content)
        else:
            print('return object did not resolve to bytes')
        print('Successfully downloaded file')

        
def chainHistory(tk, chain, start, end):
    chainURL = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Search/HistoricalChainResolution'
    chainHeaders = {
        'Prefer': 'respond-async',
        'Content-Type': 'application/json',
        'Authorization': 'Token ' + tk
    }

    def bodyGenerator(chain, start, end):
        return {
            "Request": {
                "ChainRics": [
                    chain
                ],
                "Range": {
                    "Start": start,
                    "End": end
                }
            }
        }
    chainBody = bodyGenerator(chain, start, end)
    chain = requests.post(chainURL, headers = chainHeaders, json= chainBody)
    data = json.loads(chain.content)
    for i in data['value']:
        for p in i['Constituents']:
            yield p['Identifier']

def generateHisRefRequestBody(token, start, end, instruments):
    requestHeaders={
        "Prefer":"respond-async",
        "Content-Type":"application/json",
        "Authorization": "Token " + token
    }
    
    requestBody={
        "ExtractionRequest": {
            "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.HistoricalReferenceExtractionRequest",
            "ContentFieldNames": [
                "Change Date","Trading Status","RIC", "Put Call Flag","Lot Units","Lot Size","Asset Category","Strike Price","Expiration Date","Exercise Style"
            ],
            "IdentifierList": {
                "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
                "InstrumentIdentifiers": [],
                "ValidationOptions": {"AllowHistoricalInstruments": "true"},
                "UseUserPreferencesForValidationOptions": "false"
            },
            "Condition": {
                "ReportDateRangeType": "Range",
                "QueryStartDate": start,
                "QueryEndDate": end
            }
        }
    }
    for i in instruments:
        requestBody["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append({"Identifier": i,"IdentifierType": "Ric"})
    requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
    return requestUrl,requestHeaders,requestBody

def generateHisRefFiles(token, requestUrl,requestHeaders,requestBody, fileName = 'output.csv', filePath = ''):
    r2 = requests.post(requestUrl, json=requestBody,headers=requestHeaders)
    status_code = r2.status_code
    print ("\tSTATUS: HTTP status of the response: " + str(status_code))
    files = filePath + fileName
    if status_code == 202:    
        requestUrl = r2.headers["location"]
        print ('\tSTATUS: Begin Polling Pickup Location URL')

        requestHeaders={
            "Prefer":"respond-async",
            "Content-Type":"application/json",
            "Authorization":"Token " + token
        }
        while (status_code == 202):
            print ('\tSTATUS: 202; waiting 30 seconds to poll again (need status 200 to continue)')
            time.sleep(30)
            r3 = requests.get(requestUrl,headers=requestHeaders)
            status_code = r3.status_code

        if status_code != 200 :
            print ('ERROR: An error occurred. Try to run this cell again. If it fails, re-run the previous cell.\n')

        if status_code == 200:
            pd.DataFrame(json.loads(r3.content)['Contents']).to_csv(filePath + fileName,index = False)
            print('Successfully downloaded file')
    elif status_code == 200:
        pd.DataFrame(json.loads(r2.content)['Contents']).to_csv(filePath + fileName,index = False)
        print('Successfully downloaded file')

def trthElektronTimeseries(ident, pw, start, finish, chain, fileRoot = 'pricingOutput', filePath = ''):
    tk = generateToken(ident,pw)
    optionConstituents = list(chainHistory(tk,chain,start,finish))
    delta = (datetime.strptime(finish, '%Y-%m-%d') - datetime.strptime(start, '%Y-%m-%d')).days
    ric_days = delta * len(optionConstituents)
    num_rics = math.floor(20000/delta)
    num_files = math.ceil(len(optionConstituents) / num_rics)
    beg = 0
    end = int(num_rics)
    for i in range(1,num_files+1):
        ric_set = optionConstituents[beg:end]
        url,head,body = generateEkTsRequestBody(tk,start,finish, ric_set)
        generateEkTsFiles(tk,url,head,body, fileName = fileRoot + str(i) +'.csv', filePath= filePath)
        beg += num_rics
        end += num_rics
        
def trthHistoricalReference(ident, pw, start, finish, chain, fileRoot = 'referenceOutput', filePath = ''):
    tk = generateToken(ident,pw)
    optionConstituents = list(chainHistory(tk,chain,start,finish))
    delta = (datetime.strptime(finish, '%Y-%m-%d') - datetime.strptime(start, '%Y-%m-%d')).days    
    ric_days = delta * len(optionConstituents)
    num_rics = math.floor(20000/delta)
    num_files = math.ceil(len(optionConstituents) / num_rics)
    beg = 0
    end = int(num_rics)
    for i in range(1,num_files+1):
        ric_set = optionConstituents[beg:end]
        url,head,body = generateHisRefRequestBody(tk,start,finish, ric_set)
        generateHisRefFiles(tk,url,head,body, fileName = fileRoot + str(i) +'.csv', filePath = filePath)
        beg += num_rics
        end += num_rics

trthHistoricalReference(ident = joshDSSid, 
             pw = joshDSSpw, 
             start = '2019-12-01', 
             finish = '2019-12-04', 
             chain = '0#CL:', 
             fileRoot = 'CLreferenceOutput', 
             filePath = 'C:\\Users\\u6037148\\projects\\')

trthElektronTimeseries(ident = joshDSSid, 
             pw = joshDSSpw, 
             start = '2019-11-25', 
             finish = '2019-12-02', 
             chain = '0#AAPL*.U', 
             fileRoot = 'priceOutput', 
             filePath = 'C:\\Users\\u6037148\\projects\\')
