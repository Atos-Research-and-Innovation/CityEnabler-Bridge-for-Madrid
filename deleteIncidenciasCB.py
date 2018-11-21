import json
#import pandas as pd
import numpy as np
import copy
import requests
import threading
from datetime import datetime as dt
from configAverageScript import datesIntervals, folderWithCSVs, averageLogFile, averageExportFile, listOfDataToRecoverToCalculateAverages, \
    USE_THREADING, CB_FIWARE_SERVICE, CB_FIWARE_SERVICEPATH, CB_URL_TO_SEND_DATA

def deleteItem(item):
    print(str(item['id']))
    
    rDelete = requests.delete(url=CB_URL_TO_SEND_DATA+"/"+str(item['id']), timeout=300, data='', headers=headersDelete)
    print(rDelete)
    print (rDelete.json)
    
    
    
if __name__ == '__main__':
    
    print("Starts!!!")
    startdate = dt.utcnow()
    print(startdate)
    print("[INFO] ["+str(dt.utcnow())+"] ------------------------------------------------")
    print("[INFO] ["+str(dt.utcnow())+"] Starts Delete PoI data from CB")

    #headers to send data to CB
    headersGet = {'Fiware-Service': CB_FIWARE_SERVICE, 'Fiware-ServicePath': CB_FIWARE_SERVICEPATH}
    headersDelete = {'Fiware-Service': CB_FIWARE_SERVICE, 'Fiware-ServicePath': CB_FIWARE_SERVICEPATH}
    
    threads = list()
    
    i = 0
    while i <= 100:
        
        print("i="+str(i))
        
        r = requests.get(url=CB_URL_TO_SEND_DATA+"?type=PointOfInterest&limit=1000", timeout=300, data='', headers=headersGet)
        
        
        #print("CB_URL_TO_SEND_DATA")
        #print(CB_URL_TO_SEND_DATA)
        #print ("response")
        #print (r.status_code)
        #print (r.json)
        #print (r.text)
        #print("---------")        
        

        data = r.json()

        
        for item in data:
            
            #print(item['id'])
            
            t = threading.Thread(target=deleteItem, args=(item,))
            
            threads.append(t)
            t.start()
            
        
        i += 1    
        
    print("End!!!")
    currentdate = dt.utcnow()
    print("start date")
    print(startdate)
    print("end date")
    print(currentdate)        