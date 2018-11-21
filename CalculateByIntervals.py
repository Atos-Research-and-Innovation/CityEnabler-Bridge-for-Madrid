import sys
import os
from pathlib import Path
import time
#import pandas as pd 
import numpy as np
import csv
import json
from datetime import datetime as dt
from datetime import datetime
from configAverageScript import datesIntervals, folderWithCSVs, averageLogFile, averageExportFile, listOfDataToRecoverToCalculateAverages, \
    USE_THREADING, CB_FIWARE_SERVICE, CB_FIWARE_SERVICEPATH, CB_URL_TO_SEND_DATA, folderExportPath

from os import listdir
from os.path import isfile, join

#import holidays
from utilsBridge.extraDaysToProceesAsHolidaysInMadrid import esp_holidays

import logging
import logging.handlers
logging.basicConfig(
    format = '%(asctime)s %(levelname)s - \t%(message)s',
)
log = logging.getLogger("main")
log.setLevel(logging.DEBUG)

log.addHandler(logging.handlers.RotatingFileHandler(averageLogFile,maxBytes=10485760,backupCount=10))

typeOfDays = ['Holiday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday']


fileNameToUseToStoreTmpData = str(folderExportPath)+"/TMP_DATA.json"

#To store tmp data in the tmp file
def storeDataInTmpFileByInterval():

    #we must evaluate all temporal json to merge them in one per period and type of day
    print_Ceduslogs("---------------START storeDataInTmpFileByInterval -----")
    
    for interval in temporalData:
        print(interval)
        fileNameToUse = str(folderExportPath)+"/csv_"+str(interval)+".json"
        print_Ceduslogs("--Before opening the export file in: "+str(fileNameToUse))
        
        dataInTmpFile = {}
        
        my_file = Path(fileNameToUse)
        
        if my_file.exists():
            print_Ceduslogs("File "+fileNameToUse+" exists!!!")
            
            with open(fileNameToUse) as json_data_initial:
                dataInTmpFile = json.load(json_data_initial)
                print_Ceduslogs("Loading data!!!")
                needAnUpdate = True
         
        for h_t in temporalData[interval]:
            for c_t in temporalData[interval][h_t]:
                for id_pm in temporalData[interval][h_t][c_t]:
                    #we need to add this data to the data stored into the tempoal file
                    #if the id is not in the tmp data, we set defaults values for it
                    if (id_pm not in dataInTmpFile[str(h_t)][str(c_t)]):                         
                        dataInTmpFile[str(h_t)][str(c_t)][id_pm] = initialValuePerPM()
                    
                    valueIntensidad = temporalData[interval][h_t][c_t][id_pm]['intensity']['total']
                    cntIntensidad = temporalData[interval][h_t][c_t][id_pm]['intensity']['cnt']                     
                    dataInTmpFile[str(h_t)][str(c_t)][id_pm]['intensity']['total'] += (float(valueIntensidad))
                    dataInTmpFile[str(h_t)][str(c_t)][id_pm]['intensity']['cnt'] += cntIntensidad
                    
                    valueOcupancy = temporalData[interval][h_t][c_t][id_pm]['ocupancy']['total']
                    cntOcupancy = temporalData[interval][h_t][c_t][id_pm]['ocupancy']['cnt']
                    dataInTmpFile[str(h_t)][str(c_t)][id_pm]['ocupancy']['total']  += (float(valueOcupancy))
                    dataInTmpFile[str(h_t)][str(c_t)][id_pm]['ocupancy']['cnt'] += cntOcupancy
                    
                    valueLoad = temporalData[interval][h_t][c_t][id_pm]['load']['total']
                    cntLoad = temporalData[interval][h_t][c_t][id_pm]['load']['cnt']
                    dataInTmpFile[str(h_t)][str(c_t)][id_pm]['load']['total'] += (float(valueLoad))
                    dataInTmpFile[str(h_t)][str(c_t)][id_pm]['load']['cnt'] += cntLoad
                     
         
        print_Ceduslogs("Before updating data in the export file in: "+str(fileNameToUse))
        
        jsonToStore = dataInTmpFile
        
        with open(fileNameToUse, 'w') as outfile:
            json.dump(jsonToStore, outfile)     
            #print_Ceduslogs("jsonToStore: ")
            #print_CeduslogsDict(jsonToStore)
            print_Ceduslogs("****** File "+str(fileNameToUse)+" updated!!!")             
    
    print_Ceduslogs("---------------END storeDataInTmpFileByInterval -----")

    
#function to plot data in the log
def print_CeduslogsDict(message):
    log.info(message)
    
#function to plot data in the log
def print_Ceduslogs(message):
    log.info("[INFO] ["+str(dt.utcnow())+"] "+str(message))
    #print("[INFO] ["+str(dt.utcnow())+"] "+str(message))

#function to recover the correct string label for the day per date    
def recoverTypeOfDay(dateTocheck):
    
    td = dt.strptime(dateTocheck,'%Y-%m-%d %H:%M:%S')
    dayOfTheWeek = td.weekday()
    
    stringDay = ''                
    
    if ((td in esp_holidays) or (dayOfTheWeek>=6)):
        stringDay = 'Holiday'
    elif (dayOfTheWeek==0):
        stringDay = 'Monday'
    elif (dayOfTheWeek==1):
        stringDay = 'Tuesday'
    elif (dayOfTheWeek==2):
        stringDay = 'Wednesday'
    elif (dayOfTheWeek==3):
        stringDay = 'Thursday'
    elif (dayOfTheWeek==4):
        stringDay = 'Friday'
    elif (dayOfTheWeek==5):
        stringDay = 'Saturday'
        
    return stringDay

#function to load the intital values per PM
def initialValuePerPM():
    
    #data = {'intensity':[], 'ocupancy':[], 'load':[]}
    
    data = {'intensity':{'total':0, 'cnt':0}, 'ocupancy':{'total':0, 'cnt':0}, 'load':{'total':0, 'cnt':0}}
    return data
 
#function to load the intital values per interval of date
def initialValuePerInterval():
    
    data = {}
    for x in range(0, 24):
        data[x] = {
                   0: {}, 
                   1: {}, 
                   2: {}, 
                   3: {}
                }
    
    return data    

#function to create the initial Jsons 
def createInitialFiles(numberOfCSVFiles):
    
    cntJsonsByInterval = 0
    '''
    for x in range(1, int(numberOfCSVFiles)+1):
        print("x="+str(x))
    '''
    posi = 0
    intial_json = {
                   '0': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '1': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '2': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '3': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '4': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '5': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '6': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '7': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '8': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '9': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '10': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '11': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '12': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '13': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '14': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '15': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '16': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '17': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '18': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '19': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '20': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '21': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '22': { '0': {}, '1': {}, '2': {}, '3':{}},
                   '23': { '0': {}, '1': {}, '2': {}, '3':{}}
                   }
    
    
    #to calculate dynamically the intervals arra
    while posi < len(datesIntervals):
        #print(datesIntervals[posi])
        intervalStartDate = str(datesIntervals[posi]['startDate'])
        intervalEndDate = str(datesIntervals[posi]['endDate'])
        
        intervalStartDate = intervalStartDate.replace("/", "-")
        intervalEndDate = intervalEndDate.replace("/", "-")
        
        
            
        
        posTD = 0
        while posTD < len(typeOfDays):
            stringTypeOfDay = str(typeOfDays[posTD])
            fileNameToUse = str(folderExportPath)+"/csv_"+str(intervalStartDate)+"_"+intervalEndDate+"_"+stringTypeOfDay+".json"
            
            print_Ceduslogs("Before creating the export file in: "+str(fileNameToUse))
            
            with open(fileNameToUse, 'w') as outfile:                
                json.dump(intial_json, outfile)    
            
            '''
            fileNameToUseFinal = str(folderExportPath)+"/FINAL_"+str(intervalStartDate)+"_"+intervalEndDate+"_"+stringTypeOfDay+".json"
            
            with open(fileNameToUseFinal, 'w') as outfile:  
                json.dump({}, outfile)
            ''' 
                
            print_Ceduslogs("File "+str(fileNameToUse)+" created!!")
            posTD = posTD + 1
            cntJsonsByInterval = cntJsonsByInterval + 1
        
        posi = posi + 1    
    
    print_Ceduslogs(str(cntJsonsByInterval)+" Jsons by interval has been created")
        
if __name__ == '__main__':

    print_Ceduslogs("------------------------------------------------")
    print_Ceduslogs("Starts Average process by Interval")
    
    #create The initial Files
    print_Ceduslogs("Let's create the initial jsons per interval and per type of day")
    temporalData = {}
    
    ##read CSV files to recover data
    mypath = folderWithCSVs
    print_Ceduslogs("The files that are going to be used are the csv files that are in the folder: "+str(mypath))
        
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]              

    suffix = ".csv";
    
    onlyCSVfiles = []
    
    for csvfile in onlyfiles:
        
        if (csvfile.endswith(suffix)): 
            onlyCSVfiles.append(csvfile)
    
    
    
    print_Ceduslogs("There are "+str(len(onlyCSVfiles))+" csv files in the folder "+str(mypath))    
    
    createInitialFiles(len(onlyCSVfiles))
    
    stringNumberOfCSVFilesToCheck = str(len(onlyCSVfiles))
    
    #different separators we can use
    sep = ";"
    sep2 = ","
    
    cntFiles = 0
    cntStore = 0
    for csvfile in onlyCSVfiles:
        row_count_of_a_file = 0
        
        if (csvfile.endswith(suffix)):
            
            print_Ceduslogs(csvfile)
            pathToFile = mypath+"/"+csvfile
                
            cntFiles = cntFiles + 1
            
            print_Ceduslogs("Processing file #"+str(cntFiles)+". The file is: '"+str(pathToFile)+"'")
        
            posId = 0
            posFecha = 1
            posIntensidad = 0;
            posOcupacion = 0;
            posCarga = 0;
            cntRowsForMemory = 0
            
            with open(pathToFile, newline='') as csvfile:
                spamreader = csv.reader(csvfile, delimiter=sep, quotechar='|')
                
                #spamreader = csv.DictReader(csvfile, delimiter=sep, quotechar='|')
                #commented to avoif Memory errors
                #row_count_of_a_file = len(list(csv.reader(open(pathToFile))))

                procesed25 = False
                procesed50 = False
                procesed75 = False
                procesed100 = False
                                                                
                if (len(next(spamreader))<=1):
                    #print("check other sep")                
                    spamreaderFinal = csv.reader(csvfile, delimiter=sep2, quotechar='|')
                    print_Ceduslogs("This csv uses '"+str(sep2)+"' as delimiter")                                
                else:
                    spamreaderFinal = csv.reader(csvfile, delimiter=sep, quotechar='|')
                    print_Ceduslogs("This csv uses '"+str(sep)+"' as delimiter")

                cntRows = 0
                
                lastFechaProcesada = "----"
                stringPerInterval = ""
                
                for row in spamreaderFinal:
                    #print_Ceduslogs("File "+str(cntFiles)+"/"+stringNumberOfCSVFilesToCheck+". Row: "+str(cntRows))
                    print("File "+str(cntFiles)+"/"+stringNumberOfCSVFilesToCheck+". Row: "+str(cntRows)+". File: "+str(pathToFile))
                    
                    
                    #check type of header per this CSV file
                    if(cntRows==0):
                        try:
                            testvalue = float(row[3])
                            
                            print_Ceduslogs('"Header used - "id";"fecha";"tipo_elem";"intensidad";"ocupacion";"carga";"vmed";"error";"periodo_integracion" ')
                            
                            posIntensidad = 3
                            posOcupacion = 4
                            posCarga = 5

                        except ValueError:
                            
                            print_Ceduslogs('Header used - "idelem";"fecha";"identif";"tipo_elem";"intensidad";"ocupacion";"carga";"vmed";"error";"periodo_integracion" ')
                            
                            posIntensidad = 4
                            posOcupacion = 5
                            posCarga = 6
                    
                    
                    #print_Ceduslogs('posId:'+str(posId))
                    #print_CeduslogsDict(row)
                    idPM = row[posId].replace('"', '')[:]
                    
                    #print_Ceduslogs("idPM:"+str(idPM))
                    
                    #check in which interval the data value belongs
                    dateTocheck = str(row[posFecha])
                    
                    #as there are lot of rows with the exact time, is not necessary to check this data for the same timestamps
                    if (lastFechaProcesada!=dateTocheck):
                        #print("------------------------------------------different date")
                        
                        lastFechaProcesada = dateTocheck
                        
                        dateTocheck = dateTocheck.replace('"', '')
                        dt_obj_date = datetime.strptime(dateTocheck, '%Y-%m-%d %H:%M:%S')
                                                
                        posi2 = 0
                        
                        #print_Ceduslogs("------------ intervalValuePosition = 0 --------------")
                        intervalValuePosition = 0
                        while posi2 < len(datesIntervals):
                            #print_Ceduslogs("------------posi2: "+str(posi2))
                            startDate = datesIntervals[posi2]['startDate']+" 00:00:00"
                            dt_obj_startDate = datetime.strptime(startDate, '%d/%m/%Y %H:%M:%S')
                            
                            endDate = datesIntervals[posi2]['endDate']+" 23:59:59"
                            dt_obj_endDate = datetime.strptime(endDate, '%d/%m/%Y %H:%M:%S')
                            
                            #print_Ceduslogs("------------interval start date: "+str(dt_obj_startDate))
                            #print_Ceduslogs("------------interval end date: "+str(dt_obj_endDate))
                            #print_Ceduslogs("------------row date: "+str(dt_obj_date))
    
                            if (dt_obj_startDate<=dt_obj_date and dt_obj_endDate>=dt_obj_date):
                                #print_Ceduslogs("------------row date in in the interval: "+str(posi2))
                                intervalValuePosition = posi2
                                posi2 = len(datesIntervals)
                                
                                
                                #print_Ceduslogs("------------interval position: "+str(intervalValuePosition))
                                
                            #else:
                            #    print_Ceduslogs("------------row date NOT in the interval: "+str(posi2))
                                
                                
                            posi2 = posi2 + 1
                            
                        intervalStartDateRow = str(datesIntervals[intervalValuePosition]['startDate'])
                        intervalEndDateRow = str(datesIntervals[intervalValuePosition]['endDate'])
                        
                        intervalStartDateRow = intervalStartDateRow.replace("/", "-")
                        intervalEndDateRow = intervalEndDateRow.replace("/", "-")
                        
                        #print(dateTocheck in esp_holidays)
                        #tipo de dia
                        stringDay = ''
                        stringDay = recoverTypeOfDay(dateTocheck)
                        
                        #hora y quarter
                        hourPosition = int(str(dt_obj_date.hour))
                        
                        minutePosition = int(str(dt_obj_date.minute))
                        #print ("minutePosition: "+str(minutePosition))
                        minutePositionFinal = 0
                        if (int(minutePosition)<15):
                            minutePositionFinal = 0
                        elif (int(minutePosition)<30):
                            minutePositionFinal = 1
                        elif (int(minutePosition)<45):
                            minutePositionFinal = 2
                        else:
                            minutePositionFinal = 3
                        
                        #print_Ceduslogs("creating by interval")  
                        stringPerInterval = str(intervalStartDateRow)+"_"+str(intervalEndDateRow)+"_"+stringDay
                        #print_Ceduslogs("stringPerInterval:"+stringPerInterval)
                        
                        if (stringPerInterval not in temporalData):
                            temporalData[stringPerInterval] = initialValuePerInterval()

                    #else:
                        #print("same date!! ->"+str(stringPerInterval)+"<----lastFechaProcesada = "+str(lastFechaProcesada)+"---dateTocheck="+(dateTocheck))
                    
                    #end if if that avoids to execute this part of the code if the datetime is the same
                    
                    if (idPM not in temporalData[stringPerInterval][hourPosition][minutePositionFinal]):
                        temporalData[stringPerInterval][hourPosition][minutePositionFinal][idPM] = initialValuePerPM()

                    '''
                    #print(temporalData)
                    if (1==1):
                    #if (float(row[posIntensidad])==780) :
                        print_Ceduslogs("MMP stringPerInterval: "+str(stringPerInterval))
                        
                        print_Ceduslogs("intervalStartDateRow: "+str(intervalStartDateRow))
                        print_Ceduslogs("intervalEndDateRow: "+str(intervalEndDateRow))
                        print_Ceduslogs("stringDay: "+str(stringDay))
                        print_Ceduslogs("interval position: "+str(intervalValuePosition))
                        
                        print_Ceduslogs("interval start date: "+str(datesIntervals[intervalValuePosition]['startDate']))
                        print_Ceduslogs("interval end date: "+str(datesIntervals[intervalValuePosition]['endDate']))
                        print_Ceduslogs("row date: "+str(dt_obj_date))
                        
                        print_Ceduslogs("dateTocheck: "+str(dateTocheck))
                        
                        print_CeduslogsDict(row)
                        
                        #if (str(idPM)=='1001'):
                        #if stringPerInterval.find("01-01-2017_14-01-2017") >= 0:
                        if stringPerInterval.find("15-01-2017_30-06-2017") >= 0:
                        
                            aa = 7 / 0 
                    '''        
                        
                                
                    temporalData[stringPerInterval][hourPosition][minutePositionFinal][idPM]['intensity']['total'] += (float(row[posIntensidad]))
                    temporalData[stringPerInterval][hourPosition][minutePositionFinal][idPM]['intensity']['cnt'] += 1
                    
                    temporalData[stringPerInterval][hourPosition][minutePositionFinal][idPM]['ocupancy']['total']  += (float(row[posOcupacion]))
                    temporalData[stringPerInterval][hourPosition][minutePositionFinal][idPM]['ocupancy']['cnt'] += 1
                    
                    temporalData[stringPerInterval][hourPosition][minutePositionFinal][idPM]['load']['total'] += (float(row[posCarga]))
                    temporalData[stringPerInterval][hourPosition][minutePositionFinal][idPM]['load']['cnt'] += 1

                    #cntRows = cntRows + 1
                    cntRows += 1
                    
                    #This is to avoid memory problems due the fact that we are processing huge csv files
                    if(cntRowsForMemory==2000000):
                    #if(cntRowsForMemory==2):
                        print("FREE MEMORY----------------------------------------------------------")  
                        storeDataInTmpFileByInterval()
                        cntStore = cntStore +1
                        cntRowsForMemory = 0
                        lastFechaProcesada = '______'
                        temporalData = None
                        temporalData = {}
                        
                    
                    cntRowsForMemory = cntRowsForMemory + 1 
                    
                    '''
                    percentatgeFileProcessed = (cntRows * 100) / (row_count_of_a_file - 1)
                    
                    if (percentatgeFileProcessed>=25 and procesed25==False):
                        procesed25 = True
                        print_Ceduslogs("25% of the file #"+str(cntFiles)+" ('"+str(pathToFile)+"') has been processed [ "+str(cntRows)+" of "+str(row_count_of_a_file-1)+"]")
                    elif (percentatgeFileProcessed>=50 and procesed50==False):
                        procesed50 = True
                        print_Ceduslogs("50% of the file #"+str(cntFiles)+" ('"+str(pathToFile)+"') has been processed [ "+str(cntRows)+" of "+str(row_count_of_a_file-1)+"]")                              
                    elif (percentatgeFileProcessed>=75 and procesed75==False):
                        procesed75 = True
                        print_Ceduslogs("75% of the file #"+str(cntFiles)+" ('"+str(pathToFile)+"') has been processed [ "+str(cntRows)+" of "+str(row_count_of_a_file-1)+"]")
                    elif (percentatgeFileProcessed>=100 and procesed100==False):
                        procesed100 = True                        
                        print_Ceduslogs("100% of the file #"+str(cntFiles)+" ('"+str(pathToFile)+"') has been processed [ "+str(cntRows)+" of "+str(row_count_of_a_file-1)+"]")
                    '''
                    

                print_Ceduslogs("---------End processing file "+str(cntFiles))
                #we are going to create temporal files to store the accumate data for this CSV file
                '''
                print_Ceduslogs("Before creating the temporal files for the CSV  in: "+str(cntFiles))
                numberOfExportFiles = 0
                for keyInterval in temporalData:
                    fileNameToUse = str(folderExportPath)+"/csv_"+str(cntFiles)+"_"+str(keyInterval)+".json"
                    print_Ceduslogs("Before creating the export file in: "+str(fileNameToUse))
                    
                    intial_json = temporalData[keyInterval]
                    
                    with open(fileNameToUse, 'w') as outfile:
                        json.dump(intial_json, outfile)
                        print_Ceduslogs("File "+str(fileNameToUse)+" created!!!")
                        #numberOfExportFiles = numberOfExportFiles + 1
                        numberOfExportFiles += 1
                                        
                print_Ceduslogs(str(numberOfExportFiles)+" temporal files has been created for CSV "+str(cntFiles))    
                '''    
                
                
                if (cntRowsForMemory>1):
                    storeDataInTmpFileByInterval()
                    cntRowsForMemory = 0
                    lastFechaProcesada = '______'

                    
                del temporalData
                temporalData = None
                temporalData={}
                
                

    
    if (cntRowsForMemory>1):
        storeDataInTmpFileByInterval()
        cntRowsForMemory = 0
        lastFechaProcesada = '______'

        
    del temporalData
    temporalData = None
    temporalData={}
           
    print_Ceduslogs("Ends Average process by Interval")
    print_Ceduslogs("------------------------------------------------")