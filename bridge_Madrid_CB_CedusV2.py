import pandas as pd 
import numpy as np
from pathlib import Path
import json
import utm
import csv
import requests
import copy
import threading
from datetime import date
import time
from datetime import datetime

from dateutil.relativedelta import relativedelta
from pprint import pprint

from config import CSV_FILE, MADRID_PM_ENDPOINT, CB_FIWARE_SERVICE, CB_FIWARE_SERVICEPATH, CB_URL_TO_SEND_DATA, \
    JSON_FILE_TO_EXPORT_DATA, USE_THREADING, CB_URL_TO_SEND_DATA_BATCH_OPERATIONS, fileInstenidadSat, logFileBridge

from configAverageScript import averageExportFile, datesIntervals, folderExportPath

#import holidays
from utilsBridge.extraDaysToProceesAsHolidaysInMadrid import esp_holidays

from lxml import etree
from lxml.etree import fromstring
from datetime import datetime as dt
from datetime import timedelta

import xml.etree.ElementTree as ET

import logging
import logging.handlers
logging.basicConfig(
    format = '%(asctime)s %(levelname)s - \t%(message)s',
)
log = logging.getLogger("main")
log.setLevel(logging.DEBUG)

log.addHandler(logging.handlers.RotatingFileHandler(logFileBridge,maxBytes=10485760,backupCount=10))

#function to plot data in the log
def print_Ceduslogs(message):
    log.info("[INFO] ["+str(dt.utcnow())+"] "+str(message))
    print("[INFO] ["+str(dt.utcnow())+"] "+str(message))


def send_data_with_bach_operation(entities, action):

    datoToSend = {
                  "actionType": str(action),
                  "entities": entities
                  }
    
    dataToPostBatchOperation = json.dumps(datoToSend)
        
    endpointBatchOperation = CB_URL_TO_SEND_DATA_BATCH_OPERATIONS
    
    
    try:

        r = requests.post(url=endpointBatchOperation, data=dataToPostBatchOperation, headers=headersPost)    

        #print("endpointBatchOperation")
        #print(endpointBatchOperation)
        #print ("response")
        #print (r.status_code)
        #print (r.json)
        #print (r.text)
        #print("---------")
        
        if ((r.status_code==201) or (r.status_code==204)):
            #print( "send_data_with_bach_operation OK!! ")
            print_Ceduslogs( "send_data_with_bach_operation OK!! ")            
        else:
            #print("problem in send_data_with_bach_operation - error code:"+str(r.status_code))
            print_Ceduslogs("problem in send_data_with_bach_operation - error code:"+str(r.status_code))
        #json_data = json.loads(r.text)
    
    except ValueError:
        print("[ERROR] Error in send_data_with_bach_operation ")
        print(ValueError)
        print_Ceduslogs("[ERROR] Error in send_data_with_bach_operation ")
        print_Ceduslogs(str(ValueError))

def processPM(dataToPostIN):
    #print("processPM")

    dataToPost = copy.deepcopy(dataToPostIN)    
    
    #try to update if update fails we create the new entity
    del dataToPost['id']
    del dataToPost['type']

    dataToPost = json.dumps(dataToPost)
    endpointToUpdatePost = endpoint+"/"+str(dataToPostIN["id"])+"/attrs"
    
    try:

        r = requests.post(url=endpointToUpdatePost, data=dataToPost, headers=headersPost)
        
        #print("endpointToUpdatePost")
        #print(endpointToUpdatePost)
        #print ("response")
        #print (r.status_code)
        #print (r.json)
        #print (r.text)
        #print("---------")
                        
        if (r.status_code==204):
            #update OK
            print("entity "+str(dataToPostIN["id"])+" updated")
        
    
        elif (r.status_code==404):
            #entity not found we must create it
            print("entity "+str(dataToPostIN["id"])+" not found we must create it")
    
            endpointToPost = endpoint
            #endpointToPost = endpoint+"?options=keyValues"
            #print("endpointToPost")
            #print(endpointToPost)
            #print("dataToPost")
            #print(dataToPost)
    
            dataToPostINJson = json.dumps(dataToPostIN)
            
            try:
                
                r = requests.post(url=endpointToPost, data=dataToPostINJson, headers=headersPost)
                
                #print("endpointToPost")
                #print(endpointToPost)
                #print ("response")
                #print (r.status_code)
                #print (r.json)
                #print (r.text)
                #print("---------")
                if (r.status_code==201):
                    #print("entity "+str(dataToPostIN["id"])+" created")
                    print_Ceduslogs("entity "+str(dataToPostIN["id"])+" created")
                else:
                    #print("problem creatin entity "+str(dataToPostIN["id"])+" - code:"+str(r.status_code))
                    print_Ceduslogs("problem creatin entity "+str(dataToPostIN["id"])+" - code:"+str(r.status_code))
                #json_data = json.loads(r.text)
            
            except ValueError:
                #print("[ERROR] Error trying create data for entity: "+str(dataToPostIN["id"]))
                #print(ValueError)
                print_Ceduslogs("[ERROR] Error trying create data for entity: "+str(dataToPostIN["id"]))
                print_Ceduslogs(str(ValueError))
                 
    except ValueError:
        #print("[ERROR] Error trying to post data for entity: "+str(dataToPostIN["id"]))
        #print(ValueError)
        print_Ceduslogs("[ERROR] Error trying to post data for entity: "+str(dataToPostIN["id"]))
        print_Ceduslogs(str(ValueError))


if __name__ == '__main__':
    #print("start script ")
    print_Ceduslogs("--------------------------------")
    print_Ceduslogs("Starting script")
    currentdate = dt.utcnow()
    #currentdate = dt.utcnow()+timedelta(days=4)
    #print("currentdate:"+str(currentdate))
    print_Ceduslogs("currentdate:"+str(currentdate))

    #if dayOfTheWeek = 5 or 6 is saturday or sunday, will be used as holliday day
    dayOfTheWeek = dt.today().weekday()
    #print("day of the week: "+str(dayOfTheWeek))
    print_Ceduslogs("day of the week: "+str(dayOfTheWeek))

    #print("Start reading intensitysat file")
    print_Ceduslogs("Start reading intensitysat file")
    
    intensitdadSaturacionDict = {}

    df = pd.read_csv(fileInstenidadSat, header=None, delimiter = ';')

    for index, row in df.iterrows():
        if (float(row[2])>0):
            intensitdadSaturacionDict[str(row[1])] = {'cod_cent': row[0], 'id': row[1], 'intensidadSat': row[2]}


    #print("intensitysat file readed")
    print_Ceduslogs("intensitysat file readed")
    
    #use_Threading = True
    use_Threading = USE_THREADING



    #print("Start read xml from Madrid")
    print_Ceduslogs("Start read xml from Madrid")
    #source_url = 'http://informo.munimadrid.es/informo/tmadrid/pm.xml'
    source_url = MADRID_PM_ENDPOINT
    response = requests.get(source_url)
    #print("response")
    #print(response)

    tree = etree.fromstring(response.content)

    textelemFechaXML = tree.find('fecha_hora').text
    textelemFechaXML = str(textelemFechaXML)

    #textelemFechaXML = '31/12/2017 08:33:24'
    #print("textelemFechaXML:"+str(textelemFechaXML))
    print_Ceduslogs("textelemFechaXML:"+str(textelemFechaXML))
    
    x = str(textelemFechaXML)
    x=x.split(" ")
    print_Ceduslogs("x")
    print_Ceduslogs(x)
    print_Ceduslogs("x[0]")
    print_Ceduslogs(x[0])
    dt_obj_date = datetime.strptime(x[0], '%d/%m/%Y')
    print_Ceduslogs("dt_obj_date="+str(dt_obj_date))
    dt_obj_date = dt_obj_date - relativedelta(years=1)
    print_Ceduslogs("dt_obj_date="+str(dt_obj_date))
    
    textelemFechaXML = dt.strptime(textelemFechaXML,'%d/%m/%Y %H:%M:%S')

    hora = ''
    cuarto = ''
    
    hora = textelemFechaXML.hour
    minute = textelemFechaXML.minute
    
    cuarto = 0
    if (int(minute)<15):
        cuarto = 0
    elif (int(minute)<30):
        cuarto = 1
    elif (int(minute)<45):
        cuarto = 2
    else:
        cuarto = 3
    
    hora = str(hora)
    cuarto = str(cuarto)

    print_Ceduslogs("--------------------------------------------")
    print_Ceduslogs("textelemFechaXML="+str(textelemFechaXML))    
    print_Ceduslogs("hora="+str(hora))
    print_Ceduslogs("minute="+str(minute))
    print_Ceduslogs("cuarto="+str(cuarto))
    print_Ceduslogs("--------------------------------------------")

    #print (textelemFechaXML.text)
    
    print_Ceduslogs("the date "+str(textelemFechaXML)+" is holiday?")
    dayOfTheWeek = textelemFechaXML.weekday()
    print_Ceduslogs("day of the week "+str(dayOfTheWeek))

    stringTypeOfDay = ''
    todayIsAHolidayDay = False
    if ((textelemFechaXML in esp_holidays) or (dayOfTheWeek>=6)):
        stringTypeOfDay = 'Holiday'
        todayIsAHolidayDay = True
    elif (dayOfTheWeek==0):
        stringTypeOfDay = 'Monday'
    elif (dayOfTheWeek==1):
        stringTypeOfDay = 'Tuesday'
    elif (dayOfTheWeek==2):
        stringTypeOfDay = 'Wednesday'
    elif (dayOfTheWeek==3):
        stringTypeOfDay = 'Thursday'
    elif (dayOfTheWeek==4):
        stringTypeOfDay = 'Friday'
    elif (dayOfTheWeek==5):
        stringTypeOfDay = 'Saturday'
    
    print("stringDay"+str(stringTypeOfDay))


    posInterval = 0
    while posInterval < len(datesIntervals):
        print_Ceduslogs("posInterval="+str(posInterval))
        
        print_Ceduslogs("startDate="+str(datesIntervals[posInterval]["startDate"]))
        print_Ceduslogs("endDate="+str(datesIntervals[posInterval]["endDate"]))
        
        
        startDate = str(datesIntervals[posInterval]["startDate"])+" 00:00:00"
        dt_obj_startDate = datetime.strptime(startDate, '%d/%m/%Y %H:%M:%S')
                            
        endDate = str(datesIntervals[posInterval]["endDate"])+" 23:59:59"
        dt_obj_endDate = datetime.strptime(endDate, '%d/%m/%Y %H:%M:%S')
        
        
        print_Ceduslogs("dt_obj_startDate="+str(dt_obj_startDate))
        print_Ceduslogs("dt_obj_endDate="+str(dt_obj_endDate))
        
        print_Ceduslogs("dt_obj_date="+str(dt_obj_date))
        
        
        
        if (dt_obj_startDate<=dt_obj_date and dt_obj_endDate>=dt_obj_date):
            print_Ceduslogs("interval found ="+str(posInterval))
            intervalValue = posInterval
            posInterval = 99999
        
        posInterval = posInterval + 1
        
    print_Ceduslogs("---------")
    
    print_Ceduslogs("posInterval="+str(posInterval))
    print_Ceduslogs("intervalValue="+str(intervalValue))


    intervalStartDate = str(datesIntervals[intervalValue]['startDate'])
    intervalEndDate = str(datesIntervals[intervalValue]['endDate'])
    
    intervalStartDate = intervalStartDate.replace("/", "-")
    intervalEndDate = intervalEndDate.replace("/", "-")
    fileNameToUse = str(folderExportPath)+"/csv_"+str(intervalStartDate)+"_"+intervalEndDate+"_"+stringTypeOfDay+".json"
    
    
    print_Ceduslogs("Reading data from the historial data in fileNameToUse="+str(fileNameToUse))
    datajson = {}
    fileExists = False
    my_file = Path(fileNameToUse)
    if my_file.exists():    
        with open(fileNameToUse) as f:
            print_Ceduslogs("loading data from historical values")
            datajson = json.load(f)
            fileExists = True
    else:
        print_Ceduslogs("historical values file don't exists")

    #listOfValues = {'codigo', 'idelem', 'descripcion', 'accesoAsociado', 'intensidad', 'ocupacion', 'carga', 'nivelServicio', 'intensidadSat', 'velocidad', 'error', 'subarea'}
    listOfValues = {'idelem', 'descripcion', 'accesoAsociado', 'intensidad', 'ocupacion', 'carga', 'nivelServicio', 'intensidadSat', 'velocidad', 'error', 'subarea'}

    pmsdataFinal = [];
    cnt = 0;


    #headers to send data to CB
    headersGet = {'Fiware-Service': CB_FIWARE_SERVICE, 'Fiware-ServicePath': CB_FIWARE_SERVICEPATH}
    headersPost = {'Fiware-Service': CB_FIWARE_SERVICE, 'Fiware-ServicePath': CB_FIWARE_SERVICEPATH, 'Content-Type':'application/json'}
    headersDelete = {'Fiware-Service': CB_FIWARE_SERVICE, 'Fiware-ServicePath': CB_FIWARE_SERVICEPATH}

    #date used to send timestamps
    #currentdate = dt.utcnow()
    #currentdateToSend = currentdate.isoformat()
    currentdate = dt.now().strftime("%Y-%m-%dT%H:%M:%S")
    currentdateToSend = currentdate+"Z"

    textelemFechaXMLFinal = textelemFechaXML.strftime("%Y-%m-%dT%H:%M:%S")
    textelemFechaXMLFinal = textelemFechaXMLFinal+"Z"
    
    print_Ceduslogs("currentdateToSend")
    print_Ceduslogs(currentdateToSend)

    threads = list()
    cntEntities = 0
    cntSemaforo = 0 
    
    entities = []

    for child in tree.iter('pm'):
        #print("child")
        #print(child.tag, child.attrib)
        
        #elem = child.findtext("codigo")
        elem = child.findtext("idelem")
        
        #print("elem:"+str(elem))
                
        #pos_index = pmsToCheck.index(elem) if elem in pmsToCheck else -1
        pos_index  = 999
                    
        #print("pos_index")
        #print(pos_index)
        
        
        if (pos_index>0):
            
            latlon= utm.to_latlon(float(child.findtext("st_x").replace(",",".")), float(child.findtext("st_y").replace(",",".")),30,'U')
    
    
            elementExists = False
            intensityHistorical = ""
            occupancyHistorical = ""
            loadHistorical = ""
            if (fileExists):
                if child.findtext("idelem") in datajson[hora][cuarto]:
                    print_Ceduslogs("element "+str(child.findtext("idelem"))+" exists in json "+str(fileNameToUse)+"--h:"+str(hora)+"--cuarto_"+str(cuarto))
                    elementExists = True
                    elementData = datajson[hora][cuarto][child.findtext("idelem")]
                    #print(elementData)
                else:
                    print_Ceduslogs("element "+str(child.findtext("idelem"))+" NOT exists in json "+str(fileNameToUse)+"--h:"+str(hora)+"--cuarto_"+str(cuarto))
                    elementExists = False

            intensityHistorical = None
            occupancyHistorical = None
            loadHistorical = None
            if (elementExists):
                #print("------------")
                intensityHistorical = str(elementData["intensity"]["total"] / elementData["intensity"]["cnt"])
                occupancyHistorical = str(elementData["ocupancy"]["total"] / elementData["ocupancy"]["cnt"])
                loadHistorical = str(elementData["load"]["total"] / elementData["load"]["cnt"])                
                #print("intensityHistorical="+str(intensityHistorical))
                #print("occupancyHistorical="+str(occupancyHistorical))
                #print("loadHistorical="+str(loadHistorical))
                        
    
            dataPMFinal = {
                           'id':"Madrid-TrafficFlowObserved-"+str(child.findtext("idelem")),
                           'descripcion': child.findtext("descripcion"),
                           'accesoAsociado': child.findtext("accesoAsociado"),
                           'intensidad': child.findtext("intensidad"),
                           'intensityHistorical': intensityHistorical,
                           'ocupacion': child.findtext("ocupacion"),
                           'occupancyHistorical': occupancyHistorical,
                           'carga': child.findtext("carga"),
                           'loadHistorical': loadHistorical,
                           'nivelServicio': child.findtext("nivelServicio"),
                           'intensidadSat': child.findtext("intensidadSat"),
                           'velocidad': child.findtext("velocidad"),
                           'error': child.findtext("error"),
                           'subarea': child.findtext("subarea"),
                           'lat': latlon[0],
                           'lon':latlon[1],
                           }
                    
            #print(dataPMFinal)
    
            pmsdataFinal.append(dataPMFinal)
    
            #json data to send
            dataToPost = {}
            #comment/uncomment next line to do test and insert all data in the same entity
            #dataToPost["id"] = "madrid-pm-1"
            #dataToPost["id"] = "madrid-pm-"+str(dataPMFinal['id'])
            #dataToPost["id"] = "madrid-pm-"+str(dataPMFinal['id']+"-"+dataPMFinal['cod_cent'])
            dataToPost["id"] = str(dataPMFinal['id'])
            
            #dataToPost["code"] = {"type": "Text", "value": dataPMFinal['cod_cent']}
    
            newdescription = str(dataPMFinal['descripcion'])
            newdescription = newdescription.replace("\t", ",")
            newdescription = newdescription.replace("\n", ",")        
            newdescription = newdescription.replace(";", ",")
            newdescription = newdescription.replace("(", " ")
            newdescription = newdescription.replace(")", " ")
            newdescription = newdescription.replace("[", " ")
            newdescription = newdescription.replace("]", " ")
            newdescription = newdescription.replace("{", " ")
            newdescription = newdescription.replace("}", " ")
            newdescription = newdescription.replace(">", " ")
            newdescription = newdescription.replace("<", " ")
            newdescription = newdescription.replace('"', " ")
    
            dataToPost["type"] = "TrafficFlowObserved"
          
            dataToPost["location"] = {"type": "geo:json", "value":{"type": "Point","coordinates": [dataPMFinal['lon'], dataPMFinal['lat']]}}
            dataToPost["address"] = {"type": "address", "value":{"streetAddress": newdescription,"addressLocality": "Madrid","addressCountry": "ES"}}
            
            #dataToPost["dateModified"] = {"type": "DateTime", "value": currentdateToSend}        
            dataToPost["dateModified"] = {"type": "DateTime", "value": (textelemFechaXMLFinal)}
            
            dataToPost["laneId"] = {"type": "Number", "value": 1}
            
            #dataToPost["dateObserved"] = {"type": "Text", "value": (currentdateToSend)+"/"+(currentdateToSend)}
            dataToPost["dateObserved"] = {"type": "Text", "value": str(textelemFechaXMLFinal)+"/"+str(textelemFechaXMLFinal)}
            
            #dataToPost["name"] = {"type": "Text", "value": dataPMFinal['id']+"-"+dataPMFinal['cod_cent']}
            dataToPost["name"] = {"type": "Text", "value": dataPMFinal['id']}
            #dataToPost["description"] = {"type": "Text", "value": dataPMFinal['cod_cent']+"-"+newdescription}
            dataToPost["description"] = {"type": "Text", "value": newdescription}
    
            if (dataPMFinal['error']):
                
                try:
                    dataToPost["error"] = {"type": "Text", "value": str(dataPMFinal['error'])}
                        
                except ValueError:
                    print_Ceduslogs("Error en error:"+str((dataPMFinal['error'])))
                    print_Ceduslogs(str(ValueError))
    
    
            if (dataPMFinal['intensidad']):
                try:
                    dataToPost["intensity"] = {"type": "Number", "value": float(dataPMFinal['intensidad'])}
                    
                    #if (dataPMFinal['intensidad_relativa']):
                    #    dataToPost["intensity_relative"] = {"type": "Number", "value": float(dataPMFinal['intensidad_relativa'])}
                        
                except ValueError:
                    print_Ceduslogs("Error intensidad 1:"+str((dataPMFinal['intensidad'])))
                    print_Ceduslogs(str(ValueError))

            else:
                try:
                    dataToPost["intensity"] = {"type": "Number", "value": None}
                        
                except ValueError:
                    print_Ceduslogs("Error intensidad 2:"+str((dataPMFinal['intensidad'])))
                    print_Ceduslogs(str(ValueError))    
    
            if (dataPMFinal['intensityHistorical']):
                try:
                    dataToPost["intensityAVG"] = {"type": "Number", "value": float(dataPMFinal['intensityHistorical'])}
                    
                        
                except ValueError:
                    print_Ceduslogs("Error intensityHistorical:"+str((dataPMFinal['intensityHistorical'])))
                    print_Ceduslogs(str(ValueError))
            else:
                try:
                    dataToPost["intensityAVG"] = {"type": "Number", "value": None}
                    
                        
                except ValueError:
                    print_Ceduslogs("Error intensityHistorical:"+str((dataPMFinal['intensityHistorical'])))
                    print_Ceduslogs(str(ValueError))
                    
            if (dataPMFinal['ocupacion']):
                try:
                    dataToPost["occupancy"] = {"type": "Number", "value": float(dataPMFinal['ocupacion'])}
                    
                    #if (dataPMFinal['ocupacion_relativa']):
                    #    dataToPost["occupancy_relative"] = {"type": "Number", "value": float(dataPMFinal['ocupacion_relativa'])}
                        
                except ValueError:
                    print_Ceduslogs("Error ocupacion 1:"+str((dataPMFinal['ocupacion'])))
                    print_Ceduslogs(str(ValueError))

            else:
                try:
                    dataToPost["occupancy"] = {"type": "Number", "value": None}
                    
                except ValueError:
                    print_Ceduslogs("Error occupancy 2"+str((dataPMFinal['carga'])))
                    print_Ceduslogs(ValueError)
                                        
    
            if (dataPMFinal['occupancyHistorical']):
                try:
                    dataToPost["occupancyAVG"] = {"type": "Number", "value": float(dataPMFinal['occupancyHistorical'])}
                    
                        
                except ValueError:
                    print_Ceduslogs("Error occupancyHistorical 1:"+str((dataPMFinal['occupancyHistorical'])))
                    print_Ceduslogs(str(ValueError))                
            else:
                try:
                    dataToPost["occupancyAVG"] = {"type": "Number", "value": None}
                    
                        
                except ValueError:
                    print_Ceduslogs("Error occupancyAVG 2:"+str((dataPMFinal['occupancyHistorical'])))
                    print_Ceduslogs(str(ValueError))
                                        
            if (dataPMFinal['velocidad']):
                try:
                    dataToPost["averageVehicleSpeed"] = {"type": "Number", "value": float(dataPMFinal['velocidad'])}
                    
                except ValueError:
                    print_Ceduslogs("Error velocidad 1:"+str((dataPMFinal['velocidad'])))
                    print_Ceduslogs(str(ValueError))

            else:
                try:
                    dataToPost["averageVehicleSpeed"] = {"type": "Number", "value": None}
                    
                except ValueError:
                    print_Ceduslogs("Error velocidad 2:"+str((dataPMFinal['carga'])))
                    print_Ceduslogs(str(ValueError))
                    
                    
            if (dataPMFinal['carga']):
                try:
                    dataToPost["load"] = {"type": "Number", "value": float(dataPMFinal['carga'])}
                    
                except ValueError:
                    print_Ceduslogs("Error carga 1:"+str((dataPMFinal['carga'])))
                    print_Ceduslogs(str(ValueError))            
                    
            else:
                try:
                    dataToPost["load"] = {"type": "Number", "value": None}
                    
                except ValueError:
                    print_Ceduslogs("Error carga 2:"+str((dataPMFinal['carga'])))
                    print_Ceduslogs(ValueError)    
                           
            if (dataPMFinal['loadHistorical']):
                try:
                    dataToPost["loadAVG"] = {"type": "Number", "value": float(dataPMFinal['loadHistorical'])}
                    
                        
                except ValueError:
                    print_Ceduslogs("Error loadHistorical 1:"+str((dataPMFinal['loadHistorical'])))
                    print_Ceduslogs(str(ValueError))                
            else:
                try:
                    dataToPost["loadAVG"] = {"type": "Number", "value": None}
                    
                except ValueError:
                    print_Ceduslogs("Error loadHistorical 2:"+str((dataPMFinal['loadHistorical'])))
                    print_Ceduslogs(str(ValueError))
                            
            #intensidadSat debe sacarse del csv
                   
            if str(dataPMFinal['id']) in intensitdadSaturacionDict:
                print_Ceduslogs("intensidadSat del CSV")
                
                try:
                    dataToPost["intensidadSat"] = {"type": "Number", "value": float(intensitdadSaturacionDict[str(dataPMFinal['id'])]['intensidadSat'])}
                except ValueError:
                    print_Ceduslogs("Error intensidadSat by csv:"+str(intensitdadSaturacionDict[str(dataPMFinal['id'])]['intensidadSat']))
                    print_Ceduslogs(str(ValueError))
                
                #try:
                #    dataToPost["code"] = {"type": "Text", "value": string(intensitdadSaturacionDict[str(dataPMFinal['id'])]['cod_cent'])}
                #except ValueError:
                #    print("Error cod_cent by csv")
                #    print(ValueError)
                        
            else: 
                print_Ceduslogs("intensidadSat del XML")
                #if (dataPMFinal['intensidadSat'] or float(dataPMFinal['intensidadSat'])==0):
                if (dataPMFinal['intensidadSat']):
                    try:
                        dataToPost["intensidadSat"] = {"type": "Number", "value": float(dataPMFinal['intensidadSat'])}
                    except ValueError:
                        print_Ceduslogs("Error intensidadSat by XML:"+str((dataPMFinal['intensidadSat'])))
                        print_Ceduslogs(ValueError)
    
            #dataToPost["congested"] = False
            congestedPM = False
            if 'nivelServicio' in dataPMFinal:
                try:
                    dataToPost["nivelServicio"] = {"type": "Number", "value": float(dataPMFinal['nivelServicio'])}
                except ValueError:
                    print_Ceduslogs("Error nivelServicio:"+str((dataPMFinal['nivelServicio'])))
                    print_Ceduslogs(ValueError)
                
                if (dataPMFinal['nivelServicio']==3):
                    congestedPM = True
                else:
                    congestedPM = False
            else:
                congestedPM = False
                
            dataToPost["congested"] = {"type": "Boolean", "value": congestedPM}
            dataToPost["isHoliday"] = {"type": "Boolean", "value": todayIsAHolidayDay}
            
                      
    
            endpoint = CB_URL_TO_SEND_DATA
            
           
            #print("PM="+dataToPost["id"])
            #if (str(dataPMFinal['id'])=="9954"):
            if (1==1):
                #print("hora:"+str(hora))
                #print("cuarto:"+str(cuarto))
                #print(dataToPost)
                
                #aa=8/0
                entities.append(dataToPost)
                                
                if (cntSemaforo==600):
                #if (1==1):
                    #print(entities)
                    #fist send function with APPEND to create any new entity
                    send_data_with_bach_operation(entities, 'APPEND')
                    #second send function with UPDATE to updare entities
                    send_data_with_bach_operation(entities, 'UPDATE')
                    entities = []
                    
                    cntSemaforo = 0
                    
                    
    
            cntSemaforo = cntSemaforo + 1
            cntEntities = cntEntities +1

    if (len(entities)>0):
        #fist send function with APPEND to create any new entity
        send_data_with_bach_operation(entities, 'APPEND')
        #second send function with UPDATE to updare entities
        send_data_with_bach_operation(entities, 'UPDATE')
        
    '''
    with open(JSON_FILE_TO_EXPORT_DATA, 'w') as f:
    
        json.dump(pmsdataFinal, f)
    '''    
    
    print_Ceduslogs("end")
    print_Ceduslogs ("total entities="+str(cntEntities))
    currentdate = dt.utcnow()
    print_Ceduslogs(currentdate)
    print_Ceduslogs("--------------------------------")