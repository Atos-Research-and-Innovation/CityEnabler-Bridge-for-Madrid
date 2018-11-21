import json
import utm
import csv
import requests
import copy
import threading
from datetime import date
import time

from config import CSV_FILE, MADRID_INCIDENCIAS_ENDPOINT, CB_FIWARE_SERVICE, CB_FIWARE_SERVICEPATH, CB_URL_TO_SEND_DATA, \
    USE_THREADING, CB_URL_TO_SEND_DATA_BATCH_OPERATIONS, logFileIncidencias


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

log.addHandler(logging.handlers.RotatingFileHandler(logFileIncidencias,maxBytes=10485760,backupCount=10))

#function to plot data in the log
def print_Ceduslogs(message):
    log.info("[INFO] ["+str(dt.utcnow())+"] "+str(message))
    print("[INFO] ["+str(dt.utcnow())+"] "+str(message))


print_Ceduslogs("------------------")
print_Ceduslogs("start script")
currentdate = dt.utcnow()
#currentdate = dt.utcnow()+timedelta(days=4)
print_Ceduslogs("UTC DATE")
print_Ceduslogs(currentdate)

print_Ceduslogs("scriptStartDate DATE")
scriptStartDate = dt.now().strftime("%Y-%m-%dT%H:%M:%S")
print_Ceduslogs(scriptStartDate)


#use_Threading = True
use_Threading = USE_THREADING

endpoint = CB_URL_TO_SEND_DATA


def send_data_with_batch_operation(entities, action):
    
    print("print_Ceduslogs:"+str(action))
    datoToSend = {
                  "actionType": str(action),
                  "entities": entities
                  }
    
    dataToPostBatchOperation = json.dumps(datoToSend)
        
    endpointBatchOperation = CB_URL_TO_SEND_DATA_BATCH_OPERATIONS
    
    
    try:

        r = requests.post(url=endpointBatchOperation, data=dataToPostBatchOperation, headers=headersPost)    

        print_Ceduslogs("endpointBatchOperation")
        print_Ceduslogs(endpointBatchOperation)
        print_Ceduslogs ("response")
        print_Ceduslogs (r.status_code)
        #print (r.json)
        #print (r.text)
        print_Ceduslogs("---------")
        
        if ((r.status_code==201) or (r.status_code==204)):
            print_Ceduslogs( "send_data_with_bach_operation OK!! ")
        else:
            print_Ceduslogs("problem in send_data_with_bach_operation - error code:"+str(r.status_code))
            if (r.status_code==400):
                print_Ceduslogs("dataToPostBatchOperation")
                print_Ceduslogs(r)
                for f in r:
                    print_Ceduslogs(f)
                    
                    
                
                print_Ceduslogs(dataToPostBatchOperation)
                #a=7/0
                
            
        #json_data = json.loads(r.text)
    
    except ValueError:
        print_Ceduslogs("[ERROR] Error in send_data_with_bach_operation ")
        print_Ceduslogs(ValueError)

def processIncidncia(dataToPostIN):
    print_Ceduslogs("processIncidncia")
    print_Ceduslogs(dataToPostIN)
    
    dataToPost = copy.deepcopy(dataToPostIN)    
    
    #try to update if update fails we create the new entity
    del dataToPost['id']
    del dataToPost['type']

    dataToPost = json.dumps(dataToPost)
    endpointToUpdatePost = endpoint+"/"+str(dataToPostIN["id"])+"/attrs"
    
    try:
        
        print_Ceduslogs("dataToPost")
        print_Ceduslogs(dataToPost)
        
        r = requests.post(url=endpointToUpdatePost, data=dataToPost, headers=headersPost)
        
        print_Ceduslogs("endpointToUpdatePost")
        print_Ceduslogs(endpointToUpdatePost)
        print_Ceduslogs ("response")
        print_Ceduslogs (r.status_code)
        print_Ceduslogs (r.json)
        print_Ceduslogs (r.text)
        print_Ceduslogs("---------")
                        
        if (r.status_code==204):
            #update OK
            print_Ceduslogs("entity "+str(dataToPostIN["id"])+" updated")
        
    
        elif (r.status_code==404):
            #entity not found we must create it
            print_Ceduslogs("entity "+str(dataToPostIN["id"])+" not found we must create it")
    
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
                    print_Ceduslogs("entity "+str(dataToPostIN["id"])+" created")
                else:
                    print_Ceduslogs("problem creatin entity "+str(dataToPostIN["id"])+" - code:"+str(r.status_code))
                #json_data = json.loads(r.text)
            
            except ValueError:
                print_Ceduslogs("[ERROR] Error trying create data for entity: "+str(dataToPostIN["id"]))
                print_Ceduslogs(ValueError)
                 
    except ValueError:
        print_Ceduslogs("[ERROR] Error trying to post data for entity: "+str(dataToPostIN["id"]))
        print_Ceduslogs(ValueError)    

    


print_Ceduslogs("Start read xml")

#source_url = 'http://informo.munimadrid.es/informo/tmadrid/pm.xml'
source_url = MADRID_INCIDENCIAS_ENDPOINT

response = requests.get(source_url)
#print("response")
#print(response)

tree = etree.fromstring(response.content)

currenttime = dt.utcnow()

#print("tree")
#print(tree)


listOfValues = {'codigo', 'descripcion', 'accesoAsociado', 'intensidad', 'ocupacion', 'carga', 'nivelServicio', 'intensidadSat', 'velocidad', 'error', 'subarea'}

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

print_Ceduslogs("currentdateToSend")
print_Ceduslogs(currentdateToSend)

threads = list()
cntEntities = 0
cntSemaforo = 0 

entities = []

for child in tree.iter('Incidencias'):
    print_Ceduslogs("child")
    
    dataToPost = {}
    
    #dataToPost["type"] = "TrafficIncident"
    dataToPost["type"] = "PointOfInterest"
    
    id_incidencia = child.findtext("id_incidencia")
    print_Ceduslogs("id_incidencia:"+str(id_incidencia))
    #dataToPost["id"] = str(id_incidencia)
    
    dataToPost["id"] = "Madrid-PointOfInterest-"+str(id_incidencia)
    
    #dataToPost["id"] = "TrafficIncident1"
    
    codigo = child.findtext("codigo")
    #print("codigo:"+str(codigo))
   
    dataToPost["source"] = {"type": "URL", "value": source_url}
    
    name_incidencia = child.findtext("codigo")
    dataToPost["name"] = {"type": "Text", "value": name_incidencia}
    
    dataToPost["category"] = {"type": "List", "value": ["107"]}
    
    
    dataToPost["lastDateInProvider"] = {"type": "DateTime", "value": currentdateToSend}
    
    cod_tipo_incidencia = child.findtext("cod_tipo_incidencia")
    dataToPost["cod_tipo_incidencia"] = {"type": "Text", "value": cod_tipo_incidencia}
    #print("cod_tipo_incidencia:"+str(cod_tipo_incidencia))
    
    nom_tipo_incidencia = child.findtext("nom_tipo_incidencia")
    dataToPost["nom_tipo_incidencia"] = {"type": "Text", "value": nom_tipo_incidencia}
    #print("nom_tipo_incidencia:"+str(nom_tipo_incidencia))    

    fh_inicio = child.findtext("fh_inicio")
    #print("fh_inicio:"+str(fh_inicio))
    dataToPost["fh_inicio"] = {"type": "DateTime", "value": fh_inicio}

    fh_final = child.findtext("fh_final")
    #print("fh_final:"+str(fh_final))
    dataToPost["fh_final"] = {"type": "DateTime", "value": fh_final}
    
    incid_prevista = child.findtext("incid_prevista")
    dataToPost["incid_prevista"] = {"type": "Text", "value": incid_prevista}
    #print("incid_prevista:"+str(incid_prevista))    

    incid_planificada = child.findtext("incid_planificada")
    dataToPost["incid_planificada"] = {"type": "Text", "value": incid_planificada}
    #print("incid_planificada:"+str(incid_planificada))

    incid_estado = child.findtext("incid_estado")
    dataToPost["incid_estado"] = {"type": "Text", "value": incid_estado}
    #print("incid_estado:"+str(incid_estado))

    descripcion = child.findtext("descripcion")

    newdescription = str(descripcion)
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

            
    dataToPost["descripcion"] = {"type": "Text", "value": newdescription}
    #print("descripcion:"+str(descripcion))

    longitud = float(child.findtext("longitud"))
    #print("longitud:"+str(longitud))
    
    latitud = float(child.findtext("latitud"))
    #print("latitud:"+str(latitud))

    dataToPost["location"] = {"type": "geo:json", "value":{"type": "Point","coordinates": [longitud, latitud]}}

    tipoincid = child.findtext("tipoincid")
    dataToPost["tipoincid"] = {"type": "Text", "value": tipoincid}
    #print("tipoincid:"+str(tipoincid))

    es_obras = child.findtext("es_obras")
    es_obras_bool = False
    if (es_obras=='S' or es_obras=='s'):
        es_accidente_bool = True
        
    dataToPost["es_obras"] = {"type": "Boolean", "value": es_obras_bool}
    #print("es_obras:"+str(es_obras))

    es_accidente = child.findtext("es_accidente")
    es_accidente_bool = False
    if (es_accidente=='S' or es_accidente=='s'):
        es_accidente_bool = True
        
    dataToPost["es_accidente"] = {"type": "Boolean", "value": es_accidente_bool}
    #print("es_accidente:"+str(es_accidente))


    entities.append(dataToPost)
    #processIncidncia(dataToPost)
          
    
    if (cntSemaforo==600):
    #if (1==1):
        #print(entities)
        #fist send function with APPEND to create any new entity
        send_data_with_batch_operation(entities, 'APPEND')
        #second send function with UPDATE to updare entities
        send_data_with_batch_operation(entities, 'UPDATE')
        entities = []
        
        cntSemaforo = 0
            
            

    cntSemaforo = cntSemaforo + 1
    cntEntities = cntEntities +1

    
    '''
    if (use_Threading):
        t = threading.Thread(target=processIncidncia, args=(dataToPost,))
        threads.append(t)
        t.start()
        
        if (cntSemaforo==200):
            print("sleep!!!")
            time.sleep(3)
            cntSemaforo = 0
                        
    else:
        processIncidncia(dataToPost)
    '''

if (len(entities)>0):
    #fist send function with APPEND to create any new entity
    send_data_with_batch_operation(entities, 'APPEND')
    #second send function with UPDATE to updare entities
    send_data_with_batch_operation(entities, 'UPDATE')
    
print_Ceduslogs("end")
print_Ceduslogs ("total entities="+str(cntEntities))
currentdate = dt.utcnow()
print_Ceduslogs("UTC DATE")
print_Ceduslogs(currentdate)

print_Ceduslogs("scriptEndDate DATE")
scriptEndDate = dt.now().strftime("%Y-%m-%dT%H:%M:%S")
print_Ceduslogs(scriptStartDate)