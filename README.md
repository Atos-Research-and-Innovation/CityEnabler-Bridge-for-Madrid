# CEDUS2 - Scripts Piloto Madrid 

## 1. Manual  
This manual is tested under Ubuntu 14.04.    
The scripts has been tested with Ubuntu 14.04 and in Windows 7.          

## 2. Installation
In the following section there are the instructions about how to install and run this component.

### 2.1 Requirements
The requirements to install this component are:
* [Python 3] (https://www.python.org/download/releases/3.0/) 
* [GIT] (http://git-scm.com/downloads)    

#### 2.1.1 Pyhton 3

The code has been written in [Python 3] (https://www.python.org/download/releases/3.0/)

In order to check the current Python version in your O.S., you can execute the following command:

```
ubuntu@cedus-vm1:~$ python -V
Python 2.7.6
```

If you need to create a Python 3 virtual environment, you can do it following the next steps:   

##### 2.1.1.1 Create a new directory for the project

```
mkdir services
```
	
##### 2.1.1.2 Create a Python Virtual Environment with Python 3 and activate it
	
```
virtualenv services --python=python3
```
	
##### 2.1.1.3 Enable the virtual environment
    
```    
ubuntu@cedus-vm1:~/CEDUS$ cd services
ubuntu@cedus-vm1:~/CEDUS/services$ source bin/activate
(services)ubuntu@cedus-vm1:~/CEDUS/services$ 
```

##### 2.1.1.4 Check the version again with
    
```    
(services)ubuntu@cedus-vm1:~/CEDUS/services$ python -V
Python 3.4.3
```

#### 2.1.2 GIT

To clone this repository, you need to install Git in your environment. You can download Git from [here] (http://git-scm.com/downloads).    

    
### 2.2 Clone this repository

To clone the repository, you must execute the following command replacing "repository_path" by the correct path.   
 
```
git clone <repository_path>
```

#### 2.2.1 Folder with the scripts for Madrid´s Pilot  

After do the clone of the repository, ensure that the following files/folders are in your local repository:

*	README.md: Is this file.
*	requirements.txt: Is the file that help us to install the dependencies.   
*	Config files: There are two different config files that are used to configure correctly these scripts. The files are "config.py" and "configAverageScript.py".      
*	externalFiles: Is a folder where we store the external files provided by Madrid that the scripts uses to recover data.   
*	utilsBridge: In the "utilsBridge" folder there are the libraries shared between scripts.   
*	bridge_Madrid_CB_CedusV2.py: Script that reads real-time PMs data from "http://informo.munimadrid.es/informo/tmadrid/pm.xml" and sends it to an Orion Context Broker. It adds historical average data of intensity, occupancy and load from data recovered from JSON files that has been created by the script "CalculateByIntervals.py".   
*	bridge_Madrid_CB_Cedus_Incidencias.py: Script that reads Incidents data from "http://informo.munimadrid.es/informo/tmadrid/incid_aytomadrid.xml" and sends it to an Orion Context Broker.   
*	CalculateByIntervals.py: Script to calculate the average values (intensity, occupancy and load) per PM, split by date ranges and 15-minute intervals. The output of the script is a list of JSON files that are used by the “bridge_Madrid_CB_CedusV2.py” script to add average data before sending it to the Context Broker. The script uses as input the .csv files that are downloaded from "https://datos.madrid.es/sites/v/index.jsp?vgnextoid=33cb30c367e78410VgnVCM1000000b205a0aRCRD&vgnextchannel=374512b9ace9f310VgnVCM100000171f5a0aRCRD" to calculate the average per PM.   
*	Other files like deleteEnititiesCB.py, deleteHistoricalData.py, deleteIncidenciasCB.py are to delete data from the Orion Context Broker.   

   
#### 2.2.2 Files in this repository:

In this section we give more details about all the files of this repository.

##### README.md

Is this file, in it there is the instructions of how to install and run this component.

##### requirements.txt:      

Is the file with all the dependencies appear. It contains information about the libraries and the versions that this component needs. It is similar to this:

```
json==2.6.0
utm==0.4.2
requests==2.18.4
lxml==4.2.1
pandas==0.22.0
```
   
##### Config files:   

There are two different config files that are used in the scripts that are in this folder:

*	config.py: It is used in the scripts "bridge_Madrid_CB_CedusV2.py" and "CalculateByIntervals.py" to configure main data.
*	configAverageScript.py: It is used in the CalculateByIntervals.py


##### utilsBridge:   

In the "utilsBridge" folder are located the shared libraries between scripts:

*	holidays.py: Is the file that helps us to recover the list of holidays in Madrid (autonomous community). The original file was downloaded from GitHub (https://github.com/dr-prodigy/python-holidays)
*	extraDaysToProceesAsHolidaysInMadrid.py: Is the file where we can add extra holidays days for the city. By default the days that are included in this file are "San Isidro" (15th of May) and "La Almudena" (9th of November). If you need to add more days you can edit the file "extraDaysToProceesAsHolidaysInMadrid.py" and add the day like the following example:


``` 
extraDaysToProcessAsHolidays.append({"day":"Month-Day","name":"Name"})
```

Where:
* "Month" is a value between 1 and 12
* "Day" is a value between 1 and 31
* "Name" is the label of the day

A real example:

``` 
extraDaysToProcessAsHolidays.append({"day":"5-15","name":"San Isidro"})
extraDaysToProcessAsHolidays.append({"day":"11-9","name":"Almudena"})
``` 


##### bridge_Madrid_CB_CedusV2.py:

To execute the script (Remember to use Python 3):
```
python bridge_Madrid_CB_CedusV2.py 
```

*	This script reads the data of PMs from "http://informo.munimadrid.es/informo/tmadrid/pm.xml" (This URL is a parameter in the "config.py" file) and sends data to the Orion Context Broker. 
*	This script should be executed by a cron task every 5', since real-time data is updated every 5 minutes.
*	This script uses a list of JSON files (the path "folderExportPath" must be configured into the "configAverageScript.py" file) to recover the historical avg values of intensity, occupancy and load per PM. 
*	This script also uses a csv file (the path must be configured into the "config.py" file, CSV_FILE parameter) to recover the intensidadSat per PM. The script discards the rows of the CSV file where the value of the intensidadSat <=0 (in the csv file this value is the third column). If the value is not in the CSV file, the script uses the filed "intensidadSat" recovered from the xml (if it exists).

This script converts the data to the TrafficFlowObserved FIWARE data model (https://fiware-datamodels.readthedocs.io/en/latest/Transportation/TrafficFlowObserved/doc/spec/index.html).
The TrafficFlowObserved datamodel is extended to accommodate the following additional fields:


* error: This value is linked with the value "error" that appears in Madrid´s XML file.
	- Attribute type: Text. Possible values "Y" and "N"
	- Optional
* intensityHistorical: Average number of vehicles detected from the calculateYearAverage.py script
	- Attribute type: Number. Positive integer.
	- Optional
* occupancyHistorical: Average number of occupancy detected from the calculateYearAverage.py script
	- Attribute type: Number. Positive integer.
	- Optional
* loadHistorical: Average number of load detected from the calculateYearAverage.py script
	- Attribute type: Number. Positive integer.
	- Optional
* isHoliday: Flag used to identify if the data refer to a holiday in Madrid (Sunday is also considered as holiday)
	- Attribute type: Boolean
	- Optional

##### bridge_Madrid_CB_Cedus_Incidencias.py:   

To execute this script:
```
python bridge_Madrid_CB_Cedus_Incidencias.py 
```

This scripts reads data of Incidents from "http://informo.munimadrid.es/informo/tmadrid/incid_aytomadrid.xml" (Is the  MADRID_INCIDENCIAS_ENDPOINT parameter in the "config.py" file) and sends the data to the Context Broker. The script should be executed by a cron task every 5'.
The script converts the data to the PointOfInterest FIWARE data model before sending it to the Contect Broker (https://fiware-datamodels.readthedocs.io/en/latest/PointOfInterest/PointOfInterest/doc/spec/index.html )
The model is extended with the following additional fields:



* lastDateInProvider: Date field used to control if the incident is old
	- Attribute type: DateTime
	- Optional
* cod_tipo_incidencia: used to send the field cod_tipo_incidencia that we can find in the XML
	- Attribute type: Text. Possible values "Y" and "N"
	- Optional
* nom_tipo_incidencia: used to send the field nom_tipo_incidencia that we can find in the XML
	- Attribute type: Text.
	- Optional
* fh_inicio: used to send the field fh_inicio that we can find in the XML
	- Attribute type: DateTime
	- Optional
* fh_final: used to send the field fh_final that we can find in the XML
	- Attribute type: DateTime
	- Optional
* incid_prevista: used to send the field incid_prevista that we can find in the XML
	- Attribute type: Text.
	- Optional
* incid_planificada: used to send the field incid_planificada that we can find in the XML
	- Attribute type: Text.
	- Optional
* incid_estado: used to send the field incid_estado that we can find in the XML
	- Attribute type: Text.
	- Optional
* tipoincid: used to send the field tipoincid that we can find in the XML
	- Attribute type: Text.
	- Optional
* es_obras: used to send the field es_obras that we can find in the XML
	- Attribute type: Boolean
	- Optional
* es_accidente: used to send the field es_accidente that we can find in the XML
	- Attribute type: Boolean
	- Optional

##### CalculateByIntervals.py:    


This script processes all traffic intensity data generated in the previous year and creates a list of JSON files with the average data of each PM. Uses the list of .csv files stored in the folder "folderWithCSVs" (configurable in the file "configAverageScript.py").
The script generates as outcome 42 JSON files, one per date interval (these periods are defined in the "configAverageScript.py") and per type of day ('Holiday', 'Monday', 'Tuesday' ,'Wednesday', 'Thursday', 'Friday', 'Saturday').
The date intervals are parametrised as follows:
   
```
datesIntervals = [
                  {'startDate': '01/01/2017', 'endDate': '14/01/2017'},
                  {'startDate': '15/01/2017', 'endDate': '30/06/2017'},
                  {'startDate': '01/07/2017', 'endDate': '31/07/2017'},
                  {'startDate': '01/08/2017', 'endDate': '31/08/2017'},
                  {'startDate': '01/09/2017', 'endDate': '15/12/2017'},
                  {'startDate': '16/12/2017', 'endDate': '31/12/2017'}
                  ]
```                      

- Input:  

The .csv files that the script uses to generate average data must be downloaded from https://datos.madrid.es/sites/v/index.jsp?vgnextoid=33cb30c367e78410VgnVCM1000000b205a0aRCRD&vgnextchannel=374512b9ace9f310VgnVCM100000171f5a0aRCRD
There are two possible headers in the CSV that the script can handle:


```  
"id";"fecha";"tipo_elem";"intensidad";"ocupacion";"carga";"vmed";"error";"periodo_integracion"
```  

or   

```  
"idelem";"fecha";"identif";"tipo_elem";"intensidad";"ocupacion";"carga";"vmed";"error";"periodo_integracion"
```  

the CSVs can use "," or ";" as separator.   
 

- Output: 

Name:
The name of the output files follows this pattern "csv_"+interval_start_date+"-"+interval_end_date+"-"+type_of_day".json".
Example: "csv_01-09-2017_15-12-2017_Friday.json"
Data:
The JSON format of the output files is:
 

```
	{
		"hour": {
			"quarter0":{"id_pm":{"intensity":{"total":0, "cnt":0}, "ocupancy":{"total":0, "cnt":0}, "load":{"total":0, "cnt":0}}},
			"quarter1":{"id_pm":{"intensity":{"total":0, "cnt":0}, "ocupancy":{"total":0, "cnt":0}, "load":{"total":0, "cnt":0}}},
			"quarter2":{"id_pm":{"intensity":{"total":0, "cnt":0}, "ocupancy":{"total":0, "cnt":0}, "load":{"total":0, "cnt":0}}},
			"quarter3":{"id_pm":{"intensity":{"total":0, "cnt":0}, "ocupancy":{"total":0, "cnt":0}, "load":{"total":0, "cnt":0}}}
		},
		...
```

Where:   
* hour: is a value between 0 and 23
* quarter0 = 0 -> minutes between 0 - 14
* quarter1 = 1 -> minutes between 15 - 29
* quarter2 = 2 -> minutes between 29 - 44
* quarter3 = 3 -> minutes between 45 - 59
* id_pm: is the id of a PM 
* intensity: It has the sum of all the intensity values in that period in "total", and the number of times the value has been found in that interval in "cnt". To get the average: avg = "total" / "cnt"
* occupancy: It has the sum of all the occupancy values in that period in "total", and the number of times the value has been found in that interval in "cnt". To get the average: avg = "total" / "cnt"
* load: It has the sum of all the load values in that period in "total", and the number of times the value has been found in that interval in "cnt". To get the average: avg =  "total" / "cnt"

Example:   
   		
```
		{"0": 
			{"0": 
					{"6141": 
						{
						"intensity": {"cnt": 8, "total": 203.0}, 
						"ocupancy": {"cnt": 8, "total": 3.0}, 
						"load": {"cnt": 8, "total": 15.0}
						}, 
					"7011": 
						{"intensity": {"cnt": 9, "total": 1330.0},
						"ocupancy": {"cnt": 9, "total": 4.0}, 
						"load": {"cnt": 9, "total": 12.0}
						}
					}
			},
			{"1": 
					{"6141": 
						{
						"intensity": {"cnt": 7, "total": 223.0}, 
						"ocupancy": {"cnt": 7, "total": 4.0}, 
						"load": {"cnt": 7, "total": 18.0}
						}, 
					"7011": 
						{"intensity": {"cnt": 6, "total": 1030.0},
						"ocupancy": {"cnt": 6, "total": 2.0}, 
						"load": {"cnt": 6, "total": 17.0}
						}
					}
			},			
			{"2": 
					{"6141": 
						{
						"intensity": {"cnt": 12, "total": 263.0}, 
						"ocupancy": {"cnt": 12, "total": 7.0}, 
						"load": {"cnt": 12, "total": 12.0}
						}, 
					"7011": 
						{"intensity": {"cnt": 14, "total": 1621.0},
						"ocupancy": {"cnt": 14, "total": 2.0}, 
						"load": {"cnt": 14, "total": 18.0}
						}
					}
			},			
			{"3": 
				{"6141": 
					{
					"intensity": {"cnt": 10, "total": 303.0}, 
					"ocupancy": {"cnt": 10, "total": 0.0}, 
					"load": {"cnt": 10, "total": 13.0}
					}, 
				"7011": 
					{"intensity": {"cnt": 11, "total": 1620.0},
					"ocupancy": {"cnt": 11, "total": 3.0}, 
					"load": {"cnt": 11, "total": 15.0}
					}
				}
			},
			...
		}
	
```


This script needs to be executed once per year when the .csv of the previous year (12) will be published and downloaded into the folder "folderWithCSVs" (this is a configurable parameter in the config files).


##### deleteEnititiesCB.py:    

To execute this script (Remember to use Python 3):
```
python deleteEnititiesCB.py 
```

If you need to clean the PM entities in the Orion Context Broker, you can execute this script.

##### deleteIncidenciasCB.py:    

To execute this script (Remember to use Python 3):
```
python deleteIncidenciasCB.py 
```
If you need to clean the incidents entities in the Orion Context Broker, you can execute this script.


### 2.3 Install dependencies

To install the dependencies you need to execute (Remember to use Python 3):      
    
```
pip install -r requirements.txt
```

### 2.4 Check config files

Edit the files "config.py" and "configAverageScript.py" and config them according your needs.

#### 2.4.1 config.py

The main parameters of this config file are:

* MADRID_PM_ENDPOINT: Is the URL of the end point where Madrid publish the PM data.
* MADRID_INCIDENCIAS_ENDPOINT: Is the URL of the end point where Madrid publish the traffic incidents data.
* CB_URL_TO_SEND_DATA: Is the URL of the CB where the scripts are going to send data.
* CB_URL_TO_SEND_DATA_BATCH_OPERATIONS: Is the URL of the CB where the scripts are going to send data using the batch functionality.
* CB_FIWARE_SERVICE: Is the service that the scripts are going to use to send dat to the Orion Context Broker.
* CB_FIWARE_SERVICEPATH: Is the service path that the scripts are going to use to send data to the Orion Context Broker.
* folderExportPath: Is the path where the script will find the average values of the PMs.
* fileInstenidadSat: Is the path to an external CSV file that the scripts uses to recover the intensity of saturation of a PM.
* logFileBridge: Is the path where the script "bridge_Madrid_CB_CedusV2.py" will create their log.
* logFileIncidencias: Is the path where the script "bridge_Madrid_CB_Cedus_Incidencias.py" will create their log.

#### 2.4.2 configAverageScript.py

The main parameters of this config file are:

* averageLogFile: Is the paths to logs files
* folderExportPath: Path to the folder wher the script will create the output of the average data.
* folderWithCSVs: Is the folder wher we have all the CSV files that we wish to use to calculate the average. The csv must be downloaded from # https://datos.madrid.es/sites/v/index.jsp?vgnextoid=33cb30c367e78410VgnVCM1000000b205a0aRCRD&vgnextchannel=374512b9ace9f310VgnVCM100000171f5a0aRCRD
* datesIntervals: Is an array with the intervals that we wish to use to group data. 


### 2.5 Generate historical data

To generate previous year´s averages data we must execute the script "CalculateByIntervals.py" but before that you must read carefully the information detailed previously in this document about this file.
Remember that before executing this script you must: 

*	Copy all the CSVs files that you wish to use (typically 12, one per month) to generate the average data in the folder that the "configAverageScript.py" has configured into the "folderWithCSVs" variable.
-	Revise the date intervals within configAverageScript.py, “datesIntervals” variable.

To execute this script you need to execute (Remember to use Python 3):

```
python CalculateByIntervals.py 
```

As the CSV files can be very heavy (more than 10.000.000 of lines each one) this script can run few hours. If you need to execute the script in the background you can do it with the following command line:

```
nohup python CalculateByIntervals.py & 
```

### 2.6 Create a cron task for bridge_Madrid_CB_CedusV2.py

This script reads data from PMs and send it to an Orion Context Broker. As Madrid publish this data every 5 minutes we need to create a cron task in the server that execute this script in a similar periodicity.

To edit the cron you must execute 
```
crontab -e
```

the cron the line you must add is something like this:

```
 */6 * * * * python /path_to_file/bridge_Madrid_CB_CedusV2.py
```

If you need to use the Python 3 virtual environment, you can create the cron task like this

```
 */6 * * * * /python3_virtual_environment_path/python /path_to_file/bridge_Madrid_CB_CedusV2.py
```


### 2.7 Create a cron task for bridge_Madrid_CB_Cedus_Incidencias.py
   

This script reads data from traffic incidents and send it to an Orion Context Broker. As Madrid publish this data every 5 minutes we need to create a cron task in the server that execute this script in a similar periodicity.

To edit the cron you must execute 
```
crontab -e
```

the cron the line you must add is something like this:

```
 */6 * * * * python /path_to_file/bridge_Madrid_CB_Cedus_Incidencias.py
```

If you need to use the Python 3 virtual environment, you can create the cron task like this

```
 */6 * * * * /python3_virtual_environment/python /path_to_file/bridge_Madrid_CB_Cedus_Incidencias.py
```

### 2.8 Validation 

To validate that both scripts works "bridge_Madrid_CB_CedusV2.py" and "bridge_Madrid_CB_Cedus_Incidencias.py" you can use any REST client and do the following queries:

To validate bridge_Madrid_CB_CedusV2.py:   

- Method: GET
- Headers:
	- Fiware-Service: The same value you use in the config file
	- Fiware-ServicePath: The same value you use in the config file
- URL: http://OrionContextBroker_IP:OrionContextBroker_PORT/v2/entities?type=TrafficFlowObserved

To validate bridge_Madrid_CB_Cedus_Incidencias.py:   

- Method: GET
- Headers:
	- Fiware-Service: The same value you use in the config file
	- Fiware-ServicePath: The same value you use in the config file
- URL: http://OrionContextBroker_IP:OrionContextBroker_PORT/v2/entities?type=PointOfInterest

