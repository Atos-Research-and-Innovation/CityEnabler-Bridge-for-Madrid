#config data for calculate year data analytics

#paths to logs files
averageLogFile = 'logs/averagelog.txt'
averageExportFile = 'logs/average_export.json'
folderExportPath = 'logstest'

#is the folder wher we have all the CSV files that we wish to use to calculate the average
#the csv must be downloaded from # https://datos.madrid.es/sites/v/index.jsp?vgnextoid=33cb30c367e78410VgnVCM1000000b205a0aRCRD&vgnextchannel=374512b9ace9f310VgnVCM100000171f5a0aRCRD
folderWithCSVs = 'folder_path_to_csv_file'


#is the list of intervals that we wish to use to split analyticas
datesIntervals = [
                  {'startDate': '01/01/2017', 'endDate': '14/01/2017'},
                  {'startDate': '15/01/2017', 'endDate': '30/06/2017'},
                  {'startDate': '01/07/2017', 'endDate': '31/07/2017'},
                  {'startDate': '01/08/2017', 'endDate': '31/08/2017'},
                  {'startDate': '01/09/2017', 'endDate': '15/12/2017'},
                  {'startDate': '16/12/2017', 'endDate': '31/12/2017'}
                  ]

#list of data that the script will use to get averages
listOfDataToRecoverToCalculateAverages = ['intensity', 'ocupancy', 'load' ]

#CB data

CB_URL_TO_SEND_DATA = 'http://OCB_IP:OCB_PORT/v2/entities'
CB_FIWARE_SERVICE = 'your_fiware_service'
CB_FIWARE_SERVICEPATH = 'your_fiware_service_path'
USE_THREADING = True