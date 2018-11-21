from datetime import datetime as dt
import utilsBridge.holidays as holidays

extraDaysToProcessAsHolidays=[]

#example for San Isidro: {"day":"5-15","name":"San Isidro"}
#day => format MM-DD (without year)
#name => name of the day
extraDaysToProcessAsHolidays.append({"day":"5-15","name":"San Isidro"})
extraDaysToProcessAsHolidays.append({"day":"11-9","name":"Almudena"})

esp_holidays = holidays.Spain(prov='MAD')


#adding custom holidays
currentyear = dt.today().year
preivousyear = currentyear - 1

for customHolidayDay in extraDaysToProcessAsHolidays:
    esp_holidays.append({str(currentyear)+"-"+str(customHolidayDay["day"]): str(customHolidayDay["name"])+": "+str(currentyear)})
    esp_holidays.append({str(preivousyear)+"-"+str(customHolidayDay["day"]): str(customHolidayDay["name"])+": "+str(preivousyear)})
 
 
#print (esp_holidays)