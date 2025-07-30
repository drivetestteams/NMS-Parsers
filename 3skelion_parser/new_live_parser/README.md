# The new live parser - live_parser.py
# RESUME
  
This parser works alongside the telemetry app, being fed from its csv export with an ultimate goal to provide the database with the live instance of the deployed systems. It locates the export's path, opens it, reads it, parses the provided dataframe making essential transformations and finally inserts it into the appropriate db table. Moreover, it exports the alarms from that instance into a new csv file (3SKELION+_ALARMS.csv) which can be manipulated in various ways in the future. It runs periodically every 2 minutes (this is configured from the controller script).

Every process included in this file is logged and exported into equivalent logfiles.

================================ FUNCTIONS ================================

# def is_valid_date(d)
Arguments:
- d : Provided date value
This function simply checks if given date (d) is in the correct format. If yes, returns True, else returns False.


================================ MAIN ================================

In the beginning, the main function of the script reads the Telemetry.csv located in the provided path (default: C:\Program Files (x86)\Telemetry Standalone\Telemetry.csv), and stores its data into a dataframe.
Afterwards, the data parsing process starts. Invalid columns ([Unnamed: 170]) are dropped, as well as empty values (NULL Date/Time, Serial values). 
In case of GPS Error, the Date/Time is fixed to current time and Date to avoid making the whole set of radio information invalid.
Removes Serials starting with specific test/development prefixes like: FAS, PG, CMT, etc., as well as ecluded serials for operational reasons.
Next is the Date/Time validation using the is_valid_date() function. In case of a not valid date value, a transformation is executed to achieve equal form of dates inside the Database.
Invalid radio KPI values (RSRP, SINR) are replaced with NULL.
Also, a new column is created [SECTORID] using values from existing one [BESTS.CID].
Last process to be done - before the db insertion - is the alarm export.
For this purpose, a new column is created to calculate the time difference between the parsing time and the last given measurement info from each system.
Times are transformed from UTC to NTP time through get_localzone(), which gains the time zone from the machine running the whole process.
A series of new columns are created to signal the existence of alarms. These columns are:
- COMM. ALARM -> 0 if last measurement info is within 12 hours, 1 otherwise.
- GPS ALARM -> 0 if last measurement info doesn't come with a GPS Error (Lat,Lon=0), 1 otherwise.
- RSRP ALARM -> 0 if last measurement info doesn't come with an RSRP Alarm (RSRP value lower than given threshold, default is -120dBm), 1 otherwise.
- SINR ALARM -> 0 if last measurement info doesn't come with an SINR Alarm (SINR value lower than given threshold, default is 0dB), 1 otherwise.
- TEMP ALARM -> 0 if last measurement info doesn't come with a Temperature Alarm (Temperature value higher than 75 degrees celsius), 1 otherwise.
All these columns along with equivalent KPIs and information is inserted into a new dataframe and written into 3SKELION+_ALARMS.csv.

At the end, we have the db insertion where an engine is created with the following configuration:
mssql+pyodbc:///?odbc_connect=DRIVER={SQL Server Native Client 11.0};SERVER=SERVER NAME;DATABASE=DB NAME;UID=USERNAME;PWD=PASSWORD

** REPLACE SERVER NAME, DB NAME, USERNAME, PASSWORD with own configurations **

Using the above engine template, the script uses the to_sql() function with arguments:
- DB_TABLE_NAME: name of the table where the live data of the systems are stored.
- engine: The engine template above.
- if_exists='replace': replaces already stored data and overwrites them with the new dataframe collected and parsed.
- index=False
- dtype={...}: which contains a detailed mapping set of the dataframe's columns with the correct datatypes, to avoid type differentiation inside the db table.