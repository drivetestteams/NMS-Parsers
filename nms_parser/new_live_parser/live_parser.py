import pandas as pd
import pytz
from tzlocal import get_localzone
from sqlalchemy import create_engine
from sqlalchemy.types import Float, DateTime, NVARCHAR, Date, Time
import urllib
from datetime import datetime
from ast import literal_eval


def is_valid_date(d):
    try:
        datetime.strptime(d, "%m/%d/%Y")
        return True
    except:
        return False

print("\n\n===RUNNING: 4SKELION LIVE PARSER ")

csv_path = "C:\\Program Files (x86)\\APP TELEMETRY\\Telemetry.csv"
dateparse = lambda x: [datetime.strptime(d, '%m/%d/%Y').date() for d in x]
df = pd.read_csv(csv_path, delimiter='$', parse_dates=['DATE'])
try:
    df = df.drop(columns=['Unnamed: 170'])  # In case of appearance in the CSV
except:
    print("No column Unnamed was found")
df['DATETIME'] = None                       # Create new column DATETIME 
df.head()                                   # Print headers

# Filter out rows where 'NAME' column is blank or 'Ship name HERE' and BLANK DATE and TIME Columns
df = df.dropna(subset=['DATE'])
df = df.dropna(subset=['TIME'])
df = df[df['NAME'] != 0]
df = df[df['NAME'] != 'Ship name HERE']
df = df[df['NAME'] != 'DEMO ROTATING ANTENNA']
# Filter out rows where SERIAL where length != 8
df = df[df['SERIAL'].str.len() == 8]

# Check If LAT,LOT is 0. If yes, replace zero datetime with current Datetime
if 0 in df['LATITUDE'].values:
    indices = df.index[df['LATITUDE'] == 0].tolist()
    indices += df.index[df['LONGITUDE'] == 0].tolist()
    for index in indices:
        current_time = datetime.now(pytz.utc)
        formatted_time = current_time.strftime("%m/%d/%Y %H:%M:%S")
        date_component, time_component = formatted_time.split()
        df.at[index, 'DATE'] = date_component
        df.at[index, 'TIME'] = time_component
        df.at[index, 'DATETIME'] = formatted_time


# Exclude rows where 'ColumnName' starts with 'FAS'
df = df[~df['SERIAL'].str.startswith('FAS')]

# Exclude rows where 'ColumnName' starts with 'PG'
df = df[~df['SERIAL'].str.startswith('PG')]

# Exclude rows where 'SERIAL' starts with 'x'
df = df[~df['SERIAL'].str.startswith('x')]

# Exclude rows where 'SERIAL' starts with 'CMT'
df = df[~df['SERIAL'].str.startswith('CMT')]

#-----------------------------# START #### Exclude particular serial from importing ------------------------------#
df = df[df['SERIAL'] != '4GC00146']
#-----------------------------# END #### Exclude particular serial from importing ------------------------------#

# First , we convert the column to datetime type and then isolate date
# df['DATE'] = pd.to_datetime(df['DATE'])
# df['DATE'] = df['DATE'].dt.date

df = df[df['DATE'].apply(is_valid_date)]  # remove rows with bad dates
invalid_dates = df[~df['DATE'].apply(is_valid_date)]
if not invalid_dates.empty:
    print("Found invalid DATE entries:")
    print(invalid_dates['DATE'].unique())
df['DATE'] = pd.to_datetime(df['DATE'], format="%m/%d/%Y")  # now it's safe to parse
df['DATE'] = df['DATE'].dt.date

# First , we convert the column to datetime type and then isolate time
df['TIME'] = pd.to_datetime(df['TIME'],format="%H:%M:%S")
df['TIME'] = df['TIME'].dt.time
# Also we keep a datetime columnm with the 2 individual columns combined
df['DATETIME'] = pd.to_datetime(df['DATE'].astype(str) + ' ' + df['TIME'].astype(str))

# replace all values of BEST_RSRP == -150.00 with NULL
df['BESTS.RSRP'].replace(-150, pd.NA, inplace=True)
# Replace 'BEST.SNR' if 'BESTS.RSRP' is null
df.loc[df['BESTS.RSRP'].isna(), 'BESTS.SNR'] = pd.NA
df['S0.RSRP'].replace(-150, pd.NA, inplace=True)
df.loc[df['S0.RSRP'].isna(), 'SECT0.SNR'] = pd.NA
df['S1.RSRP'].replace(-150, pd.NA, inplace=True)
df.loc[df['S1.RSRP'].isna(), 'SECT1.SNR'] = pd.NA
df['S2.RSRP'].replace(-150, pd.NA, inplace=True)
df.loc[df['S2.RSRP'].isna(), 'SECT2.SNR'] = pd.NA
# Check if the column exists
column_name = 'S3.RSRP'
if column_name in df.columns:
    df['S3.RSRP'].replace(-150, pd.NA, inplace=True)
    df.loc[df['S3.RSRP'].isna(), 'SECT3.SNR'] = pd.NA
    
df['BESTS.RSRP'] = pd.to_numeric(df['BESTS.RSRP'], errors='coerce')

# Extract the last two characters, append '0x' to each, then convert to decimal
hex_sec = '0x' + df['BESTS.CID'].str[-2:]

# Convert the hexadecimal string to decimal for each entry in the Series
dec_sec = hex_sec.apply(lambda x: int(x, 16))
df['SECTORID'] = dec_sec

# df['S0.RSRP'] = df['S0.RSRP'].astype(int)

# ---------------------------------------------ERROR HANDLING--------------------------------------------------------------
# Convert 'DATE' column to datetime and handle errors
# try:
#     df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')  # Convert invalid dates to NaT
#     df['DATE'] = df['DATE'].dt.date
# except Exception as e:
#     print(f"Error converting 'DATE' column: {e}")

# # Convert 'TIME' column to datetime and handle errors
# try:
#     df['TIME'] = pd.to_datetime(df['TIME'], format="%H:%M:%S", errors='coerce').dt.time  # Convert invalid times to NaT
# except Exception as e:
#     print(f"Error converting 'TIME' column: {e}")

# # Combine 'DATE' and 'TIME' into a new 'DATETIME' column, only if neither is NaT
# df['DATETIME'] = df.apply(
#     lambda row: pd.to_datetime(f"{row['DATE']} {row['TIME']}", errors='coerce') if pd.notna(row['DATE']) and pd.notna(row['TIME']) else pd.NaT,
#     axis=1
# )
# ----------------------------------------------------------------------------------------------------------------------------

# ---------------------------------------------ALARM EXPORT--------------------------------------------------------------
# Get the current datetime
now = pd.Timestamp.now()
# Compute the time difference and add it as a new column
df['time_diff'] = now - df['DATETIME']


# ===============================================UTC TO NTP TIMES===============================================================

# Get the current UTC time
utc_time = df['DATETIME']

# Make the UTC time timezone-aware (convert to UTC)
utc_time = utc_time.apply(lambda x: pytz.utc.localize(x))

# Detect the local time zone of the client system
local_timezone = get_localzone()

# Remove time zone information and microseconds from the datetime
local_time_without_tz = utc_time.apply(lambda x: x.astimezone(local_timezone).replace(tzinfo=None, microsecond=0))

df['DATETIME'] = local_time_without_tz
df['DATE'] = local_time_without_tz.apply(lambda x: x.date())   # Extract Date
df['TIME'] = local_time_without_tz.apply(lambda x: x.time())   # Extract Time

alarm_df = df[(df['time_diff'] >= pd.Timedelta(hours=12)) | (df['LATITUDE'] == 0) | (df['LONGITUDE'] == 0) | (df['BESTS.SNR'] <= -2) | (df['BESTS.RSRP'] <= -120) | (df['BESTS.TEMP.'] >= 75)]
alarm_df['COMM. ALARM'] = None # COMMUNICATION ALARM
alarm_df['GPS ALARM'] = None   # GPS ALARM
alarm_df['RSRP ALARM'] = None  # RSRP ALARM
alarm_df['SINR ALARM'] = None  # SINR ALARM
alarm_df['TEMP ALARM'] = None  # TEMPERATURE ALARM
columns_to_keep = ['SERIAL','NAME','DATETIME','time_diff','BESTS.RSRP', 'BESTS.RSRQ', 'BESTS.SNR','BESTS.TEMP.','S0.RSRP','S0.RSRQ','SECT0.SNR','S1.RSRP','S1.RSRQ','SECT1.SNR','S2.RSRP','S2.RSRQ','SECT2.SNR','S3.RSRP','S3.RSRQ','SECT3.SNR','COMM. ALARM','GPS ALARM','RSRP ALARM','SINR ALARM','TEMP ALARM']

# Alarm columns setup
if not alarm_df.empty:
    alarm_df['COMM. ALARM'] = (alarm_df['time_diff'] >= pd.Timedelta(hours=12)).astype(int)             # COMMUNICATION ALARM
    alarm_df['GPS ALARM'] = ((alarm_df['LATITUDE'] == 0) | (alarm_df['LONGITUDE'] == 0)).astype(int)    # GPS ALARM
    alarm_df['RSRP ALARM'] = (alarm_df['BESTS.RSRP'] <= -120).astype(int)                               # RSRP ALARM
    alarm_df['SINR ALARM'] = (alarm_df['BESTS.SNR'] <= -2).astype(int)                                  # SINR ALARM
    alarm_df['TEMP ALARM'] = (alarm_df['BESTS.TEMP.'] >= 75).astype(int)                                # TEMPERATURE ALARM
else:
    print("Empty DataFrame")
    print("No Alarms Detected")
    
alarm_df = alarm_df[columns_to_keep]
df = df.drop(columns=['time_diff'])
print(df.iloc[:, :8])
print("\n======================================ALARMS======================================\n")
print(alarm_df.iloc[:, :])
alarm_df.to_csv("C:\\inetpub\\wwwroot\\Platform\\NMS_ALARMS.csv", sep='$', encoding='utf-8', index=False, header=True)
# ----------------------------------------------------------------------------------------------------------------------------

# Insert Data with correct types into SQL Database
columns_live_to_keep = ['SERIAL','NAME','LATITUDE','LONGITUDE','HEADING','SPEED','DATE','TIME','SCAN#','S0.SECT#','S0.MCC','S0.MNC','S0.ISREG.','S0.EARFCN','S0.BAND','S0.CID','S0.NODEB_dec','S0.CID_dec','S0.TAC','S0.PCI','S0.RSRP','S0.RSRQ','SECT0.SNR','S1.SECT#','S1.MCC','S1.MNC','S1.ISREG.','S1.EARFCN','S1.BAND','S1.CID','S1.NODEB_dec','S1.CID_dec','S1.TAC','S1.PCI','S1.RSRP','S1.RSRQ','SECT1.SNR','S2.SECT#','S2.MCC','S2.MNC','S2.ISREG.','S2.EARFCN','S2.BAND','S2.CID','S2.NODEB_dec','S2.CID_dec','S2.TAC','S2.PCI','S2.RSRP','S2.RSRQ','SECT2.SNR','S3.SECT#','S3.MCC','S3.MNC','S3.ISREG.','S3.EARFCN','S3.BAND','S3.CID','S3.NODEB_dec','S3.CID_dec','S3.TAC','S3.PCI','S3.RSRP','S3.RSRQ','SECT3.SNR']
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};SERVER=win-45ntjeb05tt\sqlexpress;DATABASE=3skelion;UID=admin;PWD=fasmetrics")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
export_df=df[columns_live_to_keep]
export_df.to_csv("C:\\inetpub\\wwwroot\\Platform\\NMS_LIVEVIEW.csv", sep='$', encoding='utf-8', index=False, header=True)
df.to_sql('LiveSheet$', con=engine, if_exists='replace', index=False, dtype={
                                                                            'SERIAL': NVARCHAR,
                                                                            'NAME': NVARCHAR,
                                                                            'LATITUDE': Float,
                                                                            'LONGITUDE': Float,
                                                                            'HEADING': Float,
                                                                            'SPEED': Float,
                                                                            'DATE': Date,
                                                                            'TIME': Time,
                                                                            'SCAN#': Float,
                                                                            'GEOLOCK.INLOCK': Float,
                                                                            'GEOLOCK.ID': NVARCHAR,
                                                                            'GEOLOCK_LAT': Float,
                                                                            'GEOLOCK_LONG': Float,
                                                                            'CIDLOCK.INLOCK': Float,
                                                                            'CIDLOCK.ID': NVARCHAR,
                                                                            'CIDLOCK_LAT': Float,
                                                                            'CIDLOCK_LONG': Float,
                                                                            'NBCIDLOCK.INLOCK': Float,
                                                                            'NBCIDLOCK.ID': NVARCHAR,
                                                                            'NBCIDLOCK_LAT': Float,
                                                                            'NBCIDLOCK_LONG': Float,
                                                                            'DNR1.ENABLED': Float,
                                                                            'DNR1.TYPE': NVARCHAR,
                                                                            'DNR1.CTRL_BY': NVARCHAR,
                                                                            'DNR1.-3dBAz': Float,
                                                                            'DNR1.ACT_SECTOR': Float,
                                                                            'DNR1.OFFSET': Float,
                                                                            'DNR1.AZIMUTH': Float,
                                                                            'DNR2.ENABLED': Float,
                                                                            'DNR2.TYPE': NVARCHAR,
                                                                            'DNR2.CTRL_BY': NVARCHAR,
                                                                            'DNR2.-3dBAz': Float,
                                                                            'DNR2.ACT_SECTOR': Float,
                                                                            'DNR2.OFFSET': Float,
                                                                            'DNR2.AZIMUTH': Float,
                                                                            'BESTS.SECT#': Float,
                                                                            'BESTS.MCC': Float,
                                                                            'BESTS.MNC': Float,
                                                                            'BESTS.ISREG': Float,
                                                                            'BESTS.RAT': Float,
                                                                            'BESTS.EARFCN': Float,
                                                                            'BESTS.BAND': Float,
                                                                            'BESTS.CID': NVARCHAR,
                                                                            'BESTS_NODEB_dec': Float,
                                                                            'BESTS.CID_dec': NVARCHAR,
                                                                            'BESTS.TAC': NVARCHAR,
                                                                            'BESTS.PCI': Float,
                                                                            'BESTS.BW': Float,
                                                                            'BESTS.RSRP': Float,
                                                                            'BESTS.RSRQ': Float,
                                                                            'BESTS.SNR': Float,
                                                                            'BESTS.RTT': Float,
                                                                            'BESTS.HTTP(KBPS)': Float,
                                                                            'BESTS.PCI#': Float,
                                                                            'BESTS.SCORE': Float,
                                                                            'BESTS.OFFSET': Float,
                                                                            'BESTS.AZIMUTH': Float,
                                                                            'BESTS.NBCID': NVARCHAR,
                                                                            'BESTS.NB_NODEB_dec': Float,
                                                                            'BESTS.NB_CID_dec': NVARCHAR,
                                                                            'BESTS.NBCIDRSRP': Float,
                                                                            'BESTS.TEMP.': Float,
                                                                            'S0.SECT#':NVARCHAR,
                                                                            'S0.MCC':Float,
                                                                            'S0.MNC':Float,
                                                                            'S0.ISREG.':Float,
                                                                            'S0.RAT':Float,
                                                                            'S0.EARFCN':Float,
                                                                            'S0.BAND':Float,
                                                                            'S0.CID':NVARCHAR,
                                                                            'S0.NODEB_dec':Float,
                                                                            'S0.CID_dec':NVARCHAR,
                                                                            'S0.TAC':NVARCHAR,
                                                                            'S0.PCI': Float,
                                                                            'S0.BW': Float,
                                                                            'S0.RSRP': Float,
                                                                            'S0.RSRQ': Float,
                                                                            'SECT0.SNR': Float,
                                                                            'S0.RTT': Float,
                                                                            'S0.HTTP(KBPS)': Float,
                                                                            'S0.PCI#': Float,
                                                                            'S0.OFFSET': Float,
                                                                            'S0.AZIMUTH': Float,
                                                                            'S0.NBCID': NVARCHAR,
                                                                            'S0.NB_NODEB_dec': Float,
                                                                            'S0.NB_CID_dec': NVARCHAR,
                                                                            'S0.NBCIDRSRP': Float,
                                                                            'S0.TEMP': Float,
                                                                            'S1.SECT#':Float,
                                                                            'S1.MCC':Float,
                                                                            'S1.MNC':Float,
                                                                            'S1.ISREG.':Float,
                                                                            'S1.RAT':Float,
                                                                            'S1.EARFCN':Float,
                                                                            'S1.BAND':Float,
                                                                            'S1.CID':NVARCHAR,
                                                                            'S1.NODEB_dec':Float,
                                                                            'S1.CID_dec':NVARCHAR,
                                                                            'S1.TAC':NVARCHAR,
                                                                            'S1.PCI': Float,
                                                                            'S1.BW': Float,
                                                                            'S1.RSRP': Float,
                                                                            'S1.RSRQ': Float,
                                                                            'SECT1.SNR': Float,
                                                                            'S1.RTT': Float,
                                                                            'S1.HTTP(KBPS)': Float,
                                                                            'S1.PCI#': Float,
                                                                            'S1.OFFSET': Float,
                                                                            'S1.AZIMUTH': Float,
                                                                            'S1.NBCID': NVARCHAR,
                                                                            'S1.NB_NODEB_dec': Float,
                                                                            'S1.NB_CID_dec': NVARCHAR,
                                                                            'S1.NBCIDRSRP': Float,
                                                                            'S1.TEMP': Float,
                                                                            'S2.SECT#':NVARCHAR,
                                                                            'S2.MCC':Float,
                                                                            'S2.MNC':Float,
                                                                            'S2.ISREG.':Float,
                                                                            'S2.RAT':Float,
                                                                            'S2.EARFCN':Float,
                                                                            'S2.BAND':Float,
                                                                            'S2.CID':NVARCHAR,
                                                                            'S2.NODEB_dec':Float,
                                                                            'S2.CID_dec':NVARCHAR,
                                                                            'S2.TAC':NVARCHAR,
                                                                            'S2.PCI': Float,
                                                                            'S2.BW': Float,
                                                                            'S2.RSRP': Float,
                                                                            'S2.RSRQ': Float,
                                                                            'SECT2.SNR': Float,
                                                                            'S2.RTT': Float,
                                                                            'S2.HTTP(KBPS)': Float,
                                                                            'S2.PCI#': Float,
                                                                            'S2.OFFSET': Float,
                                                                            'S2.AZIMUTH': Float,
                                                                            'S2.NBCID': NVARCHAR,
                                                                            'S2.NB_NODEB_dec': Float,
                                                                            'S2.NB_CID_dec': NVARCHAR,
                                                                            'S2.NBCIDRSRP': Float,
                                                                            'S2.TEMP': Float,
                                                                            'S3.SECT#':NVARCHAR,
                                                                            'S3.MCC':Float,
                                                                            'S3.MNC':Float,
                                                                            'S3.ISREG.':Float,
                                                                            'S3.RAT':Float,
                                                                            'S3.EARFCN':Float,
                                                                            'S3.BAND':Float,
                                                                            'S3.CID':NVARCHAR,
                                                                            'S3.NODEB_dec':Float,
                                                                            'S3.CID_dec':NVARCHAR,
                                                                            'S3.TAC':NVARCHAR,
                                                                            'S3.PCI': Float,
                                                                            'S3.BW': Float,
                                                                            'S3.RSRP': Float,
                                                                            'S3.RSRQ': Float,
                                                                            'SECT3.SNR': Float,
                                                                            'S3.RTT': Float,
                                                                            'S3.HTTP(KBPS)': Float,
                                                                            'S3.PCI#': Float,
                                                                            'S3.OFFSET': Float,
                                                                            'S3.AZIMUTH': Float,
                                                                            'S3.NBCID': NVARCHAR,
                                                                            'S3.NB_NODEB_dec': Float,
                                                                            'S3.NB_CID_dec': NVARCHAR,
                                                                            'S3.NBCIDRSRP': Float,
                                                                            'S3.TEMP': Float,
                                                                            'DATETIME': DateTime,
                                                                            'SECTORID': Float
                                                                             })                                                              
print("\n IMPORT IS DONE \n")
