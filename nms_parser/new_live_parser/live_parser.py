import pandas as pd
import pytz
from tzlocal import get_localzone
from sqlalchemy import create_engine
from sqlalchemy.types import Float, DateTime, NVARCHAR, Date, Time
import urllib
from datetime import datetime, timedelta
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


###################################################__CODE_FOR_REFRESH_PREV_WITH_LAST_VALID__#####################################################################
def refresh_prev_with_last_valid(df, copy_datetime_on_scan_change=True):
    """
    Update prev1.csv using current telemetry (df).
    If copy_datetime_on_scan_change is True, when a SERIAL exists in prev but SCAN# differs,
    copy the current SCAN#, DATETIME (and DATE/TIME text) from telemetry into prev.
    """
    csv_path1 = r"C:\\Program Files (x86)\\APP TELEMETRY\\prev_csv\\prev.csv"

    # --- Load prev and minimal cleanup ---
    df_prev = pd.read_csv(csv_path1, delimiter="$")

    # Keep only rows that have DATE and TIME (so we can build DATETIME reliably)
    df_prev = df_prev.dropna(subset=["DATE", "TIME"])

    # Build DATETIME in prev
    df_prev["DATETIME"] = pd.to_datetime(
        df_prev["DATE"].astype(str).str.strip() + " " + df_prev["TIME"].astype(str).str.strip(),
        format="%m/%d/%Y %H:%M:%S",
        errors="coerce",
    )

    # Normalize types
    df_prev["SERIAL"] = df_prev["SERIAL"].astype(str).str.strip()
    df_prev["SCAN#"] = pd.to_numeric(df_prev["SCAN#"], errors="coerce")

    df = df.copy()
    df.columns = df.columns.str.strip()
    df["SERIAL"] = df["SERIAL"].astype(str).str.strip()
    df["SCAN#"] = pd.to_numeric(df["SCAN#"], errors="coerce")
    # print(df["DATE"])

    # Ensure df has DATETIME (build it if missing and DATE/TIME exist)
    if "DATETIME" not in df.columns:
        if {"DATE", "TIME"}.issubset(df.columns):
            df["DATETIME"] = pd.to_datetime(
                df["DATE"].astype(str).str.strip() + " " + df["TIME"].astype(str).str.strip(),
                errors="coerce",
            )
        else:
            df["DATETIME"] = pd.NaT

    # If df has multiple rows per SERIAL, keep the last one
    df_last = df.drop_duplicates(subset=["SERIAL"], keep="last")

    # --- Build lookup: SERIAL -> (scan_cur, dt_cur) from current telemetry ---
    lookup = {row["SERIAL"]: (row["SCAN#"], row["DATE"], row["TIME"], row["LATITUDE"], row["LONGITUDE"]) for _, row in df_last.iterrows()}

    # --- For each row in prev: if same SERIAL exists but SCAN# differs, copy SCAN# and DATETIME (and DATE/TIME) ---
    for i in range(len(df_prev)):
        serial = df_prev.at[i, "SERIAL"]
        prev_scan = df_prev.at[i, "SCAN#"]
        if serial in lookup:
            scan_cur, dt_cur ,tm_cur, lat_cur, lon_cur = lookup[serial]

            # Only update when we have a meaningful current scan and it's different from prev
            if pd.notna(scan_cur) and scan_cur != 0 and (pd.isna(prev_scan) or scan_cur != prev_scan):
                # copy scan
                df_prev.at[i, "SCAN#"] = scan_cur
                # print(f"Updated SERIAL {serial}: SCAN# {prev_scan} -> {scan_cur}")
                # copy DATETIME and DATE/TIME textual columns from telemetry if option enabled
           
                if not (lat_cur == 0 and lon_cur == 0):
                    df_prev.at[i, "DATE"] = dt_cur
                    df_prev.at[i, "TIME"] = tm_cur
                    # print(f"Updated SERIAL {serial}: DATE/TIME to {dt_cur} {tm_cur}")
                else :
                    current_datetime_utc = datetime.now(pytz.utc)
                    formatted_time = current_datetime_utc.strftime("%m/%d/%Y %H:%M:%S")
                    date_component, time_component = formatted_time.split()
                    df_prev.at[i, "DATE"] = date_component
                    df_prev.at[i, "TIME"] = time_component
                    # print(f"Updated SERIAL {serial}: DATE/TIME to current {date_component} {time_component}")
            
                   
    # --- Append brand-new SERIALs from df to the end of prev.csv ---
    prev_serials = set(df_prev["SERIAL"])
    df_last = df_last[df_last["SERIAL"].notna()]
    new_serials = set(df_last["SERIAL"]) - prev_serials

    if new_serials:
        df_new = df_last[df_last["SERIAL"].isin(new_serials)].copy()

        # We want to append rows matching prev's column order.
        # Start with the intersection; then add any missing columns that prev expects.
        prev_cols = list(df_prev.columns)

        # If df_new lacks DATE/TIME but has DATETIME, derive them.
        if "DATE" in prev_cols and "DATE" not in df_new.columns:
            if "DATETIME" in df_new.columns:
                df_new["DATE"] = df_new["DATETIME"].dt.strftime("%m/%d/%Y")
            else:
                df_new["DATE"] = np.nan

        if "TIME" in prev_cols and "TIME" not in df_new.columns:
            if "DATETIME" in df_new.columns:
                df_new["TIME"] = df_new["DATETIME"].dt.strftime("%H:%M:%S")
            else:
                df_new["TIME"] = np.nan

        # Ensure all columns prev expects exist (fill missing with NaN)
        for col in prev_cols:
            if col not in df_new.columns:
                df_new[col] = np.nan

        # Keep only prev columns and in the same order
        df_new = df_new[prev_cols]

        # Append to the end
        df_prev = pd.concat([df_prev, df_new], ignore_index=True)

    # --- Save back to prev.csv ---
    df_prev.to_csv(csv_path1, index=False, sep="$")

    # Return updated prev and a simple (SERIAL, SCAN#) from the current df
    serial_scan_df = df[["SERIAL", "SCAN#"]].copy()

    return df_prev, serial_scan_df


# call with default behavior (copy DATETIME when scan changes)
df_prev, serial_scan_df = refresh_prev_with_last_valid(df)


# Check If LAT,LOT is 0. If yes, replace zero datetime with current Datetime
# For each row in df, if SERIAL and SCAN# match a row in df_prev, take DATETIME from df_prev

# Normalize join keys once (prevents rare “looks equal but mismatches”)
df['SERIAL'] = df['SERIAL'].astype(str).str.strip()
df_prev['SERIAL'] = df_prev['SERIAL'].astype(str).str.strip()
df['SCAN#']  = pd.to_numeric(df['SCAN#'], errors='coerce')
df_prev['SCAN#'] = pd.to_numeric(df_prev['SCAN#'], errors='coerce')

serial_scan_to_datetime = df_prev.set_index(['SERIAL', 'SCAN#'])['DATETIME']# krataei tis times tou prev csv

for idx, row in df.iterrows():
    key = (row['SERIAL'], row['SCAN#'])
    if key in serial_scan_to_datetime and (row['LATITUDE'] == 0 or row['LONGITUDE'] == 0):
        prev_datetime = serial_scan_to_datetime[key]
        #print(f"Match found for SERIAL: {row['SERIAL']}, SCAN#: {row['SCAN#']} -> Prev DATETIME: {prev_datetime}")
        if pd.notnull(prev_datetime):
            #print(f"Match found for SERIAL: {row['SERIAL']}, SCAN#: {row['SCAN#']} -> Setting time to previous time")  
            prev_datetime = pd.to_datetime(prev_datetime)
            formatted_time = prev_datetime.strftime("%m/%d/%Y %H:%M:%S")
            date_component, time_component = formatted_time.split()
            df.at[idx, 'DATE'] = date_component
            df.at[idx, 'TIME'] = time_component
            df.at[idx, 'DATETIME'] = formatted_time
    elif (key not in serial_scan_to_datetime) and (row['LATITUDE'] == 0 or row['LONGITUDE'] == 0):
        #print(f"No Match found for SERIAL: {row['SERIAL']}, SCAN#: {row['SCAN#']} -> Setting time to current time")
        current_datetime_utc = datetime.now(pytz.utc)
        formatted_time = current_datetime_utc.strftime("%m/%d/%Y %H:%M:%S")
        date_component, time_component = formatted_time.split()
        df.at[idx, 'DATE'] = date_component
        df.at[idx, 'TIME'] = time_component
        df.at[idx, 'DATETIME'] = formatted_time


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
cid = df['BESTS.CID'].astype(str)
last2 = cid.str[-2:].where(cid.str.len() >= 2)

def _hex_to_int(s):
    if pd.isna(s):
        return pd.NA
    s = s.strip()
    if len(s) != 2 or any(ch not in "0123456789abcdefABCDEF" for ch in s):
        return pd.NA
    return int("0x" + s,16)

df['SECTORID'] = last2.map(_hex_to_int)

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
utc_time = pd.to_datetime(df['DATETIME'], errors='coerce')

def _localize_safe(x):
    if pd.isna(x):
        return x
    # pandas Timestamp has .tz None means naive
    return x.tz_localize('UTC') if getattr(x, 'tz', None) is None else x

utc_time = utc_time.apply(_localize_safe)
local_timezone = get_localzone()

# Remove time zone information and microseconds from the datetime
local_time_without_tz = utc_time.apply(lambda x: x.astimezone(local_timezone).replace(tzinfo=None, microsecond=0))

df['DATETIME'] = local_time_without_tz
df['DATE'] = local_time_without_tz.apply(lambda x: x.date())   # Extract Date
df['TIME'] = local_time_without_tz.apply(lambda x: x.time())   # Extract Time

alarm_df = df[(df['time_diff'] >= pd.Timedelta(hours=12)) | (df['LATITUDE'] == 0) | (df['LONGITUDE'] == 0) | (df['BESTS.SNR'] <= -2) | (df['BESTS.RSRP'] <= -120) | (df['BESTS.TEMP.'] >= 75)].copy()
alarm_df.loc[:, 'COMM. ALARM'] = 0 # COMMUNICATION ALARM
alarm_df.loc[:, 'GPS ALARM'] = 0   # GPS ALARM
alarm_df.loc[:, 'RSRP ALARM'] = 0  # RSRP ALARM
alarm_df.loc[:, 'SINR ALARM'] = 0  # SINR ALARM
alarm_df.loc[:, 'TEMP ALARM'] = 0  # TEMPERATURE ALARM
columns_to_keep = ['SERIAL','NAME','DATETIME','time_diff','BESTS.RSRP', 'BESTS.RSRQ', 'BESTS.SNR','BESTS.EARFCN','BESTS.BAND','BESTS.CID','BESTS.NODEB_dec','BESTS.TEMP.','S0.RSRP','S0.RSRQ','SECT0.SNR','S0.EARFCN','S0.BAND','S0.CID','S0.NODEB_dec','S1.RSRP','S1.RSRQ','SECT1.SNR','S1.EARFCN','S1.BAND','S1.CID','S1.NODEB_dec','S2.RSRP','S2.RSRQ','SECT2.SNR','S2.EARFCN','S2.BAND','S2.CID','S2.NODEB_dec','S3.RSRP','S3.RSRQ','SECT3.SNR','S3.EARFCN','S3.BAND','S3.CID','S3.NODEB_dec','COMM. ALARM','GPS ALARM','RSRP ALARM','SINR ALARM','TEMP ALARM']

# Alarm columns setup
if not alarm_df.empty:
    alarm_df.loc[:, 'COMM. ALARM'] = (alarm_df['time_diff'] >= pd.Timedelta(hours=12)).astype(int)             # COMMUNICATION ALARM
    alarm_df.loc[:, 'GPS ALARM'] = ((alarm_df['LATITUDE'] == 0) | (alarm_df['LONGITUDE'] == 0)).astype(int)    # GPS ALARM
    alarm_df.loc[:, 'RSRP ALARM'] = (alarm_df['BESTS.RSRP'] <= -120).astype(int)                               # RSRP ALARM
    alarm_df.loc[:, 'SINR ALARM'] = (alarm_df['BESTS.SNR'] <= -2).astype(int)                                  # SINR ALARM
    alarm_df.loc[:, 'TEMP ALARM'] = (alarm_df['BESTS.TEMP.'] >= 75).astype(int)                                # TEMPERATURE ALARM
else:
    print("Empty DataFrame")
    print("No Alarms Detected")
    
alarm_df = alarm_df[columns_to_keep]
alarm_df = alarm_df[alarm_df['SERIAL'].isin([
    '4GN00100', '4GN00101', '4GN00102', '4GN00103', '4GN00104', '4GN00105',
    '4GN00106', '4GN00107', '4GN00108', '4GN00109', '4GN00110',
    '4GV00101', '4GV00103', '4GV00104', '4GV00106', '4GV00107', '4GV00108',
    '4GV00110', '4GV00111', '4GV00112', '4GV00113', '4GV00114', '4GV00115',
    '4GV00116', '4GV00117', '4GV00118',
    '4GW00800', '4GW00802', '4GW00805'
])]
df = df.drop(columns=['time_diff'])
print(df.iloc[:, :8])
print("\n======================================ALARMS======================================\n")
print(alarm_df.iloc[:, :])
alarm_df.to_csv("C:\\inetpub\\wwwroot\\Platform\\NMS_ALARMS.csv", sep='$', encoding='utf-8', index=False, header=True)
# ----------------------------------------------------------------------------------------------------------------------------

# ================================================================
# Insert Data with correct types into SQL Database (safe + resilient)
# ================================================================

import urllib.parse
import time
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.types import NVARCHAR, Float, DateTime, Date, Time

# ------------------------------------------------
# Retry helper for transient SQL connection errors
# ------------------------------------------------
def safe_to_sql(df, table, engine, if_exists='replace', dtype=None, tries=3, delay=3):
    """
    Writes DataFrame to SQL Server with retry.
    Retries transient OperationalError (e.g., network hiccups) up to `tries` times.
    """
    for i in range(1, tries + 1):
        try:
            df.to_sql(
                table,
                con=engine,
                if_exists=if_exists,
                index=False,
                dtype=dtype,
                chunksize=1000
            )
            print(f"[to_sql] Successful on attempt {i}")
            return
        except OperationalError as e:
            print(f"[to_sql retry {i}/{tries}] OperationalError: {e}")
            if i == tries:
                raise
            time.sleep(delay)
            delay *= 2  # exponential backoff

# ------------------------------------------------
# Keep SQL Server Native Client 11.0
# ------------------------------------------------
params = urllib.parse.quote_plus(
    "DRIVER={SQL Server Native Client 11.0};"
    "SERVER=win-45ntjeb05tt\\sqlexpress;"
    "DATABASE=3skelion;"
    "UID=admin;PWD=fasmetrics;"
    "TrustServerCertificate=Yes;"
)
engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    pool_pre_ping=True,   # drop dead connections automatically
    pool_recycle=1800     # recycle connections older than 30 min
)

# ------------------------------------------------
# Columns to export for the CSV
# ------------------------------------------------
columns_live_to_keep = [
    'SERIAL','NAME','LATITUDE','LONGITUDE','HEADING','SPEED','DATE','TIME','SCAN#',
    'BESTS.SECT#','BESTS.MCC','BESTS.MNC','BESTS.ISREG.','BESTS.EARFCN','BESTS.BAND','BESTS.CID','BESTS.NODEB_dec',
    'BESTS.CID_dec','BESTS.TAC','BESTS.PCI','BESTS.RSRP','BESTS.RSRQ','BESTS.SNR',
    'S0.SECT#','S0.MCC','S0.MNC','S0.ISREG.','S0.EARFCN','S0.BAND','S0.CID','S0.NODEB_dec',
    'S0.CID_dec','S0.TAC','S0.PCI','S0.RSRP','S0.RSRQ','SECT0.SNR',
    'S1.SECT#','S1.MCC','S1.MNC','S1.ISREG.','S1.EARFCN','S1.BAND','S1.CID','S1.NODEB_dec',
    'S1.CID_dec','S1.TAC','S1.PCI','S1.RSRP','S1.RSRQ','SECT1.SNR',
    'S2.SECT#','S2.MCC','S2.MNC','S2.ISREG.','S2.EARFCN','S2.BAND','S2.CID','S2.NODEB_dec',
    'S2.CID_dec','S2.TAC','S2.PCI','S2.RSRP','S2.RSRQ','SECT2.SNR',
    'S3.SECT#','S3.MCC','S3.MNC','S3.ISREG.','S3.EARFCN','S3.BAND','S3.CID','S3.NODEB_dec',
    'S3.CID_dec','S3.TAC','S3.PCI','S3.RSRP','S3.RSRQ','SECT3.SNR'
]

# ------------------------------------------------
# Export CSV for the live view
# ------------------------------------------------
export_df = df[columns_live_to_keep]
export_df = export_df[export_df['SERIAL'].isin([
    '4GN00100','4GN00101','4GN00102','4GN00103','4GN00104','4GN00105',
    '4GN00106','4GN00107','4GN00108','4GN00109','4GN00110',
    '4GV00101','4GV00103','4GV00104','4GV00106','4GV00107','4GV00108',
    '4GV00110','4GV00111','4GV00112','4GV00113','4GV00114','4GV00115',
    '4GV00116','4GV00117','4GV00118',
    '4GW00800','4GW00802','4GW00805'
])]
export_df.to_csv(
    r"C:\inetpub\wwwroot\Platform\NMS_LIVEVIEW.csv",
    sep='$', encoding='utf-8', index=False, header=True
)

# ------------------------------------------------
# Define dtype map (explicit NVARCHAR lengths)
# ------------------------------------------------
dtype_map = {
    'SERIAL': NVARCHAR(length=32),
    'NAME': NVARCHAR(length=255),
    'LATITUDE': Float, 'LONGITUDE': Float, 'HEADING': Float, 'SPEED': Float,
    'DATE': Date, 'TIME': Time, 'SCAN#': Float,
    'GEOLOCK.INLOCK': Float, 'GEOLOCK.ID': NVARCHAR(length=64),
    'GEOLOCK_LAT': Float, 'GEOLOCK_LONG': Float,
    'CIDLOCK.INLOCK': Float, 'CIDLOCK.ID': NVARCHAR(length=64),
    'CIDLOCK_LAT': Float, 'CIDLOCK_LONG': Float,
    'NBCIDLOCK.INLOCK': Float, 'NBCIDLOCK.ID': NVARCHAR(length=64),
    'NBCIDLOCK_LAT': Float, 'NBCIDLOCK_LONG': Float,
    'DNR1.ENABLED': Float, 'DNR1.TYPE': NVARCHAR(length=32),
    'DNR1.CTRL_BY': NVARCHAR(length=32), 'DNR1.-3dBAz': Float,
    'DNR1.ACT_SECTOR': Float, 'DNR1.OFFSET': Float, 'DNR1.AZIMUTH': Float,
    'DNR2.ENABLED': Float, 'DNR2.TYPE': NVARCHAR(length=32),
    'DNR2.CTRL_BY': NVARCHAR(length=32), 'DNR2.-3dBAz': Float,
    'DNR2.ACT_SECTOR': Float, 'DNR2.OFFSET': Float, 'DNR2.AZIMUTH': Float,
    'BESTS.SECT#': Float, 'BESTS.MCC': Float, 'BESTS.MNC': Float, 'BESTS.ISREG': Float,
    'BESTS.RAT': Float, 'BESTS.EARFCN': Float, 'BESTS.BAND': Float,
    'BESTS.CID': NVARCHAR(length=64),
    'BESTS_NODEB_dec': Float, 'BESTS.CID_dec': NVARCHAR(length=64),
    'BESTS.TAC': NVARCHAR(length=32), 'BESTS.PCI': Float,
    'BESTS.BW': Float, 'BESTS.RSRP': Float, 'BESTS.RSRQ': Float,
    'BESTS.SNR': Float, 'BESTS.RTT': Float, 'BESTS.HTTP(KBPS)': Float,
    'BESTS.PCI#': Float, 'BESTS.SCORE': Float, 'BESTS.OFFSET': Float, 'BESTS.AZIMUTH': Float,
    'BESTS.NBCID': NVARCHAR(length=64), 'BESTS.NB_NODEB_dec': Float,
    'BESTS.NB_CID_dec': NVARCHAR(length=64), 'BESTS.NBCIDRSRP': Float,
    'BESTS.TEMP.': Float,
    # Example for sectors (same style for S0–S3)
    'S0.SECT#': NVARCHAR(length=32), 'S0.MCC': Float, 'S0.MNC': Float,
    'S0.ISREG.': Float, 'S0.RAT': Float, 'S0.EARFCN': Float, 'S0.BAND': Float,
    'S0.CID': NVARCHAR(length=64), 'S0.NODEB_dec': Float,
    'S0.CID_dec': NVARCHAR(length=64), 'S0.TAC': NVARCHAR(length=32),
    'S0.PCI': Float, 'S0.BW': Float, 'S0.RSRP': Float, 'S0.RSRQ': Float,
    'SECT0.SNR': Float, 'S0.RTT': Float, 'S0.HTTP(KBPS)': Float,
    'S0.PCI#': Float, 'S0.OFFSET': Float, 'S0.AZIMUTH': Float,
    'S0.NBCID': NVARCHAR(length=64), 'S0.NB_NODEB_dec': Float,
    'S0.NB_CID_dec': NVARCHAR(length=64), 'S0.NBCIDRSRP': Float, 'S0.TEMP': Float,
    'S1.SECT#': Float, 'S1.MCC': Float, 'S1.MNC': Float, 'S1.ISREG.': Float,
    'S1.RAT': Float, 'S1.EARFCN': Float, 'S1.BAND': Float, 'S1.CID': NVARCHAR(length=64),
    'S1.NODEB_dec': Float, 'S1.CID_dec': NVARCHAR(length=64), 'S1.TAC': NVARCHAR(length=32),
    'S1.PCI': Float, 'S1.BW': Float, 'S1.RSRP': Float, 'S1.RSRQ': Float,
    'SECT1.SNR': Float, 'S1.RTT': Float, 'S1.HTTP(KBPS)': Float,
    'S1.PCI#': Float, 'S1.OFFSET': Float, 'S1.AZIMUTH': Float,
    'S1.NBCID': NVARCHAR(length=64), 'S1.NB_NODEB_dec': Float,
    'S1.NB_CID_dec': NVARCHAR(length=64), 'S1.NBCIDRSRP': Float, 'S1.TEMP': Float,
    'S2.SECT#': NVARCHAR(length=32), 'S2.MCC': Float, 'S2.MNC': Float,
    'S2.ISREG.': Float, 'S2.RAT': Float, 'S2.EARFCN': Float, 'S2.BAND': Float,
    'S2.CID': NVARCHAR(length=64), 'S2.NODEB_dec': Float,
    'S2.CID_dec': NVARCHAR(length=64), 'S2.TAC': NVARCHAR(length=32),
    'S2.PCI': Float, 'S2.BW': Float, 'S2.RSRP': Float, 'S2.RSRQ': Float,
    'SECT2.SNR': Float, 'S2.RTT': Float, 'S2.HTTP(KBPS)': Float,
    'S2.PCI#': Float, 'S2.OFFSET': Float, 'S2.AZIMUTH': Float,
    'S2.NBCID': NVARCHAR(length=64), 'S2.NB_NODEB_dec': Float,
    'S2.NB_CID_dec': NVARCHAR(length=64), 'S2.NBCIDRSRP': Float, 'S2.TEMP': Float,
    'S3.SECT#': NVARCHAR(length=32), 'S3.MCC': Float, 'S3.MNC': Float,
    'S3.ISREG.': Float, 'S3.RAT': Float, 'S3.EARFCN': Float, 'S3.BAND': Float,
    'S3.CID': NVARCHAR(length=64), 'S3.NODEB_dec': Float,
    'S3.CID_dec': NVARCHAR(length=64), 'S3.TAC': NVARCHAR(length=32),
    'S3.PCI': Float, 'S3.BW': Float, 'S3.RSRP': Float, 'S3.RSRQ': Float,
    'SECT3.SNR': Float, 'S3.RTT': Float, 'S3.HTTP(KBPS)': Float,
    'S3.PCI#': Float, 'S3.OFFSET': Float, 'S3.AZIMUTH': Float,
    'S3.NBCID': NVARCHAR(length=64), 'S3.NB_NODEB_dec': Float,
    'S3.NB_CID_dec': NVARCHAR(length=64), 'S3.NBCIDRSRP': Float, 'S3.TEMP': Float,
    'DATETIME': DateTime, 'SECTORID': Float
}

# ------------------------------------------------
# Safe SQL write (replaces df.to_sql)
# ------------------------------------------------
safe_to_sql(df, 'LiveSheet$', engine, if_exists='replace', dtype=dtype_map)
print("\n IMPORT IS DONE \n")
