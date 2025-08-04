import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Float, DateTime, NVARCHAR, Date, Time
import urllib
from datetime import datetime
import pytz, pyodbc
from tzlocal import get_localzone
import shutil

def nullify_snr_if_rsrp_missing(df, rsrp_col, snr_col):
    if rsrp_col in df.columns and snr_col in df.columns:
        df.loc[df[rsrp_col] == -150, rsrp_col] = pd.NA
        df.loc[df[rsrp_col].isna(), snr_col] = pd.NA


def csv_to_database(csv_path, engine):
    # reads the csv file on the specified path of the arguement if error continues to the next
    df = pd.read_csv(csv_path, delimiter='$', parse_dates=['DATE'])
    df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')

    # Identify columns containing 'Unnamed'
    unnamed_columns = df.filter(like='Unnamed: ').columns
    # delete all columns containing 'Unnamed'
    for column in unnamed_columns:
        del df[column]
        
    df = parse_dataframe_for_importing(df)

    if not df.empty:
        # imports the csv on the database specified with the engine specified, appended(we want to add the data)
        # This ensures no values exceed BIGINT limits. It's a good safeguard, especially if you're dealing with unexpected large values.
        # df = df.applymap(lambda x: x if isinstance(x, (int, float)) and abs(x) <= 9223372036854775807 else None)
        df.to_sql('HistoricNewConfiguration$', con=engine, if_exists='append', index=False, dtype={
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
        print("Imported:", csv_path)
        # return successful
        return True
    else:
        # not appends the file because of some cases set by the parser() funciton
        print("Not appended:", csv_path)
        # return unsuccessful
        return False


def parse_dataframe_for_importing(df):
    try:
        df['DATETIME'] = None
        # Filter out rows where 'NAME' column equals 0 and BLANK DATE and TIME Columns
        # df = df.dropna(subset=['DATE'])
        # df = df.dropna(subset=['TIME'])
        df.head()
        
        df = df.dropna(subset=['SERIAL'])

        # Check If LAT,LOT is 0. If yes, replace zero datetime with current Datetime
        if 0.0 in df['LATITUDE'].values:
            indices = df.index[df['LATITUDE'] == 0.0].tolist()
            for index in indices:
                current_time = datetime.now(pytz.utc)
                formatted_time = current_time.strftime("%d-%m-%Y %H:%M:%S")
                date_component, time_component = formatted_time.split()
                df.at[index, 'DATE'] = date_component
                df.at[index, 'TIME'] = time_component
                df.at[index, 'DATETIME'] = formatted_time
                df.at[index, 'HEADING'] = 0
                df.at[index, 'SPEED'] = 0
        # First , we convert the column to datetime type and then isolate date
        df['DATE'] = pd.to_datetime(df['DATE'], dayfirst=True, format='%d-%m-%Y')
        df['DATE'] = df['DATE'].dt.date
        
        if (df['DATE'] > datetime.today().date()).any() or df['DATE'].isnull().any():
            print(datetime.today().date())
            print("Invalid Date From SERIAL:", df["DATE"])
            return pd.DataFrame()                           # returns empty Dataframe

        # First , we convert the column to datetime type and then isolate time
        df['TIME'] = pd.to_datetime(df['TIME'], format='%H:%M:%S')
        df['TIME'] = df['TIME'].dt.time
        # Also we keep a datetime columnm with the 2 individual columns combined
        df['DATETIME'] = pd.to_datetime(df['DATE'].astype(str) + ' ' + df['TIME'].astype(str))

        # Check if there are any dates in 'DATETIME' column older than April 1, 2024
        if any(df['DATETIME'] < pd.Timestamp('2024-04-01')):
            print("Invalid Date From SERIAL:", df["SERIAL"])
            return pd.DataFrame()                   # returns empty Dataframe
        
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
        
        # replace all values of BEST_SNR == -16.00 with NULL
        nullify_snr_if_rsrp_missing(df, 'S0.RSRP', 'SECT0.SNR')
        nullify_snr_if_rsrp_missing(df, 'S1.RSRP', 'SECT1.SNR')
        nullify_snr_if_rsrp_missing(df, 'S2.RSRP', 'SECT2.SNR')
        nullify_snr_if_rsrp_missing(df, 'S3.RSRP', 'SECT3.SNR')
        nullify_snr_if_rsrp_missing(df, 'BESTS.RSRP', 'BESTS.SNR')
            
        # Extract the last two characters, append '0x' to each, then convert to decimal
        hex_sec = '0x' + df['BESTS.CID'].str[-2:]

        # Convert the hexadecimal string to decimal for each entry in the Series
        dec_sec = hex_sec.apply(lambda x: int(x, 16))
        df['SECTORID'] = dec_sec
            
        return df

    except Exception as e:
        print(f"Error processing DataFrame: {e}")
        return pd.DataFrame()                       # returns empty Datafr

def full_historic_export():

    # Connection parameters
    server = 'win-45ntjeb05tt\sqlexpress'  # e.g., 'localhost' or '192.168.1.100\SQLEXPRESS' *** WHEN SERVER IS HOSTED EXTERNALLY, REPLACE SERVER NAME WITH IP ADDRESS ***
    database = '3skelion'
    username = 'admin'
    password = 'fasmetrics'
    table = 'HistoricLast15Days'

    # Create connection string
    conn_str = f'DRIVER={{SQL Server Native Client 11.0}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    # Connect and run query
    try:
        with pyodbc.connect(conn_str) as conn:
            df = pd.read_sql_query(f'SELECT * FROM {table}', conn)
            df.to_csv("C:\\inetpub\\wwwroot\\Platform\\NMS_FULL_HISTORIC_VIEW.csv", index=False)
            print("Data written to output.csv")

    except Exception as e:
        print("Error:", e)


# ------------------------ MAIN ------------------------
# creates the engine on the specified path
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};SERVER=win-45ntjeb05tt\sqlexpress;DATABASE=3skelion;UID=admin;PWD=fasmetrics")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

# traverse all the files in the subdirecories
for root, dirs, files in os.walk("C:/TRISKELION_LOG_DATA/", topdown=False):
    # Skip the IMPORTED and PROBLEMTACIc and FAS directories and their contents
    if "IMPORTED" in root or "PROBLEMATIC" in root or "FAS" in root :
        continue
    for name in files:
        # find all the csvs in the log data
        if name.endswith(".csv"):
            try:
                # joins the paths in windows style .\path\like\this
                path = os.path.join(root, name)
                # transforms it like this path for windows
                path = path.replace("\\" , "/")
                print(path)
                print("------- "+ path)
                #imports the csv to database
                result = csv_to_database(path, engine)
                if result is True:
                    # moves the csvs to the imported folder if imported correctly
                    move = "C:/TRISKELION_LOG_DATA/IMPORTED/" + name
                    shutil.move(path, move)
                else:
                    # moves the csvs to the imported folder if imported correctly
                    move = "C:/TRISKELION_LOG_DATA/PROBLEMATIC/" + name
                    shutil.move(path, move)
            except Exception as e:
                # moves the csvs to the imported folder if imported correctly
                move = "C:/TRISKELION_LOG_DATA/PROBLEMATIC/" + name
                shutil.move(path, move)
                continue
full_historic_export()