import os
import pandas as pd
from sqlalchemy import create_engine
import urllib
from datetime import datetime
import pytz
import shutil

def csv_to_database(csv_path, engine):
    # reads the csv file on the specified path of the arguement if error continues to the next
    df = pd.read_csv(csv_path, delimiter='$', parse_dates=['DATE'])

    # Identify columns containing 'Unnamed'
    unnamed_columns = df.filter(like='Unnamed: ').columns
    # delete all columns containing 'Unnamed'
    for column in unnamed_columns:
        del df[column]

    if parse_dataframe_for_importing(df):
        # imports the csv on the database specified with the engine specified, appended(we want to add the data)
        df.to_sql('HistoricNewConfiguration$', con=engine, if_exists='append', index=False)
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
        df.head()

        # Check If LAT,LOT is 0. If yes, replace zero datetime with current Datetime
        if 0 in df['LATITUDE'].values:
            indices = df.index[df['LATITUDE'] == 0].tolist()
            for index in indices:
                current_time = datetime.now(pytz.utc)
                formatted_time = current_time.strftime("%d-%m-%Y %H:%M:%S")
                date_component, time_component = formatted_time.split()
                df.at[index, 'DATE'] = date_component
                df.at[index, 'TIME'] = time_component
                df.at[index, 'DATETIME'] = formatted_time

        # First , we convert the column to datetime type and then isolate date
        df['DATE'] = pd.to_datetime(df['DATE'], dayfirst=True, format='%d-%m-%Y')
        df['DATE'] = df['DATE'].dt.date

        # First , we convert the column to datetime type and then isolate time
        df['TIME'] = pd.to_datetime(df['TIME'], format='%H:%M:%S')
        df['TIME'] = df['TIME'].dt.time
        # Also we keep a datetime columnm with the 2 individual columns combined
        df['DATETIME'] = pd.to_datetime(df['DATE'].astype(str) + ' ' + df['TIME'].astype(str))

        # Check if there are any dates in 'DATETIME' column older than April 1, 2024
        if any(df['DATETIME'] < pd.Timestamp('2024-04-01')):
            return False

        return True

    except Exception as e:
        print(f"Error processing DataFrame: {e}")
        return False


# ----------- MAIN ------------------------
# creates the engine on the specified path
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};SERVER=swissqual-srv;DATABASE=3skelion;UID=sa;PWD=swissqual")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

for root, dirs, files in os.walk(".", topdown=False):
    # Skip the IMPORTED and PROBLEMTACI directories and its contents
    if "IMPORTED" in root or "PROBLEMATIC" in root:
        continue
    for name in files:
            if name.endswith(".csv"):
                # joins the paths in windows style .\path\like\this
                path = os.path.join(root, name)
                # transforms it like this path for windows
                print(path)
                path = path.replace("\\" , "/")
                # imports the csv to database
                result = csv_to_database(path, engine)
                if result is True:
                    # moves the csvs to the imported folder if imported correctly
                    move = "./IMPORTED/" + name
                    shutil.move(path, move)
                else:
                    # moves the csvs to the imported folder if imported correctly
                    move = "./PROBLEMATIC/" + name
                    shutil.move(path, move)
