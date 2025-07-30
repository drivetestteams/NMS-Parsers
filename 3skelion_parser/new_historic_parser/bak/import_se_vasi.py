import os
import pandas as pd
from sqlalchemy import create_engine
import urllib
from datetime import datetime
import pytz
import warnings

def csv_to_database(csv_path, engine):
    # reads the csv file on the specified path of the arguement if error continues to the next
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = pd.read_csv(csv_path, delimiter='$', parse_dates=['DATE'], dayfirst=True)
    except Exception as e:
        return None

    #if its not importable return
    if not is_importable(df):
        return None

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = df.drop(columns=['Unnamed: 170'])
    except Exception as e:
        return None

    df = correct_dates_of_dataframe_for_database(df)

    # imports the csv on the database specified with the engine specified, appended(we want to add the data)
    df.to_sql('HistoricNewConfiguration$', con=engine, if_exists='append', index=False)
    print("Appended:", csv_path, "\n")
    # print(df)

def is_importable(df):
    if df['NAME'].str.contains("NOT INSTALLED YET").any():
        return False
    elif df['NAME'].str.contains("DEFAULT").any():
        return False
    else:
        print(df['NAME'].iloc[0])  # Access the first element using iloc
        return True  # Return True if it's importable

def correct_dates_of_dataframe_for_database(df):
    df['DATETIME'] = None
    df.head()

    # print("\n")
    # print(df.iloc[:, :8])

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

    return df


# ----------- MAIN ------------------------
# creates the engine on the specified path
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};SERVER=swissqual-srv;DATABASE=3skelion;UID=sa;PWD=swissqual")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

for root, dirs, files in os.walk(".", topdown=False):
   for name in files:
       if name.endswith(".csv"):
            # joins the paths in windows style .\path\like\this
            path = os.path.join(root, name)
            # transforms it like this path for windows
            path = path.replace("\\","/")
            # imports the csv to database
            csv_to_database(path,engine)
            # says it did it

