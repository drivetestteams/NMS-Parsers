import pandas as pd


# A function that import a saved csv file to a database table set by the arguements
def csv_to_sql(csv_path, engine, table_of_database, delimiter):
    # reads the csv file on the specified path of the arguement if error continues to the next
    df = pd.read_csv(csv_path, delimiter=delimiter, parse_dates= column_to_parse_dates)

    # Identify columns containing 'Unnamed'
    unnamed_columns = df.filter(like='Unnamed: ').columns
    # delete all columns containing 'Unnamed'
    for column in unnamed_columns:
        del df[column]


    # imports the csv on the database specified with the engine specified, appended(we want to add the data)
    df.to_sql(table_of_database, con=engine, if_exists='append', index=False)
    print("Added to database:", csv_path, "\n")



