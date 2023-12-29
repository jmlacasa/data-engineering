import pandas as pd
import numpy as np
import pandas_market_calendars as mcal
import yfinance as yf
from psycopg2.extras import execute_values


# %%
## CONNECT to PostgreSQL

from dotenv.main import dotenv_values
import os
import psycopg2

from os.path import expanduser
home = expanduser("~")

dag_path = os.getcwd() 
# print(os.path.exists(os.path.join(dag_path,"keys", "db.env")))
# env_values = dotenv_values(os.path.join(dag_path,"keys", "db.env"))
env_values = dotenv_values('/opt/airflow/keys/db.env')
schema = 'deliverable3'
print(env_values)
connection_params = dict(
        host=env_values['host']
        , dbname=env_values['dbname']
        , user=env_values['user']
        , password=env_values['password']
        , port=env_values['port']
)


try:
    conn = psycopg2.connect(
        **connection_params
    )
    print("Connected to Redshift successfully!")
    
except Exception as e:
    print("Unable to connect to Redshift.")
    print(e)


# # EXTRACT
# set start date as yesterday, end date as 3 days from today
start_date = (pd.Timestamp.today() - pd.Timedelta(days=3)).strftime('%Y-%m-%d')
end_date = (pd.Timestamp.today() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')


# Setup the parameters for the data to be fetched
ticker_info = dict(ticker_symbol = 'AAPL'
                   , data_source='Yahoo Finance'
                   , source_type = 'Platform'
                   , interval='1d'
                   , start_date = start_date
                   , end_date = end_date)

# Fetch data for Apple Inc. with a daily interval
data = yf.download(ticker_info['ticker_symbol']
                   , interval=ticker_info['interval']
                   , start=ticker_info['start_date']
                   , end=ticker_info['end_date'])

# Display the data
data.head()


## TRANSFORM

def get_or_create_record(conn, search_value, table_name, entity):
    ''''
    This function ensures that a record with a specific search value exists in the database.
    If the record already exists, it returns its ID.
    If the record does not exist, it creates one and then returns the new ID.
    '''

    # --- Search for Existing Record ---
    with conn.cursor() as cur:
        # Construct and execute a SELECT query to search for an existing record
        cur.execute(f"SELECT {entity}_id FROM {schema}.{table_name} WHERE {entity}_name = '{search_value}';")
        try:
            # If a record is found, retrieve its ID
            record_id = cur.fetchone()[0]
        except TypeError:
            # If no record is found, set record_id to None
            record_id = None

    # --- Insert Record if Not Found ---
    if not record_id:
        with conn.cursor() as cur:
            # Construct and execute an INSERT query to create a new record
            cur.execute(f"INSERT INTO {schema}.{table_name} ({entity}_name) VALUES ('{search_value}');")
            # Retrieve the ID of the newly created record
            cur.execute(f"SELECT {entity}_id FROM {schema}.{table_name} WHERE {entity}_name = '{search_value}';")
            record_id = cur.fetchone()[0]
            # Commit the transaction to save the new record in the database
            conn.commit()

    # --- Return the Record ID ---
    return record_id


# get or create asset, data source and source type
asset_id = get_or_create_record(conn, ticker_info['ticker_symbol'], "assets", "asset")
data_source_id = get_or_create_record(conn, ticker_info['data_source'], "data_sources", "source")
source_type_id = get_or_create_record(conn, ticker_info['source_type'], "source_types", "type")
print(asset_id, source_type_id, data_source_id)


# add asset_id, source_id and source_type_id to data
data.reset_index(inplace=True)
data.columns = [c.lower().replace(' ', '_') for c in data.columns]
data.rename(columns={'date':'ts'}, inplace=True)
data['asset_id'] = asset_id
data['source_id'] = data_source_id


# Check for missing dates (excluding weekends and holidays)
all_days = mcal.date_range(mcal.get_calendar('NYSE').schedule(start_date=ticker_info['start_date'], end_date=ticker_info['end_date'])
                , frequency='1D')[:-1]
missing_days = np.setdiff1d(all_days.date, data['ts'].dt.date)

if len(missing_days) > 0:
    print("Missing dates:", missing_days)
else:
    print("No missing dates!")


# check for and drop duplicates
data.drop_duplicates('ts', inplace=True)
data.duplicated('ts').sum()


## LOAD

# define functions to load and merge data into PostgreSQL and Redshift
def load_staging_postgresql(conn, table_name, dataframe):
    '''
    This function loads data from a Pandas DataFrame into a specified table in a PostgreSQL database.
    If records with the same primary key exist, the function updates them with the new values.
    '''
    clean_staging = f'''
    DELETE FROM {schema}.staging_{table_name}
    '''
    # --- Convert DataFrame to List of Tuples ---
    # Convert each row of the DataFrame into a tuple and create a list of these tuples
    values = [tuple(x) for x in dataframe.to_numpy()]

    # --- Format Column Names ---
    # Construct a string of column names separated by commas
    cols = '"'+'''", "'''.join(dataframe.columns)+'"'

    # --- Prepare SQL Queries ---
    # Construct the base INSERT INTO query and append the ON CONFLICT clause
    insert_sql = f"INSERT INTO {schema}.staging_{table_name} ({cols}) VALUES %s"

    # --- Execute Transaction ---
    # Execute the query using execute_values for batch insertion
    with conn.cursor() as curs:
        # Construct and execute an INSERT query to create a new record
        curs.execute(clean_staging)
        execute_values(curs, insert_sql, values)
        conn.commit()

    print('Finished loading data into staging PostgreSQL.')

def merge_redshift(conn, table_name):

    # --- Prepare SQL Queries ---
    # Construct DELETE FROM query to delete duplicate records from target table
    # Create an INSERT INTO query to insert records from the staging table into the target table


    delete_sql = f'''
    DELETE FROM {schema}.{table_name}
    USING {schema}.staging_{table_name} AS staging
    WHERE {schema}.{table_name}.asset_id = staging.asset_id AND
        {schema}.{table_name}.source_id = staging.source_id AND
        {schema}.{table_name}.ts = staging.ts;
    '''
    
    insert_sql = f'''
    INSERT INTO {schema}.{table_name}
    SELECT * FROM {schema}.staging_{table_name};
    '''

    # --- Execute Transaction ---
    # Execute the query
    with conn.cursor() as cur:
        cur.execute(delete_sql)
        cur.execute(insert_sql)
        # Commit the transaction to save the new record in the database
        conn.commit()

    print('Finished loading data into PostgreSQL.')

# load to staging and merge into redshift
load_staging_postgresql(conn, 'ohlcv_data', data)
merge_redshift(conn, 'ohlcv_data')

# close redshift connection
conn.close()

# with conn.cursor() as curs:
#     curs.execute("ROLLBACK")
#     conn.commit()

# write email text with ticker symbol, date, scope and number of records loaded
email_text = f'''

    Data for {ticker_info['ticker_symbol']} from {ticker_info['data_source']} was loaded into the database on {pd.Timestamp.today().strftime('%Y-%m-%d')}.
    
    The data covers the period from {ticker_info['start_date']} to {ticker_info['end_date']} and includes {len(data)} records.
    '''

# return XCOM value
import json
print(json.dumps({"body_text": email_text}))