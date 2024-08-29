# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

log_file = "code_log.txt" 
target_file = "transformed_data.csv"

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) # to convert the timestamp to a string format
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    table = data.find_all('tbody')
    rows = table[0].find_all('tr')[1:]
    
    #print(rows)

    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[1].find('a'):
#                name = col[1].a.get_text(strip=True) if col[1].a else col[1].get_text(strip=True)
#                market_cap_text = col[2].get_text(strip=True).replace('\n', '').replace(',', '')    
#                print(name, market_cap_text)
                data_dict = {"Name": col[1].get_text(strip=True),
                             "MC_USD_Billion": col[2].get_text(strip=True).replace('\n', '').replace(',', '')}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)

    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    rate_dict = dataframe.set_index('Col_1_header').to_dict()['Col_2_header']

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
exrate_csv_path = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
    
log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)
print(df)
