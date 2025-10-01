# Code for ETL operations on Country-GDP data

# Importing the required libraries
import numpy as np
import pandas as pd
import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime 

# initialization
url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
csv_path = '/home/project/project1/exchange_rate.csv'
output_path = "./Largest_banks_data.csv"
db_name = "Banks.db"
table_name = "Largest_banks"
log_file  = "code_log.txt"

# Task 1: Logging function
def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''

    # Use datetime.now() function to get the current timestamp
    # Year-Monthname-Day-Hour-Minute-Second 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

# Task 2: Extraction of data      
def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    
    # Extract the web page as text
    html_page = requests.get(url).text
    # Parse the text into an HTML object
    data = BeautifulSoup(html_page, 'lxml')
    # Find table that contains headers ('Bank name' and 'Market cap')
    tables = data.find_all('table', class_='wikitable')
    target_table = None
    for table in tables:
        headers = ' '.join([th.get_text(strip=True).lower() for th in table.find_all('th')])
        if 'bank name' in headers and 'market cap' in headers:
            target_table = table
            break
    # Extract data from table rows
    records = []
    rows = target_table.find_all('tr')
    for row in rows[1:]:  # Skip header row
        cols = row.find_all('td')
        if len(cols) >= 3:
            # Get bank name from second column
            bank_name = cols[1].get_text(strip=True)
            # Get market cap value from third column - remove last character (\n) and convert to float
            mc_value = float(cols[2].contents[0][:-1])
            records.append([bank_name, mc_value])
        if len(records) == 10:
            break
    # Create DataFrame with the extracted data
    df = pd.DataFrame(records, columns=table_attribs)
    
    return df

# Task 3: Transformation of data
def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    # Read the exchange rate CSV file
    exchange_rate_df = pd.read_csv(csv_path)
    # Convert the contents to a dictionary so that the contents of the first columns are the keys (currency) to the dictionary and the contents of the second column are the corresponding values (rate)
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']
    # Add 3 different columns to the dataframe:  MC_GBP_Billion, MC_EUR_Billion and MC_INR_Billion, each containing the content of MC_USD_Billion
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    return df

# Task 4: Loading to CSV
def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    # Use 'to_csv()' function 
    df.to_csv(output_path)

# Task 5: Loading to Database
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    # Use 'to_sql()' function 
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

# Task 6: Function to Run queries on Database
def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    # Use pandas.read_sql() function to run the query on the database table
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')
print(df)  # Print the extracted dataframe

df = transform(df, csv_path)

log_progress('Data transformation complete. Initiating loading process')
print(df)
print(" \n Market capitalization of 5th largest bank in EUR Billion:")
print(df['MC_EUR_Billion'][4])

load_to_csv(df, output_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('Banks.db')

log_progress('SQL Connection initiated')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as a table, Executing queries')

# Query 1: Print the contents of the entire table
query_statement = "SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)

# Query 2: Print the average market capitalization of all the banks in Billion GBP
query_statement = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)

# Query 3: Print only the names of the top 5 banks
query_statement = "SELECT Name FROM Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)

log_progress('Process Complete')



sql_connection.close()

log_progress('Server Connection closed')