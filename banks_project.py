# Code for ETL operations on Country-GDP data

# Importing the required libraries
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import requests

def log_progress(message):
    dt = datetime.now().strftime("%d-%m-%y %H:%M:%S")
    with open(log_file,'a') as log:
        log.write(dt + ": " + message)


def extract(url, table_attribs):
    df = pd.DataFrame(columns=table_attribs)

    html_page = requests.get(url).text

    data = BeautifulSoup(html_page, 'html.parser')

    table_banks = data.find_all('tbody')[0].find_all('tr')
    
    for row in table_banks:
        col = row.find_all('td')
        if len(col) != 0:
            bank = col[1].find_all('a')[1].text
            market_cap = col[2].text[0:-1]
            df2 = pd.DataFrame({
                'Name': bank,
                'MC_USD_Billion': float(market_cap)
            }, index=[0])

            df = pd.concat([df, df2], ignore_index=True)

    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

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

log_file = 'code_log.txt'

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

print(extract('https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks', ["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]))
