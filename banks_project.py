# Code for ETL operations on Country-GDP data

# Importing the required libraries
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import requests
import sqlite3

def log_progress(message):
    dt = datetime.now().strftime("%d-%m-%y %H:%M:%S")
    with open(log_file,'a') as log:
        log.write(dt + ": " + message + "\n")


def extract(url, table_attribs):
    df = pd.DataFrame(columns=table_attribs)

    html_page = requests.get(url, headers={'User-Agent': 'CoolBot/0.0 (https://example.org/coolbot/; coolbot@example.org) generic-library/0.0'}).text

    data = BeautifulSoup(html_page, 'html.parser')

    table_banks = data.find_all('tbody')[0].find_all('tr')
    
    for row in table_banks:
        col = row.find_all('td')
        if len(col) != 0:
            bank = col[1].find_all('a')[1].text
            market_cap = col[2].text[0:-1].replace(',','')
            df2 = pd.DataFrame({
                'Name': bank,
                'MC_USD_Billion': float(market_cap)
            }, index=[0])

            df = pd.concat([df, df2], ignore_index=True)

    return df

def transform(df, csv_path):
    exchange_rate_file = pd.read_csv(csv_path)
    rates_dict = exchange_rate_file.set_index('Currency').to_dict()['Rate']
    
    df["MC_GBP_Billion"] = round(df["MC_USD_Billion"] * float(rates_dict["GBP"]),2)
    df["MC_EUR_Billion"] = round(df["MC_USD_Billion"] * float(rates_dict["EUR"]),2)
    df["MC_INR_Billion"] = round(df["MC_USD_Billion"] * float(rates_dict["INR"]),2)
    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    print(pd.read_sql(query_statement, sql_connection))
    print('\n')

bank_info_src_website = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
output_file_fields = ["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]
output_csv = 'Largest_banks_data.csv'
log_file = 'code_log.txt'
exchange_rate_file = 'exchange_rate.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'

log_progress('Preliminaries complete. Initiating ETL process')
df = extract(bank_info_src_website, output_file_fields)

log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df, exchange_rate_file)

log_progress('Data transformation complete. Initiating Loading process')
load_to_csv(df, output_csv)
log_progress('Data saved to CSV file')

sql_conn = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')

load_to_db(df, sql_conn, table_name)
log_progress('Data loaded to Database as a table, Executing queries')
run_query('SELECT * FROM Largest_banks',sql_conn)
run_query('SELECT AVG(MC_GBP_Billion) FROM Largest_banks',sql_conn)
run_query('SELECT Name from Largest_banks LIMIT 5',sql_conn)
log_progress('Process Complete')

sql_conn.close()
log_progress('Server Connection closed')
