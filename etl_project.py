import requests
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
csv_path = '/home/project/Countries_by_GDP.csv'

db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'

log_file = 'etl_project_log.txt'

def extract_from_web(url):
    df = pd.DataFrame(columns=['Country','GDP_USD_mills'])
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page,'html.parser')

    table_gdp_rows = data.find_all('tbody')[2].find_all('tr')
    
    for row in table_gdp_rows:
        col = row.find_all('td')
        if len(col) != 0:
            country = col[0].find('a')
            imf_gdp = col[2].contents[0].replace(",","")
            if country is not None and imf_gdp != '—':
                df2 = pd.DataFrame({
                    'Country':country.contents[0],
                    'GDP_USD_mills':float(imf_gdp)
                }, index = [0])
            
                df = pd.concat([df,df2], ignore_index=True)
    
    return df


def transform(data):
    data["GDP_USD_mills"] = round(data["GDP_USD_mills"] / 1000,2)
    data = data.rename(columns={"GDP_USD_mills":"GDP_USD_billion"})
    return data


def load_to_csv(data,path):
    data.to_csv(path)


def load_to_sql(data,db_name,table_name):
    conn = sqlite3.connect(db_name)
    data.to_sql(table_name,conn,if_exists='replace',index=False)
    conn.close()

def log_message(message):
    dt = datetime.now().strftime( "%d-%m-%y %H:%M:%S")
    with open(log_file,'a') as file:
        file.write(dt + ": " + message + "\n")


log_message("Pipeline start")
log_message("extract start")
data = extract_from_web(url)
log_message("extract end")
log_message("transform start")
data = transform(data)
log_message("transform end")
log_message("load start")
log_message("load in csv start")
load_to_csv(data,csv_path)
log_message("load in csv end")
log_message("load in db start")
load_to_sql(data,db_name,table_name)
log_message("load in db end")
log_message("load end")
log_message("Pipeline end")


conn = sqlite3.connect(db_name)
sql_query = f'select * from {table_name} where GDP_USD_billion > 100'
sql_res = pd.read_sql(sql_query,conn)
conn.close()
print(sql_res)
