import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
import requests
import sqlite3

def extract(url, table_attributes):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')

    df = pd.DataFrame(columns = table_attributes)

    tables = data.find_all('table', {"class": "wikitable"})
    rows = tables[1].find_all('tr')  # We're working wit the second table on the page, which contains the relevant data

    for row in rows[1:]:  # Skip the header row
        col = row.find_all('td')
        
        if len(col) != 0:
            data_dict = {"Company": col[2].text.strip(),
                            "Revenue ($B)": col[3].text.strip(),
                            "Employees": col[4].text.strip(),
                            "Revenue per Employee ($K)": col[5].text.strip(),
                            "Headquarters": col[6].text.strip()
                        }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df):
    df["Revenue ($B)"] = df["Revenue ($B)"].str.replace("$", "").str.replace("B", "").astype(float)
    df["Employees"] = df["Employees"].str.replace(",", "").astype(int)
    df["Revenue per Employee ($K)"] = df["Revenue per Employee ($K)"].str.replace(",", "").str.replace("K", "").str.replace('$', '').astype(float)
    df["Headquarters"] = df["Headquarters"].str.replace(r"\[.*\]", "", regex=True).str.strip()
    df["Profit Margin (%)"] = ((df["Revenue ($B)"] * 1000) / df["Employees"] * 100).round(2).astype(float)

    df.sort_values(by="Profit Margin (%)", ascending=False, inplace=True)

    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)

def load_to_sql(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace')

def run_query(sql_connection, query):
    query_output = pd.read_sql_query(query, sql_connection)
    return query_output

def log_process(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    with open("etl_log.txt", "a") as log_file:
        log_file.write(f"{timestamp} - {message}\n")


url = "https://web.archive.org/web/20230901213946/https://en.wikipedia.org/wiki/List_of_largest_technology_companies_by_revenue"
table_attributes = ["Company", "Revenue ($B)", "Employees", "Revenue per Employee ($K)", "Headquarters"]
db_name = "tech_companies.db"
table_name = "largest_tech_companies"
output_csv = "largest_tech_companies.csv"

log_process("ETL process started.")

df = extract(url, table_attributes)

log_process("Data extraction completed.")

df = transform(df)

log_process("Data transformation completed.")

load_to_csv(df, output_csv)

log_process("Data loaded to CSV completed.")

sql_connection = sqlite3.connect(db_name)

log_process("Database connection established.")

load_to_sql(df, sql_connection, table_name)

log_process("Data loaded to SQL database completed.")

query_statement = f'SELECT * FROM {table_name} ORDER BY "Revenue ($B)" DESC LIMIT 3;'
top_revenue = run_query(sql_connection, query_statement)
print("Top 3 Companies by Revenue:")
print(top_revenue, "\n")


query_statement = f'SELECT * FROM {table_name} ORDER BY "Profit Margin (%)" DESC LIMIT 3;'
top_profit = run_query(sql_connection, query_statement)
print("Top 3 Companies by Profit Margin:")
print(top_profit, "\n")


query_statement = f'SELECT * FROM {table_name} ORDER BY "Revenue per Employee ($K)" DESC LIMIT 1;'
top_rev_per_employee = run_query(sql_connection, query_statement)
print("Company with Highest Revenue per Employee:")
print(top_rev_per_employee, "\n")

log_process("ETL process completed.")
sql_connection.close()