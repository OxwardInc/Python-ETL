import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
import requests


def extract(url, table_attributes):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')

    df = pd.DataFrame(columns = table_attributes)

    tables = data.find_all('table')
    rows = tables[1].find_all('tr')  # We're working wit the second table on the page, which contains the relevant data

    for row in rows[1:]:  # Skip the header row
        col = row.find_all('td')
        
        if len(col) != 0:
            if col[0].find('a') is not None:
                data_dict = {"Company": col[1].a.contents[0],
                             "Revenue ($B)": col[2].contents[0],
                             "Employees": col[3].contents[0],
                             "Revenue per Employee ($K)": col[4].contents[0],
                             "Headquarters": col[5].contents[0]
                            }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    return df


url = "https://web.archive.org/web/20230901213946/https://en.wikipedia.org/wiki/List_of_largest_technology_companies_by_revenue"
table_attributes = ["Company", "Revenue ($B)", "Employees", "Revenue per Employee ($K)", "Headquarters"]
df = extract(url, table_attributes)
print(df.head())