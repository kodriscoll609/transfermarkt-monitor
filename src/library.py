import requests
from bs4 import BeautifulSoup
import time
import random
import hashlib
from datetime import datetime
import pandas as pd
import sqlite3

from io import StringIO

    
#Create headers to circumvent non-browser activity blocking (pd.read_html blocked)
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
}

#int'l tournaments without market-value data
blacklist_league_codes = {
    "CL", "EL", "UCOL", "CLI", "AFCL", "ACL", "CCL", "KLUB",
    "EM24", "EMQ", "CAM4", "WMQ4", "WM22", "AM23", "AFCN",
    "GC23", "19YL", "CLIY", "berater"
}

def table(table, db = "transfermarkt.db"):
    conn = sqlite3.connect(db)
    
    query = f"SELECT * FROM {table}"
    df = pd.read_sql_query(query, conn)

    conn.close()
    
    return df

def append_table(df, table_name, db = "transfermarkt.db"):
    conn = sqlite3.connect(db)

    # Save DataFrame to a new table (schema) named "players"
    df.to_sql(table_name, conn, if_exists="append", index=False)

    # Close connection
    conn.close()

def hash_row(row):
    row_string = ''.join(str(value) for value in row.values)
    return hashlib.sha256(row_string.encode()).hexdigest()

def historizer(df):
    df['hash_key'] = df.apply(hash_row, axis=1)
    df = df.drop_duplicates(subset = ["hash_key"]).reset_index(drop = True)
    df['effective_start_date'] = datetime.today().date()
    df['effective_end_date'] = pd.to_datetime('2099-12-31').date()
    return df

def get_soup(url):
    time.sleep(random.uniform(1, 5))
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return BeautifulSoup(response.text, "html.parser")
    else:
        print(f"Request failed with status: {response.status_code}")
        return None
  
def get_competitions():
    url = "https://www.transfermarkt.com/wettbewerbe/national"
    soup = get_soup(url)
    if not soup:
        return []

    competitions = []

    # Look for all <ul class="tm-button-list"> sections
    sections = soup.find_all("ul", class_="tm-button-list")
    for section in sections:
        links = section.find_all("a", class_="tm-button-list__list-item")
        for link in links:
            name = link.get("title")
            href = link.get("href")
            if name and href and 'agent' not in name.lower() and href.split("/")[-1] not in blacklist_league_codes:
                full_url = f"https://www.transfermarkt.com{href}"
                competitions.append((name, full_url))

    return competitions

def get_teams(url):
    print(url)
    response = requests.get(url, headers=headers)
    time.sleep(random.uniform(1, 5))
    
    if response.status_code == 200:
        
        try:
            data = pd.read_html(StringIO(response.text))
            if len(data) > 0:
                df = [i for i in data if 'total market value' in str(i).lower()][0]
                df["link"] = url
                return df
        
        except:
            print(f"Table not found: {url}")
    else:
        print(f"Request failed with status code {response.status_code} ({url})")
        
def load_keys(table, primary_key, db = "transfermarkt.db"):
    conn = sqlite3.connect(db)

    # SQL query to fetch data
    query = f"SELECT DISTINCT {', '.join(primary_key)}, hash_key FROM {table}"

    # Load data into a DataFrame
    df = pd.read_sql_query(query, conn)

    # Close the connection
    conn.close()
    
    return df
