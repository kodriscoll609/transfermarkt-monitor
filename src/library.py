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


def hash_row(row):
    # Select only non-metadata fields
    selection = [col for col in row.index if col not in ['hash_key', 'effective_start_date', 'effective_end_date']]
    row_string = ''.join(str(row[col]) for col in selection)
    return hashlib.sha256(row_string.encode()).hexdigest()


def historize(df):
    
    # hash row
    def hash_row(row):
        
        #exclude metadata fields
        selection = [col for col in row.index if col not in ['hash_key', 'effective_start_date', 'effective_end_date']]
        selection = [i for i in selection if ~i.endswith("_ID")]
        row_string = ''.join(str(row[col]) for col in selection)
        return hashlib.sha256(row_string.encode()).hexdigest()
    
    df['hash_key'] = df.apply(hash_row, axis=1) 
    df = df.drop_duplicates(subset=["hash_key"]).reset_index(drop=True)
    
    # Set validity dates
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

def get_clubs(url):

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
        
class Historize():
    
    """This class is used to add hash keys and effective dates to a pandas df"""
    
    def __init__(self, df, primary_key = False):
        self.df = df
        self.primary_key = primary_key

    def __hash_list(self, li):
        data = ' '.join(str(i) for i in li)
        return hashlib.sha256(data.encode()).hexdigest()

    def run(self):
        
        #1. Deduplicate
        self.df = self.df.drop_duplicates().reset_index(drop=True)
        
        #2. Hash data (include only effective end_date in hash key - start date will change the key every refresh)
        self.df['effective_end_date'] = pd.to_datetime('2099-12-31').date()
        self.df['hash_key'] = self.df.apply(lambda row: self.__hash_list([row[i] for i in self.df.columns]), axis=1) 
        self.df['effective_start_date'] = datetime.today().date()
        
        #3. Add ID
        if self.primary_key:
            self.df["ID"] = self.df.apply(lambda row: self.__hash_list([row[i] for i in self.primary_key]), axis=1)

#sqlite3 Functions

def table(table, db = "transfermarkt.db"):
    
    """Shortcut to load a sqlite3 table"""
    
    conn = sqlite3.connect(db)
    
    query = f"SELECT * FROM {table}"
    df = pd.read_sql_query(query, conn)

    conn.close()
    
    return df

def tables(db_path = "transfermarkt.db"):
    
    """This function lists all of the tables in a given database"""
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    return tables

def drop_table(table_name, db_path = "transfermarkt.db"):
    
    """This function removes a table from the given database"""
    
    print(f"This action will permanently delete {table_name}. Continue? (y/n)")
    x = input()
    if x == "y":
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()

        print(f"Table '{table_name}' dropped successfully (if it existed).")

        conn.close()
    else:
        print("Aborted")
    
def load_keys(table, primary_key, db = "transfermarkt.db"):
    
    """ 
    This function loads primary kays and hash_keys from a given table.
    It's used to check for updates during historization.
    """
    conn = sqlite3.connect(db)

    # SQL query to fetch data
    query = f"SELECT DISTINCT {', '.join(primary_key)}, hash_key FROM {table}"

    # Load data into a DataFrame
    df = pd.read_sql_query(query, conn).assign(joined = 1).drop_duplicates().rename({"hash_key":"hash_key_current"}, axis = 1)

    # Close the connection
    conn.close()
    
    return df

def replace_table(table_name, df, db = "transfermarkt.db"):
    
    """The function drops (if exists) and replaces a table in the database"""
    
    print("Are you sure you want to overwrite this table? This will overwrite the existing data permanently (y/n)")
    x = input()
    
    if x == 'y':
        conn = sqlite3.connect(db)

        # Save DataFrame to a new table (schema) named "players"
        df.to_sql(table_name, conn, if_exists="replace", index=False)

        # Close connection
        conn.close()
    
def append_table(df, table_name, db = "transfermarkt.db"):
    
    """This function appends data to an existing db"""
    
    conn = sqlite3.connect(db)

    # Save DataFrame to a new table (schema) named "players"
    df.to_sql(table_name, conn, if_exists="append", index=False)

    # Close connection
    conn.close()