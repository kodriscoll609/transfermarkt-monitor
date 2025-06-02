import requests
from bs4 import BeautifulSoup
import time
import random
import hashlib
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
from io import StringIO
import logging

logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for more detail
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"logs/app_{str(datetime.today().date()).replace('-', '')}.log"),      # Log to a file
        logging.StreamHandler()              # Also log to console (optional)
    ]
)


request_headers = {
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


def get_soup(url):
    
    time.sleep(random.uniform(1, 5))
    response = requests.get(url, headers=request_headers)
    if response.status_code == 200:
        return BeautifulSoup(response.text, "html.parser")
    else:
        logging.info(f"Request failed with status: {response.status_code}")
#         print(f"Request failed with status: {response.status_code}")

        return None
  
def fetch_competitions():
    
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

def createDetailedURL(url):
    return url.replace('startseite', 'kader') + r"/plus/1"

def polite_request(url):
    """Request wrapper for respectful scraping (rotating headers, delays, exponential backoff)"""
    
    USER_AGENTS = [
    # Chrome (Windows)
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"},
        
    # Firefox (Windows)
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"},

    # Edge (Windows)
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.2478.80"},

    # Chrome (Mac)
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"},

    # Safari (Mac)
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/16.4 Safari/605.1.15"},

    # Firefox (Mac)
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.3; rv:125.0) Gecko/20100101 Firefox/125.0"}
    ]
    
    user_agent = random.choice(USER_AGENTS)
    response = requests.get(url, headers=user_agent)
    time.sleep(random.uniform(4, 7))
    if response.status_code == 200:
        logging.info(f"Successfully retrieved {url}")
        return response
    
    else:
        # Exponential backoff delay
        failure_count = 1
        other_agents = [ua for ua in USER_AGENTS if ua["User-Agent"] != user_agent["User-Agent"]]
        
        while failure_count <= 3:
            new_user_agent = random.choice(other_agents)
            pause = 3**(failure_count) +  random.uniform(4, 5)
            time.sleep(pause)
            failure_count +=1

            
            new_response = requests.get(url, headers=new_user_agent)
            if new_response.status_code == 200:
                logging.info(f"Successfully retrieved {url} on attempt {failure_count}")
                return new_response

        # Return failed url on retry for logging
        logging.error(f"Failed to fetch {url} on {failure_count} retries. Status code: {response.status_code}")
        return url
    
def commit_changes(df, table_name, primary_key, db_path="transfermarkt.db"):
    
    yesterday = str((datetime.today() - timedelta(days=1)).date())

    # Get most recent hash key only
    query = f"""
    SELECT hash_key
    FROM (
        SELECT hash_key,
               ROW_NUMBER() OVER (
                   PARTITION BY {primary_key}
                   ORDER BY effective_start_date DESC
               ) as rn
        FROM {table_name}
    )
    WHERE rn = 1
    """

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        existing_hash_keys = {row[0] for row in cursor.fetchall()}
        
    # Find new records (based on hash_key or player_id)
    update_df = df[~df.hash_key.isin(existing_hash_keys)]
    logging.info(f"Adding {len(update_df)} records to {table_name}")
    update_keys = update_df[primary_key].unique()

    logging.info(f"{len(update_keys)} records being updated - effective_end_date == {yesterday}\n")
    print("Continue? (y/n)")
#     x = input()
#     if x.lower() != 'y':
#         print("Aborting update.")
#         return

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        for key in update_keys:
            update_query = f"""
            UPDATE {table_name}
            SET effective_end_date = ?
            WHERE {primary_key} = ?
              AND effective_end_date = '2099-12-31'
            """
            cursor.execute(update_query, (yesterday, key))

        # Append new records
        update_df.to_sql(table_name, conn, if_exists="append", index=False)

    logging.info(f"Update and append complete ({table_name})")

        
class Historize():
    
    """This class is used to add hash keys and effective dates to a pandas df"""
    
    def __init__(self, df, primary_key = False):
        self.df = df
        self.primary_key = primary_key

    def __hash_list(self, li):
        data = ' '.join(str(i).lower().replace(" ", "").replace("/", "") for i in li)
        return hashlib.sha256(data.encode()).hexdigest()

    def run(self):
        
        #1. Deduplicate
        self.df = self.df.drop_duplicates().reset_index(drop=True)
        
        #2. Hash data (include only effective end_date in hash key - start date will change the key every refresh)
        self.df['hash_key'] = self.df.apply(lambda row: self.__hash_list([row[i] for i in self.df.columns]), axis=1) 
        self.df['effective_end_date'] = pd.to_datetime('2099-12-31').date()
        self.df['effective_start_date'] = datetime.today().date()
        
        #3. Add ID
        if self.primary_key:
            self.df["ID"] = self.df.apply(lambda row: self.__hash_list([row[i] for i in self.primary_key]), axis=1)
            
def createDetailedURL(url):
    return url.replace('startseite', 'kader') + r"/plus/1"