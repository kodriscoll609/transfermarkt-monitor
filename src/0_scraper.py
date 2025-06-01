from utilities import fetch_competitions, request_headers, Historize, commit_changes, createDetailedURL, polite_request
from sql_shortcuts import table, append_table, load_keys, drop_table, tables, replace_table
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from io import StringIO
import pandas as pd
import sqlite3
import hashlib
import requests
import time
import random
import logging

#1. LEAGUES
# Load retrieved league data
logging.info("FETCHING LEAGUES")
leagues_history = table("b1_league")

# Scrape new league data
league_data = fetch_competitions()
leagues =  pd.DataFrame(league_data, columns =  ['name', 'league_link'])
leagues = pd.merge(leagues.assign(joined =  1), leagues_history, on =  ['name', 'league_link'], how = 'outer')
leagues = leagues[leagues.joined != 1]

#Update b1_league
if len(leagues > 0):
    logging.info(f"Committing {len(leagues)} league records")
    leagues = leagues.drop("joined", axis = 1)
    append_table(leagues, league)
else:
    logging.info("League table up-to-date")
    
#2. CLUBS
logging.info("FETCHING CLUBS")

leagues = table("b1_league")
league_urls = list(leagues.league_link.unique())
club_df = pd.DataFrame()
b2_failures = []


#Scrape clubs
for url in league_urls:

    response = polite_request(url)
    if not isinstance(response, str):
        data = pd.read_html(StringIO(response.text))
        df = [i for i in data if 'total market value' in str(i).lower()][0]
        df["league_link"] = url
        club_df = pd.concat([club_df, df])
    else:
        b2_failures.append(url)

# deduplicate, clean columns
club_df = club_df.reset_index(drop = True)
club_df = club_df.dropna(subset = 'Club.1')
selection = ['Club.1', 'league_link']
club_df = club_df[selection].drop_duplicates().rename({"Club.1" : "club"}, axis = 1)

if len(b2_failures)>0:
    logging.error(f"b2_club failures: {b2_failures}")
    
processor = Historize(
    club_df,
    primary_key = ['league_link', 'club']
)

processor.run()

club_updates = processor.df

club_updates['last_updated'] = datetime.today()
club_updates = club_updates.rename({"ID": "club_id"}, axis = 1)
selection = ["club_id", 'club', 'league_link', 'hash_key', 'effective_start_date', 'effective_end_date', 'last_updated']
club_updates = club_updates[selection] 

commit_changes(
    df = club_updates, 
    table_name = 'b2_club', 
    primary_key= 'club_id'
)

#3. CLUB LINKS
logging.info("FETCHING CLUB LINKS")
league_urls = table('b2_club')['league_link'].unique()
club_links = []

for idx, league_url in enumerate(league_urls):
        
    response = requests.get(league_url, headers=request_headers)
    soup = BeautifulSoup(response.content, "html.parser")

    # Find the table containing the teams
    temp = soup.select('table.items tbody tr')
    for row in soup.select('table.items tbody tr'):
        link_tag = row.select_one('td.hauptlink a')
        if link_tag:
            club_name = link_tag.text.strip()
            relative_link = link_tag['href']
            #filter out top scorer links
            if r"profil/spieler" not in relative_link:
                full_link = "https://www.transfermarkt.com" + relative_link
                club_links.append((league_url, club_name, full_link))

# deduplicate / filter links
club_links_df = pd.DataFrame(club_links, columns = ['league_link', 'club', 'club_link'])
club_links_df = club_links_df[club_links_df.club_link.str.contains("start")] 

processor = Historize(
    club_links_df,
    primary_key = ['league_link', 'club']
)

processor.run()
updates = processor.df
updates['last_updated'] = datetime.today()
updates = updates.rename({"ID": "club_link_id"}, axis = 1)
selection = ['club_link_id', 'league_link', 'club', 'club_link', 'hash_key', 'effective_start_date', 'effective_end_date', 'last_updated']
updates = updates[selection]

commit_changes(
    df = updates, 
    table_name = 'b3_club_link', 
    primary_key='club_link_id'
)

#4. PLAYERS
logging.info("FETCHING PLAYERS")
club_links = table("b3_club_link")
club_links["club_link_detailed"] = club_links['club_link'].apply(lambda x: createDetailedURL(x))
urls = club_links.club_link_detailed.unique()

start_time = time.time()

conn = sqlite3.connect("transfermarkt.db")
cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS b4_player_staging")
conn.commit()
conn.close()

b4_failures = []
for idx, url in enumerate(urls):
    
    response = polite_request(url)

    if not isinstance(response, str):
        data = pd.read_html(StringIO(response.text))
        players = [i for i in data if 'Contract' in i.columns][0].dropna(subset = "#").copy()
        players['club_link_detailed'] = url
        logging.info(f"Adding {len(players)} to staging: {url}")
        append_table(players, "b4_player_staging")
        
    else:
        b4_failures.append(url)
        
logging.info(f"{round((time.time() - start_time)/60)} minutes to process")

if len(b4_failures)>0:
    logging.error(f"b4_player failures: {b4_failures}")
    
for idx, url in enumerate(new_urls):
    
    response = polite_request(url)

    if not isinstance(response, str):
        data = pd.read_html(StringIO(response.text))
        players = [i for i in data if 'Contract' in i.columns][0].dropna(subset = "#").copy()
        players['club_link_detailed'] = url
        logging.info(f"Adding {len(players)} to staging: {url}")
        append_table(players, "b4_player_staging")
        
    else:
        b4_failures.append(url)
        
logging.info(f"{round((time.time() - start_time)/60)} minutes to process")

if len(b4_failures)>0:
    logging.error(f"b4_player failures: {b4_failures}")
    
#Staging cleaning
players = table("b4_player_staging")
players = players.dropna(subset = "#")
mapping = {'#':'number', 'Date of birth/Age':'age'}
players = players.rename(mapping, axis = 1)
players.columns = [i.lower().strip().replace(" ", "_") for i in players.columns]
players = players.drop(["nat.", "signed_from"], axis = 1)

player_historizer = Historize(
    players,
    primary_key = ['player', 'club_link_detailed']
)

player_historizer.run()
player_updates = player_historizer.df
player_updates = player_updates.rename({"ID": "player_id"}, axis = 1)
player_updates['last_updated'] = datetime.today()

commit_changes(
    df = player_updates, 
    table_name = 'b4_player', 
    primary_key='player_id'
)