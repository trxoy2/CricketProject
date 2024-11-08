import requests
import zipfile
import os
import pandas as pd
import sqlite3
from modules.process_json import process_json_file

url = 'https://cricsheet.org/downloads/odis_json.zip'
zip_path = './downloads/odis_json.zip'
extract_dir = './data/odis_json'

print('Downloading and extracting json files...')
# Download the zip file
response = requests.get(url)
with open(zip_path, 'wb') as file:
    file.write(response.content)

# Extract the zip file
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_dir)

# Process JSON files and create DataFrames
print('Processing JSON files and creating DataFrames...')
info_dataframes = []
people_dataframes = []
innings_dataframes = []

for root, dirs, files in os.walk(extract_dir):
    for file in files:
        if file.endswith('.json'):
            file_path = os.path.join(root, file)
            match_df, players_df, ballbyball_df = process_json_file(file_path)
            if match_df is not None:
                info_dataframes.append(match_df)
            if players_df is not None:
                people_dataframes.append(players_df)
            if ballbyball_df is not None:
                innings_dataframes.append(ballbyball_df)

# Concatenate DataFrames in chunks
def concatenate_in_chunks(dfs, chunk_size=100):
    chunks = [dfs[i:i + chunk_size] for i in range(0, len(dfs), chunk_size)]
    concatenated = pd.concat([pd.concat(chunk, ignore_index=True) for chunk in chunks], ignore_index=True)
    return concatenated

# Concatenate all DataFrames
match_df = concatenate_in_chunks(info_dataframes) if info_dataframes else pd.DataFrame()
players_df = concatenate_in_chunks(people_dataframes) if people_dataframes else pd.DataFrame()
ballbyball_df = concatenate_in_chunks(innings_dataframes) if innings_dataframes else pd.DataFrame()

print('Creating database tables...')
# SQLite connection
conn = sqlite3.connect('./data/database.db')
cursor = conn.cursor()

# Create tables in the SQLite database
cursor.execute('''
CREATE TABLE IF NOT EXISTS matches_raw (
    match_id INTEGER PRIMARY KEY,
    date_added TEXT,
    data_version TEXT,
    created TEXT,
    revision INTEGER,
    balls_per_over INTEGER,
    city TEXT,
    dates TEXT,
    event_name TEXT,
    event_match_number INTEGER,
    gender TEXT,
    match_type TEXT,
    match_type_number INTEGER,
    officials_match_referees TEXT,
    officials_tv_umpires TEXT,
    officials_umpires TEXT,
    outcome_winner TEXT,
    outcome_by_runs INTEGER,
    overs INTEGER,
    player_of_match TEXT,
    season TEXT,
    team_type TEXT,
    teams TEXT,
    toss_decision TEXT,
    toss_winner TEXT,
    venue TEXT
)
''')
cursor.execute('''
CREATE TABLE IF NOT EXISTS players_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_added TEXT,
    player_name TEXT,
    player_id TEXT,
    team TEXT,
    gender TEXT,
    season TEXT
)
''')
cursor.execute('''
CREATE TABLE IF NOT EXISTS ballbyball_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_added TEXT,
    match_id INTEGER,
    team TEXT,
    over INTEGER,
    batter TEXT,
    bowler TEXT,
    extras_legbyes INTEGER,
    non_striker TEXT,
    runs_batter INTEGER,
    runs_extras INTEGER,
    runs_total INTEGER,
    event_name TEXT,
    match_number INTEGER,
    dates TEXT,
    venue TEXT,
    gender TEXT,
    FOREIGN KEY (match_id) REFERENCES matches (match_id)
)
''')
conn.commit()

# Save DataFrames to SQLite database
match_df.to_sql('matches_raw', conn, if_exists='replace', index=False)
players_df.to_sql('players_raw', conn, if_exists='replace', index=False)
ballbyball_df.to_sql('ballbyball_raw', conn, if_exists='replace', index=False)

conn.close()