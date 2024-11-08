import pandas as pd
import json
import sqlite3
import time

# Connect to the SQLite database
conn = sqlite3.connect('./data/database.db')

# Read the raw tables into DataFrames
match_df = pd.read_sql_query("SELECT * FROM matches_raw", conn)
players_df = pd.read_sql_query("SELECT * FROM players_raw", conn)
ballbyball_df = pd.read_sql_query("SELECT * FROM ballbyball_raw", conn)

# Close the connection
conn.close()

# Remove supersubs columns from match_df
match_df = match_df.drop(columns=[col for col in match_df.columns if col.startswith('supersubs')], errors='ignore')

# Add date_added column to each DataFrame
current_time = time.strftime('%Y-%m-%d %H:%M:%S')
match_df.insert(0, 'date_added', current_time)
players_df.insert(0, 'date_added', current_time)
ballbyball_df.insert(0, 'date_added', current_time)

# Add primary key column to match_df
match_df.insert(0, 'match_id', range(1, len(match_df) + 1))
# update type of columns to string event_match_number
match_df['event_match_number'] = match_df['event_match_number'].astype(str)
ballbyball_df['event_match_number'] = ballbyball_df['event_match_number'].astype(str)
# merge primary key column to ballbyball_df
ballbyball_df = ballbyball_df.merge(match_df[['match_id', 'venue', 'dates']], on=['venue', 'dates'], how='left')

# Convert dates from JSON string to list
def parse_dates(dates):
    try:
        # Try to parse as JSON
        return json.loads(dates)
    except json.JSONDecodeError:
        # If JSON parsing fails, split by comma
        return [date.strip() for date in dates.split(',')]

match_df['dates'] = match_df['dates'].apply(parse_dates)

# Create startdate and enddate columns from dates column
def extract_start_end_dates(dates):
    if isinstance(dates, list) and dates:
        try:
            startdate = pd.to_datetime(dates[0], errors='coerce').strftime('%Y-%m-%d')
        except Exception as e:
            print(f"Error parsing startdate: {dates[0]} - {e}")
            startdate = None
        try:
            enddate = pd.to_datetime(dates[-1], errors='coerce').strftime('%Y-%m-%d')
        except Exception as e:
            print(f"Error parsing enddate: {dates[-1]} - {e}")
            enddate = None
        return startdate, enddate
    return None, None

match_df[['startdate', 'enddate']] = match_df['dates'].apply(lambda x: pd.Series(extract_start_end_dates(x)))

match_df.drop(columns=['dates'], inplace=True)

# Save the cleaned DataFrames to a SQLite database
print('Saving cleaned tables to database...')
conn = sqlite3.connect('./data/database.db')
cursor = conn.cursor()

# Create the matches table
cursor.execute('''
CREATE TABLE IF NOT EXISTS matches_publish (
    match_id INTEGER PRIMARY KEY,
    date_added TEXT,
    balls_per_over INTEGER,
    city TEXT,
    gender TEXT,
    match_type TEXT,
    match_type_number INTEGER,
    overs INTEGER,
    player_of_match TEXT,
    season TEXT,
    team_type TEXT,
    teams TEXT,
    venue TEXT,
    event_match_number TEXT,
    event_name TEXT,
    officials_match_referees TEXT,
    officials_reserve_umpires TEXT,
    officials_tv_umpires TEXT,
    officials_umpires TEXT,
    outcome_by_runs INTEGER,
    outcome_winner TEXT,
    toss_decision TEXT,
    toss_winner TEXT,
    outcome_by_wickets INTEGER,
    event_sub_name TEXT,
    outcome_result TEXT,
    missing TEXT,
    outcome_method TEXT,
    event_group TEXT,
    event_stage TEXT,
    outcome_eliminator TEXT,
    startdate TIMESTAMP,
    enddate TIMESTAMP
)
''')

# Create the players_publish table
cursor.execute('''
CREATE TABLE IF NOT EXISTS players_publish (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_added TEXT,
    player_name TEXT,
    player_id TEXT,
    team TEXT,
    gender TEXT,
    season TEXT
)
''')

# Create the ballbyball table
cursor.execute('''
CREATE TABLE IF NOT EXISTS ballbyball_publish (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_added TEXT,
    batter TEXT,
    bowler TEXT,
    non_striker TEXT,
    runs_batter INTEGER,
    runs_extras INTEGER,
    runs_total INTEGER,
    extras_wides INTEGER,
    wickets INTEGER,
    runs_non_boundary INTEGER,
    review_by TEXT,
    review_umpire TEXT,
    review_batter TEXT,
    review_decision TEXT,
    extras_legbyes INTEGER,
    extras_noballs INTEGER,
    extras_byes INTEGER,
    replacements_role TEXT,
    team TEXT,
    over INTEGER,
    event_name TEXT,
    event_match_number TEXT,
    dates TEXT,
    venue TEXT,
    gender TEXT,
    extras_penalty INTEGER,
    review_umpires_call TEXT,
    replacements_match TEXT,
    review_type TEXT,
    match_id INTEGER,
    FOREIGN KEY (match_id) REFERENCES matches_publish (match_id)
)
''')

# Create tables in the SQLite database
match_df.to_sql('matches_publish', conn, if_exists='replace', index=False)
players_df.to_sql('players_publish', conn, if_exists='replace', index=False)
ballbyball_df.to_sql('ballbyball_publish', conn, if_exists='replace', index=False)