import os
import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor
from modules.convert_to_jsonstring import convert_to_jsonstring

def process_json_file(file_path):
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            if data:  # Check if the file is not empty
                # Extract unique match attributes
                match_info = data['info']
                event_info = match_info.get('event', {})
                dates_str = ', '.join(match_info['dates'])  # Convert dates list to a consistent string representation
                match_identifier = {
                    'event_name': event_info.get('name', ''),
                    'event_match_number': event_info.get('match_number', ''),
                    'dates': dates_str,
                    'venue': match_info.get('venue', '')
                }

                # Normalize the 'info' part of the JSON without the 'players' and 'registry' keys
                match_df = pd.json_normalize(
                    {k: v for k, v in match_info.items() if k not in ['players', 'registry']},
                    sep='_'
                )
                match_df['dates'] = dates_str  # Add dates column to match_df

                # Create a mapping of player names to their respective teams using a list comprehension
                player_team_mapping = {
                    player: team for team, players in match_info['players'].items() for player in players
                }

                # Create a separate DataFrame for the people dictionary
                players_df = pd.DataFrame(
                    list(match_info['registry']['people'].items()), 
                    columns=['player_name', 'player_id']
                )

                # Add the team, gender, and season each player plays for to the players_df DataFrame
                players_df['team'] = players_df['player_name'].map(player_team_mapping)
                players_df['gender'] = match_info['gender']
                players_df['season'] = match_info['season']

                # Normalize the 'innings' part of the JSON and include match identifier and over number
                ballbyball_df = pd.json_normalize(
                    data['innings'], 
                    record_path=['overs', 'deliveries'], 
                    meta=['team', ['overs', 'over']], 
                    sep='_'
                )

                # Rename the 'overs_over' column to 'over'
                ballbyball_df.rename(columns={'overs_over': 'over'}, inplace=True)

                # Add match identifier columns and gender to the ballbyball_df
                for key, value in match_identifier.items():
                    ballbyball_df[key] = value
                ballbyball_df['gender'] = match_info['gender']

                # Convert lists and dictionaries to JSON strings in the DataFrames
                match_df = convert_to_jsonstring(match_df)
                players_df = convert_to_jsonstring(players_df)
                ballbyball_df = convert_to_jsonstring(ballbyball_df)

                return match_df, players_df, ballbyball_df
    except json.JSONDecodeError:
        print(f"Error decoding JSON from file: {file_path}")
    except ValueError as e:
        print(f"Value error for file: {file_path}: {e}")
    return None, None, None