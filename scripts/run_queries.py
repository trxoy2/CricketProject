import sqlite3
import pandas as pd

def run_query_from_file(db_path, sql_file_path, message=""):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)

    # Read the SQL query from the file
    with open(sql_file_path, 'r') as file:
        sql_query = file.read()

    # Execute the SQL query and fetch the results into a DataFrame
    df = pd.read_sql_query(sql_query, conn)

    # Print the message and the DataFrame with column headers
    print(message)
    if not df.empty:
        print(df)
    else:
        print("No results found.")

    # Close the connection
    conn.close()

run_query_from_file('./data/database.db', './queries/win_records.sql',  message="Win records:")

run_query_from_file('./data/database.db', './queries/top_win_percentage.sql',  message="Top win percentages by gender in 2019:")

run_query_from_file('./data/database.db', './queries/strike_rate.sql',  message="Top strike rates in 2019:")