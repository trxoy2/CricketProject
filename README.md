## Introduction
The job will extract and download the data locally for processing and save the data in SQLite tables. The data will then be cleaned and transformed to prepare it for end-user querying. The query results will print in terminal for preview. The job should take under 3 minutes to finish completely.

## Instructions to run job
Open git bash terminal
Navigate to the project directory: /CricketProject
Start the job by running this command: bash run.sh

## Expected output in terminal
-----Extract script running...
Downloading and extracting json files...
Processing JSON files and creating DataFrames...
Creating database tables...
-----Transform script running...
Saving cleaned tables to database...
-----SQL queries running...
Win records:
     year gender                 team_name  total_wins  win_percentage
0    2002   male               New Zealand           1           100.0
1    2003   male                 Australia          21            84.0
2    2003   male                    Canada           1            17.0
3    2003   male                   England          12            50.0
4    2003   male                     India          12            57.0
..    ...    ...                       ...         ...             ...
422  2024   male                  Scotland           5            56.0
423  2024   male              South Africa           3            50.0
424  2024   male                 Sri Lanka           8            67.0
425  2024   male      United Arab Emirates           2            25.0
426  2024   male  United States of America           7            64.0

[427 rows x 5 columns]
Top win percentages by gender in 2019:
   year  gender    team_name  win_percentage
0  2019  female    Australia           100.0
1  2019    male  Netherlands           100.0
Top strike rates in 2019:
   year       player_name  total_runs  balls_faced  strike_rate
0  2019         SN Thakur          17            7       242.86
1  2019        NP Kenjige           7            3       233.33
2  2019        PM Seelaar          32           15       213.33
3  2019     KW Richardson           6            3       200.00
4  2019             A Nao           5            3       166.67
5  2019          T Maruma          39           24       162.50
6  2019     EH Hutchinson          70           45       155.56
7  2019  Mohammad Hasnain          28           18       155.56
8  2019   Mohammad Naveed          51           33       154.55
9  2019        PWA Mulder          17           11       154.55