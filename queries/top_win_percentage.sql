WITH tg AS (
    SELECT 
        strftime('%Y', startdate) AS year,
        gender,
        json_each.value AS team_name,
        COUNT(*) AS total_games
    FROM 
        matches_publish m,
        json_each(m.teams)
    WHERE 
        outcome_method IS NULL OR outcome_method != 'D/L'
    GROUP BY 
        year, gender, team_name
),
win_percentage AS (
    SELECT 
        tg.year,
        tg.gender,
        tg.team_name,
        COUNT(m.outcome_winner) AS total_wins,
        ROUND((COUNT(m.outcome_winner) * 100.0 / tg.total_games), 0) AS win_percentage
    FROM 
        matches_publish m
    JOIN 
        tg
    ON 
        strftime('%Y', m.startdate) = tg.year
        AND m.gender = tg.gender
        AND m.outcome_winner = tg.team_name
    WHERE 
        (m.outcome_method IS NULL OR m.outcome_method != 'D/L')
        AND m.outcome_winner IS NOT NULL
    GROUP BY 
        tg.year, tg.gender, tg.team_name, tg.total_games
)
SELECT 
    wp.year,
    wp.gender,
    wp.team_name,
    wp.win_percentage
FROM 
    win_percentage wp
JOIN (
    SELECT 
        gender,
        MAX(win_percentage) AS max_win_percentage
    FROM 
        win_percentage
    WHERE 
        year = '2019'
    GROUP BY 
        gender
) max_wp
ON 
    wp.gender = max_wp.gender
    AND wp.win_percentage = max_wp.max_win_percentage
WHERE 
    wp.year = '2019'
ORDER BY 
    wp.gender;