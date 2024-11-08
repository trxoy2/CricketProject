WITH team_games AS (
    SELECT 
        strftime('%Y', startdate) AS year,
        gender,
        json_each.value AS team_name,
        COUNT(*) AS total_games
    FROM 
        matches_publish,
        json_each(matches_publish.teams)
    WHERE 
        outcome_method IS NULL OR outcome_method != 'D/L'
    GROUP BY 
        year, gender, team_name
)
SELECT 
    team_games.year,
    team_games.gender,
    team_games.team_name,
    COUNT(matches_publish.outcome_winner) AS total_wins,
    ROUND((COUNT(matches_publish.outcome_winner) * 100.0 / team_games.total_games), 0) AS win_percentage
FROM 
    matches_publish matches_publish
JOIN 
    team_games team_games
ON 
    strftime('%Y', matches_publish.startdate) = team_games.year
    AND matches_publish.gender = team_games.gender
    AND matches_publish.outcome_winner = team_games.team_name
WHERE 
    (matches_publish.outcome_method IS NULL OR matches_publish.outcome_method != 'D/L')
    AND matches_publish.outcome_winner IS NOT NULL
GROUP BY 
    team_games.year, team_games.gender, team_games.team_name, team_games.total_games
ORDER BY 
    team_games.year, team_games.gender, team_games.team_name;