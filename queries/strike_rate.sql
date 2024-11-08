SELECT 
    strftime('%Y', m.startdate) AS year,
    b.batter AS player_name,
    SUM(b.runs_batter) AS total_runs,
    COUNT(b.batter) AS balls_faced,
    ROUND((SUM(b.runs_batter) * 100.0 / COUNT(b.batter)), 2) AS strike_rate
FROM 
    ballbyball_publish b
JOIN 
    matches_publish m
ON 
    b.match_id = m.match_id
WHERE
    strftime('%Y', m.startdate) = '2019'
GROUP BY 
    year, b.batter
ORDER BY 
    strike_rate DESC
LIMIT 10;