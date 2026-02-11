-- MVP win rate per player
CREATE VIEW IF NOT EXISTS v_mvp_win_rate AS
SELECT
    p.id AS player_id,
    p.name AS player_name,
    COUNT(*) AS mvp_matches,
    SUM(CASE WHEN m.result = 'win' THEN 1 ELSE 0 END) AS mvp_wins,
    ROUND(
        CAST(SUM(CASE WHEN m.result = 'win' THEN 1 ELSE 0 END) AS REAL)
        / COUNT(*),
        3
    ) AS mvp_win_rate
FROM matches m
JOIN players p ON p.id = m.team_mvp_player_id
WHERE m.team_mvp_player_id IS NOT NULL
GROUP BY p.id, p.name;
