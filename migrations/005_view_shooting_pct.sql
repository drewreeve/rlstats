-- Shooting percentage per player
CREATE VIEW IF NOT EXISTS v_shooting_pct AS
SELECT
    p.id AS player_id,
    p.name AS player_name,
    SUM(mp.goals) AS total_goals,
    SUM(mp.shots) AS total_shots,
    ROUND(
        CAST(SUM(mp.goals) AS REAL)
        / NULLIF(SUM(mp.shots), 0),
        3
    ) AS shooting_pct
FROM match_players mp
JOIN players p ON p.id = mp.player_id
GROUP BY p.id, p.name;
