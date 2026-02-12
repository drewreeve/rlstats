-- Aggregated per-player stats
CREATE VIEW IF NOT EXISTS v_player_stats AS
SELECT
    p.id AS player_id,
    p.name AS player_name,
    COUNT(*) AS matches_played,
    SUM(mp.goals) AS total_goals,
    SUM(mp.assists) AS total_assists,
    SUM(mp.saves) AS total_saves,
    SUM(mp.shots) AS total_shots
FROM match_players mp
JOIN players p ON p.id = mp.player_id
GROUP BY p.id, p.name;
