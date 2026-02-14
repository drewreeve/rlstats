-- Average score per player per game mode

CREATE VIEW IF NOT EXISTS v_avg_score_3v3 AS
SELECT
    p.id AS player_id,
    p.name AS player_name,
    COUNT(*) AS matches_played,
    SUM(mp.score) AS total_score,
    ROUND(
        CAST(SUM(mp.score) AS REAL) / COUNT(*),
        1
    ) AS avg_score
FROM match_players mp
JOIN players p ON p.id = mp.player_id
JOIN matches m ON m.id = mp.match_id
WHERE m.team_size = 3
GROUP BY p.id, p.name;

CREATE VIEW IF NOT EXISTS v_avg_score_2v2 AS
SELECT
    p.id AS player_id,
    p.name AS player_name,
    COUNT(*) AS matches_played,
    SUM(mp.score) AS total_score,
    ROUND(
        CAST(SUM(mp.score) AS REAL) / COUNT(*),
        1
    ) AS avg_score
FROM match_players mp
JOIN players p ON p.id = mp.player_id
JOIN matches m ON m.id = mp.match_id
WHERE m.game_mode = '2v2'
GROUP BY p.id, p.name;

CREATE VIEW IF NOT EXISTS v_avg_score_hoops AS
SELECT
    p.id AS player_id,
    p.name AS player_name,
    COUNT(*) AS matches_played,
    SUM(mp.score) AS total_score,
    ROUND(
        CAST(SUM(mp.score) AS REAL) / COUNT(*),
        1
    ) AS avg_score
FROM match_players mp
JOIN players p ON p.id = mp.player_id
JOIN matches m ON m.id = mp.match_id
WHERE m.game_mode = 'hoops'
GROUP BY p.id, p.name;
