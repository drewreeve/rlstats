-- 2v2 views

CREATE VIEW IF NOT EXISTS v_mvp_win_rate_2v2 AS
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
  AND m.game_mode = '2v2'
GROUP BY p.id, p.name;

CREATE VIEW IF NOT EXISTS v_mvp_in_losses_2v2 AS
SELECT
    p.id AS player_id,
    p.name AS player_name,
    COUNT(*) AS loss_mvps
FROM matches m
JOIN players p ON p.id = m.team_mvp_player_id
WHERE m.result = 'loss'
  AND m.game_mode = '2v2'
GROUP BY p.id, p.name;

CREATE VIEW IF NOT EXISTS v_shooting_pct_2v2 AS
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
JOIN matches m ON m.id = mp.match_id
WHERE m.game_mode = '2v2'
GROUP BY p.id, p.name;

CREATE VIEW IF NOT EXISTS v_player_stats_2v2 AS
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
JOIN matches m ON m.id = mp.match_id
WHERE m.game_mode = '2v2'
GROUP BY p.id, p.name;

-- Hoops views

CREATE VIEW IF NOT EXISTS v_mvp_win_rate_hoops AS
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
  AND m.game_mode = 'hoops'
GROUP BY p.id, p.name;

CREATE VIEW IF NOT EXISTS v_mvp_in_losses_hoops AS
SELECT
    p.id AS player_id,
    p.name AS player_name,
    COUNT(*) AS loss_mvps
FROM matches m
JOIN players p ON p.id = m.team_mvp_player_id
WHERE m.result = 'loss'
  AND m.game_mode = 'hoops'
GROUP BY p.id, p.name;

CREATE VIEW IF NOT EXISTS v_shooting_pct_hoops AS
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
JOIN matches m ON m.id = mp.match_id
WHERE m.game_mode = 'hoops'
GROUP BY p.id, p.name;

CREATE VIEW IF NOT EXISTS v_player_stats_hoops AS
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
JOIN matches m ON m.id = mp.match_id
WHERE m.game_mode = 'hoops'
GROUP BY p.id, p.name;
