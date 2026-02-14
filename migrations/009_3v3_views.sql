-- Replace all stat views with 3v3-specific versions (team_size = 3)

DROP VIEW IF EXISTS v_mvp_win_rate;
CREATE VIEW IF NOT EXISTS v_mvp_win_rate_3v3 AS
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
  AND m.team_size = 3
GROUP BY p.id, p.name;

DROP VIEW IF EXISTS v_mvp_in_losses;
CREATE VIEW IF NOT EXISTS v_mvp_in_losses_3v3 AS
SELECT
    p.id AS player_id,
    p.name AS player_name,
    COUNT(*) AS loss_mvps
FROM matches m
JOIN players p ON p.id = m.team_mvp_player_id
WHERE m.result = 'loss'
  AND m.team_size = 3
GROUP BY p.id, p.name;

DROP VIEW IF EXISTS v_win_loss_by_weekday;
CREATE VIEW IF NOT EXISTS v_win_loss_by_weekday_3v3 AS
SELECT
    CASE strftime('%w', played_at)
        WHEN '0' THEN 'Sunday'
        WHEN '1' THEN 'Monday'
        WHEN '2' THEN 'Tuesday'
        WHEN '3' THEN 'Wednesday'
        WHEN '4' THEN 'Thursday'
        WHEN '5' THEN 'Friday'
        WHEN '6' THEN 'Saturday'
    END AS weekday,
    COUNT(*) AS matches_played,
    SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END) AS losses,
    ROUND(
        CAST(SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) AS REAL)
        / NULLIF(SUM(CASE WHEN result IN ('win','loss') THEN 1 ELSE 0 END), 0),
        3
    ) AS win_rate
FROM matches
WHERE result IN ('win', 'loss')
  AND played_at IS NOT NULL
  AND team_size = 3
GROUP BY strftime('%w', played_at)
ORDER BY CAST(strftime('%w', played_at) AS INTEGER);

DROP VIEW IF EXISTS v_shooting_pct;
CREATE VIEW IF NOT EXISTS v_shooting_pct_3v3 AS
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
WHERE m.team_size = 3
GROUP BY p.id, p.name;

DROP VIEW IF EXISTS v_player_stats;
CREATE VIEW IF NOT EXISTS v_player_stats_3v3 AS
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
WHERE m.team_size = 3
GROUP BY p.id, p.name;

DROP VIEW IF EXISTS v_win_loss_daily;
CREATE VIEW IF NOT EXISTS v_win_loss_daily_3v3 AS
SELECT
    DATE(played_at) AS play_date,
    SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END) AS losses,
    ROUND(
        CAST(SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) AS REAL)
        / COUNT(*),
        3
    ) AS win_rate
FROM matches
WHERE result IN ('win', 'loss')
  AND played_at IS NOT NULL
  AND team_size = 3
GROUP BY DATE(played_at)
ORDER BY play_date;
