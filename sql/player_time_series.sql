-- name: player_time_series(player_name, game_mode)
-- Per-session-day time series stats for a single tracked player.
WITH player_matches AS (
    SELECT
        m.id AS match_id,
        m.played_at,
        m.team_mvp_player_id,
        p.id AS player_id,
        mp.goals,
        mp.assists,
        mp.saves,
        mp.shots,
        mp.score,
        mp.avg_speed,
        LAG(m.played_at) OVER (ORDER BY m.played_at) AS prev_played_at
    FROM match_players mp
    JOIN matches m ON m.id = mp.match_id
    JOIN players p ON p.id = mp.player_id
    WHERE p.name = :player_name
      AND p.is_tracked = 1
      AND m.game_mode = :game_mode
      AND m.played_at IS NOT NULL
),
session_markers AS (
    SELECT *,
        CASE
            WHEN prev_played_at IS NULL THEN 1
            WHEN (julianday(played_at) - julianday(prev_played_at)) * 24 * 60 > 60 THEN 1
            ELSE 0
        END AS new_session
    FROM player_matches
),
sessions AS (
    SELECT *,
        SUM(new_session) OVER (ORDER BY played_at) AS session_id
    FROM session_markers
)
SELECT
    DATE(MIN(played_at)) AS date,
    SUM(goals) AS goals,
    SUM(assists) AS assists,
    SUM(saves) AS saves,
    SUM(shots) AS shots,
    ROUND(AVG(CAST(score AS REAL)), 1) AS avg_score,
    SUM(CASE WHEN team_mvp_player_id = player_id THEN 1 ELSE 0 END) AS mvp_count,
    ROUND(CAST(SUM(goals) AS REAL) / NULLIF(SUM(shots), 0) * 100, 1) AS shooting_pct,
    ROUND(AVG(avg_speed), 1) AS avg_speed
FROM sessions
GROUP BY session_id
ORDER BY date;

-- name: player_career_stats(player_name, game_mode)^
-- Career totals for a single tracked player.
SELECT
    p.name AS player,
    COUNT(*) AS matches,
    SUM(mp.goals) AS goals,
    SUM(mp.assists) AS assists,
    SUM(mp.saves) AS saves,
    SUM(mp.shots) AS shots,
    SUM(mp.demos) AS demos,
    ROUND(AVG(CAST(mp.score AS REAL)), 1) AS avg_score,
    ROUND(CAST(SUM(mp.goals) AS REAL) / NULLIF(SUM(mp.shots), 0) * 100, 1) AS shooting_pct,
    SUM(CASE WHEN m.team_mvp_player_id = p.id THEN 1 ELSE 0 END) AS mvp_count,
    SUM(CASE WHEN m.result = 'win' THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN m.result = 'loss' THEN 1 ELSE 0 END) AS losses
FROM match_players mp
JOIN players p ON p.id = mp.player_id
JOIN matches m ON m.id = mp.match_id
WHERE p.name = :player_name
  AND p.is_tracked = 1
  AND m.game_mode = :game_mode
GROUP BY p.id, p.name;
