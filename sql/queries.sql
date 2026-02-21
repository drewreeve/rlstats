-- name: win_loss_daily(game_mode)
-- Win/loss record aggregated by session date for a given game mode.
SELECT
    session_date AS play_date,
    SUM(wins) AS wins,
    SUM(losses) AS losses,
    ROUND(
        CAST(SUM(wins) AS REAL) / NULLIF(SUM(wins) + SUM(losses), 0),
        3
    ) AS win_rate
FROM v_session_summary
WHERE game_mode = :game_mode
GROUP BY session_date
ORDER BY play_date;

-- name: shooting_pct(game_mode)
-- Shooting percentage per player for a given game mode.
SELECT
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
WHERE m.game_mode = :game_mode
GROUP BY p.id, p.name
ORDER BY p.name;

-- name: player_stats(game_mode)
-- Aggregated per-player stats for a given game mode.
SELECT
    p.name AS player_name,
    COUNT(*) AS matches_played,
    SUM(mp.goals) AS total_goals,
    SUM(mp.assists) AS total_assists,
    SUM(mp.saves) AS total_saves,
    SUM(mp.shots) AS total_shots
FROM match_players mp
JOIN players p ON p.id = mp.player_id
JOIN matches m ON m.id = mp.match_id
WHERE m.game_mode = :game_mode
GROUP BY p.id, p.name
ORDER BY p.name;

-- name: mvp_wins(game_mode)
-- MVP win rate per player for a given game mode.
SELECT
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
  AND m.game_mode = :game_mode
GROUP BY p.id, p.name
ORDER BY p.name;

-- name: mvp_losses(game_mode)
-- MVP appearances in losses per player for a given game mode.
SELECT
    p.name AS player_name,
    COUNT(*) AS loss_mvps
FROM matches m
JOIN players p ON p.id = m.team_mvp_player_id
WHERE m.result = 'loss'
  AND m.game_mode = :game_mode
GROUP BY p.id, p.name
ORDER BY p.name;

-- name: avg_score(game_mode)
-- Average score per player for a given game mode.
SELECT
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
WHERE m.game_mode = :game_mode
GROUP BY p.id, p.name
ORDER BY p.name;

-- name: score_differential(game_mode)
-- Score differential distribution for a given game mode.
SELECT
    team_score - opponent_score AS differential,
    COUNT(*) AS match_count
FROM matches
WHERE result IN ('win', 'loss')
  AND game_mode = :game_mode
GROUP BY differential
ORDER BY differential;

-- name: streaks(game_mode)
-- Longest win and loss streaks for a given game mode.
WITH ordered AS (
    SELECT
        result,
        played_at,
        ROW_NUMBER() OVER (ORDER BY played_at) AS rn,
        ROW_NUMBER() OVER (PARTITION BY result ORDER BY played_at) AS grp
    FROM matches
    WHERE result IN ('win', 'loss')
      AND game_mode = :game_mode
),
islands AS (
    SELECT result, COUNT(*) AS streak_len
    FROM ordered
    GROUP BY result, rn - grp
)
SELECT
    MAX(CASE WHEN result = 'win' THEN streak_len ELSE 0 END) AS longest_win_streak,
    MAX(CASE WHEN result = 'loss' THEN streak_len ELSE 0 END) AS longest_loss_streak
FROM islands;

-- name: avg_goal_contribution(game_mode)
-- Average goal contribution per player for a given game mode.
SELECT
    p.name AS player_name,
    COUNT(*) AS matches_played,
    ROUND(
        AVG(
            CAST(mp.goals + mp.assists AS REAL)
            / NULLIF(m.team_score, 0)
        ),
        3
    ) AS avg_goal_contribution
FROM match_players mp
JOIN players p ON p.id = mp.player_id
JOIN matches m ON m.id = mp.match_id
WHERE m.game_mode = :game_mode
GROUP BY p.id, p.name
ORDER BY p.name;

-- name: weekday(game_mode)
-- Win/loss record by day of week for a given game mode.
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
  AND game_mode = :game_mode
GROUP BY strftime('%w', played_at)
ORDER BY CAST(strftime('%w', played_at) AS INTEGER);
