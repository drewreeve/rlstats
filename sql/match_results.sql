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
