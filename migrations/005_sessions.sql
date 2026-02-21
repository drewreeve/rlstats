-- Session grouping: matches within 60 minutes are one session
DROP VIEW IF EXISTS v_sessions;
CREATE VIEW IF NOT EXISTS v_sessions AS
WITH ordered_matches AS (
    SELECT
        id AS match_id,
        played_at,
        game_mode,
        result,
        LAG(played_at) OVER (PARTITION BY game_mode ORDER BY played_at) AS prev_played_at
    FROM matches
    WHERE played_at IS NOT NULL
      AND result IN ('win', 'loss')
),
session_markers AS (
    SELECT
        match_id,
        played_at,
        game_mode,
        result,
        CASE
            WHEN prev_played_at IS NULL THEN 1
            WHEN (julianday(played_at) - julianday(prev_played_at)) * 24 * 60 > 60 THEN 1
            ELSE 0
        END AS new_session
    FROM ordered_matches
)
SELECT
    match_id,
    played_at,
    game_mode,
    result,
    SUM(new_session) OVER (PARTITION BY game_mode ORDER BY played_at) AS session_id
FROM session_markers;

DROP VIEW IF EXISTS v_session_summary;
CREATE VIEW IF NOT EXISTS v_session_summary AS
SELECT
    session_id,
    game_mode,
    DATE(MIN(played_at)) AS session_date,
    COUNT(*) AS match_count,
    SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END) AS losses,
    ROUND(
        CAST(SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) AS REAL)
        / COUNT(*),
        3
    ) AS win_rate
FROM v_sessions
GROUP BY game_mode, session_id;
