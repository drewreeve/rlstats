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
