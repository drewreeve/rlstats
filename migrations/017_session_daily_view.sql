-- Replace daily view to aggregate by session start date
DROP VIEW IF EXISTS v_win_loss_daily_3v3;
CREATE VIEW IF NOT EXISTS v_win_loss_daily_3v3 AS
SELECT
    session_date AS play_date,
    SUM(wins) AS wins,
    SUM(losses) AS losses,
    ROUND(
        CAST(SUM(wins) AS REAL) / NULLIF(SUM(wins) + SUM(losses), 0),
        3
    ) AS win_rate
FROM v_session_summary
WHERE game_mode = '3v3'
GROUP BY session_date
ORDER BY play_date;
