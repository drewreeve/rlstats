-- Daily win/loss record over time
DROP VIEW IF EXISTS v_win_loss_daily;
CREATE VIEW IF NOT EXISTS v_win_loss_daily AS
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
GROUP BY DATE(played_at)
ORDER BY play_date;
