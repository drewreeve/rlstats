-- Win/Loss ratio by day of week
CREATE VIEW IF NOT EXISTS v_win_loss_by_weekday AS
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
GROUP BY strftime('%w', played_at)
ORDER BY CAST(strftime('%w', played_at) AS INTEGER);
