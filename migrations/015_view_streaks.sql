CREATE VIEW IF NOT EXISTS v_streaks_3v3 AS
WITH ordered AS (
    SELECT
        result,
        played_at,
        ROW_NUMBER() OVER (ORDER BY played_at) AS rn,
        ROW_NUMBER() OVER (PARTITION BY result ORDER BY played_at) AS grp
    FROM matches
    WHERE result IN ('win', 'loss')
      AND team_size = 3
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

CREATE VIEW IF NOT EXISTS v_streaks_2v2 AS
WITH ordered AS (
    SELECT
        result,
        played_at,
        ROW_NUMBER() OVER (ORDER BY played_at) AS rn,
        ROW_NUMBER() OVER (PARTITION BY result ORDER BY played_at) AS grp
    FROM matches
    WHERE result IN ('win', 'loss')
      AND game_mode = '2v2'
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

CREATE VIEW IF NOT EXISTS v_streaks_hoops AS
WITH ordered AS (
    SELECT
        result,
        played_at,
        ROW_NUMBER() OVER (ORDER BY played_at) AS rn,
        ROW_NUMBER() OVER (PARTITION BY result ORDER BY played_at) AS grp
    FROM matches
    WHERE result IN ('win', 'loss')
      AND game_mode = 'hoops'
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
