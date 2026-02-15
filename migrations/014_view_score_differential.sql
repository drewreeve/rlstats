CREATE VIEW IF NOT EXISTS v_score_differential_3v3 AS
SELECT
    team_score - opponent_score AS differential,
    COUNT(*) AS match_count
FROM matches
WHERE result IN ('win', 'loss')
  AND team_size = 3
GROUP BY differential
ORDER BY differential;

CREATE VIEW IF NOT EXISTS v_score_differential_2v2 AS
SELECT
    team_score - opponent_score AS differential,
    COUNT(*) AS match_count
FROM matches
WHERE result IN ('win', 'loss')
  AND game_mode = '2v2'
GROUP BY differential
ORDER BY differential;

CREATE VIEW IF NOT EXISTS v_score_differential_hoops AS
SELECT
    team_score - opponent_score AS differential,
    COUNT(*) AS match_count
FROM matches
WHERE result IN ('win', 'loss')
  AND game_mode = 'hoops'
GROUP BY differential
ORDER BY differential;
