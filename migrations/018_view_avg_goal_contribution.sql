-- Average goal contribution per player per game mode
-- (goals + assists) / team_score, averaged across matches
-- Matches with team_score = 0 are excluded via NULLIF (AVG skips NULLs)

CREATE VIEW IF NOT EXISTS v_avg_goal_contribution_3v3 AS
SELECT
    p.id AS player_id,
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
WHERE m.team_size = 3
GROUP BY p.id, p.name;

CREATE VIEW IF NOT EXISTS v_avg_goal_contribution_2v2 AS
SELECT
    p.id AS player_id,
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
WHERE m.game_mode = '2v2'
GROUP BY p.id, p.name;

CREATE VIEW IF NOT EXISTS v_avg_goal_contribution_hoops AS
SELECT
    p.id AS player_id,
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
WHERE m.game_mode = 'hoops'
GROUP BY p.id, p.name;
