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
