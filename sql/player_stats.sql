-- name: shooting_pct(game_mode)
-- Shooting percentage per player for a given game mode.
SELECT
    p.name AS player,
    SUM(mp.goals) AS goals,
    SUM(mp.shots) AS shots,
    ROUND(
        CAST(SUM(mp.goals) AS REAL)
        / NULLIF(SUM(mp.shots), 0),
        3
    ) AS shooting_pct
FROM match_players mp
JOIN players p ON p.id = mp.player_id
JOIN matches m ON m.id = mp.match_id
WHERE m.game_mode = :game_mode AND p.is_tracked = 1
GROUP BY p.id, p.name
ORDER BY p.name;

-- name: player_stats(game_mode)
-- Aggregated per-player stats for a given game mode.
SELECT
    p.name AS player,
    COUNT(*) AS matches,
    SUM(mp.goals) AS goals,
    SUM(mp.assists) AS assists,
    SUM(mp.saves) AS saves,
    SUM(mp.shots) AS shots,
    SUM(mp.demos) AS demos
FROM match_players mp
JOIN players p ON p.id = mp.player_id
JOIN matches m ON m.id = mp.match_id
WHERE m.game_mode = :game_mode AND p.is_tracked = 1
GROUP BY p.id, p.name
ORDER BY p.name;

-- name: avg_score(game_mode)
-- Average score per player for a given game mode.
SELECT
    p.name AS player,
    COUNT(*) AS matches,
    SUM(mp.score) AS total_score,
    ROUND(
        CAST(SUM(mp.score) AS REAL) / COUNT(*),
        1
    ) AS avg_score
FROM match_players mp
JOIN players p ON p.id = mp.player_id
JOIN matches m ON m.id = mp.match_id
WHERE m.game_mode = :game_mode AND p.is_tracked = 1
GROUP BY p.id, p.name
ORDER BY p.name;

-- name: score_range(game_mode)
-- Min and max score per player for a given game mode.
SELECT
    p.name AS player,
    MIN(mp.score) AS min,
    MAX(mp.score) AS max
FROM match_players mp
JOIN players p ON p.id = mp.player_id
JOIN matches m ON m.id = mp.match_id
WHERE m.game_mode = :game_mode AND p.is_tracked = 1
GROUP BY p.id, p.name
ORDER BY p.name;

-- name: avg_goal_contribution(game_mode)
-- Average goal contribution per player for a given game mode.
SELECT
    p.name AS player,
    COUNT(*) AS matches,
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
WHERE m.game_mode = :game_mode AND p.is_tracked = 1
GROUP BY p.id, p.name
ORDER BY p.name;
