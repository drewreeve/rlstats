-- name: shooting_pct(game_mode)
-- Shooting percentage per player for a given game mode.
SELECT
    player,
    SUM(goals) AS goals,
    SUM(shots) AS shots,
    ROUND(
        CAST(SUM(goals) AS REAL)
        / NULLIF(SUM(shots), 0) * 100,
        1
    ) AS shooting_pct
FROM tracked_player_match_stats
WHERE game_mode = :game_mode
GROUP BY player_id, player
ORDER BY player;

-- name: player_stats(game_mode)
-- Aggregated per-player stats for a given game mode.
SELECT
    player,
    COUNT(*) AS matches,
    SUM(goals) AS goals,
    SUM(assists) AS assists,
    SUM(saves) AS saves,
    SUM(shots) AS shots,
    SUM(demos) AS demos
FROM tracked_player_match_stats
WHERE game_mode = :game_mode
GROUP BY player_id, player
ORDER BY player;

-- name: n_by_n_stats(game_mode)
-- Count of "N by N" matches per player for a given game mode, where N is the
-- largest value such that goals, assists, and saves are all >= N in that match.
WITH per_match AS (
    SELECT
        player_id,
        player,
        MIN(goals, assists, saves) AS n
    FROM tracked_player_match_stats
    WHERE game_mode = :game_mode
)
SELECT
    player,
    n,
    COUNT(*) AS matches
FROM per_match
WHERE n >= 1
GROUP BY player_id, player, n
ORDER BY player, n;

-- name: avg_score(game_mode)
-- Average score per player for a given game mode.
SELECT
    player,
    COUNT(*) AS matches,
    SUM(score) AS total_score,
    ROUND(
        CAST(SUM(score) AS REAL) / COUNT(*),
        1
    ) AS avg_score
FROM tracked_player_match_stats
WHERE game_mode = :game_mode
GROUP BY player_id, player
ORDER BY player;

-- name: score_range(game_mode)
-- Min and max score per player for a given game mode.
SELECT
    player,
    MIN(score) AS min,
    MAX(score) AS max
FROM tracked_player_match_stats
WHERE game_mode = :game_mode
GROUP BY player_id, player
ORDER BY player;

-- name: avg_goal_contribution(game_mode)
-- Average goal contribution per player for a given game mode.
SELECT
    player,
    COUNT(*) AS matches,
    ROUND(
        AVG(
            CAST(goals + assists AS REAL)
            / NULLIF(team_score, 0)
        ),
        3
    ) AS avg_goal_contribution
FROM tracked_player_match_stats
WHERE game_mode = :game_mode
GROUP BY player_id, player
ORDER BY player;

-- name: offensive_pairings(game_mode)
-- Goal-assist pairings between tracked players for a given game mode.
SELECT
    p_assister.name || ' → ' || p_scorer.name AS pairing,
    p_assister.name AS assister,
    COUNT(*) AS goals
FROM offensive_pairings op
JOIN players p_scorer ON op.scorer_player_id = p_scorer.id
JOIN players p_assister ON op.assister_player_id = p_assister.id
JOIN matches m ON m.id = op.match_id
WHERE m.game_mode = :game_mode
GROUP BY p_scorer.id, p_assister.id
ORDER BY goals DESC;
