-- name: mvp_wins(game_mode)
-- MVP win rate per player for a given game mode.
SELECT
    p.name AS player_name,
    COUNT(*) AS mvp_matches,
    SUM(CASE WHEN m.result = 'win' THEN 1 ELSE 0 END) AS mvp_wins,
    ROUND(
        CAST(SUM(CASE WHEN m.result = 'win' THEN 1 ELSE 0 END) AS REAL)
        / COUNT(*),
        3
    ) AS mvp_win_rate
FROM matches m
JOIN players p ON p.id = m.team_mvp_player_id
WHERE m.team_mvp_player_id IS NOT NULL
  AND m.game_mode = :game_mode
GROUP BY p.id, p.name
ORDER BY p.name;

-- name: mvp_losses(game_mode)
-- MVP appearances in losses per player for a given game mode.
SELECT
    p.name AS player_name,
    COUNT(*) AS loss_mvps
FROM matches m
JOIN players p ON p.id = m.team_mvp_player_id
WHERE m.result = 'loss'
  AND m.game_mode = :game_mode
GROUP BY p.id, p.name
ORDER BY p.name;
