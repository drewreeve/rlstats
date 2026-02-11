-- MVPs in losses
CREATE VIEW IF NOT EXISTS v_mvp_in_losses AS
SELECT
    p.id AS player_id,
    p.name AS player_name,
    COUNT(*) AS loss_mvps
FROM matches m
JOIN players p ON p.id = m.team_mvp_player_id
WHERE m.result = 'loss'
GROUP BY p.id, p.name;
