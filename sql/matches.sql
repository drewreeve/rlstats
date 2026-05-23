-- name: count_matches(game_mode, result, search, date_from, date_to)^
-- Total number of matches matching the given optional filters.
SELECT COUNT(*)
FROM matches m
LEFT JOIN players p ON m.team_mvp_player_id = p.id
WHERE (:game_mode IS NULL OR m.game_mode = :game_mode)
  AND (:result IS NULL OR m.result = :result)
  AND (:search IS NULL OR p.name LIKE :search ESCAPE '\')
  AND (:date_from IS NULL OR m.played_at >= :date_from)
  AND (:date_to IS NULL OR m.played_at < :date_to);

-- name: list_matches(game_mode, result, search, date_from, date_to, per_page, offset)
-- Paginated match list with optional filters ordered by date descending.
SELECT m.id, m.game_mode, m.result, m.forfeit,
       m.team_score || '-' || m.opponent_score AS score,
       m.played_at, p.name AS mvp
FROM matches m
LEFT JOIN players p ON m.team_mvp_player_id = p.id
WHERE (:game_mode IS NULL OR m.game_mode = :game_mode)
  AND (:result IS NULL OR m.result = :result)
  AND (:search IS NULL OR p.name LIKE :search ESCAPE '\')
  AND (:date_from IS NULL OR m.played_at >= :date_from)
  AND (:date_to IS NULL OR m.played_at < :date_to)
ORDER BY m.played_at DESC
LIMIT :per_page OFFSET :offset;
