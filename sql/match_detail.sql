-- name: match_metadata(match_id)^
-- Match metadata for a single match.
SELECT
    m.id,
    m.played_at,
    m.game_mode,
    m.result,
    m.forfeit,
    m.team_score,
    m.opponent_score,
    m.duration_seconds,
    m.team,
    m.team_possession_seconds,
    m.opponent_possession_seconds,
    m.defensive_third_seconds,
    m.neutral_third_seconds,
    m.offensive_third_seconds
FROM matches m
WHERE m.id = :match_id;

-- name: match_players(match_id)
-- All players in a match with computed shooting percentage.
SELECT
    p.name,
    mp.team,
    mp.score,
    mp.goals,
    mp.assists,
    mp.saves,
    mp.shots,
    mp.demos,
    CASE WHEN mp.shots > 0
         THEN ROUND(CAST(mp.goals AS REAL) / mp.shots * 100, 1)
         ELSE 0 END AS shooting_pct
FROM match_players mp
JOIN players p ON mp.player_id = p.id
WHERE mp.match_id = :match_id
ORDER BY mp.score DESC;
