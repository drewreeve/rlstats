CREATE VIEW IF NOT EXISTS tracked_player_match_stats AS
SELECT
    p.id AS player_id,
    p.name AS player,
    m.game_mode AS game_mode,
    m.result AS result,
    m.team_mvp_player_id AS team_mvp_player_id,
    mp.goals AS goals,
    mp.assists AS assists,
    mp.saves AS saves,
    mp.shots AS shots,
    mp.demos AS demos,
    mp.demos_received AS demos_received,
    mp.score AS score,
    m.team_score AS team_score,
    mp.boost_per_minute AS boost_per_minute,
    mp.time_supersonic_pct AS time_supersonic_pct,
    mp.defensive_zone_seconds AS defensive_zone_seconds,
    mp.neutral_zone_seconds AS neutral_zone_seconds,
    mp.offensive_zone_seconds AS offensive_zone_seconds
FROM match_players mp
JOIN players p ON p.id = mp.player_id
JOIN matches m ON m.id = mp.match_id
WHERE p.is_tracked = 1;
