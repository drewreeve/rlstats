-- Recorded stats: per-player counters the game itself keeps, first present in
-- season 24 replays (see CONTEXT.md: Recorded Stat). Deliberately nullable with
-- no default: NULL means the replay predates the stat (unknown), which must not
-- be averaged in as zero. goal_frame_hits is the game's MatchCrossbarHits,
-- which also counts post hits.
-- Populated going forward by ingest; backfill season 24 matches with
-- `uv run python process.py --force` (older replays stay NULL).
ALTER TABLE match_players ADD COLUMN ball_touches INTEGER;
ALTER TABLE match_players ADD COLUMN car_touches INTEGER;
ALTER TABLE match_players ADD COLUMN dodges INTEGER;
ALTER TABLE match_players ADD COLUMN aerial_hits INTEGER;
ALTER TABLE match_players ADD COLUMN bicycle_hits INTEGER;
ALTER TABLE match_players ADD COLUMN centers INTEGER;
ALTER TABLE match_players ADD COLUMN clears INTEGER;
ALTER TABLE match_players ADD COLUMN epic_saves INTEGER;
ALTER TABLE match_players ADD COLUMN first_touches INTEGER;
ALTER TABLE match_players ADD COLUMN flip_resets INTEGER;
ALTER TABLE match_players ADD COLUMN goal_frame_hits INTEGER;
ALTER TABLE match_players ADD COLUMN high_fives INTEGER;
ALTER TABLE match_players ADD COLUMN juggle_hits INTEGER;
ALTER TABLE match_players ADD COLUMN low_fives INTEGER;
ALTER TABLE match_players ADD COLUMN pool_shots INTEGER;
ALTER TABLE match_players ADD COLUMN power_ups_used INTEGER;
