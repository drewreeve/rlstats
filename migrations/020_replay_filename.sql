-- The source .replay file each match was ingested from. Replay files are named
-- by the uploader, not by MatchGUID, so replay_hash cannot locate the file on
-- disk; this column stores the actual filename (basename, no directory).
-- Populated going forward by ingest; backfill existing rows with
-- `uv run python process.py --force`.
ALTER TABLE matches ADD COLUMN replay_filename TEXT;
