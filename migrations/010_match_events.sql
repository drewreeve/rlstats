CREATE TABLE IF NOT EXISTS match_events (
    id INTEGER PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES matches(id),
    event_type TEXT NOT NULL CHECK(event_type IN ('goal', 'shot', 'save', 'demo')),
    game_seconds REAL NOT NULL,
    player_id INTEGER NOT NULL REFERENCES players(id),
    team INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_match_events_match ON match_events(match_id);
