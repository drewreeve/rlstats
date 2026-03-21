DROP TABLE IF EXISTS match_events;
CREATE TABLE match_events (
    id INTEGER PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES matches(id),
    event_type TEXT NOT NULL CHECK(event_type IN ('goal', 'shot', 'save', 'demo', 'assist')),
    game_seconds REAL NOT NULL,
    player_id INTEGER NOT NULL REFERENCES players(id),
    team INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_match_events_match ON match_events(match_id);

CREATE TABLE IF NOT EXISTS offensive_pairings (
    id INTEGER PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES matches(id),
    game_seconds REAL NOT NULL,
    scorer_player_id INTEGER NOT NULL REFERENCES players(id),
    assister_player_id INTEGER NOT NULL REFERENCES players(id),
    team INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_offensive_pairings_match ON offensive_pairings(match_id);
