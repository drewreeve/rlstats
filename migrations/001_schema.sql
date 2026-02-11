PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS players (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    steam_id TEXT UNIQUE,
    name TEXT
);

CREATE TABLE IF NOT EXISTS matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    replay_hash TEXT UNIQUE,
    played_at TEXT,
    duration_seconds INTEGER,
    team_size INTEGER,
    team INTEGER,
    team_score INTEGER,
    opponent_score INTEGER,
    result TEXT,
    team_mvp_player_id INTEGER,
    FOREIGN KEY (team_mvp_player_id) REFERENCES players(id)
);

CREATE TABLE IF NOT EXISTS match_players (
    match_id INTEGER,
    player_id INTEGER,
    team INTEGER,
    goals INTEGER,
    assists INTEGER,
    saves INTEGER,
    shots INTEGER,
    PRIMARY KEY (match_id, player_id),
    FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE CASCADE,
    FOREIGN KEY (player_id) REFERENCES players(id) ON DELETE CASCADE
);
