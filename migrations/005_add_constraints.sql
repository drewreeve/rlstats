-- Add NOT NULL and CHECK constraints to all tables.
-- SQLite doesn't support ALTER COLUMN, so we recreate each table.

-- Recreate players
PRAGMA foreign_keys = OFF;

BEGIN;
CREATE TABLE players_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    steam_id TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL
);
INSERT INTO players_new SELECT * FROM players;
DROP TABLE players;
ALTER TABLE players_new RENAME TO players;
PRAGMA foreign_key_check;
COMMIT;

-- Recreate matches
BEGIN;
CREATE TABLE matches_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    replay_hash TEXT UNIQUE NOT NULL,
    played_at TEXT,
    duration_seconds INTEGER,
    team_size INTEGER,
    team INTEGER NOT NULL,
    team_score INTEGER NOT NULL,
    opponent_score INTEGER NOT NULL,
    result TEXT NOT NULL CHECK(result IN ('win', 'loss')),
    team_mvp_player_id INTEGER,
    map_name TEXT,
    game_mode TEXT,
    forfeit INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (team_mvp_player_id) REFERENCES players(id)
);
INSERT INTO matches_new SELECT * FROM matches;
DROP TABLE matches;
ALTER TABLE matches_new RENAME TO matches;
PRAGMA foreign_key_check;
COMMIT;

-- Recreate match_players
BEGIN;
CREATE TABLE match_players_new (
    match_id INTEGER,
    player_id INTEGER,
    team INTEGER NOT NULL,
    goals INTEGER NOT NULL DEFAULT 0,
    assists INTEGER NOT NULL DEFAULT 0,
    saves INTEGER NOT NULL DEFAULT 0,
    shots INTEGER NOT NULL DEFAULT 0,
    score INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (match_id, player_id),
    FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE CASCADE,
    FOREIGN KEY (player_id) REFERENCES players(id) ON DELETE CASCADE
);
INSERT INTO match_players_new SELECT * FROM match_players;
DROP TABLE match_players;
ALTER TABLE match_players_new RENAME TO match_players;
PRAGMA foreign_key_check;
COMMIT;

PRAGMA foreign_keys = ON;
