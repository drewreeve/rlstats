-- Replace steam_id with platform + platform_id
-- Also adds is_tracked column for easier player filtering (required since
-- aiosql doesn't support passing a list to an IN clause)
PRAGMA foreign_keys = OFF;

BEGIN;
CREATE TABLE players_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL CHECK(platform IN ('steam', 'epic', 'ps4', 'switch', 'xbox')),
    platform_id TEXT NOT NULL,
    name TEXT NOT NULL,
    is_tracked INTEGER NOT NULL DEFAULT 0,
    UNIQUE(platform, platform_id)
);
INSERT INTO players_new (id, platform, platform_id, name, is_tracked)
    SELECT id, 'steam', steam_id, name,
        CASE WHEN steam_id IN ('76561197969365901', '76561198008422893', '76561197964215253')
             THEN 1 ELSE 0 END
    FROM players;
DROP TABLE players;
ALTER TABLE players_new RENAME TO players;
PRAGMA foreign_key_check;
COMMIT;

PRAGMA foreign_keys = ON;
