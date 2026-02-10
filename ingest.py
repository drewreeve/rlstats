# Replay Ingestion Pipeline
# rrrocket JSON -> SQLite

import json
import sqlite3
from pathlib import Path
from typing import Dict

############################
# schema.sql
############################

SCHEMA_SQL = """
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
"""

TRACKED_PLAYERS = {
    "76561197969365901": "Drew",
    "76561198008422893": "Steve",
    "76561197964215253": "Jeff",
}

DB_PATH = Path("db/rl_stats.sqlite")
PARSED_REPLAY_DIR = Path("replays")


def ensure_schema(conn: sqlite3.Connection):
    conn.executescript(SCHEMA_SQL)


def get_or_create_player(conn: sqlite3.Connection, steam_id: str, name: str) -> int:
    conn.execute(
        "INSERT OR IGNORE INTO players (steam_id, name) VALUES (?, ?)",
        (steam_id, name),
    )
    return conn.execute(
        "SELECT id FROM players WHERE steam_id = ?",
        (steam_id,),
    ).fetchone()[0]


def ingest_match(conn: sqlite3.Connection, replay: Dict):
    props = replay.get("properties", {})

    replay_hash = props.get("Id")
    # rrrocket Date is not guaranteed to be SQLite-parseable
    raw_played_at = props.get("Date")
    played_at_sql = None

    if raw_played_at:
        try:
            # rrrocket format: YYYY-MM-DD HH-MM-SS
            date_part, time_part = raw_played_at.split(" ")
            h, m, s = time_part.split("-")
            played_at_sql = f"{date_part} {h}:{m}:{s}"
        except Exception:
            played_at_sql = None

    duration = props.get("TotalSecondsPlayed")
    team_size = props.get("TeamSize")

    team0_score = props.get("Team0Score", 0)
    team1_score = props.get("Team1Score", 0)

    # Determine which team the tracked players are on
    tracked_teams = {
        p.get("Team")
        for p in props.get("PlayerStats", [])
        if p.get("OnlineID") in TRACKED_PLAYERS
    }

    team = tracked_teams.pop() if tracked_teams else None

    if team == 0:
        team_score = team0_score
        opponent_score = team1_score
    elif team == 1:
        team_score = team1_score
        opponent_score = team0_score
    else:
        team_score = None
        opponent_score = None

    if team_score is not None and opponent_score is not None:
        if team_score > opponent_score:
            result = "win"
        elif team_score < opponent_score:
            result = "loss"
        else:
            result = "draw"
    else:
        result = None

    # Determine team MVP (highest Score among tracked players)
    tracked_players = [
        p for p in props.get("PlayerStats", []) if p.get("OnlineID") in TRACKED_PLAYERS
    ]

    mvp_player_id = None
    if tracked_players:
        mvp_stats = max(tracked_players, key=lambda p: p.get("Score", 0))
        mvp_sid = mvp_stats.get("OnlineID")
        mvp_name = TRACKED_PLAYERS[mvp_sid]
        mvp_player_id = get_or_create_player(conn, mvp_sid, mvp_name)

    conn.execute(
        """
        INSERT OR IGNORE INTO matches (
            replay_hash,
            played_at, duration_seconds, team_size,
            team, team_score, opponent_score, result, team_mvp_player_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            replay_hash,
            played_at_sql,
            duration,
            team_size,
            team,
            team_score,
            opponent_score,
            result,
            mvp_player_id,
        ),
    )

    match_id = conn.execute(
        "SELECT id FROM matches WHERE replay_hash = ?",
        (replay_hash,),
    ).fetchone()[0]

    for player in props.get("PlayerStats", []):
        steam_id = player.get("OnlineID")
        if steam_id not in TRACKED_PLAYERS:
            continue

        name = TRACKED_PLAYERS[steam_id]
        player_id = get_or_create_player(conn, steam_id, name)

        conn.execute(
            """
            INSERT OR REPLACE INTO match_players (
                match_id, player_id, team,
                goals, assists, saves, shots
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                match_id,
                player_id,
                player.get("Team"),
                player.get("Goals", 0),
                player.get("Assists", 0),
                player.get("Saves", 0),
                player.get("Shots", 0),
            ),
        )


def ingest_all():
    DB_PATH.parent.mkdir(exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    ensure_schema(conn)
    ensure_analytics_views(conn)

    for path in sorted(PARSED_REPLAY_DIR.glob("*.json")):
        with open(path, "r", encoding="utf-8") as f:
            replay = json.load(f)
        ingest_match(conn, replay)

    conn.commit()
    conn.close()


############################
# analytics_views.sql
############################

ANALYTICS_VIEWS_SQL = """
-- MVP win rate per player
CREATE VIEW IF NOT EXISTS v_mvp_win_rate AS
SELECT
    p.id AS player_id,
    p.name AS player_name,
    COUNT(*) AS mvp_matches,
    SUM(CASE WHEN m.result = 'win' THEN 1 ELSE 0 END) AS mvp_wins,
    ROUND(
        CAST(SUM(CASE WHEN m.result = 'win' THEN 1 ELSE 0 END) AS REAL)
        / COUNT(*),
        3
    ) AS mvp_win_rate
FROM matches m
JOIN players p ON p.id = m.team_mvp_player_id
WHERE m.team_mvp_player_id IS NOT NULL
GROUP BY p.id, p.name;

-- MVPs in losses
CREATE VIEW IF NOT EXISTS v_mvp_in_losses AS
SELECT
    p.id AS player_id,
    p.name AS player_name,
    COUNT(*) AS loss_mvps
FROM matches m
JOIN players p ON p.id = m.team_mvp_player_id
WHERE m.result = 'loss'
GROUP BY p.id, p.name;

-- Win/Loss ratio by day of week
CREATE VIEW IF NOT EXISTS v_win_loss_by_weekday AS
SELECT
    CASE strftime('%w', played_at)
        WHEN '0' THEN 'Sunday'
        WHEN '1' THEN 'Monday'
        WHEN '2' THEN 'Tuesday'
        WHEN '3' THEN 'Wednesday'
        WHEN '4' THEN 'Thursday'
        WHEN '5' THEN 'Friday'
        WHEN '6' THEN 'Saturday'
    END AS weekday,
    COUNT(*) AS matches_played,
    SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END) AS losses,
    ROUND(
        CAST(SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) AS REAL)
        / NULLIF(SUM(CASE WHEN result IN ('win','loss') THEN 1 ELSE 0 END), 0),
        3
    ) AS win_rate
FROM matches
WHERE result IN ('win', 'loss')
  AND played_at IS NOT NULL
GROUP BY strftime('%w', played_at)
ORDER BY CAST(strftime('%w', played_at) AS INTEGER);
"""


def ensure_analytics_views(conn: sqlite3.Connection):
    conn.executescript(ANALYTICS_VIEWS_SQL)


if __name__ == "__main__":
    ingest_all()
