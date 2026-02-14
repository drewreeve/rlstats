# Replay Ingestion Pipeline
# rrrocket JSON -> SQLite

import json
import sqlite3
from pathlib import Path
from typing import Dict

from db import apply_migrations

TRACKED_PLAYERS = {
    "76561197969365901": "Drew",
    "76561198008422893": "Steve",
    "76561197964215253": "Jeff",
}

DB_PATH = Path("db/rl_stats.sqlite")
PARSED_REPLAY_DIR = Path("replays")


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

    replay_hash = props.get("MatchGUID") or props.get("MatchGuid")
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
    forfeit = 1 if props.get("bForfeit") else 0
    team_size = props.get("TeamSize")
    map_name = props.get("MapName")

    if team_size == 3:
        game_mode = "3v3"
    elif team_size == 2 and map_name and "hoop" in map_name.lower():
        game_mode = "hoops"
    elif team_size == 2:
        game_mode = "2v2"
    else:
        game_mode = None

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
            played_at, duration_seconds, forfeit, team_size,
            team, team_score, opponent_score, result, team_mvp_player_id,
            map_name, game_mode
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            replay_hash,
            played_at_sql,
            duration,
            forfeit,
            team_size,
            team,
            team_score,
            opponent_score,
            result,
            mvp_player_id,
            map_name,
            game_mode,
        ),
    )

    row = conn.execute(
        "SELECT id FROM matches WHERE replay_hash = ?",
        (replay_hash,),
    ).fetchone()

    if row is None:
        raise RuntimeError("Match insert failed; no row found for replay_hash")

    match_id = row[0]

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
                goals, assists, saves, shots, score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                match_id,
                player_id,
                player.get("Team"),
                player.get("Goals", 0),
                player.get("Assists", 0),
                player.get("Saves", 0),
                player.get("Shots", 0),
                player.get("Score", 0),
            ),
        )


def ingest_all():
    DB_PATH.parent.mkdir(exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    apply_migrations(conn)

    for path in sorted(PARSED_REPLAY_DIR.glob("*.json")):
        with open(path, "r", encoding="utf-8") as f:
            replay = json.load(f)
        ingest_match(conn, replay)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    ingest_all()
