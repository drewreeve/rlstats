# Replay Ingestion Pipeline
# rrrocket JSON -> SQLite

import json
import sqlite3
from pathlib import Path
from typing import Any

from db import apply_migrations

TRACKED_PLAYERS = {
    ("steam", "76561197969365901"): "Drew",
    ("steam", "76561198008422893"): "Steve",
    ("steam", "76561197964215253"): "Jeff",
}

PLATFORM_MAP = {
    "OnlinePlatform_Steam": "steam",
    "OnlinePlatform_Epic": "epic",
    "OnlinePlatform_PS4": "ps4",
    "OnlinePlatform_Switch": "switch",
    "OnlinePlatform_Xbox": "xbox",
}

DB_PATH = Path("db/rl_stats.sqlite")
PARSED_REPLAY_DIR = Path("replays")


def _extract_platform_id(player: dict[str, Any]) -> tuple[str, str] | None:
    platform_value = player.get("Platform", {}).get("value", "")
    platform = PLATFORM_MAP.get(platform_value)
    if not platform:
        return None

    if platform == "epic":
        epic_id = player.get("PlayerID", {}).get("fields", {}).get("EpicAccountId", "")
        if epic_id:
            return (platform, epic_id)
        return None

    online_id = player.get("OnlineID", "0")
    if online_id and online_id != "0":
        return (platform, online_id)
    return None


def get_or_create_player(conn: sqlite3.Connection, platform: str, platform_id: str, name: str) -> int:
    tracked = 1 if (platform, platform_id) in TRACKED_PLAYERS else 0
    conn.execute(
        """INSERT INTO players (platform, platform_id, name, is_tracked) VALUES (?, ?, ?, ?)
           ON CONFLICT(platform, platform_id) DO UPDATE SET name = excluded.name""",
        (platform, platform_id, name, tracked),
    )
    return conn.execute(
        "SELECT id FROM players WHERE platform = ? AND platform_id = ?",
        (platform, platform_id),
    ).fetchone()[0]


def _normalize_played_at(raw_played_at: Any) -> str | None:
    if not raw_played_at:
        return None
    try:
        # rrrocket format: YYYY-MM-DD HH-MM-SS
        date_part, time_part = raw_played_at.split(" ")
        h, m, s = time_part.split("-")
        return f"{date_part} {h}:{m}:{s}"
    except ValueError, AttributeError:
        return None


def _detect_game_mode(team_size: Any, map_name: Any) -> str | None:
    if team_size == 3:
        return "3v3"
    if team_size == 2 and map_name and "hoop" in map_name.lower():
        return "hoops"
    if team_size == 2:
        return "2v2"
    return None


def _tracked_player_stats(props: dict[str, Any]) -> list[dict[str, Any]]:
    result = []
    for p in props.get("PlayerStats", []):
        identity = _extract_platform_id(p)
        if identity and identity in TRACKED_PLAYERS:
            result.append(p)
    return result


def _resolve_team_scores(
    tracked_players: list[dict[str, Any]], team0_score: Any, team1_score: Any
) -> tuple[Any, Any, Any]:
    tracked_teams = {p.get("Team") for p in tracked_players}
    team = tracked_teams.pop() if tracked_teams else None

    if team == 0:
        return team, team0_score, team1_score
    if team == 1:
        return team, team1_score, team0_score
    return team, None, None


def _resolve_result(team_score: Any, opponent_score: Any) -> str | None:
    if team_score is None or opponent_score is None:
        return None
    if team_score > opponent_score:
        return "win"
    if team_score < opponent_score:
        return "loss"
    return None


def _resolve_mvp_player_id(
    conn: sqlite3.Connection, tracked_players: list[dict[str, Any]]
) -> int | None:
    if not tracked_players:
        return None
    mvp_stats = max(tracked_players, key=lambda p: p.get("Score", 0))
    identity = _extract_platform_id(mvp_stats)
    if not identity:
        return None
    platform, platform_id = identity
    mvp_name = TRACKED_PLAYERS[identity]
    return get_or_create_player(conn, platform, platform_id, mvp_name)


def _upsert_match(
    conn: sqlite3.Connection,
    *,
    replay_hash: str,
    played_at_sql: str | None,
    duration: int | None,
    forfeit: int,
    team_size: int | None,
    team: int | None,
    team_score: int | None,
    opponent_score: int | None,
    result: str | None,
    mvp_player_id: int | None,
    map_name: str | None,
    game_mode: str | None,
) -> int:
    conn.execute(
        """
        INSERT OR REPLACE INTO matches (
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
    return row[0]


def _upsert_match_players(
    conn: sqlite3.Connection, match_id: int, all_players: list[dict[str, Any]]
):
    for player in all_players:
        if player.get("bBot"):
            continue
        identity = _extract_platform_id(player)
        if not identity:
            continue
        platform, platform_id = identity
        name = player.get("Name", "Unknown")
        # Use tracked name if this is a tracked player
        tracked_name = TRACKED_PLAYERS.get(identity)
        if tracked_name:
            name = tracked_name
        player_id = get_or_create_player(conn, platform, platform_id, name)

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


def ingest_match(conn: sqlite3.Connection, replay: dict):
    props = replay.get("properties", {})

    replay_hash = props.get("MatchGUID") or props.get("MatchGuid")
    if not replay_hash:
        return

    played_at_sql = _normalize_played_at(props.get("Date"))
    duration = props.get("TotalSecondsPlayed")
    forfeit = 1 if props.get("bForfeit") else 0
    team_size = props.get("TeamSize")
    map_name = props.get("MapName")
    game_mode = _detect_game_mode(team_size, map_name)

    team0_score = props.get("Team0Score", 0)
    team1_score = props.get("Team1Score", 0)
    tracked_players = _tracked_player_stats(props)
    team, team_score, opponent_score = _resolve_team_scores(
        tracked_players, team0_score, team1_score
    )
    result = _resolve_result(team_score, opponent_score)
    if result is None:
        return

    mvp_player_id = _resolve_mvp_player_id(conn, tracked_players)

    match_id = _upsert_match(
        conn,
        replay_hash=replay_hash,
        played_at_sql=played_at_sql,
        duration=duration,
        forfeit=forfeit,
        team_size=team_size,
        team=team,
        team_score=team_score,
        opponent_score=opponent_score,
        result=result,
        mvp_player_id=mvp_player_id,
        map_name=map_name,
        game_mode=game_mode,
    )
    all_players = props.get("PlayerStats", [])
    _upsert_match_players(conn, match_id, all_players)


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
