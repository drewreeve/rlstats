# Replay Ingestion Pipeline
# rrrocket JSON -> SQLite

import json
import sqlite3
from pathlib import Path
from typing import Any

from db import apply_migrations
from frame_analysis import (
    _calculate_ball_thirds,
    _calculate_possession,
    _extract_boost_stats,
    _extract_demolitions,
    _extract_match_events,
    _extract_player_movement_stats,
)

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
    "OnlinePlatform_NNX": "switch",
    "OnlinePlatform_Xbox": "xbox",  # Not sure if this is needed now
    "OnlinePlatform_Dingo": "xbox",
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


def get_or_create_player(
    conn: sqlite3.Connection, platform: str, platform_id: str, name: str
) -> int:
    tracked = 1 if (platform, platform_id) in TRACKED_PLAYERS else 0
    return conn.execute(
        """INSERT INTO players (platform, platform_id, name, is_tracked) VALUES (?, ?, ?, ?)
           ON CONFLICT(platform, platform_id) DO UPDATE SET name = excluded.name
           RETURNING id""",
        (platform, platform_id, name, tracked),
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
    return [
        p
        for p in props.get("PlayerStats", [])
        if (identity := _extract_platform_id(p)) and identity in TRACKED_PLAYERS
    ]


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
    team_possession_seconds: float | None,
    opponent_possession_seconds: float | None,
    defensive_third_seconds: float | None,
    neutral_third_seconds: float | None,
    offensive_third_seconds: float | None,
    team_boost_collected: int | None,
    opponent_boost_collected: int | None,
    team_boost_stolen: int | None,
    opponent_boost_stolen: int | None,
) -> int:
    return conn.execute(
        """
        INSERT INTO matches (
            replay_hash,
            played_at, duration_seconds, forfeit, team_size,
            team, team_score, opponent_score, result, team_mvp_player_id,
            map_name, game_mode,
            team_possession_seconds, opponent_possession_seconds,
            defensive_third_seconds, neutral_third_seconds, offensive_third_seconds,
            team_boost_collected, opponent_boost_collected,
            team_boost_stolen, opponent_boost_stolen
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(replay_hash) DO UPDATE SET
            played_at = excluded.played_at,
            duration_seconds = excluded.duration_seconds,
            forfeit = excluded.forfeit,
            team_size = excluded.team_size,
            team = excluded.team,
            team_score = excluded.team_score,
            opponent_score = excluded.opponent_score,
            result = excluded.result,
            team_mvp_player_id = excluded.team_mvp_player_id,
            map_name = excluded.map_name,
            game_mode = excluded.game_mode,
            team_possession_seconds = excluded.team_possession_seconds,
            opponent_possession_seconds = excluded.opponent_possession_seconds,
            defensive_third_seconds = excluded.defensive_third_seconds,
            neutral_third_seconds = excluded.neutral_third_seconds,
            offensive_third_seconds = excluded.offensive_third_seconds,
            team_boost_collected = excluded.team_boost_collected,
            opponent_boost_collected = excluded.opponent_boost_collected,
            team_boost_stolen = excluded.team_boost_stolen,
            opponent_boost_stolen = excluded.opponent_boost_stolen
        RETURNING id
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
            team_possession_seconds,
            opponent_possession_seconds,
            defensive_third_seconds,
            neutral_third_seconds,
            offensive_third_seconds,
            team_boost_collected,
            opponent_boost_collected,
            team_boost_stolen,
            opponent_boost_stolen,
        ),
    ).fetchone()[0]


def _upsert_match_players(
    conn: sqlite3.Connection,
    match_id: int,
    all_players: list[dict[str, Any]],
    demolitions: dict[tuple[str, str], int],
    movement_stats: dict[tuple[str, str], dict[str, float]],
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
        demos = demolitions.get(identity, 0)
        mv = movement_stats.get(identity, {})

        conn.execute(
            """
            INSERT INTO match_players (
                match_id, player_id, team,
                goals, assists, saves, shots, score, demos,
                boost_per_minute, avg_speed, time_supersonic_pct,
                small_pads, large_pads, stolen_small_pads, stolen_large_pads
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(match_id, player_id) DO UPDATE SET
                team = excluded.team,
                goals = excluded.goals,
                assists = excluded.assists,
                saves = excluded.saves,
                shots = excluded.shots,
                score = excluded.score,
                demos = excluded.demos,
                boost_per_minute = excluded.boost_per_minute,
                avg_speed = excluded.avg_speed,
                time_supersonic_pct = excluded.time_supersonic_pct,
                small_pads = excluded.small_pads,
                large_pads = excluded.large_pads,
                stolen_small_pads = excluded.stolen_small_pads,
                stolen_large_pads = excluded.stolen_large_pads
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
                demos,
                mv.get("boost_per_minute"),
                mv.get("avg_speed"),
                mv.get("time_supersonic_pct"),
                mv.get("small_pads"),
                mv.get("large_pads"),
                mv.get("stolen_small_pads"),
                mv.get("stolen_large_pads"),
            ),
        )


def analyze_replay(replay: dict) -> dict | None:
    props = replay.get("properties", {})

    replay_hash = props.get("MatchGUID") or props.get("MatchGuid")
    if not replay_hash:
        return None

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
        return None

    team_poss, opp_poss = _calculate_possession(replay, team)
    def_thirds, neu_thirds, off_thirds = _calculate_ball_thirds(replay, team)
    team_boost, opp_boost, team_stolen, opp_stolen = _extract_boost_stats(
        replay, team, game_mode
    )
    demolitions = _extract_demolitions(replay)
    movement_stats = _extract_player_movement_stats(replay, duration, game_mode)
    match_events = _extract_match_events(replay, team, set(TRACKED_PLAYERS.keys()))

    return {
        "replay_hash": replay_hash,
        "played_at_sql": played_at_sql,
        "duration": duration,
        "forfeit": forfeit,
        "team_size": team_size,
        "team": team,
        "team_score": team_score,
        "opponent_score": opponent_score,
        "result": result,
        "map_name": map_name,
        "game_mode": game_mode,
        "team_possession_seconds": team_poss,
        "opponent_possession_seconds": opp_poss,
        "defensive_third_seconds": def_thirds,
        "neutral_third_seconds": neu_thirds,
        "offensive_third_seconds": off_thirds,
        "team_boost_collected": team_boost,
        "opponent_boost_collected": opp_boost,
        "team_boost_stolen": team_stolen,
        "opponent_boost_stolen": opp_stolen,
        "tracked_players": tracked_players,
        "all_players": props.get("PlayerStats", []),
        "demolitions": demolitions,
        "movement_stats": movement_stats,
        "match_events": match_events,
    }


def write_match(conn: sqlite3.Connection, analysis: dict):
    mvp_player_id = _resolve_mvp_player_id(conn, analysis["tracked_players"])

    match_id = _upsert_match(
        conn,
        replay_hash=analysis["replay_hash"],
        played_at_sql=analysis["played_at_sql"],
        duration=analysis["duration"],
        forfeit=analysis["forfeit"],
        team_size=analysis["team_size"],
        team=analysis["team"],
        team_score=analysis["team_score"],
        opponent_score=analysis["opponent_score"],
        result=analysis["result"],
        mvp_player_id=mvp_player_id,
        map_name=analysis["map_name"],
        game_mode=analysis["game_mode"],
        team_possession_seconds=analysis["team_possession_seconds"],
        opponent_possession_seconds=analysis["opponent_possession_seconds"],
        defensive_third_seconds=analysis["defensive_third_seconds"],
        neutral_third_seconds=analysis["neutral_third_seconds"],
        offensive_third_seconds=analysis["offensive_third_seconds"],
        team_boost_collected=analysis["team_boost_collected"],
        opponent_boost_collected=analysis["opponent_boost_collected"],
        team_boost_stolen=analysis["team_boost_stolen"],
        opponent_boost_stolen=analysis["opponent_boost_stolen"],
    )

    all_players = analysis["all_players"]
    _upsert_match_players(
        conn, match_id, all_players, analysis["demolitions"], analysis["movement_stats"]
    )

    # Build identity -> player_id map (players were just upserted above)
    player_id_map: dict[tuple[str, str], int] = {}
    for player in all_players:
        identity = _extract_platform_id(player)
        if identity:
            row = conn.execute(
                "SELECT id FROM players WHERE platform = ? AND platform_id = ?",
                (identity[0], identity[1]),
            ).fetchone()
            if row:
                player_id_map[identity] = row[0]

    # Clear old events before re-inserting
    conn.execute("DELETE FROM match_events WHERE match_id = ?", (match_id,))
    for event_type, game_seconds, platform, platform_id, ev_team in analysis[
        "match_events"
    ]:
        player_id = player_id_map.get((platform, platform_id))
        if player_id is None:
            continue
        conn.execute(
            "INSERT INTO match_events (match_id, event_type, game_seconds, player_id, team) VALUES (?, ?, ?, ?, ?)",
            (match_id, event_type, game_seconds, player_id, ev_team),
        )


def ingest_match(conn: sqlite3.Connection, replay: dict):
    analysis = analyze_replay(replay)
    if analysis is None:
        return
    write_match(conn, analysis)


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
