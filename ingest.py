# Replay Ingestion Pipeline
# rrrocket JSON -> SQLite

import datetime
import sqlite3
from dataclasses import dataclass
from typing import Any

from frame_analysis import analyze_frames
from player_identity import PlayerIdentity, from_player_stats

PAIRING_WINDOW = 1.0  # seconds — max time between goal and assist to count as a pairing


@dataclass(frozen=True)
class ReplayAnalysis:
    replay_hash: str
    played_at_sql: str
    duration: int | None
    forfeit: int
    team_size: int | None
    team: int | None
    team_score: int | None
    opponent_score: int | None
    result: str
    map_name: str | None
    game_mode: str | None
    team_possession_seconds: float | None
    opponent_possession_seconds: float | None
    defensive_third_seconds: float | None
    neutral_third_seconds: float | None
    offensive_third_seconds: float | None
    team_boost_collected: int | None
    opponent_boost_collected: int | None
    team_boost_stolen: int | None
    opponent_boost_stolen: int | None
    demolitions: dict[tuple[str, str], int]
    demos_received: dict[tuple[str, str], int]
    movement_stats: dict[tuple[str, str], dict[str, float | int]]
    match_events: list[tuple[str, float, str, str, int]]
    tracked_player_stats: list[dict[str, Any]]
    all_players: list[dict[str, Any]]


def get_or_create_player(
    conn: sqlite3.Connection,
    platform: str,
    platform_id: str,
    name: str,
    is_tracked: bool,
) -> int:
    tracked = 1 if is_tracked else 0
    return int(
        conn.execute(
            """INSERT INTO players (platform, platform_id, name, is_tracked) VALUES (?, ?, ?, ?)
           ON CONFLICT(platform, platform_id) DO UPDATE SET name = excluded.name
           RETURNING id""",
            (platform, platform_id, name, tracked),
        ).fetchone()[0]
    )


_SQL_DT_FMT = "%Y-%m-%d %H:%M:%S"


def _epoch_to_played_at(epoch: Any) -> str | None:
    if not epoch:
        return None
    try:
        return datetime.datetime.fromtimestamp(int(epoch), datetime.UTC).strftime(
            _SQL_DT_FMT
        )
    except ValueError, TypeError:
        return None


def _bakkesmod_played_at(replay: dict[str, Any]) -> str | None:
    debug_info: list[dict[str, str]] = replay.get("debug_info", []) or []
    for entry in debug_info:
        if entry.get("user") == "GameStartTime":
            try:
                dt = datetime.datetime.fromisoformat(entry["text"])
                return dt.astimezone(datetime.UTC).strftime(_SQL_DT_FMT)
            except ValueError, KeyError:
                continue
    return None


def _detect_game_mode(team_size: Any, map_name: Any) -> str | None:
    if team_size == 3:
        return "3v3"
    if team_size == 2 and map_name and "hoop" in map_name.lower():
        return "hoops"
    if team_size == 2:
        return "2v2"
    return None


def _tracked_player_stats(
    props: dict[str, Any], tracked_players: dict[PlayerIdentity, str]
) -> list[dict[str, Any]]:
    return [
        p
        for p in props.get("PlayerStats", [])
        if (identity := from_player_stats(p)) and identity in tracked_players
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
    conn: sqlite3.Connection,
    tracked_player_stats: list[dict[str, Any]],
    tracked_players: dict[PlayerIdentity, str],
) -> int | None:
    if not tracked_player_stats:
        return None
    mvp_stats = max(tracked_player_stats, key=lambda p: p.get("Score", 0))
    identity = from_player_stats(mvp_stats)
    if not identity:
        return None
    platform, platform_id = identity
    mvp_name = tracked_players[identity]
    return get_or_create_player(conn, platform, platform_id, mvp_name, True)


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
    return int(
        conn.execute(
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
    )


def _upsert_match_players(
    conn: sqlite3.Connection,
    match_id: int,
    all_players: list[dict[str, Any]],
    demolitions: dict[tuple[str, str], int],
    demos_received: dict[tuple[str, str], int],
    movement_stats: dict[tuple[str, str], dict[str, float]],
    tracked_players: dict[PlayerIdentity, str],
):
    for player in all_players:
        if player.get("bBot"):
            continue
        identity = from_player_stats(player)
        if not identity:
            continue
        platform, platform_id = identity
        name = player.get("Name", "Unknown")
        tracked_name = tracked_players.get(identity)
        if tracked_name:
            name = tracked_name
        player_id = get_or_create_player(
            conn, platform, platform_id, name, tracked_name is not None
        )
        demos = demolitions.get(identity, 0)
        demos_recv = demos_received.get(identity, 0)
        mv = movement_stats.get(identity, {})

        conn.execute(
            """
            INSERT INTO match_players (
                match_id, player_id, team,
                goals, assists, saves, shots, score, demos, demos_received,
                boost_per_minute, avg_speed, time_supersonic_pct,
                small_pads, large_pads, stolen_small_pads, stolen_large_pads
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(match_id, player_id) DO UPDATE SET
                team = excluded.team,
                goals = excluded.goals,
                assists = excluded.assists,
                saves = excluded.saves,
                shots = excluded.shots,
                score = excluded.score,
                demos = excluded.demos,
                demos_received = excluded.demos_received,
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
                demos_recv,
                mv.get("boost_per_minute"),
                mv.get("avg_speed"),
                mv.get("time_supersonic_pct"),
                mv.get("small_pads"),
                mv.get("large_pads"),
                mv.get("stolen_small_pads"),
                mv.get("stolen_large_pads"),
            ),
        )


def analyze_replay(
    replay: dict[str, Any], tracked_players: dict[PlayerIdentity, str]
) -> ReplayAnalysis | None:
    props = replay.get("properties", {})

    replay_hash = props.get("MatchGUID") or props.get("MatchGuid")
    if not replay_hash:
        return None

    # MatchStartEpoch was introduced in RL patch 2.43 (September 2024); pre-2.43 replays fall back to
    # BakkesMod's GameStartTime in debug_info (absent on replays saved manually from match history)
    played_at_sql = _epoch_to_played_at(
        props.get("MatchStartEpoch")
    ) or _bakkesmod_played_at(replay)
    if not played_at_sql:
        return None
    duration = props.get("TotalSecondsPlayed")
    forfeit = 1 if props.get("bForfeit") else 0
    team_size = props.get("TeamSize")
    map_name = props.get("MapName")
    game_mode = _detect_game_mode(team_size, map_name)

    team0_score = props.get("Team0Score", 0)
    team1_score = props.get("Team1Score", 0)
    tracked = _tracked_player_stats(props, tracked_players)
    team, team_score, opponent_score = _resolve_team_scores(
        tracked, team0_score, team1_score
    )
    result = _resolve_result(team_score, opponent_score)
    if result is None:
        return None

    fa = analyze_frames(replay, team, set(tracked_players.keys()), duration, game_mode)

    return ReplayAnalysis(
        replay_hash=replay_hash,
        played_at_sql=played_at_sql,
        duration=duration,
        forfeit=forfeit,
        team_size=team_size,
        team=team,
        team_score=team_score,
        opponent_score=opponent_score,
        result=result,
        map_name=map_name,
        game_mode=game_mode,
        team_possession_seconds=fa.team_possession_seconds,
        opponent_possession_seconds=fa.opponent_possession_seconds,
        defensive_third_seconds=fa.defensive_third_seconds,
        neutral_third_seconds=fa.neutral_third_seconds,
        offensive_third_seconds=fa.offensive_third_seconds,
        team_boost_collected=fa.team_boost_collected,
        opponent_boost_collected=fa.opponent_boost_collected,
        team_boost_stolen=fa.team_boost_stolen,
        opponent_boost_stolen=fa.opponent_boost_stolen,
        demolitions=fa.demolitions,
        demos_received=fa.demos_received,
        movement_stats=fa.movement_stats,
        match_events=fa.match_events,
        tracked_player_stats=tracked,
        all_players=props.get("PlayerStats", []),
    )


def write_match(
    conn: sqlite3.Connection,
    analysis: ReplayAnalysis,
    tracked_players: dict[PlayerIdentity, str],
):
    mvp_player_id = _resolve_mvp_player_id(
        conn, analysis.tracked_player_stats, tracked_players
    )

    match_id = _upsert_match(
        conn,
        replay_hash=analysis.replay_hash,
        played_at_sql=analysis.played_at_sql,
        duration=analysis.duration,
        forfeit=analysis.forfeit,
        team_size=analysis.team_size,
        team=analysis.team,
        team_score=analysis.team_score,
        opponent_score=analysis.opponent_score,
        result=analysis.result,
        mvp_player_id=mvp_player_id,
        map_name=analysis.map_name,
        game_mode=analysis.game_mode,
        team_possession_seconds=analysis.team_possession_seconds,
        opponent_possession_seconds=analysis.opponent_possession_seconds,
        defensive_third_seconds=analysis.defensive_third_seconds,
        neutral_third_seconds=analysis.neutral_third_seconds,
        offensive_third_seconds=analysis.offensive_third_seconds,
        team_boost_collected=analysis.team_boost_collected,
        opponent_boost_collected=analysis.opponent_boost_collected,
        team_boost_stolen=analysis.team_boost_stolen,
        opponent_boost_stolen=analysis.opponent_boost_stolen,
    )

    all_players = analysis.all_players
    _upsert_match_players(
        conn,
        match_id,
        all_players,
        analysis.demolitions,
        analysis.demos_received,
        analysis.movement_stats,
        tracked_players,
    )

    # Build identity -> player_id map (players were just upserted above)
    player_id_map: dict[tuple[str, str], int] = {}
    for player in all_players:
        identity = from_player_stats(player)
        if identity:
            row = conn.execute(
                "SELECT id FROM players WHERE platform = ? AND platform_id = ?",
                identity,
            ).fetchone()
            if row:
                player_id_map[identity] = row[0]

    # Insert match events and collect goal/assist lists for pairing correlation
    goal_events: list[tuple[float, int, int]] = []
    assist_events: list[tuple[float, int, int]] = []

    conn.execute("DELETE FROM match_events WHERE match_id = ?", (match_id,))
    for (
        event_type,
        game_seconds,
        platform,
        platform_id,
        ev_team,
    ) in analysis.match_events:
        player_id = player_id_map.get((platform, platform_id))
        if player_id is None:
            continue
        conn.execute(
            "INSERT INTO match_events (match_id, event_type, game_seconds, player_id, team) VALUES (?, ?, ?, ?, ?)",
            (match_id, event_type, game_seconds, player_id, ev_team),
        )
        if event_type == "goal":
            goal_events.append((game_seconds, player_id, ev_team))
        elif event_type == "assist":
            assist_events.append((game_seconds, player_id, ev_team))

    # Correlate goal+assist events into offensive pairings
    tracked_player_ids = {
        player_id_map[identity]
        for identity in tracked_players
        if identity in player_id_map
    }

    conn.execute("DELETE FROM offensive_pairings WHERE match_id = ?", (match_id,))

    used_assists: set[int] = set()
    for g_time, g_player_id, g_team in goal_events:
        best_idx = None
        best_delta = float("inf")
        for i, (a_time, a_player_id, a_team) in enumerate(assist_events):
            if i in used_assists or a_team != g_team or a_player_id == g_player_id:
                continue
            delta = abs(g_time - a_time)
            if delta <= PAIRING_WINDOW and delta < best_delta:
                best_delta = delta
                best_idx = i

        if best_idx is None:
            continue

        _, a_player_id, _ = assist_events[best_idx]
        used_assists.add(best_idx)
        if g_player_id in tracked_player_ids and a_player_id in tracked_player_ids:
            conn.execute(
                "INSERT INTO offensive_pairings (match_id, game_seconds, scorer_player_id, assister_player_id, team) VALUES (?, ?, ?, ?, ?)",
                (match_id, g_time, g_player_id, a_player_id, g_team),
            )


def ingest_match(
    conn: sqlite3.Connection,
    replay: dict[str, Any],
    tracked_players: dict[PlayerIdentity, str],
):
    analysis = analyze_replay(replay, tracked_players)
    if analysis is None:
        raise ValueError("Replay could not be analyzed")
    write_match(conn, analysis, tracked_players)
