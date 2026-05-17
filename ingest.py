# Replay Ingestion Pipeline
# rrrocket JSON -> SQLite

import datetime
import sqlite3
from dataclasses import dataclass
from typing import Any

from frame_analysis import FrameAnalysis, analyze_frames
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
    frame_analysis: FrameAnalysis
    tracked_player_stats: list[dict[str, Any]]
    tracked_names: dict[PlayerIdentity, str]
    all_players: list[dict[str, Any]]


@dataclass(frozen=True)
class OffensivePairing:
    scorer: PlayerIdentity
    assister: PlayerIdentity
    game_seconds: float
    team: int


def correlate_pairings(
    events: list[tuple[str, float, str, str, int]],
    window: float = PAIRING_WINDOW,
) -> list[OffensivePairing]:
    goal_events: list[tuple[float, PlayerIdentity, int]] = []
    assist_events: list[tuple[float, PlayerIdentity, int]] = []
    for event_type, game_seconds, platform, platform_id, team in events:
        identity = PlayerIdentity(platform, platform_id)
        if event_type == "goal":
            goal_events.append((game_seconds, identity, team))
        elif event_type == "assist":
            assist_events.append((game_seconds, identity, team))

    pairings: list[OffensivePairing] = []
    used_assists: set[int] = set()
    for g_time, g_identity, g_team in goal_events:
        best_idx = None
        best_delta = float("inf")
        for i, (a_time, a_identity, a_team) in enumerate(assist_events):
            if i in used_assists or a_team != g_team or a_identity == g_identity:
                continue
            delta = abs(g_time - a_time)
            if delta <= window and delta < best_delta:
                best_delta = delta
                best_idx = i
        if best_idx is None:
            continue
        _, a_identity, _ = assist_events[best_idx]
        used_assists.add(best_idx)
        pairings.append(
            OffensivePairing(
                scorer=g_identity,
                assister=a_identity,
                game_seconds=g_time,
                team=g_team,
            )
        )
    return pairings


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


def _find_mvp_identity(
    tracked_player_stats: list[dict[str, Any]],
) -> PlayerIdentity | None:
    if not tracked_player_stats:
        return None
    mvp_stats = max(tracked_player_stats, key=lambda p: p.get("Score", 0))
    return from_player_stats(mvp_stats)


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
    frame_analysis: FrameAnalysis,
) -> int:
    fa = frame_analysis
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
                fa.team_possession_seconds,
                fa.opponent_possession_seconds,
                fa.defensive_third_seconds,
                fa.neutral_third_seconds,
                fa.offensive_third_seconds,
                fa.team_boost_collected,
                fa.opponent_boost_collected,
                fa.team_boost_stolen,
                fa.opponent_boost_stolen,
            ),
        ).fetchone()[0]
    )


def _upsert_players(
    conn: sqlite3.Connection,
    all_players: list[dict[str, Any]],
    tracked_names: dict[PlayerIdentity, str],
) -> dict[PlayerIdentity, int]:
    player_id_map: dict[PlayerIdentity, int] = {}
    for player in all_players:
        if player.get("bBot"):
            continue
        identity = from_player_stats(player)
        if not identity:
            continue
        platform, platform_id = identity
        name = player.get("Name", "Unknown")
        display_name = tracked_names.get(identity)
        if display_name:
            name = display_name
        player_id_map[identity] = get_or_create_player(
            conn, platform, platform_id, name, display_name is not None
        )
    return player_id_map


def _insert_match_players(
    conn: sqlite3.Connection,
    match_id: int,
    all_players: list[dict[str, Any]],
    player_id_map: dict[PlayerIdentity, int],
    frame_analysis: FrameAnalysis,
):
    for player in all_players:
        if player.get("bBot"):
            continue
        identity = from_player_stats(player)
        if not identity:
            continue
        player_id = player_id_map.get(identity)
        if player_id is None:
            continue
        demos = frame_analysis.demolitions.get(identity, 0)
        demos_recv = frame_analysis.demos_received.get(identity, 0)
        mv = frame_analysis.movement_stats.get(identity, {})
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

    tracked_names = {
        identity: tracked_players[identity]
        for p in tracked
        if (identity := from_player_stats(p))
    }

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
        frame_analysis=fa,
        tracked_player_stats=tracked,
        tracked_names=tracked_names,
        all_players=props.get("PlayerStats", []),
    )


def write_match(conn: sqlite3.Connection, analysis: ReplayAnalysis) -> None:
    player_id_map = _upsert_players(conn, analysis.all_players, analysis.tracked_names)
    mvp_identity = _find_mvp_identity(analysis.tracked_player_stats)
    mvp_player_id = player_id_map.get(mvp_identity) if mvp_identity else None

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
        frame_analysis=analysis.frame_analysis,
    )

    _insert_match_players(
        conn,
        match_id,
        analysis.all_players,
        player_id_map,
        analysis.frame_analysis,
    )

    conn.execute("DELETE FROM match_events WHERE match_id = ?", (match_id,))
    for (
        event_type,
        game_seconds,
        platform,
        platform_id,
        ev_team,
    ) in analysis.frame_analysis.match_events:
        player_id = player_id_map.get(PlayerIdentity(platform, platform_id))
        if player_id is None:
            continue
        conn.execute(
            "INSERT INTO match_events (match_id, event_type, game_seconds, player_id, team) VALUES (?, ?, ?, ?, ?)",
            (match_id, event_type, game_seconds, player_id, ev_team),
        )

    tracked_identities = set(analysis.tracked_names.keys())
    pairings = [
        p
        for p in correlate_pairings(analysis.frame_analysis.match_events)
        if p.scorer in tracked_identities and p.assister in tracked_identities
    ]

    conn.execute("DELETE FROM offensive_pairings WHERE match_id = ?", (match_id,))
    for p in pairings:
        scorer_id = player_id_map.get(p.scorer)
        assister_id = player_id_map.get(p.assister)
        if scorer_id is not None and assister_id is not None:
            conn.execute(
                "INSERT INTO offensive_pairings (match_id, game_seconds, scorer_player_id, assister_player_id, team) VALUES (?, ?, ?, ?, ?)",
                (match_id, p.game_seconds, scorer_id, assister_id, p.team),
            )


def ingest_match(
    conn: sqlite3.Connection,
    replay: dict[str, Any],
    tracked_players: dict[PlayerIdentity, str],
):
    analysis = analyze_replay(replay, tracked_players)
    if analysis is None:
        raise ValueError("Replay could not be analyzed")
    write_match(conn, analysis)
