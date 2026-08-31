# Replay Ingestion Pipeline
# rrrocket JSON -> SQLite

import logging
import sqlite3
from dataclasses import dataclass
from typing import Any, TypedDict

import db
from file_outcome import Skipped, SkipReason
from frame_analysis import FrameAnalysis, MatchEvent, PlayerMatchStats, analyze_frames
from player_identity import PlayerIdentity, from_player_stats
from rrrocket_schema import ParsedReplay, PlayerStatEntry, ReplayProperties

logger = logging.getLogger(__name__)

PAIRING_WINDOW = 1.0  # seconds — max time between goal and assist to count as a pairing


@dataclass(frozen=True)
class MatchPerspective:
    team: int | None
    team_score: int | None
    opponent_score: int | None
    result: str | None
    mvp_identity: PlayerIdentity | None


@dataclass(frozen=True)
class ReplayContext:
    """The tracked-team-relative view of a parsed replay, assembled once before
    frame analysis. See CONTEXT.md: "Replay Context".

    Wraps ``MatchPerspective`` with everything else a frame consumer needs up
    front: the bot-filtered ``player_stats`` blob, the detected ``game_mode``,
    the identity -> preferred-display-name ``player_names`` map, and the
    ``tracked_identities`` present in this match. Built by
    :func:`build_replay_context`; the pure frame reshapers (``analyze_frames``,
    ``extract_replay_frames``) take these fields unpacked, never the context
    object itself.
    """

    player_stats: dict[PlayerIdentity, PlayerStatEntry]
    perspective: MatchPerspective
    game_mode: str | None
    player_names: dict[PlayerIdentity, str]
    tracked_identities: frozenset[PlayerIdentity]


@dataclass(frozen=True)
class ReplayAnalysis:
    replay_hash: str
    played_at_sql: str
    duration: int | None
    forfeit: int
    team_size: int | None
    map_name: str | None
    frame_analysis: FrameAnalysis
    context: ReplayContext
    replay_filename: str | None = None


@dataclass(frozen=True)
class Analyzed:
    """A replay analyzed successfully; its match row is not yet written.

    Carries the heavy ``ReplayAnalysis`` through the pipeline. Mapped to
    ``file_outcome.Written`` once the DB write commits.
    """

    analysis: ReplayAnalysis


AnalysisResult = Analyzed | Skipped


@dataclass(frozen=True)
class OffensivePairing:
    scorer: PlayerIdentity
    assister: PlayerIdentity
    game_seconds: float
    team: int


def correlate_pairings(
    events: list[MatchEvent],
    window: float = PAIRING_WINDOW,
) -> list[OffensivePairing]:
    goal_events: list[tuple[float, PlayerIdentity, int]] = []
    assist_events: list[tuple[float, PlayerIdentity, int]] = []
    for e in events:
        if e.event_type == "goal":
            goal_events.append((e.game_seconds, e.identity, e.team))
        elif e.event_type == "assist":
            assist_events.append((e.game_seconds, e.identity, e.team))

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


def sync_tracked_players(
    conn: sqlite3.Connection,
    tracked_players: dict[PlayerIdentity, str],
) -> None:
    for identity, name in tracked_players.items():
        db.upsert(
            conn,
            "players",
            ["platform", "platform_id"],
            {
                "platform": identity.platform,
                "platform_id": identity.platform_id,
                "name": name,
                "is_tracked": 1,
            },
        )
    if tracked_players:
        placeholders = ",".join("(?,?)" for _ in tracked_players)
        params = [v for k in tracked_players for v in (k.platform, k.platform_id)]
        conn.execute(
            f"""UPDATE players SET is_tracked = 0
                WHERE is_tracked = 1
                AND (platform, platform_id) NOT IN ({placeholders})""",
            params,
        )
    else:
        conn.execute("UPDATE players SET is_tracked = 0 WHERE is_tracked = 1")


_SQL_DT_FMT = "%Y-%m-%d %H:%M:%S"


def detect_game_mode(team_size: Any, map_name: Any) -> str | None:
    if team_size == 3:
        return "3v3"
    if team_size == 2 and map_name and "hoop" in map_name.lower():
        return "hoops"
    if team_size == 2:
        return "2v2"
    return None


def resolve_perspective(
    player_stats: dict[PlayerIdentity, PlayerStatEntry],
    tracked_players: dict[PlayerIdentity, str],
    team0_score: Any,
    team1_score: Any,
    winning_team: int | None = None,
) -> MatchPerspective:
    tracked_items = [(k, v) for k, v in player_stats.items() if k in tracked_players]
    tracked_teams = {v.get("Team") for _, v in tracked_items}
    team = tracked_teams.pop() if tracked_teams else None

    if team == 0:
        team_score, opponent_score = team0_score, team1_score
    elif team == 1:
        team_score, opponent_score = team1_score, team0_score
    else:
        team_score, opponent_score = None, None

    result: str | None
    if team is not None and winning_team is not None:
        result = "win" if winning_team == team else "loss"
    elif team_score is None or opponent_score is None:
        result = None
    elif team_score > opponent_score:
        result = "win"
    elif team_score < opponent_score:
        result = "loss"
    else:
        result = None

    mvp_identity = (
        max(tracked_items, key=lambda kv: kv[1].get("Score", 0))[0]
        if tracked_items
        else None
    )
    return MatchPerspective(
        team=team,
        team_score=team_score,
        opponent_score=opponent_score,
        result=result,
        mvp_identity=mvp_identity,
    )


class MatchRow(TypedDict):
    replay_hash: str
    replay_filename: str | None
    played_at: str | None
    duration_seconds: int | None
    forfeit: int
    team_size: int | None
    team: int | None
    team_score: int | None
    opponent_score: int | None
    result: str | None
    team_mvp_player_id: int | None
    map_name: str | None
    game_mode: str | None
    team_possession_seconds: float | None
    opponent_possession_seconds: float | None
    defensive_zone_seconds: float | None
    neutral_zone_seconds: float | None
    offensive_zone_seconds: float | None
    team_boost_collected: int | None
    opponent_boost_collected: int | None
    team_boost_stolen: int | None
    opponent_boost_stolen: int | None


class MatchPlayerRow(TypedDict):
    match_id: int
    player_id: int
    team: int | None
    goals: int
    assists: int
    saves: int
    shots: int
    score: int
    demos: int
    demos_received: int
    boost_per_minute: float | None
    avg_speed: float | None
    time_supersonic_pct: float | None
    small_pads: int | None
    large_pads: int | None
    stolen_small_pads: int | None
    stolen_large_pads: int | None
    defensive_zone_seconds: float | None
    neutral_zone_seconds: float | None
    offensive_zone_seconds: float | None


def _build_match_player_row(
    match_id: int,
    player_id: int,
    player: PlayerStatEntry,
    stats: PlayerMatchStats,
) -> MatchPlayerRow:
    mv = stats.movement
    pz = stats.zone_seconds
    return MatchPlayerRow(
        match_id=match_id,
        player_id=player_id,
        team=player.get("Team"),
        goals=player.get("Goals", 0),
        assists=player.get("Assists", 0),
        saves=player.get("Saves", 0),
        shots=player.get("Shots", 0),
        score=player.get("Score", 0),
        demos=stats.demos,
        demos_received=stats.demos_received,
        boost_per_minute=mv.boost_per_minute if mv else None,
        avg_speed=mv.avg_speed if mv else None,
        time_supersonic_pct=mv.time_supersonic_pct if mv else None,
        small_pads=mv.small_pads if mv else None,
        large_pads=mv.large_pads if mv else None,
        stolen_small_pads=mv.stolen_small_pads if mv else None,
        stolen_large_pads=mv.stolen_large_pads if mv else None,
        defensive_zone_seconds=pz.defensive if pz else None,
        neutral_zone_seconds=pz.neutral if pz else None,
        offensive_zone_seconds=pz.offensive if pz else None,
    )


def _upsert_match(
    conn: sqlite3.Connection,
    *,
    replay_hash: str,
    replay_filename: str | None,
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
    row = MatchRow(
        replay_hash=replay_hash,
        replay_filename=replay_filename,
        played_at=played_at_sql,
        duration_seconds=duration,
        forfeit=forfeit,
        team_size=team_size,
        team=team,
        team_score=team_score,
        opponent_score=opponent_score,
        result=result,
        team_mvp_player_id=mvp_player_id,
        map_name=map_name,
        game_mode=game_mode,
        team_possession_seconds=fa.team_possession_seconds,
        opponent_possession_seconds=fa.opponent_possession_seconds,
        defensive_zone_seconds=fa.defensive_zone_seconds,
        neutral_zone_seconds=fa.neutral_zone_seconds,
        offensive_zone_seconds=fa.offensive_zone_seconds,
        team_boost_collected=fa.team_boost_collected,
        opponent_boost_collected=fa.opponent_boost_collected,
        team_boost_stolen=fa.team_boost_stolen,
        opponent_boost_stolen=fa.opponent_boost_stolen,
    )
    return int(db.upsert(conn, "matches", ["replay_hash"], row, returning="id"))


def _upsert_players(
    conn: sqlite3.Connection,
    player_names: dict[PlayerIdentity, str],
    tracked_identities: frozenset[PlayerIdentity],
) -> dict[PlayerIdentity, int]:
    player_id_map: dict[PlayerIdentity, int] = {}
    for identity, name in player_names.items():
        platform, platform_id = identity
        player_id_map[identity] = get_or_create_player(
            conn, platform, platform_id, name, identity in tracked_identities
        )
    return player_id_map


def _insert_match_players(
    conn: sqlite3.Connection,
    match_id: int,
    player_stats: dict[PlayerIdentity, PlayerStatEntry],
    player_id_map: dict[PlayerIdentity, int],
    per_player: dict[PlayerIdentity, PlayerMatchStats],
):
    _empty = PlayerMatchStats()
    for identity, player in player_stats.items():
        player_id = player_id_map.get(identity)
        if player_id is None:
            continue
        stats = per_player.get(identity, _empty)
        row = _build_match_player_row(match_id, player_id, player, stats)
        db.upsert(conn, "match_players", ["match_id", "player_id"], row)


def build_player_stats(
    props: ReplayProperties,
) -> dict[PlayerIdentity, PlayerStatEntry]:
    return {
        identity: p
        for p in props.get("PlayerStats", [])
        if not p.get("bBot") and (identity := from_player_stats(p))
    }


def build_replay_context(
    replay: ParsedReplay, tracked_players: dict[PlayerIdentity, str]
) -> ReplayContext:
    """Assemble the tracked-team-relative view of a parsed replay.

    The single owner of the preamble every frame consumer shares: bot-filtered
    player stats, match perspective, game-mode detection, and the identity ->
    preferred-display-name rule (configured display name, else the in-game
    ``Name``, else ``"Unknown"``). ``ingest`` and ``replay_view`` both call this;
    the pure reshapers receive its fields unpacked.
    """
    props = replay.properties
    player_stats = build_player_stats(props)
    perspective = resolve_perspective(
        player_stats,
        tracked_players,
        props.get("Team0Score", 0),
        props.get("Team1Score", 0),
        props.get("WinningTeam"),
    )
    player_names = {
        identity: tracked_players.get(identity) or entry.get("Name") or "Unknown"
        for identity, entry in player_stats.items()
    }
    tracked_identities = frozenset(
        identity for identity in player_stats if identity in tracked_players
    )
    return ReplayContext(
        player_stats=player_stats,
        perspective=perspective,
        game_mode=detect_game_mode(props.get("TeamSize"), props.get("MapName")),
        player_names=player_names,
        tracked_identities=tracked_identities,
    )


def validate_replay(
    replay: ParsedReplay, tracked_players: dict[PlayerIdentity, str]
) -> SkipReason | None:
    if not replay.match_guid:
        return SkipReason.NO_MATCH_GUID

    if replay.played_at is None:
        return SkipReason.MISSING_DATE

    player_stats = build_player_stats(replay.properties)
    tracked_raw = [v for k, v in player_stats.items() if k in tracked_players]
    if not tracked_raw:
        return SkipReason.NO_TRACKED_PLAYERS

    return None


def analyze_replay(
    replay: ParsedReplay,
    tracked_players: dict[PlayerIdentity, str],
    *,
    source_filename: str | None = None,
) -> AnalysisResult:
    skip = validate_replay(replay, tracked_players)
    if skip is not None:
        logger.debug("Skipping replay: %s", skip.value)
        return Skipped(skip)

    props = replay.properties

    replay_hash = replay.match_guid
    assert replay_hash is not None  # guaranteed by validate_replay
    assert replay.played_at is not None  # guaranteed by validate_replay
    played_at_sql = replay.played_at.strftime(_SQL_DT_FMT)
    duration = props.get("TotalSecondsPlayed")
    forfeit = 1 if props.get("bForfeit") else 0
    team_size = props.get("TeamSize")
    map_name = props.get("MapName")

    context = build_replay_context(replay, tracked_players)

    fa = analyze_frames(
        replay,
        context.perspective.team,
        context.tracked_identities,
        duration,
        context.game_mode,
    )

    return Analyzed(
        ReplayAnalysis(
            replay_hash=replay_hash,
            played_at_sql=played_at_sql,
            duration=duration,
            forfeit=forfeit,
            team_size=team_size,
            map_name=map_name,
            frame_analysis=fa,
            context=context,
            replay_filename=source_filename,
        )
    )


def write_match(conn: sqlite3.Connection, analysis: ReplayAnalysis) -> None:
    """Write a fully-analyzed replay to the DB as one atomic unit.

    This issues several separate statements (upsert match, upsert
    match_players, delete+insert match_events, delete+insert
    offensive_pairings) with no atomicity of its own otherwise. Wrapped in a
    savepoint so a failure partway through rolls back cleanly for any caller,
    including one already inside a larger multi-replay transaction.
    """
    conn.execute("SAVEPOINT write_match")
    try:
        _write_match(conn, analysis)
    except Exception:
        conn.execute("ROLLBACK TO SAVEPOINT write_match")
        conn.execute("RELEASE SAVEPOINT write_match")
        raise
    else:
        conn.execute("RELEASE SAVEPOINT write_match")


def _write_match(conn: sqlite3.Connection, analysis: ReplayAnalysis) -> None:
    context = analysis.context
    player_id_map = _upsert_players(
        conn, context.player_names, context.tracked_identities
    )
    perspective = context.perspective
    mvp_player_id = (
        player_id_map.get(perspective.mvp_identity)
        if perspective.mvp_identity
        else None
    )

    match_id = _upsert_match(
        conn,
        replay_hash=analysis.replay_hash,
        replay_filename=analysis.replay_filename,
        played_at_sql=analysis.played_at_sql,
        duration=analysis.duration,
        forfeit=analysis.forfeit,
        team_size=analysis.team_size,
        team=perspective.team,
        team_score=perspective.team_score,
        opponent_score=perspective.opponent_score,
        result=perspective.result,
        mvp_player_id=mvp_player_id,
        map_name=analysis.map_name,
        game_mode=context.game_mode,
        frame_analysis=analysis.frame_analysis,
    )

    _insert_match_players(
        conn,
        match_id,
        context.player_stats,
        player_id_map,
        analysis.frame_analysis.per_player(),
    )

    conn.execute("DELETE FROM match_events WHERE match_id = ?", (match_id,))
    for e in analysis.frame_analysis.match_events:
        player_id = player_id_map.get(e.identity)
        if player_id is None:
            continue
        conn.execute(
            "INSERT INTO match_events (match_id, event_type, game_seconds, player_id, team) VALUES (?, ?, ?, ?, ?)",
            (match_id, e.event_type, e.game_seconds, player_id, e.team),
        )

    tracked_identities = context.tracked_identities
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
