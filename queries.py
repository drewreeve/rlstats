"""Query Layer — the read seam between the web layer and the database.

Every ``/api`` read route calls one function in this module and returns its
result unchanged. Row shapes are ``*Row`` TypedDicts whose keys mirror each
query's SELECT aliases; composite results (``Streaks``, ``GoalTiming``) are
frozen dataclasses. The aiosql loader (``db.sql``, reading ``sql/*.sql``) and
all row reshaping — key renaming, rounding, the ``timeline`` game-mode branch,
empty-state defaults — are implementation details here, not part of the
interface.

``_rows()`` / ``_first()`` are the single ``Any``-to-typed hop: they ``cast``
each ``sqlite3.Row`` to its row type. That cast is unchecked;
``tests/test_stats_registry.py`` guards it via ``READ_ROW_TYPES`` by running
every query against a migrated empty database and asserting its projected
columns match the row type's keys — the read-side analogue of the ``MatchRow``
/ ``MatchPlayerRow`` drift check.

See CONTEXT.md: "Query Layer".
"""

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, NotRequired, TypedDict, cast

from db import sql

# -- Row shapes (one per list query) --


class ShootingRow(TypedDict):
    player: str
    goals: int
    shots: int
    shooting_pct: float | None


class PlayerStatsRow(TypedDict):
    player: str
    matches: int
    goals: int
    assists: int
    saves: int
    shots: int
    demos: int


class NByNRow(TypedDict):
    player: str
    n: int
    matches: int


class MvpWinsRow(TypedDict):
    player: str
    mvp_matches: int
    mvp_wins: int
    win_rate: float


class MvpLossesRow(TypedDict):
    player: str
    loss_mvps: int


class WeekdayRow(TypedDict):
    weekday: str
    matches: int
    wins: int
    losses: int
    win_rate: float | None


class AvgScoreRow(TypedDict):
    player: str
    matches: int
    total_score: int
    avg_score: float


class ScoreDifferentialRow(TypedDict):
    differential: int
    match_count: int


class GoalContributionRow(TypedDict):
    player: str
    matches: int
    avg_goal_contribution: float | None


class ScoreRangeRow(TypedDict):
    player: str
    min: int
    max: int


class OffensivePairingsRow(TypedDict):
    pairing: str
    assister: str
    goals: int


class TimelineRow(TypedDict):
    date: str
    wins: int
    losses: int
    win_rate: float | None
    pairing: NotRequired[str]  # present only for 2-per-team modes


class _StreaksRow(TypedDict):
    longest_win_streak: int | None
    longest_loss_streak: int | None


class _GoalTimingRow(TypedDict):
    avg_concede_delay: float | None
    avg_lead_duration: float | None


class MatchListRow(TypedDict):
    id: int
    game_mode: str | None
    result: str | None
    forfeit: int
    score: str  # "team-opponent"
    played_at: str
    mvp: str | None


class _CountRow(TypedDict):
    total: int


class MatchMetadataRow(TypedDict):
    id: int
    played_at: str | None
    game_mode: str | None
    result: str | None
    forfeit: int
    team_score: int | None
    opponent_score: int | None
    duration_seconds: int | None
    team: int | None
    team_possession_seconds: float | None
    opponent_possession_seconds: float | None
    defensive_zone_seconds: float | None
    neutral_zone_seconds: float | None
    offensive_zone_seconds: float | None
    team_boost_collected: int | None
    opponent_boost_collected: int | None
    team_boost_stolen: int | None
    opponent_boost_stolen: int | None


class MatchEventRow(TypedDict):
    event_type: str
    game_seconds: float
    team: int
    name: str


class MatchPlayerReadRow(TypedDict):
    name: str
    is_tracked: int
    team: int | None
    score: int
    goals: int
    assists: int
    saves: int
    shots: int
    demos: int
    shooting_pct: float | None
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


class PlayerCareerRow(TypedDict):
    player: str
    matches: int
    goals: int
    assists: int
    saves: int
    shots: int
    demos: int
    avg_score: float | None
    shooting_pct: float | None
    mvp_count: int
    wins: int
    losses: int
    avg_boost_per_minute: float | None
    avg_supersonic_pct: float | None
    avg_demos: float | None
    avg_demos_received: float | None
    avg_defensive_zone_seconds: float | None
    avg_neutral_zone_seconds: float | None
    avg_offensive_zone_seconds: float | None


class PlayerTimeSeriesRow(TypedDict):
    date: str
    goals: int
    assists: int
    saves: int
    shots: int
    avg_score: float | None
    mvp_count: int
    shooting_pct: float | None
    avg_speed: float | None


# -- Composite results --


@dataclass(frozen=True)
class Streaks:
    longest_win_streak: int
    longest_loss_streak: int


@dataclass(frozen=True)
class GoalTiming:
    avg_seconds_to_concede: int | None
    avg_lead_duration: int | None


@dataclass(frozen=True)
class MatchesPage:
    matches: list[MatchListRow]
    total: int
    page: int
    per_page: int


@dataclass(frozen=True)
class MatchDetail:
    match: MatchMetadataRow
    events: list[MatchEventRow]
    team_players: list[MatchPlayerReadRow]
    opponent_players: list[MatchPlayerReadRow]


# -- The single Any -> typed hop --


def _rows[R](
    query: Callable[..., Any],
    row_type: type[R],
    conn: sqlite3.Connection,
    /,
    **params: Any,
) -> list[R]:
    """Run an aiosql list query and cast each row to ``row_type``.

    ``row_type`` only binds ``R`` for the checker — the cast is unchecked here.
    ``READ_ROW_TYPES`` + ``tests/test_stats_registry.py`` verify the keys.
    """
    return [cast(R, dict(r)) for r in query(conn, **params)]


def _first[R](
    query: Callable[..., Any],
    row_type: type[R],
    conn: sqlite3.Connection,
    /,
    **params: Any,
) -> R | None:
    """First row of an aiosql list query, or ``None`` if it returned nothing."""
    return next(iter(_rows(query, row_type, conn, **params)), None)


def _one[R](
    query: Callable[..., Any],
    row_type: type[R],
    conn: sqlite3.Connection,
    /,
    **params: Any,
) -> R | None:
    """Row of an aiosql select-one (``^``) query, cast to ``row_type``, or
    ``None`` when the query matched nothing."""
    row = query(conn, **params)
    return None if row is None else cast(R, dict(row))


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


# -- Stat reads (registered in server.STAT_ROUTES) --


def shooting_pct(conn: sqlite3.Connection, game_mode: str) -> list[ShootingRow]:
    return _rows(sql.shooting_pct, ShootingRow, conn, game_mode=game_mode)


def player_stats(conn: sqlite3.Connection, game_mode: str) -> list[PlayerStatsRow]:
    return _rows(sql.player_stats, PlayerStatsRow, conn, game_mode=game_mode)


def n_by_n_stats(conn: sqlite3.Connection, game_mode: str) -> list[NByNRow]:
    return _rows(sql.n_by_n_stats, NByNRow, conn, game_mode=game_mode)


def mvp_wins(conn: sqlite3.Connection, game_mode: str) -> list[MvpWinsRow]:
    return _rows(sql.mvp_wins, MvpWinsRow, conn, game_mode=game_mode)


def mvp_losses(conn: sqlite3.Connection, game_mode: str) -> list[MvpLossesRow]:
    return _rows(sql.mvp_losses, MvpLossesRow, conn, game_mode=game_mode)


def weekday(conn: sqlite3.Connection, game_mode: str) -> list[WeekdayRow]:
    return _rows(sql.weekday, WeekdayRow, conn, game_mode=game_mode)


def avg_score(conn: sqlite3.Connection, game_mode: str) -> list[AvgScoreRow]:
    return _rows(sql.avg_score, AvgScoreRow, conn, game_mode=game_mode)


def score_differential(
    conn: sqlite3.Connection, game_mode: str
) -> list[ScoreDifferentialRow]:
    return _rows(
        sql.score_differential, ScoreDifferentialRow, conn, game_mode=game_mode
    )


def avg_goal_contribution(
    conn: sqlite3.Connection, game_mode: str
) -> list[GoalContributionRow]:
    return _rows(
        sql.avg_goal_contribution, GoalContributionRow, conn, game_mode=game_mode
    )


def score_range(conn: sqlite3.Connection, game_mode: str) -> list[ScoreRangeRow]:
    return _rows(sql.score_range, ScoreRangeRow, conn, game_mode=game_mode)


def offensive_pairings(
    conn: sqlite3.Connection, game_mode: str
) -> list[OffensivePairingsRow]:
    return _rows(
        sql.offensive_pairings, OffensivePairingsRow, conn, game_mode=game_mode
    )


# -- Reads that reshape (were inline in server route closures) --


def timeline(conn: sqlite3.Connection, game_mode: str) -> list[TimelineRow]:
    query = (
        sql.win_loss_daily_pairings
        if game_mode in ("2v2", "hoops")
        else sql.win_loss_daily
    )
    return _rows(query, TimelineRow, conn, game_mode=game_mode)


def streaks(conn: sqlite3.Connection, game_mode: str) -> Streaks:
    row = _first(sql.streaks, _StreaksRow, conn, game_mode=game_mode)
    if row is None:
        return Streaks(longest_win_streak=0, longest_loss_streak=0)
    return Streaks(
        longest_win_streak=row["longest_win_streak"] or 0,
        longest_loss_streak=row["longest_loss_streak"] or 0,
    )


def goal_timing(conn: sqlite3.Connection, game_mode: str) -> GoalTiming:
    row = _first(sql.goal_timing, _GoalTimingRow, conn, game_mode=game_mode)
    concede = row["avg_concede_delay"] if row is not None else None
    lead = row["avg_lead_duration"] if row is not None else None
    return GoalTiming(
        avg_seconds_to_concede=round(concede) if concede is not None else None,
        avg_lead_duration=round(lead) if lead is not None else None,
    )


# -- Match reads --


def matches(
    conn: sqlite3.Connection,
    *,
    page: int = 1,
    per_page: int = 25,
    search: str = "",
    game_mode: str = "",
    result: str = "",
    date_from: str = "",
    date_to: str = "",
) -> MatchesPage:
    filters: dict[str, Any] = dict(
        game_mode=game_mode or None,
        result=result or None,
        search=f"%{_escape_like(search)}%" if search else None,
        date_from=date_from or None,
        date_to=date_to or None,
    )
    count = _one(sql.count_matches, _CountRow, conn, **filters)
    rows = _rows(
        sql.list_matches,
        MatchListRow,
        conn,
        **filters,
        per_page=per_page,
        offset=(page - 1) * per_page,
    )
    return MatchesPage(
        matches=rows,
        total=count["total"] if count is not None else 0,
        page=page,
        per_page=per_page,
    )


def match_players(conn: sqlite3.Connection, match_id: int) -> list[MatchPlayerReadRow]:
    return _rows(sql.match_players, MatchPlayerReadRow, conn, match_id=match_id)


def match_detail(conn: sqlite3.Connection, match_id: int) -> MatchDetail | None:
    meta = _one(sql.match_metadata, MatchMetadataRow, conn, match_id=match_id)
    if meta is None:
        return None
    team_num = meta["team"]
    players = match_players(conn, match_id)
    return MatchDetail(
        match=meta,
        events=_rows(sql.match_events, MatchEventRow, conn, match_id=match_id),
        team_players=[p for p in players if p["team"] == team_num],
        opponent_players=[p for p in players if p["team"] != team_num],
    )


# -- Player reads --

# Zero-valued career row for a tracked player with no matches in the mode.
# Typed against PlayerCareerRow, so a new column in player_career_stats that
# lands in the row type but not here is a type error.
EMPTY_PLAYER_CAREER: PlayerCareerRow = {
    "player": "",
    "matches": 0,
    "goals": 0,
    "assists": 0,
    "saves": 0,
    "shots": 0,
    "demos": 0,
    "avg_score": None,
    "shooting_pct": None,
    "mvp_count": 0,
    "wins": 0,
    "losses": 0,
    "avg_boost_per_minute": None,
    "avg_supersonic_pct": None,
    "avg_demos": None,
    "avg_demos_received": None,
    "avg_defensive_zone_seconds": None,
    "avg_neutral_zone_seconds": None,
    "avg_offensive_zone_seconds": None,
}


def player_career(
    conn: sqlite3.Connection, player_name: str, game_mode: str
) -> PlayerCareerRow:
    row = _one(
        sql.player_career_stats,
        PlayerCareerRow,
        conn,
        player_name=player_name,
        game_mode=game_mode,
    )
    if row is None:
        return {**EMPTY_PLAYER_CAREER, "player": player_name}
    return row


def player_time_series(
    conn: sqlite3.Connection, player_name: str, game_mode: str
) -> list[PlayerTimeSeriesRow]:
    return _rows(
        sql.player_time_series,
        PlayerTimeSeriesRow,
        conn,
        player_name=player_name,
        game_mode=game_mode,
    )


# -- Drift guard registry --

# Each aiosql read query mapped to the row type its results are cast to.
# tests/test_stats_registry.py runs every query against a migrated empty DB
# and asserts cursor.description matches the row type's keys.
READ_ROW_TYPES: dict[Any, Any] = {
    sql.shooting_pct: ShootingRow,
    sql.player_stats: PlayerStatsRow,
    sql.n_by_n_stats: NByNRow,
    sql.mvp_wins: MvpWinsRow,
    sql.mvp_losses: MvpLossesRow,
    sql.weekday: WeekdayRow,
    sql.avg_score: AvgScoreRow,
    sql.score_differential: ScoreDifferentialRow,
    sql.avg_goal_contribution: GoalContributionRow,
    sql.score_range: ScoreRangeRow,
    sql.offensive_pairings: OffensivePairingsRow,
    sql.win_loss_daily: TimelineRow,
    sql.win_loss_daily_pairings: TimelineRow,
    sql.streaks: _StreaksRow,
    sql.goal_timing: _GoalTimingRow,
    sql.count_matches: _CountRow,
    sql.list_matches: MatchListRow,
    sql.match_metadata: MatchMetadataRow,
    sql.match_events: MatchEventRow,
    sql.match_players: MatchPlayerReadRow,
    sql.player_career_stats: PlayerCareerRow,
    sql.player_time_series: PlayerTimeSeriesRow,
}
