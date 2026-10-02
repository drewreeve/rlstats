# Guards against the shape of drift that motivated this file: a migration adds a
# column (or one gets renamed) and MatchRow, MatchPlayerRow or a recorded stat
# field isn't updated to match, so the new column is never populated and
# nothing else notices.

from typing import Any, is_typeddict

import pytest

import queries
from db import sql
from frame_analysis import RECORDED_STAT_COUNTERS
from ingest import MatchPlayerRow, MatchRow
from queries import READ_ROW_TYPES
from tests.fixtures import in_memory_db

# Dummy bind values for every :placeholder any read query uses. SQLite ignores
# keys a given query doesn't bind, so the whole dict is passed to every query;
# a query that needs a name absent here fails loudly with a clear error.
_GUARD_PARAMS: dict[str, Any] = {
    "game_mode": "3v3",
    "match_id": 1,
    "player_name": "x",
    "result": None,
    "search": None,
    "date_from": None,
    "date_to": None,
    "per_page": 1,
    "offset": 0,
}


def _table_columns(table: str) -> set[str]:
    rows = in_memory_db().execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1] for row in rows}


@pytest.mark.parametrize(
    "table, row_keys, excluded_columns",
    [
        ("matches", set(MatchRow.__annotations__), {"id"}),
        (
            "match_players",
            set(MatchPlayerRow.__annotations__) | set(RECORDED_STAT_COUNTERS),
            set[str](),
        ),
    ],
)
def test_row_keys_match_table_columns(
    table: str, row_keys: set[str], excluded_columns: set[str]
):
    assert row_keys == _table_columns(table) - excluded_columns


def test_recorded_stats_dont_shadow_match_player_columns():
    # _build_match_player_row merges them in; a shared name would overwrite.
    assert set(RECORDED_STAT_COUNTERS).isdisjoint(MatchPlayerRow.__annotations__)


@pytest.mark.parametrize(
    "query, row_type",
    list(READ_ROW_TYPES.items()),
    ids=[q.__name__ for q in READ_ROW_TYPES],
)
def test_read_row_type_matches_query_columns(query: Any, row_type: Any):
    """Read-side analogue of test_row_keys_match_table_columns: each row
    type's keys must match the columns its query projects. Runs the query
    against a migrated empty DB and compares cursor.description to the
    TypedDict's keys. Required keys must all appear; every projected column
    must be a known (required or NotRequired) key."""
    conn = in_memory_db()
    projected = {d[0] for d in conn.execute(query.sql, _GUARD_PARAMS).description}
    # Every TypedDict exposes both frozensets regardless of total=/NotRequired.
    required: set[str] = set(row_type.__required_keys__)
    allowed: set[str] = required | set(row_type.__optional_keys__)
    assert required <= projected, (
        f"{query.__name__}: row type keys missing from query: {required - projected}"
    )
    assert projected <= allowed, (
        f"{query.__name__}: query columns absent from row type: {projected - allowed}"
    )


def test_read_row_types_registry_is_complete():
    """Every row TypedDict in queries.py must be a value in READ_ROW_TYPES, and
    every aiosql read query a key — so a query or row type added later can't
    slip past test_read_row_type_matches_query_columns.

    The stat half of READ_ROW_TYPES comes from STAT_READS; this check earns its
    keep on _NON_STAT_READ_ROW_TYPES, the hand-listed half, where a new query
    would otherwise reach the unchecked cast unguarded."""
    defined = {v for v in vars(queries).values() if is_typeddict(v)}
    assert defined == set(READ_ROW_TYPES.values())

    all_queries = {
        getattr(sql, name)
        for name in sql.available_queries
        if not name.endswith("_cursor")
    }
    assert set(READ_ROW_TYPES) == all_queries
