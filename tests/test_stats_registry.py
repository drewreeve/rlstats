# Guards against the shape of drift that motivated this file: a migration adds a
# column (or one gets renamed) and MatchRow/MatchPlayerRow isn't updated to match,
# so the new column is never populated and nothing else notices.

from typing import Any, is_typeddict

import pytest

import queries
from ingest import MatchPlayerRow, MatchRow
from queries import READ_ROW_TYPES
from tests.fixtures import in_memory_db


@pytest.mark.parametrize(
    "table, row_type, excluded_columns",
    [
        ("matches", MatchRow, {"id"}),
        ("match_players", MatchPlayerRow, set[str]()),
    ],
)
def test_row_type_matches_table_columns(
    table: str, row_type: Any, excluded_columns: set[str]
):
    conn = in_memory_db()
    table_columns = {
        row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }
    assert set(row_type.__annotations__) == table_columns - excluded_columns


@pytest.mark.parametrize(
    "query, row_type",
    list(READ_ROW_TYPES.items()),
    ids=[q.__name__ for q in READ_ROW_TYPES],
)
def test_read_row_type_matches_query_columns(query: Any, row_type: Any):
    """Read-side analogue: each row type's keys must match the columns its
    query actually projects. Runs the query against a migrated empty DB and
    compares cursor.description to the TypedDict's keys. Required keys must all
    appear; every projected column must be a known (required or NotRequired)
    key."""
    conn = in_memory_db()
    projected = {
        d[0] for d in conn.execute(query.sql, {"game_mode": "3v3"}).description
    }
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
    """Every row TypedDict defined in queries.py must be a value in
    READ_ROW_TYPES, so test_read_row_type_matches_query_columns can't silently
    skip one added in a later slice."""
    defined = {v for v in vars(queries).values() if is_typeddict(v)}
    assert defined == set(READ_ROW_TYPES.values())
