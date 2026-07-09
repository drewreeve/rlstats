# Guards against the shape of drift that motivated this file: a migration adds a
# column (or one gets renamed) and MatchRow/MatchPlayerRow isn't updated to match,
# so the new column is never populated and nothing else notices.

from typing import Any

import pytest

from ingest import MatchPlayerRow, MatchRow
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
