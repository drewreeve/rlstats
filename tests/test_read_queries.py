"""Unit tests for the query layer (queries.py).

Covers the reshaping that previously lived in server.py route closures and was
only reachable through TestClient: streaks' ``or 0``, goal_timing's rounding
and rename, and timeline's game-mode branch.
"""

import sqlite3

import queries
from tests.fixtures import cached_db, in_memory_db


def _db(*replays: str) -> sqlite3.Connection:
    conn = cached_db(*replays)
    conn.row_factory = sqlite3.Row
    return conn


def _empty_db() -> sqlite3.Connection:
    conn = in_memory_db()
    conn.row_factory = sqlite3.Row
    return conn


# -- passthrough stat reads --


def test_shooting_pct_rows_have_expected_keys():
    rows = queries.shooting_pct(_db("match.json"), "3v3")
    assert rows
    assert set(rows[0]) == {"player", "goals", "shots", "shooting_pct"}


def test_stat_read_on_empty_db_returns_empty_list():
    assert queries.player_stats(_empty_db(), "3v3") == []


# -- streaks reshaping --


def test_streaks_returns_dataclass_with_real_counts():
    # one win -> MAX(CASE...) yields (1, 0): both non-NULL, passed through as-is
    result = queries.streaks(_db("match.json"), "3v3")
    assert result == queries.Streaks(longest_win_streak=1, longest_loss_streak=0)


def test_streaks_empty_db_coalesces_null_to_zero():
    # islands CTE is empty -> SELECT MAX(...) yields (NULL, NULL)
    result = queries.streaks(_empty_db(), "3v3")
    assert result == queries.Streaks(longest_win_streak=0, longest_loss_streak=0)


# -- goal_timing reshaping --


def test_goal_timing_rounds_to_int_and_renames_concede_key():
    result = queries.goal_timing(_db("match.json"), "3v3")
    assert isinstance(result, queries.GoalTiming)
    for value in (result.avg_seconds_to_concede, result.avg_lead_duration):
        assert value is None or isinstance(value, int)


def test_goal_timing_empty_db_is_all_none():
    result = queries.goal_timing(_empty_db(), "3v3")
    assert result == queries.GoalTiming(
        avg_seconds_to_concede=None, avg_lead_duration=None
    )


# -- timeline game-mode branch --


def test_timeline_3v3_omits_pairing_key():
    rows = queries.timeline(_db("match.json", "zero_score.json"), "3v3")
    assert rows
    assert "pairing" not in rows[0]
    assert {"date", "wins", "losses", "win_rate"} <= set(rows[0])


def test_timeline_2v2_includes_pairing_key():
    rows = queries.timeline(_db("team_size_2.json", "loss_2v2.json"), "2v2")
    assert rows
    assert "pairing" in rows[0]
