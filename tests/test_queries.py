import sqlite3

import pytest

from db import queries
from ingest import ingest_match
from tests.fixtures import in_memory_db, load_replay


def _db_with_replays(replay_names):
    conn = in_memory_db()
    for name in replay_names:
        ingest_match(conn, load_replay(name))
    conn.row_factory = sqlite3.Row
    return conn


def _all_modes_db():
    return _db_with_replays(
        ["zero_score.json", "match.json", "forefeit.json", "team_size_2.json", "hoops.json"]
    )


def _mvp_losses_modes_db():
    return _db_with_replays(
        [
            "zero_score.json",
            "match.json",
            "forefeit.json",
            "team_size_2.json",
            "hoops.json",
            "loss_2v2.json",
            "loss_hoops.json",
        ]
    )


def _as_tuples(rows, columns):
    return [tuple(row[column] for column in columns) for row in rows]


@pytest.mark.parametrize(
    "mode,expected",
    [
        ("3v3", [("Drew", 4, 9, 0.444), ("Jeff", 4, 5, 0.8), ("Steve", 1, 4, 0.25)]),
        ("2v2", [("Drew", 0, 4, 0.0), ("Steve", 1, 2, 0.5)]),
        ("hoops", [("Drew", 2, 5, 0.4), ("Jeff", 3, 5, 0.6)]),
    ],
)
def test_shooting_pct_values_by_mode(mode, expected):
    conn = _all_modes_db()
    rows = queries.shooting_pct(conn, game_mode=mode)
    actual = _as_tuples(rows, ("player_name", "total_goals", "total_shots", "shooting_pct"))
    assert actual == expected


@pytest.mark.parametrize(
    "mode,expected",
    [
        ("3v3", [("Drew", 3, 4, 1, 0, 9), ("Jeff", 3, 4, 4, 2, 5), ("Steve", 3, 1, 1, 0, 4)]),
        ("2v2", [("Drew", 1, 0, 1, 2, 4), ("Steve", 1, 1, 0, 1, 2)]),
        ("hoops", [("Drew", 1, 2, 1, 0, 5), ("Jeff", 1, 3, 1, 1, 5)]),
    ],
)
def test_player_stats_values_by_mode(mode, expected):
    conn = _all_modes_db()
    rows = queries.player_stats(conn, game_mode=mode)
    actual = _as_tuples(
        rows,
        ("player_name", "matches_played", "total_goals", "total_assists", "total_saves", "total_shots"),
    )
    assert actual == expected


@pytest.mark.parametrize(
    "mode,expected",
    [
        ("3v3", [("Drew", 1, 1, 1.0), ("Jeff", 2, 1, 0.5)]),
        ("2v2", [("Drew", 1, 1, 1.0)]),
        ("hoops", [("Jeff", 1, 1, 1.0)]),
    ],
)
def test_mvp_wins_values_by_mode(mode, expected):
    conn = _all_modes_db()
    rows = queries.mvp_wins(conn, game_mode=mode)
    actual = _as_tuples(rows, ("player_name", "mvp_matches", "mvp_wins", "mvp_win_rate"))
    assert actual == expected


@pytest.mark.parametrize(
    "mode,expected",
    [
        ("3v3", [("Jeff", 1)]),
        ("2v2", [("Drew", 1)]),
        ("hoops", [("Jeff", 1)]),
    ],
)
def test_mvp_losses_values_by_mode(mode, expected):
    conn = _mvp_losses_modes_db()
    rows = queries.mvp_losses(conn, game_mode=mode)
    actual = _as_tuples(rows, ("player_name", "loss_mvps"))
    assert actual == expected


@pytest.mark.parametrize(
    "mode,expected",
    [
        ("3v3", [("Drew", 3, 990, 330.0), ("Jeff", 3, 1138, 379.3), ("Steve", 3, 376, 125.3)]),
        ("2v2", [("Drew", 1, 425, 425.0), ("Steve", 1, 327, 327.0)]),
        ("hoops", [("Drew", 1, 511, 511.0), ("Jeff", 1, 696, 696.0)]),
    ],
)
def test_avg_score_values_by_mode(mode, expected):
    conn = _all_modes_db()
    rows = queries.avg_score(conn, game_mode=mode)
    actual = _as_tuples(rows, ("player_name", "matches_played", "total_score", "avg_score"))
    assert actual == expected


def test_weekday_values_3v3():
    conn = _all_modes_db()
    rows = queries.weekday(conn, game_mode="3v3")
    actual = _as_tuples(rows, ("weekday", "matches_played", "wins", "losses", "win_rate"))
    assert actual == [
        ("Sunday", 1, 1, 0, 1.0),
        ("Tuesday", 1, 1, 0, 1.0),
        ("Thursday", 1, 0, 1, 0.0),
    ]


def test_win_loss_daily_values_3v3():
    conn = _all_modes_db()
    rows = queries.win_loss_daily(conn, game_mode="3v3")
    actual = _as_tuples(rows, ("play_date", "wins", "losses", "win_rate"))
    assert actual == [
        ("2026-01-27", 1, 0, 1.0),
        ("2026-02-05", 0, 1, 0.0),
        ("2026-02-08", 1, 0, 1.0),
    ]


# -- Sessions --


def test_v_sessions():
    """Matches within 60 minutes share a session; matches >60 min apart start a new one."""
    conn = in_memory_db()

    # Insert 4 matches: first 3 within 30 min of each other (one session),
    # 4th match 2 hours later (new session)
    matches = [
        ("hash1", "2026-02-10 20:00:00", "3v3", "win", 3, 2, 3),
        ("hash2", "2026-02-10 20:15:00", "3v3", "loss", 1, 2, 3),
        ("hash3", "2026-02-10 20:30:00", "3v3", "win", 4, 1, 3),
        ("hash4", "2026-02-10 22:30:00", "3v3", "win", 3, 0, 3),
    ]
    for replay_hash, played_at, game_mode, result, team_score, opp_score, team_size in matches:
        conn.execute(
            """INSERT INTO matches (replay_hash, played_at, game_mode, result,
               team_score, opponent_score, team_size)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (replay_hash, played_at, game_mode, result, team_score, opp_score, team_size),
        )

    rows = conn.execute("""
        SELECT match_id, session_id FROM v_sessions ORDER BY played_at
    """).fetchall()

    match_ids = [r[0] for r in rows]
    session_ids = [r[1] for r in rows]

    # First 3 matches in session 1, 4th in session 2
    assert session_ids == [1, 1, 1, 2]
    assert len(match_ids) == 4


def test_v_session_summary():
    """Session summary aggregates wins/losses per session."""
    conn = in_memory_db()

    matches = [
        ("hash1", "2026-02-10 20:00:00", "3v3", "win", 3, 2, 3),
        ("hash2", "2026-02-10 20:15:00", "3v3", "loss", 1, 2, 3),
        ("hash3", "2026-02-10 20:30:00", "3v3", "win", 4, 1, 3),
        ("hash4", "2026-02-10 22:30:00", "3v3", "win", 3, 0, 3),
    ]
    for replay_hash, played_at, game_mode, result, team_score, opp_score, team_size in matches:
        conn.execute(
            """INSERT INTO matches (replay_hash, played_at, game_mode, result,
               team_score, opponent_score, team_size)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (replay_hash, played_at, game_mode, result, team_score, opp_score, team_size),
        )

    rows = conn.execute("""
        SELECT session_id, game_mode, session_date, match_count, wins, losses, win_rate
        FROM v_session_summary
        ORDER BY session_id
    """).fetchall()

    assert rows == [
        (1, "3v3", "2026-02-10", 3, 2, 1, 0.667),
        (2, "3v3", "2026-02-10", 1, 1, 0, 1.0),
    ]


def test_v_sessions_cross_midnight():
    """A session starting before midnight groups under the start date."""
    conn = in_memory_db()

    matches = [
        ("hash1", "2026-02-10 23:30:00", "3v3", "win", 3, 2, 3),
        ("hash2", "2026-02-11 00:10:00", "3v3", "loss", 1, 2, 3),
    ]
    for replay_hash, played_at, game_mode, result, team_score, opp_score, team_size in matches:
        conn.execute(
            """INSERT INTO matches (replay_hash, played_at, game_mode, result,
               team_score, opponent_score, team_size)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (replay_hash, played_at, game_mode, result, team_score, opp_score, team_size),
        )

    # Both matches within 40 min => same session
    rows = conn.execute("""
        SELECT session_id FROM v_sessions ORDER BY played_at
    """).fetchall()
    assert [r[0] for r in rows] == [1, 1]

    # Session date is the date of the first match (Feb 10)
    summary = conn.execute("""
        SELECT session_date, wins, losses FROM v_session_summary
    """).fetchone()
    assert summary == ("2026-02-10", 1, 1)
