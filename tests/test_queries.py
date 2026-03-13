import sqlite3

import pytest

from db import queries
from tests.fixtures import cached_db


def _all_modes_db():
    conn = cached_db("zero_score.json", "match.json", "forefeit.json", "team_size_2.json", "hoops.json")
    conn.row_factory = sqlite3.Row
    return conn


def _mvp_losses_modes_db():
    conn = cached_db(
        "zero_score.json",
        "match.json",
        "forefeit.json",
        "team_size_2.json",
        "hoops.json",
        "loss_2v2.json",
        "loss_hoops.json",
    )
    conn.row_factory = sqlite3.Row
    return conn


def _3v3_db():
    conn = cached_db("zero_score.json", "match.json", "forefeit.json")
    conn.row_factory = sqlite3.Row
    return conn


def _match_db():
    conn = cached_db("match.json")
    conn.row_factory = sqlite3.Row
    return conn


def _zero_score_db():
    conn = cached_db("zero_score.json")
    conn.row_factory = sqlite3.Row
    return conn


def _as_tuples(rows, columns):
    return [tuple(row[column] for column in columns) for row in rows]


@pytest.mark.parametrize(
    "mode,expected",
    [
        ("3v3", [
            ("Drew", 4, 9, 0.444),
            ("Jeff", 4, 5, 0.8),
            ("Steve", 1, 4, 0.25),
        ]),
        ("2v2", [
            ("Drew", 0, 4, 0.0),
            ("Steve", 1, 2, 0.5),
        ]),
        ("hoops", [
            ("Drew", 2, 5, 0.4),
            ("Jeff", 3, 5, 0.6),
        ]),
    ],
)
def test_shooting_pct_values_by_mode(mode, expected):
    conn = _all_modes_db()
    rows = queries.shooting_pct(conn, game_mode=mode)
    actual = _as_tuples(rows, ("player", "goals", "shots", "shooting_pct"))
    assert actual == expected


@pytest.mark.parametrize(
    "mode,expected",
    [
        ("3v3", [
            ("Drew", 3, 4, 1, 0, 9),
            ("Jeff", 3, 4, 4, 2, 5),
            ("Steve", 3, 1, 1, 0, 4),
        ]),
        ("2v2", [
            ("Drew", 1, 0, 1, 2, 4),
            ("Steve", 1, 1, 0, 1, 2),
        ]),
        ("hoops", [
            ("Drew", 1, 2, 1, 0, 5),
            ("Jeff", 1, 3, 1, 1, 5),
        ]),
    ],
)
def test_player_stats_values_by_mode(mode, expected):
    conn = _all_modes_db()
    rows = queries.player_stats(conn, game_mode=mode)
    actual = _as_tuples(
        rows,
        ("player", "matches", "goals", "assists", "saves", "shots"),
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
    actual = _as_tuples(rows, ("player", "mvp_matches", "mvp_wins", "win_rate"))
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
    actual = _as_tuples(rows, ("player", "loss_mvps"))
    assert actual == expected


@pytest.mark.parametrize(
    "mode,expected",
    [
        ("3v3", [
            ("Drew", 3, 990, 330.0),
            ("Jeff", 3, 1138, 379.3),
            ("Steve", 3, 376, 125.3),
        ]),
        ("2v2", [
            ("Drew", 1, 425, 425.0),
            ("Steve", 1, 327, 327.0),
        ]),
        ("hoops", [
            ("Drew", 1, 511, 511.0),
            ("Jeff", 1, 696, 696.0),
        ]),
    ],
)
def test_avg_score_values_by_mode(mode, expected):
    conn = _all_modes_db()
    rows = queries.avg_score(conn, game_mode=mode)
    actual = _as_tuples(rows, ("player", "matches", "total_score", "avg_score"))
    assert actual == expected


def test_weekday_values_3v3():
    conn = _all_modes_db()
    rows = queries.weekday(conn, game_mode="3v3")
    actual = _as_tuples(rows, ("weekday", "matches", "wins", "losses", "win_rate"))
    assert actual == [
        ("Sunday", 1, 1, 0, 1.0),
        ("Tuesday", 1, 1, 0, 1.0),
        ("Thursday", 1, 0, 1, 0.0),
    ]


@pytest.mark.parametrize(
    "mode,expected",
    [
        ("3v3", [
            ("Drew", 182, 420),
            ("Jeff", 340, 448),
            ("Steve", 64, 208),
        ]),
        ("2v2", [
            ("Drew", 425, 425),
            ("Steve", 327, 327),
        ]),
        ("hoops", [
            ("Drew", 511, 511),
            ("Jeff", 696, 696),
        ]),
    ],
)
def test_score_range_values_by_mode(mode, expected):
    conn = _all_modes_db()
    rows = queries.score_range(conn, game_mode=mode)
    actual = _as_tuples(rows, ("player", "min", "max"))
    assert actual == expected


def test_win_loss_daily_values_3v3():
    conn = _all_modes_db()
    rows = queries.win_loss_daily(conn, game_mode="3v3")
    actual = _as_tuples(rows, ("date", "wins", "losses", "win_rate"))
    assert actual == [
        ("2026-01-27", 1, 0, 1.0),
        ("2026-02-05", 0, 1, 0.0),
        ("2026-02-08", 1, 0, 1.0),
    ]


# -- score_differential --


def test_score_differential_values_3v3():
    conn = _3v3_db()
    rows = queries.score_differential(conn, game_mode="3v3")
    diffs = {r["differential"]: r["match_count"] for r in rows}
    assert diffs[-2] == 1  # 0-2 loss
    assert diffs[1] == 1   # 5-4 win
    assert diffs[4] == 1   # 4-0 win


def test_score_differential_sorted_by_differential():
    conn = _3v3_db()
    rows = queries.score_differential(conn, game_mode="3v3")
    differentials = [r["differential"] for r in rows]
    assert differentials == sorted(differentials)


# -- streaks --


@pytest.mark.parametrize(
    "mode,expected_win,expected_loss",
    [
        ("3v3", 1, 1),
        ("2v2", 1, 0),
        ("hoops", 1, 0),
    ],
)
def test_streaks_values_by_mode(mode, expected_win, expected_loss):
    conn = _all_modes_db()
    rows = list(queries.streaks(conn, game_mode=mode))
    if rows:
        assert rows[0]["longest_win_streak"] == expected_win
        assert rows[0]["longest_loss_streak"] == expected_loss
    else:
        assert expected_win == 0
        assert expected_loss == 0


def test_streaks_no_matches():
    conn = _3v3_db()
    rows = list(queries.streaks(conn, game_mode="2v2"))
    if rows:
        assert (rows[0]["longest_win_streak"] or 0) == 0
        assert (rows[0]["longest_loss_streak"] or 0) == 0


# -- avg_goal_contribution --


def test_avg_goal_contribution_shape():
    conn = _match_db()
    rows = queries.avg_goal_contribution(conn, game_mode="3v3")
    data = [dict(r) for r in rows]

    assert len(data) == 3
    assert {d["player"] for d in data} == {"Drew", "Jeff", "Steve"}
    by_player = {d["player"]: d for d in data}
    for player in ("Drew", "Jeff", "Steve"):
        c = by_player[player]["avg_goal_contribution"]
        assert c is not None
        assert 0 < c <= 1
    assert by_player["Jeff"]["avg_goal_contribution"] > by_player["Drew"]["avg_goal_contribution"]


def test_avg_goal_contribution_zero_team_score_excluded():
    conn = _zero_score_db()
    rows = queries.avg_goal_contribution(conn, game_mode="3v3")
    assert all(r["avg_goal_contribution"] is None for r in rows)


def test_avg_goal_contribution_values():
    conn = _3v3_db()
    rows = queries.avg_goal_contribution(conn, game_mode="3v3")
    by_player = {r["player"]: r for r in rows}
    assert by_player["Drew"]["avg_goal_contribution"] == 0.575
    assert by_player["Steve"]["avg_goal_contribution"] == 0.2
    assert by_player["Jeff"]["avg_goal_contribution"] == 0.9


def test_avg_goal_contribution_no_matches_for_mode():
    conn = _3v3_db()
    rows = list(queries.avg_goal_contribution(conn, game_mode="2v2"))
    assert rows == []
