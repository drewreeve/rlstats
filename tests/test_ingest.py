import pytest
from ingest import ingest_match
from tests.fixtures import in_memory_db, load_replay

ALL_FIXTURES = ["zero_score.json", "match.json", "forefeit.json", "team_size_2.json"]


def ingest_fixture(fixture):
    conn = in_memory_db()
    replay = load_replay(fixture)
    ingest_match(conn, replay)
    return conn


def ingest_all_fixtures():
    conn = in_memory_db()
    for name in ALL_FIXTURES:
        ingest_match(conn, load_replay(name))
    return conn


# -- Per-fixture: result and scores --


@pytest.mark.parametrize(
    "fixture,expected_result,expected_team,expected_opp",
    [
        ("zero_score.json", "loss", 0, 2),
        ("match.json", "win", 5, 4),
        ("forefeit.json", "win", 4, 0),
        ("team_size_2.json", "win", 5, 2),
    ],
)
def test_match_result_and_scores(fixture, expected_result, expected_team, expected_opp):
    conn = ingest_fixture(fixture)
    row = conn.execute(
        "SELECT result, team_score, opponent_score FROM matches"
    ).fetchone()

    assert row == (expected_result, expected_team, expected_opp)


# -- Per-fixture: team MVP --


@pytest.mark.parametrize(
    "fixture,expected_mvp",
    [
        ("zero_score.json", "Jeff"),
        ("match.json", "Jeff"),
        ("forefeit.json", "Drew"),
        ("team_size_2.json", "Jeff"),
    ],
)
def test_team_mvp(fixture, expected_mvp):
    conn = ingest_fixture(fixture)
    mvp_name = conn.execute("""
        SELECT p.name
        FROM matches m
        JOIN players p ON p.id = m.team_mvp_player_id
    """).fetchone()[0]

    assert mvp_name == expected_mvp


# -- Per-fixture: player stats --


@pytest.mark.parametrize(
    "fixture,expected_stats",
    [
        (
            "zero_score.json",
            [
                ("Drew", 0, 0, 0, 2, 182),
                ("Jeff", 0, 0, 2, 2, 340),
                ("Steve", 0, 0, 0, 0, 104),
            ],
        ),
        (
            "match.json",
            [
                ("Drew", 2, 0, 0, 3, 420),
                ("Jeff", 2, 2, 0, 2, 448),
                ("Steve", 1, 1, 0, 2, 208),
            ],
        ),
        (
            "forefeit.json",
            [
                ("Drew", 2, 1, 0, 4, 388),
                ("Jeff", 2, 2, 0, 1, 350),
                ("Steve", 0, 0, 0, 2, 64),
            ],
        ),
        (
            "team_size_2.json",
            [
                ("Drew", 2, 1, 0, 5, 511),
                ("Jeff", 3, 1, 1, 5, 696),
            ],
        ),
    ],
)
def test_player_stats_per_match(fixture, expected_stats):
    conn = ingest_fixture(fixture)
    rows = conn.execute("""
        SELECT p.name, mp.goals, mp.assists, mp.saves, mp.shots, mp.score
        FROM match_players mp
        JOIN players p ON p.id = mp.player_id
        ORDER BY p.name
    """).fetchall()

    assert rows == expected_stats


# -- Multi-match: aggregate views --


def test_v_player_stats_3v3():
    conn = ingest_all_fixtures()
    rows = conn.execute("""
        SELECT player_name, matches_played, total_goals, total_assists, total_saves, total_shots
        FROM v_player_stats_3v3
        ORDER BY player_name
    """).fetchall()

    assert rows == [
        ("Drew", 3, 4, 1, 0, 9),
        ("Jeff", 3, 4, 4, 2, 5),
        ("Steve", 3, 1, 1, 0, 4),
    ]


def test_v_shooting_pct_3v3():
    conn = ingest_all_fixtures()
    rows = conn.execute("""
        SELECT player_name, total_goals, total_shots, shooting_pct
        FROM v_shooting_pct_3v3
        ORDER BY player_name
    """).fetchall()

    assert rows == [
        ("Drew", 4, 9, 0.444),
        ("Jeff", 4, 5, 0.8),
        ("Steve", 1, 4, 0.25),
    ]


def test_v_win_loss_by_weekday_3v3():
    conn = ingest_all_fixtures()
    rows = conn.execute("""
        SELECT weekday, matches_played, wins, losses, win_rate
        FROM v_win_loss_by_weekday_3v3
    """).fetchall()

    assert rows == [
        ("Sunday", 1, 1, 0, 1.0),
        ("Tuesday", 1, 1, 0, 1.0),
        ("Thursday", 1, 0, 1, 0.0),
    ]


def test_v_win_loss_daily_3v3():
    conn = ingest_all_fixtures()
    rows = conn.execute("""
        SELECT play_date, wins, losses, win_rate
        FROM v_win_loss_daily_3v3
    """).fetchall()

    assert rows == [
        ("2026-01-27", 1, 0, 1.0),
        ("2026-02-05", 0, 1, 0.0),
        ("2026-02-08", 1, 0, 1.0),
    ]


def test_v_mvp_in_losses_3v3():
    conn = ingest_all_fixtures()
    rows = conn.execute("""
        SELECT player_name, loss_mvps FROM v_mvp_in_losses_3v3
    """).fetchall()

    assert rows == [("Jeff", 1)]


def test_v_mvp_win_rate_3v3():
    conn = ingest_all_fixtures()
    rows = conn.execute("""
        SELECT player_name, mvp_matches, mvp_wins, mvp_win_rate
        FROM v_mvp_win_rate_3v3
        ORDER BY player_name
    """).fetchall()

    assert rows == [
        ("Drew", 1, 1, 1.0),
        ("Jeff", 2, 1, 0.5),
    ]


def test_camelcase_match_guid():
    conn = ingest_fixture("camelcase_match_guid.json")
    replay_hash = conn.execute("SELECT replay_hash FROM matches").fetchone()[0]
    assert replay_hash is not None
