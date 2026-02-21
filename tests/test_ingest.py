import pytest
from ingest import ingest_match
from tests.fixtures import in_memory_db, load_replay

ALL_FIXTURES = ["zero_score.json", "match.json", "forefeit.json", "team_size_2.json", "hoops.json"]


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
        ("team_size_2.json", "win", 1, 0),
        ("hoops.json", "win", 5, 2),
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
        ("team_size_2.json", "Drew"),
        ("hoops.json", "Jeff"),
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
                ("Drew", 0, 1, 2, 4, 425),
                ("Steve", 1, 0, 1, 2, 327),
            ],
        ),
        (
            "hoops.json",
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


def test_camelcase_match_guid():
    conn = ingest_fixture("camelcase_match_guid.json")
    replay_hash = conn.execute("SELECT replay_hash FROM matches").fetchone()[0]
    assert replay_hash is not None


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
