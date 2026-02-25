import pytest
from ingest import ingest_match, get_or_create_player
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


# -- Per-fixture: player stats (all players, not just tracked) --


@pytest.mark.parametrize(
    "fixture,expected_stats",
    [
        (
            "zero_score.json",
            [
                ("AxDiabetic", 1, 0, 1, 2, 340),
                ("Drew", 0, 0, 0, 2, 182),
                ("Jeff", 0, 0, 2, 2, 340),
                ("Steve", 0, 0, 0, 0, 104),
                ("WizardsNeverrDie", 0, 0, 1, 0, 152),
                ("think_charlie", 1, 1, 1, 3, 383),
            ],
        ),
        (
            "match.json",
            [
                ("BLM_SCAM", 2, 1, 1, 3, 466),
                ("Drew", 2, 0, 0, 3, 420),
                ("Jeff", 2, 2, 0, 2, 448),
                ("Softycooks", 1, 2, 0, 1, 346),
                ("Steve", 1, 1, 0, 2, 208),
                ("stm4000", 1, 0, 2, 3, 362),
            ],
        ),
        (
            "forefeit.json",
            [
                ("Drew", 2, 1, 0, 4, 388),
                ("Jeff", 2, 2, 0, 1, 350),
                ("Steve", 0, 0, 0, 2, 64),
                ("ThatOneFatPanda1", 0, 0, 0, 0, 4),
                ("Turtle.Space", 0, 0, 0, 0, 28),
                ("Yorozuya Kiyo", 0, 0, 2, 0, 168),
            ],
        ),
        (
            "team_size_2.json",
            [
                ("BlurredVision33", 0, 0, 1, 2, 178),
                ("DQRW", 0, 0, 1, 1, 220),
                ("Drew", 0, 1, 2, 4, 425),
                ("Steve", 1, 0, 1, 2, 327),
            ],
        ),
        (
            "hoops.json",
            [
                ("Drew", 2, 1, 0, 5, 511),
                ("Jeff", 3, 1, 1, 5, 696),
                ("REpurk", 1, 1, 0, 4, 326),
                ("temp-**********", 1, 0, 1, 3, 300),
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


def test_epic_player_stored_with_correct_platform():
    conn = ingest_fixture("match.json")
    row = conn.execute(
        "SELECT platform, platform_id FROM players WHERE name = 'stm4000'"
    ).fetchone()
    assert row == ("epic", "23ce79e90944478599a96ed5402a99e6")


def test_ps4_player_stored_with_correct_platform():
    conn = ingest_fixture("zero_score.json")
    row = conn.execute(
        "SELECT platform, platform_id FROM players WHERE name = 'think_charlie'"
    ).fetchone()
    assert row == ("ps4", "8532790116262235057")


def test_steam_player_stored_with_correct_platform():
    conn = ingest_fixture("match.json")
    row = conn.execute(
        "SELECT platform, platform_id FROM players WHERE name = 'Drew'"
    ).fetchone()
    assert row == ("steam", "76561197969365901")


def test_player_name_updates_on_change():
    conn = in_memory_db()
    get_or_create_player(conn, "steam", "123", "OldName")
    row = conn.execute("SELECT name FROM players WHERE platform_id = '123'").fetchone()
    assert row[0] == "OldName"

    get_or_create_player(conn, "steam", "123", "NewName")
    row = conn.execute("SELECT name FROM players WHERE platform_id = '123'").fetchone()
    assert row[0] == "NewName"
