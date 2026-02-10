import pytest
from ingest import ingest_match
from tests.fixtures import in_memory_db, load_replay


@pytest.fixture
def real_replay():
    return load_replay("17B69BC840267CE2A9A051BDE88D830A.replay.json")


def test_match_row_is_created(real_replay):
    conn = in_memory_db()
    ingest_match(conn, real_replay)

    count = conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0]

    assert count == 1


def test_zero_score_is_not_null(real_replay):
    conn = in_memory_db()
    ingest_match(conn, real_replay)

    row = conn.execute("SELECT team_score, opponent_score FROM matches").fetchone()

    assert row == (0, 2)


def test_match_result_win(real_replay):
    conn = in_memory_db()
    ingest_match(conn, real_replay)

    result = conn.execute("SELECT result FROM matches").fetchone()[0]

    assert result == "loss"


def test_team_mvp_is_highest_score(real_replay):
    conn = in_memory_db()
    ingest_match(conn, real_replay)

    mvp_name = conn.execute("""
        SELECT p.name
        FROM matches m
        JOIN players p ON p.id = m.team_mvp_player_id
    """).fetchone()[0]

    assert mvp_name == "Jeff"


def test_mvp_in_loss_view(real_replay):
    replay = real_replay.copy()
    replay["properties"]["Team0Score"] = 1
    replay["properties"]["Team1Score"] = 3

    conn = in_memory_db()
    ingest_match(conn, replay)

    rows = conn.execute("SELECT player_name, loss_mvps FROM v_mvp_in_losses").fetchall()

    assert rows == [("Jeff", 1)]


def test_win_loss_by_weekday(real_replay):
    conn = in_memory_db()
    ingest_match(conn, real_replay)

    row = conn.execute("""
        SELECT weekday, wins, losses
        FROM v_win_loss_by_weekday
    """).fetchone()

    assert row == ("Thursday", 0, 1)
