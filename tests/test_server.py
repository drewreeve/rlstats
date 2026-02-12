from ingest import ingest_match
from server import (
    query_mvp_losses,
    query_mvp_wins,
    query_player_stats,
    query_shooting_pct,
    query_weekday,
    query_win_loss_daily,
)
from tests.fixtures import in_memory_db, load_replay


def _db_with_replay():
    conn = in_memory_db()
    replay = load_replay("zero_score.json")
    ingest_match(conn, replay)
    return conn


def test_shooting_pct_handler():
    conn = _db_with_replay()
    data = query_shooting_pct(conn)

    assert len(data) == 3
    names = [d["player"] for d in data]
    assert names == ["Drew", "Jeff", "Steve"]
    assert all("shooting_pct" in d for d in data)


def test_player_stats_handler():
    conn = _db_with_replay()
    data = query_player_stats(conn)

    assert len(data) == 3
    for d in data:
        assert "player" in d
        assert "goals" in d
        assert "assists" in d
        assert "saves" in d


def test_mvp_wins_handler():
    conn = _db_with_replay()
    data = query_mvp_wins(conn)

    assert len(data) >= 1
    assert all("player" in d and "win_rate" in d for d in data)


def test_mvp_losses_handler():
    conn = _db_with_replay()
    data = query_mvp_losses(conn)

    assert len(data) == 1
    assert data[0]["player"] == "Jeff"
    assert data[0]["loss_mvps"] == 1


def test_weekday_handler():
    conn = _db_with_replay()
    data = query_weekday(conn)

    assert len(data) == 1
    assert data[0]["weekday"] == "Thursday"
    assert data[0]["wins"] == 0
    assert data[0]["losses"] == 1


def test_win_loss_daily_handler():
    conn = _db_with_replay()
    data = query_win_loss_daily(conn)

    assert len(data) == 1
    assert data[0]["date"] == "2026-02-05"
    assert data[0]["wins"] == 0
    assert data[0]["losses"] == 1
    assert data[0]["win_rate"] == 0.0
