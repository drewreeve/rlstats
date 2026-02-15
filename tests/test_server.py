from ingest import ingest_match
from server import (
    query_avg_score,
    query_match_players,
    query_matches,
    query_mvp_losses,
    query_mvp_wins,
    query_player_stats,
    query_score_differential,
    query_shooting_pct,
    query_streaks,
    query_weekday,
    query_win_loss_daily,
)
from tests.fixtures import in_memory_db, load_replay


def _db_with_replay():
    conn = in_memory_db()
    replay = load_replay("zero_score.json")
    ingest_match(conn, replay)
    return conn


def _db_with_all_replays():
    conn = in_memory_db()
    for name in ["zero_score.json", "match.json", "forefeit.json"]:
        ingest_match(conn, load_replay(name))
    return conn


# -- query_matches --


def test_query_matches_returns_all():
    conn = _db_with_all_replays()
    data = query_matches(conn, {})

    assert data["total"] == 3
    assert len(data["matches"]) == 3
    assert data["page"] == 1


def test_query_matches_filter_by_result():
    conn = _db_with_all_replays()
    data = query_matches(conn, {"result": ["win"]})

    assert data["total"] == 2
    assert all(m["result"] == "win" for m in data["matches"])


def test_query_matches_filter_by_game_mode():
    conn = _db_with_all_replays()
    data = query_matches(conn, {"game_mode": ["3v3"]})

    assert data["total"] == 3
    assert all(m["game_mode"] == "3v3" for m in data["matches"])


def test_query_matches_pagination():
    conn = _db_with_all_replays()
    data = query_matches(conn, {"per_page": ["2"], "page": ["1"]})

    assert data["total"] == 3
    assert len(data["matches"]) == 2
    assert data["per_page"] == 2

    page2 = query_matches(conn, {"per_page": ["2"], "page": ["2"]})
    assert len(page2["matches"]) == 1


def test_query_matches_search_by_mvp_name():
    conn = _db_with_all_replays()
    data = query_matches(conn, {"search": ["Drew"]})

    assert data["total"] == 1
    assert data["matches"][0]["mvp"] == "Drew"


def test_query_matches_shape():
    conn = _db_with_replay()
    data = query_matches(conn, {})
    m = data["matches"][0]

    assert "id" in m
    assert "game_mode" in m
    assert "result" in m
    assert "forfeit" in m
    assert "score" in m
    assert "played_at" in m
    assert "mvp" in m


# -- query_match_players --


def test_query_match_players_returns_tracked_players():
    conn = _db_with_replay()
    match_id = conn.execute("SELECT id FROM matches").fetchone()[0]
    data = query_match_players(conn, match_id)

    assert len(data) == 3
    assert set(d["name"] for d in data) == {"Drew", "Jeff", "Steve"}
    scores = [d["score"] for d in data]
    assert scores == sorted(scores, reverse=True)


def test_query_match_players_shape():
    conn = _db_with_replay()
    match_id = conn.execute("SELECT id FROM matches").fetchone()[0]
    data = query_match_players(conn, match_id)

    for player in data:
        assert "name" in player
        assert "score" in player
        assert "goals" in player
        assert "assists" in player
        assert "saves" in player
        assert "shots" in player
        assert "shooting_pct" in player


def test_query_match_players_stats():
    conn = _db_with_replay()
    match_id = conn.execute("SELECT id FROM matches").fetchone()[0]
    data = query_match_players(conn, match_id)

    drew = next(d for d in data if d["name"] == "Drew")
    assert drew["goals"] == 0
    assert drew["saves"] == 0
    assert drew["shots"] == 2
    assert drew["shooting_pct"] == 0.0


def test_query_match_players_nonexistent_match():
    conn = _db_with_replay()
    data = query_match_players(conn, 9999)

    assert data == []


# -- existing tests --


def test_shooting_pct_handler():
    conn = _db_with_replay()
    data = query_shooting_pct(conn, "3v3")

    assert len(data) == 3
    names = [d["player"] for d in data]
    assert names == ["Drew", "Jeff", "Steve"]
    assert all("shooting_pct" in d for d in data)


def test_player_stats_handler():
    conn = _db_with_replay()
    data = query_player_stats(conn, "3v3")

    assert len(data) == 3
    for d in data:
        assert "player" in d
        assert "goals" in d
        assert "assists" in d
        assert "saves" in d


def test_mvp_wins_handler():
    conn = _db_with_replay()
    data = query_mvp_wins(conn, "3v3")

    assert len(data) >= 1
    assert all("player" in d and "win_rate" in d for d in data)


def test_mvp_losses_handler():
    conn = _db_with_replay()
    data = query_mvp_losses(conn, "3v3")

    assert len(data) == 1
    assert data[0]["player"] == "Jeff"
    assert data[0]["loss_mvps"] == 1


def test_weekday_handler():
    conn = _db_with_replay()
    data = query_weekday(conn, "3v3")

    assert len(data) == 1
    assert data[0]["weekday"] == "Thursday"
    assert data[0]["wins"] == 0
    assert data[0]["losses"] == 1


def test_win_loss_daily_handler():
    conn = _db_with_replay()
    data = query_win_loss_daily(conn, "3v3")

    assert len(data) == 1
    assert data[0]["date"] == "2026-02-05"
    assert data[0]["wins"] == 0
    assert data[0]["losses"] == 1
    assert data[0]["win_rate"] == 0.0


def test_avg_score_handler():
    conn = _db_with_replay()
    data = query_avg_score(conn, "3v3")

    assert len(data) == 3
    names = [d["player"] for d in data]
    assert names == ["Drew", "Jeff", "Steve"]
    assert all("avg_score" in d and "total_score" in d for d in data)


def test_score_differential_handler():
    conn = _db_with_all_replays()
    data = query_score_differential(conn, "3v3")

    diffs = {d["differential"]: d["match_count"] for d in data}
    assert diffs[-2] == 1  # 0-2 loss
    assert diffs[1] == 1   # 5-4 win
    assert diffs[4] == 1   # 4-0 win
    assert all("differential" in d and "match_count" in d for d in data)


def test_score_differential_sorted_by_differential():
    conn = _db_with_all_replays()
    data = query_score_differential(conn, "3v3")

    differentials = [d["differential"] for d in data]
    assert differentials == sorted(differentials)


# -- query_streaks --


def test_streaks_3v3():
    conn = _db_with_all_replays()
    data = query_streaks(conn, "3v3")

    # 3 matches ordered by played_at: win (2026-01-27), loss (2026-02-05), win (2026-02-08)
    # longest win streak = 1, longest loss streak = 1
    assert data["longest_win_streak"] == 1
    assert data["longest_loss_streak"] == 1


def test_streaks_no_matches():
    conn = _db_with_all_replays()
    data = query_streaks(conn, "2v2")

    # no 2v2 matches in the 3-replay fixture set
    assert data["longest_win_streak"] == 0
    assert data["longest_loss_streak"] == 0
