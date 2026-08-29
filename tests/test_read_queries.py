"""Unit tests for the query layer (queries.py) — the read seam's own tests.

Covers the reshaping that previously lived in server.py route closures and was
only reachable through TestClient (streaks' ``or 0``, goal_timing's rounding
and rename, timeline's game-mode branch) plus the match-list / match-detail
composites.
"""

import queries
from tests.fixtures import empty_row_db as _empty_db
from tests.fixtures import row_db as _db

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


# -- matches (list + pagination) --

_ALL = ("zero_score.json", "match.json", "forefeit.json")


def test_matches_returns_all():
    page = queries.matches(_db(*_ALL))
    assert page.total == 3
    assert len(page.matches) == 3
    assert page.page == 1


def test_matches_filter_by_result():
    page = queries.matches(_db(*_ALL), result="win")
    assert page.total == 2
    assert all(m["result"] == "win" for m in page.matches)


def test_matches_filter_by_game_mode():
    page = queries.matches(_db(*_ALL), game_mode="3v3")
    assert page.total == 3
    assert all(m["game_mode"] == "3v3" for m in page.matches)


def test_matches_pagination():
    page = queries.matches(_db(*_ALL), per_page=2, page=1)
    assert page.total == 3
    assert len(page.matches) == 2
    assert page.per_page == 2
    assert len(queries.matches(_db(*_ALL), per_page=2, page=2).matches) == 1


def test_matches_search_by_mvp_name():
    page = queries.matches(_db(*_ALL), search="Drew")
    assert page.total == 1
    assert page.matches[0]["mvp"] == "Drew"


def test_matches_empty_db():
    page = queries.matches(_empty_db())
    assert page.total == 0
    assert page.matches == []


# -- match_players --


def test_match_players_ordered_by_score_desc():
    conn = _db("zero_score.json")
    match_id = conn.execute("SELECT id FROM matches").fetchone()[0]
    rows = queries.match_players(conn, match_id)
    assert len(rows) == 6
    assert {"Drew", "Jeff", "Steve"} <= {r["name"] for r in rows}
    scores = [r["score"] for r in rows]
    assert scores == sorted(scores, reverse=True)


def test_match_players_computed_shooting_pct():
    conn = _db("zero_score.json")
    match_id = conn.execute("SELECT id FROM matches").fetchone()[0]
    drew = next(r for r in queries.match_players(conn, match_id) if r["name"] == "Drew")
    assert drew["shots"] == 2
    assert drew["shooting_pct"] == 0.0


def test_match_players_nonexistent_match_is_empty():
    assert queries.match_players(_db("zero_score.json"), 9999) == []


# -- match_detail (composite) --


def test_match_detail_splits_team_and_opponent():
    conn = _db("match.json")
    match_id = conn.execute("SELECT id FROM matches").fetchone()[0]
    detail = queries.match_detail(conn, match_id)
    assert detail is not None
    assert detail.match["result"] == "win"
    assert detail.match["team_score"] == 5
    assert detail.match["opponent_score"] == 4
    team_names = {p["name"] for p in detail.team_players}
    assert {"Drew", "Jeff", "Steve"} == team_names
    assert all(p["name"] not in team_names for p in detail.opponent_players)


def test_match_detail_events_present():
    conn = _db("zero_score.json")
    match_id = conn.execute("SELECT id FROM matches").fetchone()[0]
    detail = queries.match_detail(conn, match_id)
    assert detail is not None
    assert "goal" in {e["event_type"] for e in detail.events}


def test_match_detail_nonexistent_match_is_none():
    assert queries.match_detail(_db("zero_score.json"), 9999) is None


# -- player_career --


def test_player_career_returns_stats():
    data = queries.player_career(_db("zero_score.json"), "Drew", "3v3")
    assert data["player"] == "Drew"
    assert data["matches"] == 1
    assert data["shots"] == 2


def test_player_career_no_data_zero_fills():
    data = queries.player_career(_db("zero_score.json"), "Drew", "2v2")
    assert data["player"] == "Drew"
    assert data["matches"] == 0
    assert data["avg_score"] is None


# -- player_time_series --


def test_player_time_series_rows_have_expected_keys():
    rows = queries.player_time_series(_db("match.json"), "Drew", "3v3")
    assert rows
    assert set(rows[0]) == {
        "date",
        "goals",
        "assists",
        "saves",
        "shots",
        "avg_score",
        "mvp_count",
        "shooting_pct",
        "avg_speed",
    }


def test_player_time_series_empty_for_unplayed_mode():
    assert queries.player_time_series(_db("match.json"), "Drew", "2v2") == []
