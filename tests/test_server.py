import sqlite3

import pytest

from fastapi.testclient import TestClient

from server import (
    STAT_ROUTES,
    create_app,
    query_match_players,
    query_matches,
)
from tests.fixtures import cached_db, file_db


def _db_with_replay():
    conn = cached_db("zero_score.json")
    conn.row_factory = sqlite3.Row
    return conn


def _db_with_all_replays():
    conn = cached_db("zero_score.json", "match.json", "forefeit.json")
    conn.row_factory = sqlite3.Row
    return conn


@pytest.fixture
def match_client(tmp_path):
    db_path = file_db(tmp_path)
    source = cached_db("match.json")
    conn = sqlite3.connect(db_path)
    source.backup(conn)
    conn.close()
    return TestClient(create_app(db_path), base_url="https://testserver")


# -- query_matches --


def _matches(conn, **kwargs):
    defaults = dict(page=1, per_page=25, search="", game_mode="", result="", date_from="", date_to="")
    return query_matches(conn, **{**defaults, **kwargs})


def test_query_matches_returns_all():
    conn = _db_with_all_replays()
    data = _matches(conn)

    assert data["total"] == 3
    assert len(data["matches"]) == 3
    assert data["page"] == 1


def test_query_matches_filter_by_result():
    conn = _db_with_all_replays()
    data = _matches(conn, result="win")

    assert data["total"] == 2
    assert all(m["result"] == "win" for m in data["matches"])


def test_query_matches_filter_by_game_mode():
    conn = _db_with_all_replays()
    data = _matches(conn, game_mode="3v3")

    assert data["total"] == 3
    assert all(m["game_mode"] == "3v3" for m in data["matches"])


def test_query_matches_pagination():
    conn = _db_with_all_replays()
    data = _matches(conn, per_page=2, page=1)

    assert data["total"] == 3
    assert len(data["matches"]) == 2
    assert data["per_page"] == 2

    page2 = _matches(conn, per_page=2, page=2)
    assert len(page2["matches"]) == 1


def test_query_matches_per_page_capped(tmp_path):
    client = TestClient(create_app(file_db(tmp_path)), base_url="https://testserver")
    response = client.get("/api/matches?per_page=999")

    assert response.status_code == 422


def test_query_matches_search_by_mvp_name():
    conn = _db_with_all_replays()
    data = _matches(conn, search="Drew")

    assert data["total"] == 1
    assert data["matches"][0]["mvp"] == "Drew"


# -- query_match_players --


def test_query_match_players_returns_tracked_players():
    conn = _db_with_replay()
    match_id = conn.execute("SELECT id FROM matches").fetchone()[0]
    data = query_match_players(conn, match_id)

    assert len(data) == 6
    assert {"Drew", "Jeff", "Steve"}.issubset(set(d["name"] for d in data))
    scores = [d["score"] for d in data]
    assert scores == sorted(scores, reverse=True)


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


# -- HTTP routing smoke tests --


@pytest.mark.parametrize("path", [*STAT_ROUTES.keys(), "/api/stats/streaks"])
def test_stat_route_returns_200(match_client, path):
    response = match_client.get(path)

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, (list, dict))


# -- match detail endpoint --


def test_match_detail_returns_team_split(match_client):
    response = match_client.get("/api/matches/1")

    assert response.status_code == 200
    data = response.json()
    assert "match" in data
    assert "team_players" in data
    assert "opponent_players" in data
    assert "events" in data

    assert data["match"]["result"] == "win"
    assert data["match"]["team_score"] == 5
    assert data["match"]["opponent_score"] == 4

    team_names = {p["name"] for p in data["team_players"]}
    assert {"Drew", "Jeff", "Steve"} == team_names

    opponent_names = {p["name"] for p in data["opponent_players"]}
    assert len(opponent_names) == 3
    assert "Drew" not in opponent_names


def test_match_detail_404_nonexistent(match_client):
    response = match_client.get("/api/matches/9999")

    assert response.status_code == 404


def test_match_detail_events(match_client):
    response = match_client.get("/api/matches/1")
    data = response.json()

    events = data["events"]
    event_types = {e["event_type"] for e in events}
    assert "goal" in event_types
    assert "shot" in event_types
    assert "save" in event_types

    goals = [e for e in events if e["event_type"] == "goal"]
    assert len(goals) == 9  # 5 team + 4 opponent
