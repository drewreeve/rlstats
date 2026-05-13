import sqlite3
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from server import (
    STAT_ROUTES,
    compute_goal_timing,
    create_app,
    query_match_players,
    query_matches,
)
from tests.fixtures import cached_db, file_db


def _db_with_replay() -> sqlite3.Connection:
    conn = cached_db("zero_score.json")
    conn.row_factory = sqlite3.Row
    return conn


def _db_with_all_replays() -> sqlite3.Connection:
    conn = cached_db("zero_score.json", "match.json", "forefeit.json")
    conn.row_factory = sqlite3.Row
    return conn


@pytest.fixture
def match_client(tmp_path: Path) -> TestClient:
    db_path = file_db(tmp_path)
    source = cached_db("match.json")
    conn = sqlite3.connect(db_path)
    source.backup(conn)
    conn.close()
    return TestClient(create_app(db_path), base_url="https://testserver")


# -- query_matches --


def _matches(conn: sqlite3.Connection, **kwargs: Any) -> dict[str, Any]:
    defaults: dict[str, Any] = dict(
        page=1,
        per_page=25,
        search="",
        game_mode="",
        result="",
        date_from="",
        date_to="",
    )
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


def test_query_matches_per_page_capped(tmp_path: Path):
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


@pytest.mark.parametrize(
    "path",
    [
        *STAT_ROUTES.keys(),
        "/api/stats/streaks",
        "/api/stats/goal-timing",
        "/api/stats/timeline",
    ],
)
def test_stat_route_returns_200(match_client: TestClient, path: str) -> None:
    response = match_client.get(path)

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, (list, dict))


def _pairing_client(tmp_path: Path, *replay_files: str) -> TestClient:
    db_path = file_db(tmp_path)
    source = cached_db(*replay_files)
    conn = sqlite3.connect(db_path)
    source.backup(conn)
    conn.close()
    return TestClient(create_app(db_path), base_url="https://testserver")


@pytest.fixture
def client_2v2(tmp_path: Path) -> TestClient:
    return _pairing_client(tmp_path, "team_size_2.json", "loss_2v2.json")


@pytest.fixture
def client_hoops(tmp_path: Path) -> TestClient:
    return _pairing_client(tmp_path, "hoops.json", "loss_hoops.json")


@pytest.mark.parametrize(
    "mode, client_fixture",
    [("2v2", "client_2v2"), ("hoops", "client_hoops")],
)
def test_timeline_returns_pairing_rows(
    mode: str, client_fixture: str, request: pytest.FixtureRequest
) -> None:
    client: TestClient = request.getfixturevalue(client_fixture)
    response = client.get(f"/api/stats/timeline?mode={mode}")

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert data
    assert "pairing" in data[0]
    assert "win_rate" in data[0]


# -- match detail endpoint --


def test_match_detail_returns_team_split(match_client: TestClient) -> None:
    response = match_client.get("/api/matches/1")

    assert response.status_code == 200
    data: Any = response.json()
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


def test_match_detail_404_nonexistent(match_client: TestClient) -> None:
    response = match_client.get("/api/matches/9999")

    assert response.status_code == 404


def test_match_detail_events(match_client: TestClient) -> None:
    response = match_client.get("/api/matches/1")
    data: Any = response.json()

    events = data["events"]
    event_types = {e["event_type"] for e in events}
    assert "goal" in event_types
    assert "shot" in event_types
    assert "save" in event_types

    goals = [e for e in events if e["event_type"] == "goal"]
    assert len(goals) == 9  # 5 team + 4 opponent


# -- player routes --


def test_player_page_returns_200(match_client: TestClient) -> None:
    response = match_client.get("/player/Drew")
    assert response.status_code == 200


def test_player_page_unknown_returns_404(match_client: TestClient) -> None:
    response = match_client.get("/player/Unknown")
    assert response.status_code == 404


def test_player_career_returns_200(match_client: TestClient) -> None:
    response = match_client.get("/api/players/Drew?mode=3v3")
    assert response.status_code == 200
    data: Any = response.json()
    assert data["player"] == "Drew"
    assert data["matches"] >= 0


def test_player_career_unknown_returns_404(match_client: TestClient) -> None:
    response = match_client.get("/api/players/Unknown")
    assert response.status_code == 404


def test_player_career_no_data_returns_zero_matches(match_client: TestClient) -> None:
    response = match_client.get("/api/players/Drew?mode=2v2")
    assert response.status_code == 200
    data: Any = response.json()
    assert data["matches"] == 0


def test_player_time_series_returns_list(match_client: TestClient) -> None:
    response = match_client.get("/api/players/Drew/time-series?mode=3v3")
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_player_time_series_unknown_returns_404(match_client: TestClient) -> None:
    response = match_client.get("/api/players/Unknown/time-series")
    assert response.status_code == 404


def test_match_players_include_is_tracked(match_client: TestClient) -> None:
    response = match_client.get("/api/matches/1")
    data: Any = response.json()
    for player in data["team_players"] + data["opponent_players"]:
        assert "is_tracked" in player


# -- compute_goal_timing --


def test_compute_goal_timing_basic() -> None:
    # our@60, opp@120, our@180, opp@300
    # concede delays: 60-120=60s, 180-300=120s → avg=90s
    # lead intervals: 60-120=60s, 180-300=120s → avg=90s
    events = [
        {"match_id": 1, "game_seconds": 60, "is_ours": 1, "duration_seconds": 330},
        {"match_id": 1, "game_seconds": 120, "is_ours": 0, "duration_seconds": 330},
        {"match_id": 1, "game_seconds": 180, "is_ours": 1, "duration_seconds": 330},
        {"match_id": 1, "game_seconds": 300, "is_ours": 0, "duration_seconds": 330},
    ]
    avg_concede, avg_lead = compute_goal_timing(events)
    assert avg_concede == 90.0
    assert avg_lead == 90.0


def test_compute_goal_timing_hold_to_end() -> None:
    # we score at 60s, match ends at 180s → lead = 120s, no concede
    events = [
        {"match_id": 1, "game_seconds": 60, "is_ours": 1, "duration_seconds": 180},
    ]
    avg_concede, avg_lead = compute_goal_timing(events)
    assert avg_concede is None
    assert avg_lead == 120.0


def test_compute_goal_timing_no_lead() -> None:
    events = [
        {"match_id": 1, "game_seconds": 60, "is_ours": 0, "duration_seconds": 180},
    ]
    avg_concede, avg_lead = compute_goal_timing(events)
    assert avg_concede is None
    assert avg_lead is None


def test_compute_goal_timing_empty() -> None:
    avg_concede, avg_lead = compute_goal_timing([])
    assert avg_concede is None
    assert avg_lead is None


def test_compute_goal_timing_multiple_matches() -> None:
    # Match 1: our@0, opp@60 → lead 0-60=60s, concede 0→60=60s
    # Match 2: our@30, end=120 → lead 30-120=90s, no concede
    # avg_lead = (60+90)/2 = 75s, avg_concede = 60s
    events = [
        {"match_id": 1, "game_seconds": 0, "is_ours": 1, "duration_seconds": 180},
        {"match_id": 1, "game_seconds": 60, "is_ours": 0, "duration_seconds": 180},
        {"match_id": 2, "game_seconds": 30, "is_ours": 1, "duration_seconds": 120},
    ]
    avg_concede, avg_lead = compute_goal_timing(events)
    assert avg_concede == 60.0
    assert avg_lead == 75.0
