import re
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

import queries
import replay_frames
from process import process_unprocessed
from server import create_app
from tests.fixtures import TEST_DATA_DIR, TRACKED_PLAYERS, cached_db, file_db


@pytest.fixture
def match_client(tmp_path: Path) -> TestClient:
    db_path = file_db(tmp_path)
    source = cached_db("match.json")
    conn = sqlite3.connect(db_path)
    source.backup(conn)
    conn.close()
    return TestClient(create_app(db_path), base_url="https://testserver")


# -- match list route --


def test_query_matches_per_page_capped(tmp_path: Path):
    client = TestClient(create_app(file_db(tmp_path)), base_url="https://testserver")
    response = client.get("/api/matches?per_page=999")

    assert response.status_code == 422


# -- HTTP routing smoke tests --


@pytest.mark.parametrize(
    "path",
    [
        *(f"/api/stats/{slug}" for slug in queries.STAT_READS),
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


# -- replay viewer routes --

_REAL_REPLAY = "BEC7EF8411F170E7DBCA41B0676B6A04.replay"


@pytest.fixture
def replay_client(tmp_path: Path) -> TestClient:
    """A client whose match 1 was ingested from a real .replay still on disk."""
    db_path = file_db(tmp_path)
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    (replay_dir / _REAL_REPLAY).write_bytes((TEST_DATA_DIR / _REAL_REPLAY).read_bytes())
    process_unprocessed(db_path, replay_dir, TRACKED_PLAYERS)
    app = create_app(db_path, replay_dir=replay_dir)
    return TestClient(app, base_url="https://testserver")


def test_has_replay_true_when_file_present(replay_client: TestClient) -> None:
    r = replay_client.get("/api/matches/1/has-replay")
    assert r.status_code == 200
    assert r.json() == {"has_replay": True}


def test_has_replay_false_for_unknown_match(replay_client: TestClient) -> None:
    assert replay_client.get("/api/matches/9999/has-replay").json() == {
        "has_replay": False
    }


def test_has_replay_false_when_file_missing(replay_client: TestClient, tmp_path: Path):
    conn = sqlite3.connect(tmp_path / "test.sqlite")
    conn.execute(
        "INSERT INTO matches "
        "(replay_hash, replay_filename, team, team_score, opponent_score, result) "
        "VALUES ('GHOST', 'ghost.replay', 0, 1, 0, 'win')"
    )
    conn.commit()
    ghost_id = conn.execute(
        "SELECT id FROM matches WHERE replay_hash='GHOST'"
    ).fetchone()[0]
    conn.close()

    assert replay_client.get(f"/api/matches/{ghost_id}/has-replay").json() == {
        "has_replay": False
    }
    assert replay_client.get(f"/api/matches/{ghost_id}/replay").status_code == 404
    assert replay_client.get(f"/match/{ghost_id}/replay").status_code == 404


def test_replay_meta_route_serializes_the_wire_shape(replay_client: TestClient) -> None:
    # The wire shape itself is owned by tests/test_replay_wire.py (key sets, row
    # widths) and its semantics by tests/test_replay_frames.py. Here we only
    # prove the HTTP route emits it — 200, JSON, the manifest key set — through
    # FastAPI + jsonable_encoder + gzip.
    r = replay_client.get("/api/matches/1/replay")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/json")
    assert set(r.json()) == replay_frames.WIRE_META_KEYS


def test_replay_frames_bin_length_matches_meta(replay_client: TestClient) -> None:
    meta: Any = replay_client.get("/api/matches/1/replay").json()
    r = replay_client.get("/api/matches/1/replay-frames.bin")
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/octet-stream"
    assert len(r.content) == replay_frames.packed_buffer_bytes(
        len(meta["frame_times"]), len(meta["slots"])
    )


def test_replay_routes_404_for_unknown_match(replay_client: TestClient) -> None:
    assert replay_client.get("/api/matches/9999/replay").status_code == 404
    assert replay_client.get("/api/matches/9999/replay-frames.bin").status_code == 404
    assert replay_client.get("/match/9999/replay").status_code == 404


def test_replay_js_stamps_every_sibling_import(match_client: TestClient) -> None:
    """replay.js imports same-dir modules; the served copy must version-stamp
    every one so a sibling change is not masked by a stale browser cache."""
    r = match_client.get("/static/replay.js")
    assert r.status_code == 200
    # no bare `from "./x.js"` survives — all carry ?v=<hash>
    assert not re.search(r'from "\./[\w-]+\.js"', r.text)
    assert re.search(r'from "\./replay-core\.js\?v=[0-9a-f]{12}"', r.text)
    assert re.fullmatch(r'"[0-9a-f]{12}"', r.headers["etag"])


def test_replay_js_answers_conditional_get_with_304(match_client: TestClient) -> None:
    etag = match_client.get("/static/replay.js").headers["etag"]
    r = match_client.get("/static/replay.js", headers={"If-None-Match": etag})
    assert r.status_code == 304
    assert r.headers["etag"] == etag


def test_replay_page_served_when_file_present(replay_client: TestClient) -> None:
    r = replay_client.get("/match/1/replay")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]


def test_replay_page_references_versioned_module_and_css(
    replay_client: TestClient,
) -> None:
    html = replay_client.get("/match/1/replay").text
    assert 'type="module" src="/static/replay.js?v=' in html
    assert "/static/replay.css?v=" in html


@pytest.mark.parametrize("asset", ["replay.js", "replay.css"])
def test_replay_static_assets_served(match_client: TestClient, asset: str) -> None:
    r = match_client.get(f"/static/{asset}")
    assert r.status_code == 200
