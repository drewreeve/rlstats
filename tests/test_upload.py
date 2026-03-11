import os

from starlette.testclient import TestClient

from server import create_app
from tests.fixtures import file_db


def _replay_content(size=300 * 1024):
    """Generate fake replay content of given size."""
    return b"\x00" * size


def _get_csrf_token(client):
    """Get a CSRF token by hitting /api/auth/status."""
    resp = client.get("/api/auth/status")
    return resp.json()["csrf_token"]


# -- Auth tests --


def test_auth_correct_password(tmp_path):
    os.environ["UPLOAD_PASSWORD"] = "secret123"
    try:
        db_path = file_db(tmp_path)
        app = create_app(db_path, replay_dir=tmp_path)
        client = TestClient(app, base_url="https://testserver")

        token = _get_csrf_token(client)
        resp = client.post(
            "/api/auth",
            json={"password": "secret123"},
            headers={"X-CSRF-Token": token},
        )
        assert resp.status_code == 200
        assert resp.json()["authenticated"] is True
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)


def test_auth_wrong_password(tmp_path):
    os.environ["UPLOAD_PASSWORD"] = "secret123"
    try:
        db_path = file_db(tmp_path)
        app = create_app(db_path, replay_dir=tmp_path)
        client = TestClient(app, base_url="https://testserver")

        token = _get_csrf_token(client)
        resp = client.post(
            "/api/auth",
            json={"password": "wrong"},
            headers={"X-CSRF-Token": token},
        )
        assert resp.status_code == 401
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)


def test_auth_missing_env_var(tmp_path):
    os.environ.pop("UPLOAD_PASSWORD", None)
    db_path = file_db(tmp_path)
    app = create_app(db_path, replay_dir=tmp_path)
    client = TestClient(app, base_url="https://testserver")

    token = _get_csrf_token(client)
    resp = client.post(
        "/api/auth",
        json={"password": "anything"},
        headers={"X-CSRF-Token": token},
    )
    assert resp.status_code == 403


def test_auth_status_unauthenticated(tmp_path):
    db_path = file_db(tmp_path)
    app = create_app(db_path, replay_dir=tmp_path)
    client = TestClient(app, base_url="https://testserver")

    resp = client.get("/api/auth/status")
    data = resp.json()
    assert resp.status_code == 200
    assert data["authenticated"] is False
    assert "csrf_token" in data


def test_auth_status_after_login(tmp_path):
    os.environ["UPLOAD_PASSWORD"] = "secret123"
    try:
        db_path = file_db(tmp_path)
        app = create_app(db_path, replay_dir=tmp_path)
        client = TestClient(app, base_url="https://testserver")

        token = _get_csrf_token(client)
        client.post(
            "/api/auth",
            json={"password": "secret123"},
            headers={"X-CSRF-Token": token},
        )
        resp = client.get("/api/auth/status")
        assert resp.json()["authenticated"] is True
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)


# -- Upload tests --


def _authed_client(tmp_path):
    os.environ["UPLOAD_PASSWORD"] = "test"
    db_path = file_db(tmp_path)
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    app = create_app(db_path, replay_dir=replay_dir)
    client = TestClient(app, base_url="https://testserver")
    token = _get_csrf_token(client)
    client.post(
        "/api/auth",
        json={"password": "test"},
        headers={"X-CSRF-Token": token},
    )
    return client, token, replay_dir


def test_upload_valid_file(tmp_path):
    client, token, replay_dir = _authed_client(tmp_path)
    try:
        from io import BytesIO

        resp = client.post(
            "/api/upload",
            files={"file": ("match.replay", BytesIO(_replay_content()), "application/octet-stream")},
            headers={"X-CSRF-Token": token},
        )
        assert resp.status_code == 201
        assert resp.json()["filename"] == "match.replay"
        assert (replay_dir / "match.replay").exists()
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)


def test_upload_unauthenticated(tmp_path):
    db_path = file_db(tmp_path)
    app = create_app(db_path, replay_dir=tmp_path)
    client = TestClient(app, base_url="https://testserver")

    from io import BytesIO

    token = _get_csrf_token(client)
    resp = client.post(
        "/api/upload",
        files={"file": ("match.replay", BytesIO(_replay_content()), "application/octet-stream")},
        headers={"X-CSRF-Token": token},
    )
    assert resp.status_code == 401


def test_upload_wrong_extension(tmp_path):
    client, token, _ = _authed_client(tmp_path)
    try:
        from io import BytesIO

        resp = client.post(
            "/api/upload",
            files={"file": ("match.txt", BytesIO(_replay_content()), "application/octet-stream")},
            headers={"X-CSRF-Token": token},
        )
        assert resp.status_code == 400
        assert "replay" in resp.json()["error"].lower()
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)


def test_upload_too_small(tmp_path):
    client, token, _ = _authed_client(tmp_path)
    try:
        from io import BytesIO

        resp = client.post(
            "/api/upload",
            files={"file": ("small.replay", BytesIO(b"\x00" * 100), "application/octet-stream")},
            headers={"X-CSRF-Token": token},
        )
        assert resp.status_code == 400
        assert "small" in resp.json()["error"].lower()
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)


def test_upload_duplicate(tmp_path):
    client, token, _ = _authed_client(tmp_path)
    try:
        from io import BytesIO

        # Upload once
        client.post(
            "/api/upload",
            files={"file": ("dup.replay", BytesIO(_replay_content()), "application/octet-stream")},
            headers={"X-CSRF-Token": token},
        )

        # Upload again
        resp = client.post(
            "/api/upload",
            files={"file": ("dup.replay", BytesIO(_replay_content()), "application/octet-stream")},
            headers={"X-CSRF-Token": token},
        )
        assert resp.status_code == 409
        assert resp.json()["duplicate"] is True
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)


def test_upload_path_traversal_sanitized(tmp_path):
    client, token, _ = _authed_client(tmp_path)
    try:
        from io import BytesIO

        resp = client.post(
            "/api/upload",
            files={"file": ("../../../etc/match.replay", BytesIO(_replay_content()), "application/octet-stream")},
            headers={"X-CSRF-Token": token},
        )
        # _secure_filename strips path traversal, file should be saved safely
        assert resp.status_code == 201
        assert ".." not in resp.json()["filename"]
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)


# -- Page serving --


def test_upload_page_serves(tmp_path):
    db_path = file_db(tmp_path)
    app = create_app(db_path, replay_dir=tmp_path)
    client = TestClient(app, base_url="https://testserver")

    resp = client.get("/upload")
    assert resp.status_code == 200
    assert b"UPLOAD" in resp.content


# -- CSRF tests --


def test_csrf_token_required_on_auth(tmp_path):
    os.environ["UPLOAD_PASSWORD"] = "secret123"
    try:
        db_path = file_db(tmp_path)
        app = create_app(db_path, replay_dir=tmp_path)
        client = TestClient(app, base_url="https://testserver")

        # POST without CSRF token should be rejected
        resp = client.post("/api/auth", json={"password": "secret123"})
        assert resp.status_code == 403
        assert "csrf" in resp.json()["error"].lower()
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)


def test_csrf_token_required_on_upload(tmp_path):
    client, _token, _ = _authed_client(tmp_path)
    try:
        from io import BytesIO

        # POST without CSRF token should be rejected
        resp = client.post(
            "/api/upload",
            files={"file": ("match.replay", BytesIO(_replay_content()), "application/octet-stream")},
        )
        assert resp.status_code == 403
        assert "csrf" in resp.json()["error"].lower()
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)


# -- Upload status endpoint --


def _status_client(tmp_path):
    db_path = file_db(tmp_path)
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    app = create_app(db_path, replay_dir=replay_dir)
    return TestClient(app, base_url="https://testserver"), replay_dir


def test_upload_status_error_when_replay_missing(tmp_path):
    """No .replay file at all means processing failed (file was deleted)."""
    client, replay_dir = _status_client(tmp_path)

    resp = client.get("/api/upload/status?filename=nonexistent.replay")
    assert resp.status_code == 200
    assert resp.json()["status"] == "error"


def test_upload_status_pending_when_replay_exists(tmp_path):
    """.replay exists but no .json yet means still processing."""
    client, replay_dir = _status_client(tmp_path)

    (replay_dir / "test.replay").write_bytes(b"\x00")

    resp = client.get("/api/upload/status?filename=test.replay")
    assert resp.status_code == 200
    assert resp.json()["status"] == "pending"


def test_upload_status_processed_when_ingested_marker_exists(tmp_path):
    """.replay.ingested marker exists means processing succeeded."""
    client, replay_dir = _status_client(tmp_path)

    (replay_dir / "test.replay").write_bytes(b"\x00")
    (replay_dir / "test.replay.ingested").write_bytes(b"")

    resp = client.get("/api/upload/status?filename=test.replay")
    assert resp.status_code == 200
    assert resp.json()["status"] == "processed"


def test_upload_status_missing_filename(tmp_path):
    client, _ = _status_client(tmp_path)

    resp = client.get("/api/upload/status")
    assert resp.status_code == 400


def test_upload_status_sanitizes_filename(tmp_path):
    """Path traversal in filename param is sanitized."""
    client, _ = _status_client(tmp_path)

    resp = client.get("/api/upload/status?filename=../../../etc/passwd")
    assert resp.status_code == 200
    # _secure_filename strips traversal; the sanitized name won't exist
    assert resp.json()["status"] in ("error", "unknown")
