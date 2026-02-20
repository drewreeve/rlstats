import os

from server import create_app
from tests.fixtures import file_db


def _replay_content(size=300 * 1024):
    """Generate fake replay content of given size."""
    return b"\x00" * size


def _get_csrf_token(client):
    """Get a CSRF token by hitting /api/auth/status."""
    resp = client.get("/api/auth/status")
    return resp.get_json()["csrf_token"]


# -- Auth tests --


def test_auth_correct_password(tmp_path):
    os.environ["UPLOAD_PASSWORD"] = "secret123"
    try:
        db_path = file_db(tmp_path)
        app = create_app(db_path, replay_dir=tmp_path)
        app.config["TESTING"] = True
        client = app.test_client()

        token = _get_csrf_token(client)
        resp = client.post(
            "/api/auth",
            json={"password": "secret123"},
            headers={"X-CSRF-Token": token},
        )
        assert resp.status_code == 200
        assert resp.get_json()["authenticated"] is True
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)


def test_auth_wrong_password(tmp_path):
    os.environ["UPLOAD_PASSWORD"] = "secret123"
    try:
        db_path = file_db(tmp_path)
        app = create_app(db_path, replay_dir=tmp_path)
        app.config["TESTING"] = True
        client = app.test_client()

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
    app.config["TESTING"] = True
    client = app.test_client()

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
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.get("/api/auth/status")
    data = resp.get_json()
    assert resp.status_code == 200
    assert data["authenticated"] is False
    assert "csrf_token" in data


def test_auth_status_after_login(tmp_path):
    os.environ["UPLOAD_PASSWORD"] = "secret123"
    try:
        db_path = file_db(tmp_path)
        app = create_app(db_path, replay_dir=tmp_path)
        app.config["TESTING"] = True
        client = app.test_client()

        token = _get_csrf_token(client)
        client.post(
            "/api/auth",
            json={"password": "secret123"},
            headers={"X-CSRF-Token": token},
        )
        resp = client.get("/api/auth/status")
        assert resp.get_json()["authenticated"] is True
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)


# -- Upload tests --


def _authed_client(tmp_path):
    os.environ["UPLOAD_PASSWORD"] = "test"
    db_path = file_db(tmp_path)
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    app = create_app(db_path, replay_dir=replay_dir)
    app.config["TESTING"] = True
    client = app.test_client()
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

        data = {"file": (BytesIO(_replay_content()), "match.replay")}
        resp = client.post(
            "/api/upload",
            data=data,
            content_type="multipart/form-data",
            headers={"X-CSRF-Token": token},
        )
        assert resp.status_code == 201
        assert resp.get_json()["filename"] == "match.replay"
        assert (replay_dir / "match.replay").exists()
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)


def test_upload_unauthenticated(tmp_path):
    db_path = file_db(tmp_path)
    app = create_app(db_path, replay_dir=tmp_path)
    app.config["TESTING"] = True
    client = app.test_client()

    from io import BytesIO

    token = _get_csrf_token(client)
    data = {"file": (BytesIO(_replay_content()), "match.replay")}
    resp = client.post(
        "/api/upload",
        data=data,
        content_type="multipart/form-data",
        headers={"X-CSRF-Token": token},
    )
    assert resp.status_code == 401


def test_upload_wrong_extension(tmp_path):
    client, token, _ = _authed_client(tmp_path)
    try:
        from io import BytesIO

        data = {"file": (BytesIO(_replay_content()), "match.txt")}
        resp = client.post(
            "/api/upload",
            data=data,
            content_type="multipart/form-data",
            headers={"X-CSRF-Token": token},
        )
        assert resp.status_code == 400
        assert "replay" in resp.get_json()["error"].lower()
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)


def test_upload_too_small(tmp_path):
    client, token, _ = _authed_client(tmp_path)
    try:
        from io import BytesIO

        data = {"file": (BytesIO(b"\x00" * 100), "small.replay")}
        resp = client.post(
            "/api/upload",
            data=data,
            content_type="multipart/form-data",
            headers={"X-CSRF-Token": token},
        )
        assert resp.status_code == 400
        assert "small" in resp.get_json()["error"].lower()
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)


def test_upload_duplicate(tmp_path):
    client, token, _ = _authed_client(tmp_path)
    try:
        from io import BytesIO

        # Upload once
        data = {"file": (BytesIO(_replay_content()), "dup.replay")}
        client.post(
            "/api/upload",
            data=data,
            content_type="multipart/form-data",
            headers={"X-CSRF-Token": token},
        )

        # Upload again
        data = {"file": (BytesIO(_replay_content()), "dup.replay")}
        resp = client.post(
            "/api/upload",
            data=data,
            content_type="multipart/form-data",
            headers={"X-CSRF-Token": token},
        )
        assert resp.status_code == 409
        assert resp.get_json()["duplicate"] is True
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)


def test_upload_path_traversal_sanitized(tmp_path):
    client, token, _ = _authed_client(tmp_path)
    try:
        from io import BytesIO

        data = {"file": (BytesIO(_replay_content()), "../../../etc/match.replay")}
        resp = client.post(
            "/api/upload",
            data=data,
            content_type="multipart/form-data",
            headers={"X-CSRF-Token": token},
        )
        # secure_filename strips path traversal, file should be saved safely
        assert resp.status_code == 201
        assert ".." not in resp.get_json()["filename"]
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)


# -- Page serving --


def test_upload_page_serves(tmp_path):
    db_path = file_db(tmp_path)
    app = create_app(db_path, replay_dir=tmp_path)
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.get("/upload")
    assert resp.status_code == 200
    assert b"UPLOAD" in resp.data


# -- CSRF tests --


def test_csrf_token_required_on_auth(tmp_path):
    os.environ["UPLOAD_PASSWORD"] = "secret123"
    try:
        db_path = file_db(tmp_path)
        app = create_app(db_path, replay_dir=tmp_path)
        app.config["TESTING"] = True
        client = app.test_client()

        # POST without CSRF token should be rejected
        resp = client.post("/api/auth", json={"password": "secret123"})
        assert resp.status_code == 403
        assert "csrf" in resp.get_json()["error"].lower()
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)


def test_csrf_token_required_on_upload(tmp_path):
    client, _token, _ = _authed_client(tmp_path)
    try:
        from io import BytesIO

        # POST without CSRF token should be rejected
        data = {"file": (BytesIO(_replay_content()), "match.replay")}
        resp = client.post(
            "/api/upload", data=data, content_type="multipart/form-data"
        )
        assert resp.status_code == 403
        assert "csrf" in resp.get_json()["error"].lower()
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)


# -- Upload status endpoint --


def _status_client(tmp_path):
    db_path = file_db(tmp_path)
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    app = create_app(db_path, replay_dir=replay_dir)
    app.config["TESTING"] = True
    return app.test_client(), replay_dir


def test_upload_status_error_when_replay_missing(tmp_path):
    """No .replay file at all means processing failed (file was deleted)."""
    client, replay_dir = _status_client(tmp_path)

    resp = client.get("/api/upload/status?filename=nonexistent.replay")
    assert resp.status_code == 200
    assert resp.get_json()["status"] == "error"


def test_upload_status_pending_when_replay_exists(tmp_path):
    """.replay exists but no .json yet means still processing."""
    client, replay_dir = _status_client(tmp_path)

    (replay_dir / "test.replay").write_bytes(b"\x00")

    resp = client.get("/api/upload/status?filename=test.replay")
    assert resp.status_code == 200
    assert resp.get_json()["status"] == "pending"


def test_upload_status_processed_when_json_exists(tmp_path):
    """.replay.json exists means processing succeeded."""
    client, replay_dir = _status_client(tmp_path)

    (replay_dir / "test.replay").write_bytes(b"\x00")
    (replay_dir / "test.replay.json").write_bytes(b"{}")

    resp = client.get("/api/upload/status?filename=test.replay")
    assert resp.status_code == 200
    assert resp.get_json()["status"] == "processed"


def test_upload_status_missing_filename(tmp_path):
    client, _ = _status_client(tmp_path)

    resp = client.get("/api/upload/status")
    assert resp.status_code == 400


def test_upload_status_sanitizes_filename(tmp_path):
    """Path traversal in filename param is sanitized."""
    client, _ = _status_client(tmp_path)

    resp = client.get("/api/upload/status?filename=../../../etc/passwd")
    assert resp.status_code == 200
    # secure_filename strips traversal; the sanitized name won't exist
    assert resp.get_json()["status"] in ("error", "unknown")
