import os
import sqlite3

from server import create_app
from tests.fixtures import in_memory_db


def _make_client(tmp_path, env=None):
    """Create a Flask test client with a tmp replays directory."""
    old_env = {}
    env = env or {}
    for k, v in env.items():
        old_env[k] = os.environ.get(k)
        os.environ[k] = v

    conn = in_memory_db()
    conn.row_factory = sqlite3.Row
    app = create_app(conn, replay_dir=tmp_path)
    app.config["TESTING"] = True
    client = app.test_client()

    # Restore env after setup (env vars read at request time, so set them for duration)
    # We'll use a class to manage cleanup
    return client, old_env, env


def _cleanup_env(old_env, env):
    for k in env:
        if old_env.get(k) is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = old_env[k]


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
        conn = in_memory_db()
        conn.row_factory = sqlite3.Row
        app = create_app(conn, replay_dir=tmp_path)
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
        conn = in_memory_db()
        conn.row_factory = sqlite3.Row
        app = create_app(conn, replay_dir=tmp_path)
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
    conn = in_memory_db()
    conn.row_factory = sqlite3.Row
    app = create_app(conn, replay_dir=tmp_path)
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
    conn = in_memory_db()
    conn.row_factory = sqlite3.Row
    app = create_app(conn, replay_dir=tmp_path)
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
        conn = in_memory_db()
        conn.row_factory = sqlite3.Row
        app = create_app(conn, replay_dir=tmp_path)
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
    conn = in_memory_db()
    conn.row_factory = sqlite3.Row
    app = create_app(conn, replay_dir=tmp_path)
    app.config["TESTING"] = True
    client = app.test_client()
    token = _get_csrf_token(client)
    client.post(
        "/api/auth",
        json={"password": "test"},
        headers={"X-CSRF-Token": token},
    )
    return client, token


def test_upload_valid_file(tmp_path):
    client, token = _authed_client(tmp_path)
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
        assert (tmp_path / "match.replay").exists()
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)


def test_upload_unauthenticated(tmp_path):
    conn = in_memory_db()
    conn.row_factory = sqlite3.Row
    app = create_app(conn, replay_dir=tmp_path)
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
    client, token = _authed_client(tmp_path)
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
    client, token = _authed_client(tmp_path)
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
    client, token = _authed_client(tmp_path)
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
    client, token = _authed_client(tmp_path)
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
    conn = in_memory_db()
    conn.row_factory = sqlite3.Row
    app = create_app(conn, replay_dir=tmp_path)
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.get("/upload")
    assert resp.status_code == 200
    assert b"UPLOAD" in resp.data


# -- CSRF tests --


def test_csrf_token_required_on_auth(tmp_path):
    os.environ["UPLOAD_PASSWORD"] = "secret123"
    try:
        conn = in_memory_db()
        conn.row_factory = sqlite3.Row
        app = create_app(conn, replay_dir=tmp_path)
        app.config["TESTING"] = True
        client = app.test_client()

        # POST without CSRF token should be rejected
        resp = client.post("/api/auth", json={"password": "secret123"})
        assert resp.status_code == 403
        assert "csrf" in resp.get_json()["error"].lower()
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)


def test_csrf_token_required_on_upload(tmp_path):
    client, _token = _authed_client(tmp_path)
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


def test_csrf_flow_works(tmp_path):
    """Full flow: GET status -> extract token -> POST auth with token succeeds."""
    os.environ["UPLOAD_PASSWORD"] = "secret123"
    try:
        conn = in_memory_db()
        conn.row_factory = sqlite3.Row
        app = create_app(conn, replay_dir=tmp_path)
        app.config["TESTING"] = True
        client = app.test_client()

        # Get CSRF token from status endpoint
        status_resp = client.get("/api/auth/status")
        csrf_token = status_resp.get_json()["csrf_token"]
        assert csrf_token

        # Use token to authenticate
        resp = client.post(
            "/api/auth",
            json={"password": "secret123"},
            headers={"X-CSRF-Token": csrf_token},
        )
        assert resp.status_code == 200
        assert resp.get_json()["authenticated"] is True
    finally:
        os.environ.pop("UPLOAD_PASSWORD", None)
