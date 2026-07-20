from collections.abc import Callable
from io import BytesIO
from pathlib import Path

from starlette.testclient import TestClient

from config import Settings
from process import (
    UploadProcessor,
    _BatchProgress,  # pyright: ignore[reportPrivateUsage]
)
from server import create_app
from tests.fixtures import file_db


def _replay_content(size: int = 300 * 1024) -> bytes:
    """Generate fake replay content of given size."""
    return b"\x00" * size


def _get_csrf_token(client: TestClient) -> str:
    """Get a CSRF token by hitting /api/auth/status."""
    resp = client.get("/api/auth/status")
    return str(resp.json()["csrf_token"])


# -- Auth tests --


def test_auth_correct_password(tmp_path: Path, make_settings: Callable[..., Settings]):
    settings = make_settings(upload_password="secret123")
    app = create_app(file_db(tmp_path), replay_dir=tmp_path, settings=settings)
    client = TestClient(app, base_url="https://testserver")

    token = _get_csrf_token(client)
    resp = client.post(
        "/api/auth",
        json={"password": "secret123"},
        headers={"X-CSRF-Token": token},
    )
    assert resp.status_code == 200
    assert resp.json()["authenticated"] is True


def test_auth_wrong_password(tmp_path: Path, make_settings: Callable[..., Settings]):
    settings = make_settings(upload_password="secret123")
    app = create_app(file_db(tmp_path), replay_dir=tmp_path, settings=settings)
    client = TestClient(app, base_url="https://testserver")

    token = _get_csrf_token(client)
    resp = client.post(
        "/api/auth",
        json={"password": "wrong"},
        headers={"X-CSRF-Token": token},
    )
    assert resp.status_code == 401


def test_auth_no_password_configured(tmp_path: Path):
    app = create_app(file_db(tmp_path), replay_dir=tmp_path)
    client = TestClient(app, base_url="https://testserver")

    token = _get_csrf_token(client)
    resp = client.post(
        "/api/auth",
        json={"password": "anything"},
        headers={"X-CSRF-Token": token},
    )
    assert resp.status_code == 403


def test_auth_status_unauthenticated(tmp_path: Path):
    app = create_app(file_db(tmp_path), replay_dir=tmp_path)
    client = TestClient(app, base_url="https://testserver")

    resp = client.get("/api/auth/status")
    data = resp.json()
    assert resp.status_code == 200
    assert data["authenticated"] is False
    assert "csrf_token" in data


def test_auth_status_after_login(
    tmp_path: Path, make_settings: Callable[..., Settings]
):
    settings = make_settings(upload_password="secret123")
    app = create_app(file_db(tmp_path), replay_dir=tmp_path, settings=settings)
    client = TestClient(app, base_url="https://testserver")

    token = _get_csrf_token(client)
    client.post(
        "/api/auth",
        json={"password": "secret123"},
        headers={"X-CSRF-Token": token},
    )
    resp = client.get("/api/auth/status")
    assert resp.json()["authenticated"] is True


# -- Upload tests --


def _authed_client(
    tmp_path: Path, make_settings: Callable[..., Settings]
) -> tuple[TestClient, str, Path]:
    settings = make_settings(upload_password="test")
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    app = create_app(file_db(tmp_path), replay_dir=replay_dir, settings=settings)
    client = TestClient(app, base_url="https://testserver")
    token = _get_csrf_token(client)
    client.post(
        "/api/auth",
        json={"password": "test"},
        headers={"X-CSRF-Token": token},
    )
    return client, token, replay_dir


def test_upload_valid_file(tmp_path: Path, make_settings: Callable[..., Settings]):
    client, token, replay_dir = _authed_client(tmp_path, make_settings)

    resp = client.post(
        "/api/upload",
        files={
            "file": (
                "match.replay",
                BytesIO(_replay_content()),
                "application/octet-stream",
            )
        },
        headers={"X-CSRF-Token": token},
    )
    assert resp.status_code == 201
    assert resp.json()["filename"] == "match.replay"
    assert (replay_dir / "match.replay").exists()


def test_upload_unauthenticated(tmp_path: Path):
    app = create_app(file_db(tmp_path), replay_dir=tmp_path)
    client = TestClient(app, base_url="https://testserver")

    token = _get_csrf_token(client)
    resp = client.post(
        "/api/upload",
        files={
            "file": (
                "match.replay",
                BytesIO(_replay_content()),
                "application/octet-stream",
            )
        },
        headers={"X-CSRF-Token": token},
    )
    assert resp.status_code == 401


def test_upload_wrong_extension(tmp_path: Path, make_settings: Callable[..., Settings]):
    client, token, _ = _authed_client(tmp_path, make_settings)

    resp = client.post(
        "/api/upload",
        files={
            "file": (
                "match.txt",
                BytesIO(_replay_content()),
                "application/octet-stream",
            )
        },
        headers={"X-CSRF-Token": token},
    )
    assert resp.status_code == 400
    assert "replay" in resp.json()["error"].lower()


def test_upload_duplicate(tmp_path: Path, make_settings: Callable[..., Settings]):
    client, token, _ = _authed_client(tmp_path, make_settings)

    # Upload once
    client.post(
        "/api/upload",
        files={
            "file": (
                "dup.replay",
                BytesIO(_replay_content()),
                "application/octet-stream",
            )
        },
        headers={"X-CSRF-Token": token},
    )

    # Upload again
    resp = client.post(
        "/api/upload",
        files={
            "file": (
                "dup.replay",
                BytesIO(_replay_content()),
                "application/octet-stream",
            )
        },
        headers={"X-CSRF-Token": token},
    )
    assert resp.status_code == 409
    assert resp.json()["duplicate"] is True


def test_upload_path_traversal_sanitized(
    tmp_path: Path, make_settings: Callable[..., Settings]
):
    client, token, _ = _authed_client(tmp_path, make_settings)

    resp = client.post(
        "/api/upload",
        files={
            "file": (
                "../../../etc/match.replay",
                BytesIO(_replay_content()),
                "application/octet-stream",
            )
        },
        headers={"X-CSRF-Token": token},
    )
    # _secure_filename strips path traversal, file should be saved safely
    assert resp.status_code == 201
    assert ".." not in resp.json()["filename"]


# -- Page serving --


def test_upload_page_serves(tmp_path: Path):
    app = create_app(file_db(tmp_path), replay_dir=tmp_path)
    client = TestClient(app, base_url="https://testserver")

    resp = client.get("/upload")
    assert resp.status_code == 200
    assert b"UPLOAD" in resp.content


# -- CSRF tests --


def test_csrf_token_required_on_auth(
    tmp_path: Path, make_settings: Callable[..., Settings]
):
    settings = make_settings(upload_password="secret123")
    app = create_app(file_db(tmp_path), replay_dir=tmp_path, settings=settings)
    client = TestClient(app, base_url="https://testserver")

    # POST without CSRF token should be rejected
    resp = client.post("/api/auth", json={"password": "secret123"})
    assert resp.status_code == 403
    assert "csrf" in resp.json()["error"].lower()


def test_csrf_token_required_on_upload(
    tmp_path: Path, make_settings: Callable[..., Settings]
):
    client, _token, _ = _authed_client(tmp_path, make_settings)

    # POST without CSRF token should be rejected
    resp = client.post(
        "/api/upload",
        files={
            "file": (
                "match.replay",
                BytesIO(_replay_content()),
                "application/octet-stream",
            )
        },
    )
    assert resp.status_code == 403
    assert "csrf" in resp.json()["error"].lower()


# -- Upload status endpoint --


def _status_client(tmp_path: Path) -> tuple[TestClient, Path]:
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    app = create_app(file_db(tmp_path), replay_dir=replay_dir)
    return TestClient(app, base_url="https://testserver"), replay_dir


def test_upload_status_missing_filename(tmp_path: Path):
    client, _ = _status_client(tmp_path)

    resp = client.get("/api/upload/status")
    assert resp.status_code == 400


def test_upload_status_sanitizes_filename(tmp_path: Path):
    """Path traversal in filename param is sanitized."""
    client, _ = _status_client(tmp_path)

    resp = client.get("/api/upload/status?filename=../../../etc/passwd")
    assert resp.status_code == 200
    # _secure_filename strips traversal; the sanitized name won't exist
    assert resp.json()["status"] in ("error", "unknown")


def test_upload_status_calls_through_to_processor(tmp_path: Path):
    """The endpoint asks UploadProcessor.status() and serializes its answer;
    precedence/reconciliation itself is covered directly against
    UploadProcessor.status() in test_process.py."""
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    (replay_dir / "test.replay").write_bytes(b"\x00")
    (replay_dir / "test.replay.ingested").write_bytes(b"")
    app = create_app(file_db(tmp_path), replay_dir=replay_dir)
    client = TestClient(app, base_url="https://testserver")

    resp = client.get("/api/upload/status?filename=test.replay")
    assert resp.status_code == 200
    assert resp.json() == {"status": "processed"}


def test_upload_status_serializes_stage_and_batch(tmp_path: Path):
    """The stage/batch sub-object — the part the frontend polls for progress —
    round-trips through the endpoint's JSON, not just UploadProcessor.status()."""
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    processor = UploadProcessor(file_db(tmp_path), {}, replay_dir)
    processor._file_status["test.replay"] = "parsed"  # pyright: ignore[reportPrivateUsage]
    bp = _BatchProgress(total=20)
    bp.completed = 3
    processor._file_batch["test.replay"] = bp  # pyright: ignore[reportPrivateUsage]
    app = create_app(file_db(tmp_path), replay_dir=replay_dir, processor=processor)
    client = TestClient(app, base_url="https://testserver")

    resp = client.get("/api/upload/status?filename=test.replay")
    assert resp.status_code == 200
    assert resp.json() == {
        "status": "pending",
        "stage": "parsed",
        "batch": {"completed": 3, "total": 20},
    }
