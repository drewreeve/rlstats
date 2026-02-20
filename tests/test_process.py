import json
import subprocess
import threading
from unittest.mock import patch

from process import UploadProcessor, convert_replay, process_batch, process_replay
from tests.fixtures import file_db, in_memory_db, load_replay


def _make_conn():
    conn = in_memory_db()
    conn.row_factory = None  # use tuples for simplicity
    return conn


def test_convert_replay_success(tmp_path):
    """convert_replay writes a .replay.json sidecar on success."""
    replay_data = load_replay("match.json")
    replay_path = tmp_path / "test.replay"
    replay_path.write_bytes(b"\x00" * 1024)

    def fake_rrrocket(args, **kwargs):
        stdout = json.dumps(replay_data).encode()
        return subprocess.CompletedProcess(args, 0, stdout=stdout)

    with patch("process.subprocess.run", side_effect=fake_rrrocket):
        success, error = convert_replay(replay_path)

    assert success is True
    assert error is None
    json_path = tmp_path / "test.replay.json"
    assert json_path.exists()
    assert json.loads(json_path.read_text()) == replay_data


def test_convert_replay_failure(tmp_path):
    """convert_replay removes .replay on rrrocket failure."""
    replay_path = tmp_path / "corrupt.replay"
    replay_path.write_bytes(b"\x00" * 1024)

    failed = subprocess.CompletedProcess(["rrrocket"], 1, stderr=b"parse error")

    with patch("process.subprocess.run", return_value=failed):
        success, error = convert_replay(replay_path)

    assert success is False
    assert "rrrocket failed" in error
    assert not replay_path.exists()
    assert not (tmp_path / "corrupt.replay.json").exists()


def test_process_replay_success(tmp_path):
    """process_replay converts .replay to .json and ingests it."""
    conn = _make_conn()
    replay_data = load_replay("match.json")

    replay_path = tmp_path / "test.replay"
    replay_path.write_bytes(b"\x00" * 1024)

    def fake_rrrocket(args, **kwargs):
        # rrrocket outputs JSON to stdout for single files
        stdout = json.dumps(replay_data).encode()
        return subprocess.CompletedProcess(args, 0, stdout=stdout)

    with patch("process.subprocess.run", side_effect=fake_rrrocket):
        success, error = process_replay(replay_path, conn)

    assert success is True
    assert error is None
    conn.commit()
    row = conn.execute("SELECT COUNT(*) FROM matches").fetchone()
    assert row[0] == 1


def test_process_replay_rrrocket_failure(tmp_path):
    """When rrrocket fails, the .replay file is removed and False is returned."""
    conn = _make_conn()
    replay_path = tmp_path / "corrupt.replay"
    replay_path.write_bytes(b"\x00" * 1024)

    failed = subprocess.CompletedProcess(["rrrocket"], 1, stderr=b"parse error")

    with patch("process.subprocess.run", return_value=failed):
        success, error = process_replay(replay_path, conn)

    assert success is False
    assert "rrrocket failed" in error
    assert not replay_path.exists()


def test_process_replay_ingest_failure(tmp_path):
    """When ingest fails, both .replay and .json are removed."""
    conn = _make_conn()
    replay_path = tmp_path / "bad.replay"
    replay_path.write_bytes(b"\x00" * 1024)

    def fake_rrrocket(args, **kwargs):
        stdout = b'{"properties": {}}'
        return subprocess.CompletedProcess(args, 0, stdout=stdout)

    # Force ingest_match to raise
    with (
        patch("process.subprocess.run", side_effect=fake_rrrocket),
        patch("process.ingest_match", side_effect=RuntimeError("ingest broke")),
    ):
        success, error = process_replay(replay_path, conn)

    assert success is False
    assert "Ingest failed" in error
    assert not replay_path.exists()
    assert not (tmp_path / "bad.replay.json").exists()


def test_process_batch_commits(tmp_path):
    """process_batch processes multiple files and commits once."""
    conn = _make_conn()
    replay_data = load_replay("match.json")

    files = []
    for i in range(3):
        p = tmp_path / f"match{i}.replay"
        p.write_bytes(b"\x00" * 1024)
        files.append(p)

    call_count = 0

    def fake_rrrocket(args, **kwargs):
        nonlocal call_count
        # Give each match a unique GUID so they don't collide
        data = json.loads(json.dumps(replay_data))
        data["properties"]["MatchGUID"] = f"GUID-{call_count}"
        call_count += 1
        stdout = json.dumps(data).encode()
        return subprocess.CompletedProcess(args, 0, stdout=stdout)

    with patch("process.subprocess.run", side_effect=fake_rrrocket):
        process_batch(files, conn)

    row = conn.execute("SELECT COUNT(*) FROM matches").fetchone()
    assert row[0] == 3


def test_upload_processor_debounce(tmp_path):
    """Multiple enqueues within the delay result in a single batch."""
    db_path = file_db(tmp_path)
    batch_calls = []

    def fake_batch(f, c):
        batch_calls.append(list(f))
        return {p.name: (True, None) for p in f}

    with patch("process.process_batch", side_effect=fake_batch):
        proc = UploadProcessor(db_path, delay=0.1)

        for i in range(5):
            proc.enqueue(tmp_path / f"match{i}.replay")

        # Wait for the debounce timer to fire
        done = threading.Event()
        original_flush = proc._flush

        def patched_flush():
            original_flush()
            done.set()

        proc._flush = patched_flush
        # Re-enqueue to trigger our patched flush
        proc.enqueue(tmp_path / "match5.replay")
        done.wait(timeout=2.0)

    assert len(batch_calls) >= 1
    # All files should be in total across calls
    all_files = [f for call in batch_calls for f in call]
    assert len(all_files) == 6


