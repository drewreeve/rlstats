import json
import sqlite3
import subprocess
import threading
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from ingest import analyze_replay
from process import (
    UploadProcessor,
    UploadState,
    UploadStatus,
    _BatchProgress,  # pyright: ignore[reportPrivateUsage]
    _parse_and_analyze,  # pyright: ignore[reportPrivateUsage]
    _write_parsed_batch,  # pyright: ignore[reportPrivateUsage]
    parse_replay,
    process_replay,
    process_unprocessed,
)
from rrrocket_schema import parse as parse_rrrocket
from tests.fixtures import (
    TEST_DATA_DIR,
    TRACKED_PLAYERS,
    file_db,
    in_memory_db,
    load_replay,
)


def _make_conn() -> sqlite3.Connection:
    conn = in_memory_db()
    conn.row_factory = None  # use tuples for simplicity
    return conn


def test_parse_replay_success(tmp_path: Path):
    """parse_replay returns parsed dict on success."""
    replay_data = load_replay("match.json")
    replay_path = tmp_path / "test.replay"
    replay_path.write_bytes(b"\x00" * 1024)

    def fake_rrrocket(args: Any, **kwargs: Any):
        stdout = json.dumps(replay_data).encode()
        return subprocess.CompletedProcess(args, 0, stdout=stdout)

    with patch("process.subprocess.run", side_effect=fake_rrrocket):
        result, error = parse_replay(replay_path)

    assert result == parse_rrrocket(replay_data)
    assert error is None
    # No .json sidecar should be written
    assert not (tmp_path / "test.replay.json").exists()


def test_parse_replay_failure(tmp_path: Path):
    """parse_replay removes .replay on rrrocket failure."""
    replay_path = tmp_path / "corrupt.replay"
    replay_path.write_bytes(b"\x00" * 1024)

    failed = subprocess.CompletedProcess(["rrrocket"], 1, stderr=b"parse error")

    with patch("process.subprocess.run", return_value=failed):
        result, error = parse_replay(replay_path)

    assert result is None
    assert error is not None
    assert "rrrocket failed" in error
    assert not replay_path.exists()


def test_process_replay_success(tmp_path: Path):
    """process_replay parses and ingests a replay without writing a marker."""
    conn = _make_conn()
    replay_data = load_replay("match.json")

    replay_path = tmp_path / "test.replay"
    replay_path.write_bytes(b"\x00" * 1024)

    def fake_rrrocket(args: Any, **kwargs: Any):
        stdout = json.dumps(replay_data).encode()
        return subprocess.CompletedProcess(args, 0, stdout=stdout)

    with patch("process.subprocess.run", side_effect=fake_rrrocket):
        success, error = process_replay(replay_path, conn, TRACKED_PLAYERS)

    assert success is True
    assert error is None
    conn.commit()
    row = conn.execute("SELECT COUNT(*) FROM matches").fetchone()
    assert row[0] == 1
    # Marker is NOT written by process_replay; _write_parsed_batch handles it after commit
    assert not (tmp_path / "test.replay.ingested").exists()


def test_process_replay_ingest_failure(tmp_path: Path):
    """When ingest fails, the .replay is kept for retry and no marker is written."""
    conn = _make_conn()
    replay_path = tmp_path / "bad.replay"
    replay_path.write_bytes(b"\x00" * 1024)

    def fake_rrrocket(args: Any, **kwargs: Any):
        stdout = b'{"properties": {}}'
        return subprocess.CompletedProcess(args, 0, stdout=stdout)

    with (
        patch("process.subprocess.run", side_effect=fake_rrrocket),
        patch("process.analyze_replay", return_value=MagicMock()),
        patch("process.write_match", side_effect=RuntimeError("ingest broke")),
    ):
        success, error = process_replay(replay_path, conn, TRACKED_PLAYERS)

    assert success is False
    assert error is not None
    assert "Ingest failed" in error
    assert replay_path.exists()
    assert not (tmp_path / "bad.replay.ingested").exists()


def test_process_replay_skipped(tmp_path: Path):
    """When analyze_replay returns None, returns True (sentinel will be written)."""
    conn = _make_conn()
    replay_path = tmp_path / "skip.replay"
    replay_path.write_bytes(b"\x00" * 1024)

    def fake_rrrocket(args: Any, **kwargs: Any):
        stdout = b'{"properties": {}}'
        return subprocess.CompletedProcess(args, 0, stdout=stdout)

    with (
        patch("process.subprocess.run", side_effect=fake_rrrocket),
        patch("process.analyze_replay", return_value=None),
    ):
        success, error = process_replay(replay_path, conn, TRACKED_PLAYERS)

    assert success is True
    assert error is None
    assert replay_path.exists()
    row = conn.execute("SELECT COUNT(*) FROM matches").fetchone()
    assert row[0] == 0


def test_write_parsed_batch_commits(tmp_path: Path):
    """_write_parsed_batch processes multiple files and commits once."""
    conn = _make_conn()
    replay_data = load_replay("match.json")

    files: list[Path] = []
    results: dict[Path, Any] = {}
    for i in range(3):
        p = tmp_path / f"match{i}.replay"
        p.write_bytes(b"\x00" * 1024)
        files.append(p)
        # Give each match a unique GUID so they don't collide
        data = json.loads(json.dumps(replay_data))
        data["properties"]["MatchGUID"] = f"GUID-{i}"
        results[p] = analyze_replay(parse_rrrocket(data), TRACKED_PLAYERS)

    outcomes = _write_parsed_batch(conn, TRACKED_PLAYERS, results)

    row = conn.execute("SELECT COUNT(*) FROM matches").fetchone()
    assert row[0] == 3
    # Markers are written only after commit
    for p in files:
        assert (p.with_suffix(p.suffix + ".ingested")).exists()
        assert outcomes[p.name] == "processed"


def test_write_parsed_batch_rolls_back_partial_write_on_failure(tmp_path: Path):
    """A failure partway through write_match must not leave a half-written match row.

    write_match wraps its own savepoint (see _try_write_match), so a mid-write
    failure here must not get committed alongside the rest of the batch once
    _finalize_batch commits.
    """
    conn = _make_conn()
    replay_data = load_replay("match.json")
    replay_path = tmp_path / "bad.replay"
    replay_path.write_bytes(b"\x00" * 1024)
    analysis = analyze_replay(parse_rrrocket(replay_data), TRACKED_PLAYERS)

    with patch(
        "ingest._insert_match_players",
        side_effect=RuntimeError("simulated failure after match row inserted"),
    ):
        outcomes = _write_parsed_batch(conn, TRACKED_PLAYERS, {replay_path: analysis})

    assert conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0] == 0
    assert not replay_path.with_suffix(replay_path.suffix + ".ingested").exists()
    assert replay_path.exists()
    assert outcomes[replay_path.name].startswith("error:")


def test_write_parsed_batch_isolates_failure_from_other_files_in_batch(
    tmp_path: Path,
):
    """A failing file's rolled-back savepoint must not affect other files in the
    same batch, before or after it, once they share one connection/transaction."""
    from ingest import (
        _insert_match_players as real_insert,  # type: ignore[reportPrivateUsage]
    )

    conn = _make_conn()
    replay_data = load_replay("match.json")

    files = [tmp_path / f"{name}.replay" for name in ("a", "bad", "c")]
    results: dict[Path, Any] = {}
    for p in files:
        p.write_bytes(b"\x00" * 1024)
        data = json.loads(json.dumps(replay_data))
        data["properties"]["MatchGUID"] = f"GUID-{p.stem}"
        results[p] = analyze_replay(parse_rrrocket(data), TRACKED_PLAYERS)

    call_count = 0

    def insert_or_fail(*args: Any, **kwargs: Any):
        nonlocal call_count
        call_count += 1
        if call_count == 2:  # the middle ("bad") file
            raise RuntimeError("simulated failure")
        return real_insert(*args, **kwargs)

    with patch("ingest._insert_match_players", side_effect=insert_or_fail):
        outcomes = _write_parsed_batch(conn, TRACKED_PLAYERS, results)

    assert conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM match_players").fetchone()[0] > 0
    a, bad, c = files
    assert a.with_suffix(a.suffix + ".ingested").exists()
    assert not bad.with_suffix(bad.suffix + ".ingested").exists()
    assert c.with_suffix(c.suffix + ".ingested").exists()
    assert bad.exists()
    assert outcomes[a.name] == "processed"
    assert outcomes[bad.name].startswith("error:")
    assert outcomes[c.name] == "processed"


def test_write_parsed_batch_syncs_tracked_players(tmp_path: Path):
    """A player already in the DB as untracked gets flipped once they're in config.

    Regression test: get_or_create_player's ON CONFLICT doesn't update is_tracked,
    so this relies on _write_parsed_batch also running sync_tracked_players.
    """
    conn = _make_conn()
    conn.execute(
        "INSERT INTO players (platform, platform_id, name, is_tracked)"
        " VALUES (?,?,?,0)",
        ("steam", "76561197969365901", "Drew"),
    )
    conn.commit()

    replay_path = tmp_path / "BEC7EF8411F170E7DBCA41B0676B6A04.replay"
    replay_path.write_bytes(
        (TEST_DATA_DIR / "BEC7EF8411F170E7DBCA41B0676B6A04.replay").read_bytes()
    )
    analysis = _parse_and_analyze(replay_path, TRACKED_PLAYERS)
    assert analysis is not None

    _write_parsed_batch(conn, TRACKED_PLAYERS, {replay_path: analysis})

    row = conn.execute(
        "SELECT is_tracked FROM players WHERE platform_id = ?",
        ("76561197969365901",),
    ).fetchone()
    assert row[0] == 1


def test_parse_replay_end_to_end():
    """parse_replay invokes the real rrrocket binary on a .replay file."""
    replay_path = TEST_DATA_DIR / "BEC7EF8411F170E7DBCA41B0676B6A04.replay"
    result, error = parse_replay(replay_path)
    assert error is None
    assert result is not None
    assert result.match_guid is not None


def test_process_replay_end_to_end(tmp_path: Path):
    """process_replay runs rrrocket and ingests a real replay into the DB."""
    conn = _make_conn()
    replay_path = tmp_path / "BEC7EF8411F170E7DBCA41B0676B6A04.replay"
    replay_path.write_bytes(
        (TEST_DATA_DIR / "BEC7EF8411F170E7DBCA41B0676B6A04.replay").read_bytes()
    )

    success, error = process_replay(replay_path, conn, TRACKED_PLAYERS)

    assert error is None
    assert success is True
    conn.commit()
    row = conn.execute("SELECT COUNT(*) FROM matches").fetchone()
    assert row[0] == 1


def test_process_unprocessed_end_to_end(tmp_path: Path):
    """process_unprocessed runs rrrocket + ingests real replays via the process pool."""
    db_path = file_db(tmp_path)
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    replay_path = replay_dir / "BEC7EF8411F170E7DBCA41B0676B6A04.replay"
    replay_path.write_bytes(
        (TEST_DATA_DIR / "BEC7EF8411F170E7DBCA41B0676B6A04.replay").read_bytes()
    )

    process_unprocessed(db_path, replay_dir, TRACKED_PLAYERS)

    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT COUNT(*) FROM matches").fetchone()
    assert row[0] == 1
    assert replay_path.with_suffix(replay_path.suffix + ".ingested").exists()


def test_process_unprocessed_write_failure_does_not_abort_batch(tmp_path: Path):
    """One replay's write failure doesn't prevent the rest of the batch from ingesting."""
    db_path = file_db(tmp_path)
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    src = TEST_DATA_DIR / "BEC7EF8411F170E7DBCA41B0676B6A04.replay"
    bad = replay_dir / "bad.replay"
    good = replay_dir / "good.replay"
    bad.write_bytes(src.read_bytes())
    good.write_bytes(src.read_bytes())

    call_count = 0

    def fake_write_match(conn: Any, analysis: Any) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("simulated failure")

    with patch("process.write_match", side_effect=fake_write_match):
        process_unprocessed(db_path, replay_dir, TRACKED_PLAYERS)

    assert not bad.with_suffix(bad.suffix + ".ingested").exists()
    assert bad.exists()
    assert good.with_suffix(good.suffix + ".ingested").exists()


def test_upload_processor_debounce(tmp_path: Path):
    """Multiple enqueues within the delay result in a single batch."""
    db_path = file_db(tmp_path)
    parse_calls: list[list[Path]] = []

    def fake_parallel_parse(
        paths: list[Path], tracked_players: object, on_parsed: object = None
    ) -> dict[Path, None]:
        parse_calls.append(list(paths))
        return {p: None for p in paths}

    def fake_write_batch(
        conn: sqlite3.Connection, tracked_players: object, results: dict[Path, None]
    ) -> dict[str, str]:
        return {p.name: "skipped" for p in results}

    with (
        patch("process._parallel_parse", side_effect=fake_parallel_parse),
        patch("process._write_parsed_batch", side_effect=fake_write_batch),
    ):
        proc = UploadProcessor(db_path, TRACKED_PLAYERS, tmp_path, delay=0.1)

        for i in range(5):
            proc.enqueue(tmp_path / f"match{i}.replay")

        # Wait for the debounce timer to fire
        done = threading.Event()
        original_flush = proc.flush

        def patched_flush() -> None:
            original_flush()
            done.set()

        proc.flush = patched_flush  # type: ignore[method-assign]
        # Re-enqueue to trigger our patched flush
        proc.enqueue(tmp_path / "match5.replay")
        done.wait(timeout=2.0)

    assert len(parse_calls) >= 1
    # All files should be in total across calls
    all_files = [f for call in parse_calls for f in call]
    assert len(all_files) == 6


def test_upload_processor_flush_end_to_end(tmp_path: Path):
    """flush() runs rrrocket + ingests a real replay via the real process pool."""
    db_path = file_db(tmp_path)
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    replay_path = replay_dir / "BEC7EF8411F170E7DBCA41B0676B6A04.replay"
    replay_path.write_bytes(
        (TEST_DATA_DIR / "BEC7EF8411F170E7DBCA41B0676B6A04.replay").read_bytes()
    )

    proc = UploadProcessor(db_path, TRACKED_PLAYERS, replay_dir, delay=100.0)
    proc.enqueue(replay_path)
    assert proc._timer is not None  # pyright: ignore[reportPrivateUsage]
    proc._timer.cancel()  # pyright: ignore[reportPrivateUsage]

    proc.flush()

    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT COUNT(*) FROM matches").fetchone()
    assert row[0] == 1
    assert replay_path.with_suffix(replay_path.suffix + ".ingested").exists()


def test_upload_processor_status_transitions(tmp_path: Path):
    """queued -> processing -> parsed -> pruned (resolved) as flush proceeds."""
    db_path = file_db(tmp_path)
    replay_path = tmp_path / "match.replay"
    observed: dict[str, Any] = {}

    def fake_parallel_parse(
        paths: list[Path], tracked_players: object, on_parsed: Any = None
    ) -> dict[Path, None]:
        observed["processing"] = proc.file_status(replay_path.name)
        for p in paths:
            if on_parsed is not None:
                on_parsed(p)
        observed["parsed"] = proc.file_status(replay_path.name)
        observed["batch_after_parse"] = proc.batch_progress(replay_path.name)
        return {p: None for p in paths}

    def fake_write_batch(
        conn: sqlite3.Connection, tracked_players: object, results: dict[Path, None]
    ) -> dict[str, str]:
        return {p.name: "skipped" for p in results}

    with (
        patch("process._parallel_parse", side_effect=fake_parallel_parse),
        patch("process._write_parsed_batch", side_effect=fake_write_batch),
    ):
        proc = UploadProcessor(db_path, TRACKED_PLAYERS, tmp_path, delay=100.0)
        proc.enqueue(replay_path)
        assert proc.file_status(replay_path.name) == "queued"
        assert proc.batch_progress(replay_path.name) is None
        assert proc._timer is not None  # pyright: ignore[reportPrivateUsage]
        proc._timer.cancel()  # pyright: ignore[reportPrivateUsage]

        proc.flush()

    assert observed["processing"] == "processing"
    assert observed["parsed"] == "parsed"
    assert observed["batch_after_parse"] == (1, 1)
    # Skipped/processed files are pruned once resolved; the sentinel is the record.
    assert proc.file_status(replay_path.name) is None
    assert proc.batch_progress(replay_path.name) is None


def test_upload_processor_status_error_path(tmp_path: Path):
    """A write failure keeps an error:<msg> entry instead of pruning it."""
    db_path = file_db(tmp_path)
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    replay_path = replay_dir / "BEC7EF8411F170E7DBCA41B0676B6A04.replay"
    replay_path.write_bytes(
        (TEST_DATA_DIR / "BEC7EF8411F170E7DBCA41B0676B6A04.replay").read_bytes()
    )

    proc = UploadProcessor(db_path, TRACKED_PLAYERS, replay_dir, delay=100.0)
    proc.enqueue(replay_path)
    assert proc._timer is not None  # pyright: ignore[reportPrivateUsage]
    proc._timer.cancel()  # pyright: ignore[reportPrivateUsage]

    with patch("process.write_match", side_effect=RuntimeError("ingest broke")):
        proc.flush()

    status = proc.file_status(replay_path.name)
    assert status is not None
    assert status.startswith("error:")
    assert "ingest broke" in status
    assert proc.batch_progress(replay_path.name) is None


def test_upload_processor_overlapping_flushes_do_not_corrupt_batch_progress(
    tmp_path: Path,
):
    """A flush already parsing must not have its batch progress clobbered by a
    second flush that starts (and finishes) while the first is still in flight.

    This can happen because Timer.cancel() in enqueue() only prevents a timer
    that hasn't fired yet; it can't stop a flush() call already running.
    """
    db_path = file_db(tmp_path)
    a_started = threading.Event()
    a_release = threading.Event()

    def fake_parallel_parse(
        paths: list[Path], tracked_players: object, on_parsed: Any = None
    ) -> dict[Path, None]:
        if any(p.name == "a.replay" for p in paths):
            a_started.set()
            assert a_release.wait(timeout=2.0), "batch A was never released"
        for p in paths:
            if on_parsed is not None:
                on_parsed(p)
        return {p: None for p in paths}

    def fake_write_batch(
        conn: sqlite3.Connection, tracked_players: object, results: dict[Path, None]
    ) -> dict[str, str]:
        return {p.name: "skipped" for p in results}

    with (
        patch("process._parallel_parse", side_effect=fake_parallel_parse),
        patch("process._write_parsed_batch", side_effect=fake_write_batch),
    ):
        proc = UploadProcessor(db_path, TRACKED_PLAYERS, tmp_path, delay=100.0)

        proc.enqueue(tmp_path / "a.replay")
        assert proc._timer is not None  # pyright: ignore[reportPrivateUsage]
        proc._timer.cancel()  # pyright: ignore[reportPrivateUsage]

        thread_a = threading.Thread(target=proc.flush)
        thread_a.start()
        assert a_started.wait(timeout=2.0), "batch A flush never started"

        # Batch A is still blocked mid-parse; its total must reflect only its
        # own single file, not anything queued afterward.
        assert proc.batch_progress("a.replay") == (0, 1)

        proc.enqueue(tmp_path / "b.replay")
        assert proc._timer is not None  # pyright: ignore[reportPrivateUsage]
        proc._timer.cancel()  # pyright: ignore[reportPrivateUsage]
        proc.flush()  # batch B runs to completion synchronously, on the foreground thread

        # Batch B finishing (and clearing its own progress) must not have
        # touched batch A's still-in-flight progress.
        assert proc.batch_progress("a.replay") == (0, 1)
        assert proc.batch_progress("b.replay") is None
        assert proc.file_status("b.replay") is None

        a_release.set()
        thread_a.join(timeout=2.0)
        assert not thread_a.is_alive(), "batch A flush never finished"

    assert proc.batch_progress("a.replay") is None
    assert proc.file_status("a.replay") is None


def test_upload_processor_error_entries_expire(tmp_path: Path):
    """error:<msg> entries older than error_retention are pruned on the next enqueue."""
    db_path = file_db(tmp_path)
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    replay_path = replay_dir / "BEC7EF8411F170E7DBCA41B0676B6A04.replay"
    replay_path.write_bytes(
        (TEST_DATA_DIR / "BEC7EF8411F170E7DBCA41B0676B6A04.replay").read_bytes()
    )

    proc = UploadProcessor(
        db_path, TRACKED_PLAYERS, replay_dir, delay=100.0, error_retention=0.05
    )
    proc.enqueue(replay_path)
    assert proc._timer is not None  # pyright: ignore[reportPrivateUsage]
    proc._timer.cancel()  # pyright: ignore[reportPrivateUsage]

    with patch("process.write_match", side_effect=RuntimeError("ingest broke")):
        proc.flush()

    status = proc.file_status(replay_path.name)
    assert status is not None
    assert status.startswith("error:")

    time.sleep(0.1)  # outlive the tiny retention window

    # Enqueue a *different* file to trigger pruning without exercising the
    # queued-reset path (re-enqueuing replay_path itself would reset its own
    # error bookkeeping directly, proving nothing about the prune sweep).
    other_path = replay_dir / "other.replay"
    proc.enqueue(other_path)
    assert proc._timer is not None  # pyright: ignore[reportPrivateUsage]
    proc._timer.cancel()  # pyright: ignore[reportPrivateUsage]

    assert proc.file_status(replay_path.name) is None


# -- UploadProcessor.status() reconciliation --


def test_upload_status_missing_file_is_error(tmp_path: Path):
    """No .replay file at all means processing failed (file was deleted)."""
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    proc = UploadProcessor(file_db(tmp_path), TRACKED_PLAYERS, replay_dir)

    assert proc.status("nonexistent.replay") == UploadStatus(UploadState.ERROR)


def test_upload_status_bare_pending_when_replay_exists_untracked(tmp_path: Path):
    """.replay exists on disk but the processor has no record of it: pending, no stage."""
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    (replay_dir / "test.replay").write_bytes(b"\x00")
    proc = UploadProcessor(file_db(tmp_path), TRACKED_PLAYERS, replay_dir)

    assert proc.status("test.replay") == UploadStatus(UploadState.PENDING)


def test_upload_status_processed_when_ingested_marker_exists(tmp_path: Path):
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    (replay_dir / "test.replay").write_bytes(b"\x00")
    (replay_dir / "test.replay.ingested").write_bytes(b"")
    proc = UploadProcessor(file_db(tmp_path), TRACKED_PLAYERS, replay_dir)

    assert proc.status("test.replay") == UploadStatus(UploadState.PROCESSED)


def test_upload_status_reports_live_stage_and_batch(tmp_path: Path):
    """A processor with an in-flight file reports its stage and batch progress."""
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    proc = UploadProcessor(file_db(tmp_path), TRACKED_PLAYERS, replay_dir)
    proc._file_status["test.replay"] = "parsed"  # pyright: ignore[reportPrivateUsage]
    bp = _BatchProgress(total=20)
    bp.completed = 3
    proc._file_batch["test.replay"] = bp  # pyright: ignore[reportPrivateUsage]

    assert proc.status("test.replay") == UploadStatus(
        UploadState.PENDING, stage="parsed", batch=(3, 20)
    )


def test_upload_status_omits_batch_when_queued(tmp_path: Path):
    """A queued file (flush hasn't started) has no batch yet."""
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    proc = UploadProcessor(file_db(tmp_path), TRACKED_PLAYERS, replay_dir)
    proc._file_status["test.replay"] = "queued"  # pyright: ignore[reportPrivateUsage]

    assert proc.status("test.replay") == UploadStatus(
        UploadState.PENDING, stage="queued"
    )


def test_upload_status_reports_processor_error(tmp_path: Path):
    """A processor error entry surfaces as an ERROR status with the message."""
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    proc = UploadProcessor(file_db(tmp_path), TRACKED_PLAYERS, replay_dir)
    proc._file_status["test.replay"] = (  # pyright: ignore[reportPrivateUsage]
        "error:Ingest failed: boom"
    )

    assert proc.status("test.replay") == UploadStatus(
        UploadState.ERROR, error="Ingest failed: boom"
    )


def test_upload_status_sentinel_wins_over_processor_error(tmp_path: Path):
    """The .ingested sentinel is the durable record and wins over stale live state."""
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    (replay_dir / "test.replay").write_bytes(b"\x00")
    (replay_dir / "test.replay.ingested").write_bytes(b"")
    proc = UploadProcessor(file_db(tmp_path), TRACKED_PLAYERS, replay_dir)
    proc._file_status["test.replay"] = (  # pyright: ignore[reportPrivateUsage]
        "error:should be ignored"
    )

    assert proc.status("test.replay") == UploadStatus(UploadState.PROCESSED)
