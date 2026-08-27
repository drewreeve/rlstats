import sqlite3
import threading
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

from file_outcome import FileOutcome, Skipped, SkipReason
from tests.fixtures import TEST_DATA_DIR, TRACKED_PLAYERS, file_db
from upload_processor import (
    Errored,
    Parsed,
    Processing,
    Queued,
    UploadProcessor,
    UploadState,
    UploadStatus,
    _BatchProgress,  # pyright: ignore[reportPrivateUsage]
    _UploadFiles,  # pyright: ignore[reportPrivateUsage]
)


def test_upload_processor_debounce(tmp_path: Path):
    """Multiple enqueues within the delay result in a single batch."""
    db_path = file_db(tmp_path)
    parse_calls: list[list[Path]] = []

    def fake_parallel_parse(
        paths: list[Path], tracked_players: object, on_parsed: object = None
    ) -> dict[Path, FileOutcome]:
        parse_calls.append(list(paths))
        return {p: Skipped(SkipReason.NO_TRACKED_PLAYERS) for p in paths}

    def fake_write_batch(
        conn: sqlite3.Connection,
        tracked_players: object,
        results: dict[Path, FileOutcome],
    ) -> dict[str, FileOutcome]:
        return {p.name: Skipped(SkipReason.NO_TRACKED_PLAYERS) for p in results}

    with (
        patch("upload_processor.parallel_parse", side_effect=fake_parallel_parse),
        patch("upload_processor.write_parsed_batch", side_effect=fake_write_batch),
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

    proc = UploadProcessor(db_path, TRACKED_PLAYERS, replay_dir)
    proc._schedule_flush = lambda: None  # type: ignore[method-assign]  # pyright: ignore[reportPrivateUsage]
    proc.enqueue(replay_path)

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
    ) -> dict[Path, FileOutcome]:
        observed["processing"] = proc.file_status(replay_path.name)
        for p in paths:
            if on_parsed is not None:
                on_parsed(p)
        observed["parsed"] = proc.file_status(replay_path.name)
        observed["batch_after_parse"] = proc.batch_progress(replay_path.name)
        return {p: Skipped(SkipReason.NO_TRACKED_PLAYERS) for p in paths}

    def fake_write_batch(
        conn: sqlite3.Connection,
        tracked_players: object,
        results: dict[Path, FileOutcome],
    ) -> dict[str, FileOutcome]:
        return {p.name: Skipped(SkipReason.NO_TRACKED_PLAYERS) for p in results}

    with (
        patch("upload_processor.parallel_parse", side_effect=fake_parallel_parse),
        patch("upload_processor.write_parsed_batch", side_effect=fake_write_batch),
    ):
        proc = UploadProcessor(db_path, TRACKED_PLAYERS, tmp_path)
        proc._schedule_flush = lambda: None  # type: ignore[method-assign]  # pyright: ignore[reportPrivateUsage]
        proc.enqueue(replay_path)
        assert proc.file_status(replay_path.name) == "queued"
        assert proc.batch_progress(replay_path.name) is None

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

    proc = UploadProcessor(db_path, TRACKED_PLAYERS, replay_dir)
    proc._schedule_flush = lambda: None  # type: ignore[method-assign]  # pyright: ignore[reportPrivateUsage]
    proc.enqueue(replay_path)

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
    ) -> dict[Path, FileOutcome]:
        if any(p.name == "a.replay" for p in paths):
            a_started.set()
            assert a_release.wait(timeout=2.0), "batch A was never released"
        for p in paths:
            if on_parsed is not None:
                on_parsed(p)
        return {p: Skipped(SkipReason.NO_TRACKED_PLAYERS) for p in paths}

    def fake_write_batch(
        conn: sqlite3.Connection,
        tracked_players: object,
        results: dict[Path, FileOutcome],
    ) -> dict[str, FileOutcome]:
        return {p.name: Skipped(SkipReason.NO_TRACKED_PLAYERS) for p in results}

    with (
        patch("upload_processor.parallel_parse", side_effect=fake_parallel_parse),
        patch("upload_processor.write_parsed_batch", side_effect=fake_write_batch),
    ):
        proc = UploadProcessor(db_path, TRACKED_PLAYERS, tmp_path)
        proc._schedule_flush = lambda: None  # type: ignore[method-assign]  # pyright: ignore[reportPrivateUsage]

        proc.enqueue(tmp_path / "a.replay")

        thread_a = threading.Thread(target=proc.flush)
        thread_a.start()
        assert a_started.wait(timeout=2.0), "batch A flush never started"

        # Batch A is still blocked mid-parse; its total must reflect only its
        # own single file, not anything queued afterward.
        assert proc.batch_progress("a.replay") == (0, 1)

        proc.enqueue(tmp_path / "b.replay")
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

    proc = UploadProcessor(db_path, TRACKED_PLAYERS, replay_dir, error_retention=0.05)
    proc._schedule_flush = lambda: None  # type: ignore[method-assign]  # pyright: ignore[reportPrivateUsage]
    proc.enqueue(replay_path)

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


def test_upload_status_skipped_when_sentinel_records_skip_reason(tmp_path: Path):
    """A sentinel written by a Skipped outcome reports SKIPPED with the reason,
    not PROCESSED — the bug this outcome protocol exists to prevent."""
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    (replay_dir / "test.replay").write_bytes(b"\x00")
    (replay_dir / "test.replay.ingested").write_text("skipped:no_tracked_players")
    proc = UploadProcessor(file_db(tmp_path), TRACKED_PLAYERS, replay_dir)

    assert proc.status("test.replay") == UploadStatus(
        UploadState.SKIPPED, reason="No tracked players in this replay"
    )


def test_upload_status_reports_live_stage_and_batch(tmp_path: Path):
    """A processor with an in-flight file reports its stage and batch progress."""
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    proc = UploadProcessor(file_db(tmp_path), TRACKED_PLAYERS, replay_dir)
    bp = _BatchProgress(total=20)
    bp.completed = 3
    proc._files.mark_processing(  # pyright: ignore[reportPrivateUsage]
        "test.replay", bp
    )
    proc._files.mark_parsed(  # pyright: ignore[reportPrivateUsage]
        "test.replay", bp
    )

    assert proc.status("test.replay") == UploadStatus(
        UploadState.PENDING, stage="parsed", batch=(3, 20)
    )


def test_upload_status_omits_batch_when_queued(tmp_path: Path):
    """A queued file (flush hasn't started) has no batch yet."""
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    proc = UploadProcessor(file_db(tmp_path), TRACKED_PLAYERS, replay_dir)
    proc._files.mark_queued("test.replay")  # pyright: ignore[reportPrivateUsage]

    assert proc.status("test.replay") == UploadStatus(
        UploadState.PENDING, stage="queued"
    )


def test_upload_status_reports_processor_error(tmp_path: Path):
    """A processor error entry surfaces as an ERROR status with the message."""
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    proc = UploadProcessor(file_db(tmp_path), TRACKED_PLAYERS, replay_dir)
    proc._files.record_error(  # pyright: ignore[reportPrivateUsage]
        "test.replay", "Ingest failed: boom"
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
    proc._files.record_error(  # pyright: ignore[reportPrivateUsage]
        "test.replay", "should be ignored"
    )

    assert proc.status("test.replay") == UploadStatus(UploadState.PROCESSED)


# -- _UploadFiles --


def test_upload_files_mark_queued():
    files = _UploadFiles()
    files.mark_queued("a.replay")

    assert files.get("a.replay") == Queued()


def test_upload_files_mark_processing():
    files = _UploadFiles()
    bp = _BatchProgress(total=5)
    files.mark_processing("a.replay", bp)

    assert files.get("a.replay") == Processing(bp)


def test_upload_files_mark_parsed_after_processing():
    files = _UploadFiles()
    bp = _BatchProgress(total=5)
    files.mark_processing("a.replay", bp)
    files.mark_parsed("a.replay", bp)

    assert files.get("a.replay") == Parsed(bp)


def test_upload_files_mark_parsed_without_existing_record_is_noop():
    files = _UploadFiles()
    bp = _BatchProgress(total=5)
    files.mark_parsed("a.replay", bp)

    assert files.get("a.replay") is None


def test_upload_files_record_error():
    files = _UploadFiles()
    files.record_error("a.replay", "boom")

    record = files.get("a.replay")
    assert isinstance(record, Errored)
    assert record.message == "boom"


def test_upload_files_resolve_removes_record():
    files = _UploadFiles()
    files.mark_queued("a.replay")
    files.resolve("a.replay")

    assert files.get("a.replay") is None


def test_upload_files_resolve_missing_record_is_noop():
    files = _UploadFiles()
    files.resolve("a.replay")

    assert files.get("a.replay") is None


def test_upload_files_prune_stale_drops_only_expired_errors():
    files = _UploadFiles()
    files.mark_queued("queued.replay")
    files.record_error("stale_error.replay", "old")
    time.sleep(0.05)
    cutoff = time.monotonic()
    time.sleep(0.05)
    files.record_error("fresh_error.replay", "new")

    files.prune_stale(cutoff)

    assert files.get("queued.replay") == Queued()
    assert files.get("stale_error.replay") is None
    record = files.get("fresh_error.replay")
    assert isinstance(record, Errored)
    assert record.message == "new"
