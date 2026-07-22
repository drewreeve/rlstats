import functools
import logging
import os
import sqlite3
import subprocess
import threading
import time
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import cast

import orjson

from config import load_tracked_players
from ingest import (
    ReplayAnalysis,
    analyze_replay,
    sync_tracked_players,
    write_match,
)
from player_identity import PlayerIdentity
from rrrocket_schema import ParsedReplay, ReplayJSON
from rrrocket_schema import parse as _parse_rrrocket

logger = logging.getLogger(__name__)

_batch_lock = threading.Lock()


def _open_write_conn(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def parse_replay(replay_path: Path) -> tuple[ParsedReplay | None, str | None]:
    """Run rrrocket on a .replay file and return the parsed JSON.

    Returns (parsed_dict, None) on success. On failure, removes the corrupt
    .replay file and returns (None, error_message).
    """
    try:
        result = subprocess.run(
            ["rrrocket", "-n", str(replay_path)],
            capture_output=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        msg = f"rrrocket failed: {exc}"
        logger.warning("rrrocket failed for %s: %s", replay_path.name, exc)
        replay_path.unlink(missing_ok=True)
        return None, msg

    if result.returncode != 0:
        stderr = result.stderr.decode(errors="replace").strip()
        msg = f"rrrocket failed (exit {result.returncode}): {stderr}"
        logger.warning(
            "rrrocket failed for %s (exit %d): %s",
            replay_path.name,
            result.returncode,
            stderr,
        )
        replay_path.unlink(missing_ok=True)
        return None, msg

    return _parse_rrrocket(cast(ReplayJSON, orjson.loads(result.stdout))), None


def _try_write_match(
    conn: sqlite3.Connection, replay_path: Path, analysis: ReplayAnalysis
) -> str | None:
    """Write analysis to conn. Returns None on success, an error message on failure.

    write_match is atomic (wraps its own savepoint), so a failure here never
    leaves a partial write behind for _finalize_batch's later commit to pick up.
    """
    try:
        write_match(conn, analysis)
    except Exception as exc:
        logger.warning("Ingest failed for %s: %s", replay_path.name, exc)
        return f"Ingest failed: {exc}"
    return None


def _finalize_batch(
    conn: sqlite3.Connection,
    tracked_players: dict[PlayerIdentity, str],
    resolved: list[Path],
) -> None:
    """Sync tracked-player status, commit, and mark resolved files as ingested.

    Caller must hold _batch_lock.
    """
    sync_tracked_players(conn, tracked_players)
    conn.commit()
    for replay_path in resolved:
        replay_path.with_suffix(replay_path.suffix + ".ingested").touch()


def process_replay(
    replay_path: Path,
    conn: sqlite3.Connection,
    tracked_players: dict[PlayerIdentity, str],
) -> tuple[bool, str | None]:
    """Run rrrocket on a .replay file, then ingest the parsed data.

    Returns (True, None) when the file is resolved and a sentinel should be written:
    either successfully ingested, or skipped (no tracked players, missing metadata).
    Returns (False, error_message) on unexpected failure; the sentinel is not written
    so the next run retries. Corrupt files that fail rrrocket parsing are deleted.
    """
    replay, error = parse_replay(replay_path)
    if replay is None:
        return False, error

    analysis = analyze_replay(replay, tracked_players)
    if analysis is None:
        return True, None

    error = _try_write_match(conn, replay_path, analysis)
    return error is None, error


def _parse_and_analyze(
    replay_path: Path, tracked_players: dict[PlayerIdentity, str]
) -> ReplayAnalysis | None:
    """Worker for parallel processing: parse + analyze a replay without DB access."""
    replay, _ = parse_replay(replay_path)
    if replay is None:
        return None
    analysis = analyze_replay(replay, tracked_players)
    if analysis is None:
        logger.debug(
            "Skipping %s: no tracked players or missing metadata", replay_path.name
        )
    return analysis


def _parallel_parse(
    paths: list[Path],
    tracked_players: dict[PlayerIdentity, str],
    on_parsed: Callable[[Path], None] | None = None,
) -> dict[Path, ReplayAnalysis | None]:
    """Parse+analyze a batch of replays in a process pool.

    Returns path -> analysis (in the same order as paths), with None meaning
    skip (corrupt, untracked, or missing metadata). Calls on_parsed(path) as
    each file's parse completes, for progress reporting; that happens in
    completion order, but the returned dict is reordered back to match paths
    so write order stays deterministic regardless of which worker finishes first.
    """
    workers = max(1, (os.cpu_count() or 2) // 2)
    worker = functools.partial(_parse_and_analyze, tracked_players=tracked_players)
    completed: dict[Path, ReplayAnalysis | None] = {}
    with ProcessPoolExecutor(max_workers=workers) as pool:
        future_to_path = {pool.submit(worker, path): path for path in paths}
        for future in as_completed(future_to_path):
            path = future_to_path[future]
            completed[path] = future.result()
            if on_parsed is not None:
                on_parsed(path)
    return {path: completed[path] for path in paths}


def _write_parsed_batch(
    conn: sqlite3.Connection,
    tracked_players: dict[PlayerIdentity, str],
    results: dict[Path, ReplayAnalysis | None],
) -> dict[str, str]:
    """Write parsed results to conn, finalize the batch, and report per-file outcomes.

    Outcomes are "processed", "skipped" (no tracked players / missing metadata), or
    "error:<message>". Acquires _batch_lock itself, so batch-commit semantics (one
    commit + all sentinels at the end, write_match's own savepoint isolating each
    file's write) are unchanged from before this was pulled out of process_batch.
    """
    outcomes: dict[str, str] = {}
    with _batch_lock:
        resolved: list[Path] = []
        for path, analysis in results.items():
            if analysis is not None:
                error = _try_write_match(conn, path, analysis)
                if error is None:
                    resolved.append(path)
                    outcomes[path.name] = "processed"
                else:
                    outcomes[path.name] = f"error:{error}"
            elif path.exists():
                resolved.append(path)
                outcomes[path.name] = "skipped"
            else:
                outcomes[path.name] = "error:parse failed"
        _finalize_batch(conn, tracked_players, resolved)
    return outcomes


class UploadState(Enum):
    PENDING = "pending"
    PROCESSED = "processed"
    ERROR = "error"


@dataclass
class UploadStatus:
    """Reconciled status for one uploaded file: folds together the .ingested
    sentinel, this processor's in-memory pipeline state, and file existence
    into the single answer a caller needs."""

    state: UploadState
    stage: str | None = None
    batch: tuple[int, int] | None = None
    error: str | None = None


class _BatchProgress:
    """Mutable (completed, total) counter shared by every file in one flush's
    batch. Scoped per-batch (not processor-wide) so two overlapping flushes —
    possible because Timer.cancel() doesn't interrupt an in-flight flush() —
    can't clobber each other's displayed progress."""

    __slots__ = ("completed", "total")

    def __init__(self, total: int) -> None:
        self.completed = 0
        self.total = total


@dataclass(frozen=True)
class Queued:
    pass


@dataclass(frozen=True)
class Processing:
    batch: _BatchProgress


@dataclass(frozen=True)
class Parsed:
    batch: _BatchProgress


@dataclass(frozen=True)
class Errored:
    message: str
    recorded_at: float


FileRecord = Queued | Processing | Parsed | Errored


class _UploadFiles:
    """Per-file pipeline state for one UploadProcessor: queued -> processing
    -> parsed -> resolved (removed), or -> errored (kept until it expires).

    Not thread-safe on its own — the owning UploadProcessor serializes all
    access via its own lock, the same lock that also guards the upload queue
    and timer, so a flush's "drain queue, mark files processing" step stays
    one atomic unit.
    """

    def __init__(self) -> None:
        self._records: dict[str, FileRecord] = {}

    def get(self, name: str) -> FileRecord | None:
        return self._records.get(name)

    def mark_queued(self, name: str) -> None:
        self._records[name] = Queued()

    def mark_processing(self, name: str, batch: _BatchProgress) -> None:
        self._records[name] = Processing(batch)

    def mark_parsed(self, name: str, batch: _BatchProgress) -> None:
        if name in self._records:
            self._records[name] = Parsed(batch)

    def record_error(self, name: str, message: str) -> None:
        self._records[name] = Errored(message, time.monotonic())

    def resolve(self, name: str) -> None:
        self._records.pop(name, None)

    def prune_stale(self, cutoff: float) -> None:
        stale = [
            name
            for name, record in self._records.items()
            if isinstance(record, Errored) and record.recorded_at < cutoff
        ]
        for name in stale:
            self._records.pop(name, None)


class UploadProcessor:
    """Debounced batch processor for uploaded replay files."""

    def __init__(
        self,
        db_path: str | Path,
        tracked_players: dict[PlayerIdentity, str],
        replay_dir: str | Path,
        delay: float = 2.0,
        error_retention: float = 1800.0,
    ):
        self.db_path = db_path
        self.tracked_players = tracked_players
        self.replay_dir = Path(replay_dir)
        self.delay = delay
        self.error_retention = error_retention
        self._queue: list[Path] = []
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None
        # Progress state for the server's /api/upload/status endpoint. Resolved
        # (processed or skipped) files are removed once flush ends; the .ingested
        # sentinel is the durable record from then on. Errored records are kept
        # for reporting, but expire after error_retention so abandoned uploads
        # don't accumulate.
        self._files = _UploadFiles()

    def enqueue(self, path: Path):
        with self._lock:
            self._files.prune_stale(time.monotonic() - self.error_retention)
            self._queue.append(path)
            self._files.mark_queued(path.name)
            if self._timer is not None:
                self._timer.cancel()
            self._timer = threading.Timer(self.delay, self.flush)
            self._timer.daemon = True
            self._timer.start()

    def file_status(self, name: str) -> str | None:
        with self._lock:
            record = self._files.get(name)
        if record is None:
            return None
        if isinstance(record, Queued):
            return "queued"
        if isinstance(record, Processing):
            return "processing"
        if isinstance(record, Parsed):
            return "parsed"
        return f"error:{record.message}"

    def batch_progress(self, name: str) -> tuple[int, int] | None:
        with self._lock:
            record = self._files.get(name)
            batch = record.batch if isinstance(record, Processing | Parsed) else None
            return None if batch is None else (batch.completed, batch.total)

    def status(self, name: str) -> UploadStatus:
        """Reconcile the .ingested sentinel, this processor's pipeline state,
        and file existence into one answer.

        Precedence: the sentinel is the durable record and wins over any
        stale in-memory state; a recorded error is next; then pipeline
        stage/batch; then a missing file is an error; otherwise pending.
        """
        replay_path = self.replay_dir / name
        ingested_path = replay_path.with_suffix(replay_path.suffix + ".ingested")
        if ingested_path.exists():
            return UploadStatus(UploadState.PROCESSED)

        stage = self.file_status(name)
        if stage is not None and stage.startswith("error:"):
            return UploadStatus(UploadState.ERROR, error=stage[len("error:") :])
        if stage is not None:
            return UploadStatus(
                UploadState.PENDING, stage=stage, batch=self.batch_progress(name)
            )
        if not replay_path.exists():
            return UploadStatus(UploadState.ERROR)
        return UploadStatus(UploadState.PENDING)

    def _on_parsed(self, path: Path, batch: _BatchProgress) -> None:
        with self._lock:
            self._files.mark_parsed(path.name, batch)
            batch.completed += 1

    def flush(self) -> None:
        with self._lock:
            files = list(self._queue)
            self._queue.clear()
            self._timer = None
            batch = _BatchProgress(total=len(files)) if files else None
            if files and batch is not None:
                for path in files:
                    self._files.mark_processing(path.name, batch)
        if not files or batch is None:
            return
        logger.info("Processing %d uploaded replay(s)", len(files))
        results = _parallel_parse(
            files, self.tracked_players, on_parsed=lambda p: self._on_parsed(p, batch)
        )
        conn = _open_write_conn(self.db_path)
        try:
            outcomes = _write_parsed_batch(conn, self.tracked_players, results)
        finally:
            conn.close()
        with self._lock:
            for name, outcome in outcomes.items():
                if outcome.startswith("error:"):
                    self._files.record_error(name, outcome[len("error:") :])
                else:
                    self._files.resolve(name)


def process_unprocessed(
    db_path: Path,
    replay_dir: Path,
    tracked_players: dict[PlayerIdentity, str],
    *,
    force: bool = False,
):
    """Parse and ingest .replay files.

    By default only processes files without an .ingested sentinel.
    With force=True, reprocesses all .replay files.
    """
    if force:
        replay_paths = sorted(replay_dir.glob("*.replay"))
    else:
        already_ingested = {p.stem for p in replay_dir.glob("*.replay.ingested")}
        replay_paths = sorted(
            p for p in replay_dir.glob("*.replay") if p.name not in already_ingested
        )
    if not replay_paths:
        return

    logger.info("Processing %d replay(s)...", len(replay_paths))

    results = _parallel_parse(replay_paths, tracked_players)

    conn = _open_write_conn(db_path)
    try:
        _write_parsed_batch(conn, tracked_players, results)
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse

    from db import apply_migrations

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(
        description="Process .replay files into the database"
    )
    parser.add_argument(
        "--force", action="store_true", help="Reprocess all replays, not just new ones"
    )
    args = parser.parse_args()

    db_path = Path("db/rl_stats.sqlite")
    replay_dir = Path("replays")

    db_path.parent.mkdir(exist_ok=True)
    conn = _open_write_conn(db_path)
    apply_migrations(conn)
    conn.close()

    tracked_players = load_tracked_players()
    process_unprocessed(db_path, replay_dir, tracked_players, force=args.force)
