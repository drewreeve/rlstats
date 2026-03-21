import logging
import sqlite3
import subprocess

import orjson
import threading
from pathlib import Path

from concurrent.futures import ProcessPoolExecutor

from ingest import analyze_replay, ingest_match, write_match

logger = logging.getLogger(__name__)

_batch_lock = threading.Lock()


def _open_write_conn(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def parse_replay(replay_path: Path) -> tuple[dict | None, str | None]:
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

    return orjson.loads(result.stdout), None


def process_replay(replay_path: Path, conn: sqlite3.Connection) -> tuple[bool, str | None]:
    """Run rrrocket on a .replay file, then ingest the parsed data.

    Returns (True, None) on success. On failure, removes corrupt files and
    returns (False, error_message).
    """
    replay, error = parse_replay(replay_path)
    if replay is None:
        return False, error

    try:
        ingest_match(conn, replay)
    except Exception as exc:
        msg = f"Ingest failed: {exc}"
        logger.warning("Ingest failed for %s: %s", replay_path.name, exc)
        replay_path.unlink(missing_ok=True)
        return False, msg

    return True, None


def process_batch(files: list[Path], conn: sqlite3.Connection) -> dict[str, tuple[bool, str | None]]:
    """Process a list of replay files in a single DB transaction.

    Returns a dict mapping filename to (success, error_message) for each file.
    """
    results = {}
    with _batch_lock:
        for replay_path in files:
            results[replay_path.name] = process_replay(replay_path, conn)
        conn.commit()
        for replay_path in files:
            if results[replay_path.name][0]:
                replay_path.with_suffix(replay_path.suffix + ".ingested").touch()
    return results


class UploadProcessor:
    """Debounced batch processor for uploaded replay files."""

    def __init__(self, db_path, delay=2.0):
        self.db_path = db_path
        self.delay = delay
        self._queue: list[Path] = []
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None

    def enqueue(self, path: Path):
        with self._lock:
            self._queue.append(path)
            if self._timer is not None:
                self._timer.cancel()
            self._timer = threading.Timer(self.delay, self._flush)
            self._timer.daemon = True
            self._timer.start()

    def _flush(self):
        with self._lock:
            files = list(self._queue)
            self._queue.clear()
            self._timer = None
        if files:
            logger.info("Processing %d uploaded replay(s)", len(files))
            conn = _open_write_conn(self.db_path)
            try:
                process_batch(files, conn)
            finally:
                conn.close()


def _parse_and_analyze(replay_path):
    """Worker for parallel processing: parse + analyze a replay without DB access."""
    replay, error = parse_replay(replay_path)
    if replay is None:
        return None
    return analyze_replay(replay)


def process_unprocessed(db_path: Path, replay_dir: Path):
    """Parse and ingest any .replay files that haven't been processed yet."""
    unprocessed = [
        p
        for p in replay_dir.glob("*.replay")
        if not p.with_suffix(p.suffix + ".ingested").exists()
    ]
    if not unprocessed:
        return

    replay_paths = sorted(unprocessed)
    logger.info("Processing %d unprocessed replay(s) at startup...", len(replay_paths))

    with ProcessPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(_parse_and_analyze, replay_paths))

    conn = _open_write_conn(db_path)
    try:
        ingested = []
        for path, analysis in zip(replay_paths, results):
            if analysis is not None:
                write_match(conn, analysis)
                ingested.append(path)
        conn.commit()
        for replay_path in ingested:
            replay_path.with_suffix(replay_path.suffix + ".ingested").touch()
    finally:
        conn.close()
