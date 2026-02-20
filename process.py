import json
import logging
import sqlite3
import subprocess
import threading
from pathlib import Path

from ingest import ingest_match

logger = logging.getLogger(__name__)

_batch_lock = threading.Lock()


def process_replay(replay_path: Path, conn) -> tuple[bool, str | None]:
    """Run rrrocket on a .replay file, then ingest the resulting JSON.

    Returns (True, None) on success. On failure, removes corrupt files and
    returns (False, error_message).
    """
    json_path = replay_path.with_suffix(replay_path.suffix + ".json")

    # Run rrrocket to convert .replay -> .json
    try:
        result = subprocess.run(
            ["rrrocket", str(replay_path)],
            capture_output=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        msg = f"rrrocket failed: {exc}"
        logger.warning("rrrocket failed for %s: %s", replay_path.name, exc)
        replay_path.unlink(missing_ok=True)
        return False, msg

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
        return False, msg

    # Write rrrocket stdout to JSON file and ingest
    try:
        json_path.write_bytes(result.stdout)
        replay = json.loads(result.stdout)
        ingest_match(conn, replay)
    except Exception as exc:
        msg = f"Ingest failed: {exc}"
        logger.warning("Ingest failed for %s: %s", replay_path.name, exc)
        replay_path.unlink(missing_ok=True)
        json_path.unlink(missing_ok=True)
        return False, msg

    return True, None


def process_batch(files: list[Path], conn) -> dict[str, tuple[bool, str | None]]:
    """Process a list of replay files in a single DB transaction.

    Returns a dict mapping filename to (success, error_message) for each file.
    """
    results = {}
    with _batch_lock:
        for replay_path in files:
            results[replay_path.name] = process_replay(replay_path, conn)
        conn.commit()
    return results


class UploadProcessor:
    """Debounced batch processor for uploaded replay files."""

    def __init__(self, db_path, delay=2.0):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
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
            process_batch(files, self.conn)
