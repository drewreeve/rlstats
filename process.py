import functools
import logging
import os
import sqlite3
import subprocess
import threading
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

import orjson

from config import load_tracked_players
from ingest import (
    ReplayAnalysis,
    analyze_replay,
    ingest_match,
    sync_tracked_players,
    write_match,
)
from player_identity import PlayerIdentity

logger = logging.getLogger(__name__)

_batch_lock = threading.Lock()


def _open_write_conn(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def parse_replay(replay_path: Path) -> tuple[dict[str, Any] | None, str | None]:
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


def process_replay(
    replay_path: Path,
    conn: sqlite3.Connection,
    tracked_players: dict[PlayerIdentity, str],
) -> tuple[bool, str | None]:
    """Run rrrocket on a .replay file, then ingest the parsed data.

    Returns (True, None) on success. On failure, removes corrupt files and
    returns (False, error_message).
    """
    replay, error = parse_replay(replay_path)
    if replay is None:
        return False, error

    try:
        ingest_match(conn, replay, tracked_players)
    except Exception as exc:
        msg = f"Ingest failed: {exc}"
        logger.warning("Ingest failed for %s: %s", replay_path.name, exc)
        replay_path.unlink(missing_ok=True)
        return False, msg

    return True, None


def process_batch(
    files: list[Path],
    conn: sqlite3.Connection,
    tracked_players: dict[PlayerIdentity, str],
) -> dict[str, tuple[bool, str | None]]:
    """Process a list of replay files in a single DB transaction.

    Returns a dict mapping filename to (success, error_message) for each file.
    """
    results: dict[str, tuple[bool, str | None]] = {}
    with _batch_lock:
        for replay_path in files:
            results[replay_path.name] = process_replay(
                replay_path, conn, tracked_players
            )
        conn.commit()
        for replay_path in files:
            if results[replay_path.name][0]:
                replay_path.with_suffix(replay_path.suffix + ".ingested").touch()
    return results


class UploadProcessor:
    """Debounced batch processor for uploaded replay files."""

    def __init__(
        self,
        db_path: str | Path,
        tracked_players: dict[PlayerIdentity, str],
        delay: float = 2.0,
    ):
        self.db_path = db_path
        self.tracked_players = tracked_players
        self.delay = delay
        self._queue: list[Path] = []
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None

    def enqueue(self, path: Path):
        with self._lock:
            self._queue.append(path)
            if self._timer is not None:
                self._timer.cancel()
            self._timer = threading.Timer(self.delay, self.flush)
            self._timer.daemon = True
            self._timer.start()

    def flush(self) -> None:
        with self._lock:
            files = list(self._queue)
            self._queue.clear()
            self._timer = None
        if files:
            logger.info("Processing %d uploaded replay(s)", len(files))
            conn = _open_write_conn(self.db_path)
            try:
                process_batch(files, conn, self.tracked_players)
            finally:
                conn.close()


def _parse_and_analyze(
    replay_path: Path, tracked_players: dict[PlayerIdentity, str]
) -> ReplayAnalysis | None:
    """Worker for parallel processing: parse + analyze a replay without DB access."""
    replay, _ = parse_replay(replay_path)
    if replay is None:
        return None
    analysis = analyze_replay(replay, tracked_players)
    if analysis is None:
        logger.warning(
            "Skipping %s: replay could not be analyzed",
            replay_path.name,
        )
    return analysis


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

    workers = max(1, (os.cpu_count() or 2) // 2)
    worker = functools.partial(_parse_and_analyze, tracked_players=tracked_players)
    with ProcessPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(worker, replay_paths))

    conn = _open_write_conn(db_path)
    try:
        sync_tracked_players(conn, tracked_players)
        ingested: list[Path] = []
        for path, analysis in zip(replay_paths, results, strict=True):
            if analysis is not None:
                write_match(conn, analysis)
                ingested.append(path)
        conn.commit()
        for replay_path in ingested:
            replay_path.with_suffix(replay_path.suffix + ".ingested").touch()
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
