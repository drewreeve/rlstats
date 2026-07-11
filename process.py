import functools
import logging
import os
import sqlite3
import subprocess
import threading
from concurrent.futures import ProcessPoolExecutor
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


def process_batch(
    files: list[Path],
    conn: sqlite3.Connection,
    tracked_players: dict[PlayerIdentity, str],
) -> None:
    """Process a list of replay files in a single DB transaction."""
    with _batch_lock:
        resolved = [
            replay_path
            for replay_path in files
            if process_replay(replay_path, conn, tracked_players)[0]
        ]
        _finalize_batch(conn, tracked_players, resolved)


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
        logger.debug(
            "Skipping %s: no tracked players or missing metadata", replay_path.name
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
        with _batch_lock:
            resolved: list[Path] = []
            for path, analysis in zip(replay_paths, results, strict=True):
                if analysis is not None:
                    if _try_write_match(conn, path, analysis) is None:
                        resolved.append(path)
                elif path.exists():
                    resolved.append(path)
            _finalize_batch(conn, tracked_players, resolved)
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
