import functools
import logging
import os
import sqlite3
import subprocess
import threading
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import cast

import orjson

from config import load_tracked_players
from file_outcome import Failed, FileOutcome, Skipped, Written, encode, sentinel_path
from ingest import (
    AnalysisResult,
    Analyzed,
    analyze_replay,
    sync_tracked_players,
    write_match,
)
from player_identity import PlayerIdentity
from rrrocket_schema import ParsedReplay, ReplayJSON
from rrrocket_schema import parse as _parse_rrrocket

logger = logging.getLogger(__name__)

_batch_lock = threading.Lock()

# What flows out of the parse+analyze step, before the DB write decides the
# terminal FileOutcome. Analyzed becomes Written once its match row commits.
ParseOutcome = AnalysisResult | Failed


def open_write_conn(db_path: str | Path) -> sqlite3.Connection:
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


def _finalize_batch(
    conn: sqlite3.Connection,
    tracked_players: dict[PlayerIdentity, str],
    resolved: list[tuple[Path, Written | Skipped]],
) -> None:
    """Sync tracked-player status, commit, and mark resolved files as ingested.

    Caller must hold _batch_lock.
    """
    sync_tracked_players(conn, tracked_players)
    conn.commit()
    for replay_path, outcome in resolved:
        sentinel_path(replay_path).write_text(encode(outcome))


def _parse_and_analyze(
    replay_path: Path, tracked_players: dict[PlayerIdentity, str]
) -> ParseOutcome:
    """Worker for parallel processing: parse + analyze a replay without DB access."""
    replay, error = parse_replay(replay_path)
    if replay is None:
        return Failed(error or "parse failed")
    return analyze_replay(replay, tracked_players)


def parallel_parse(
    paths: list[Path],
    tracked_players: dict[PlayerIdentity, str],
    on_parsed: Callable[[Path], None] | None = None,
) -> dict[Path, ParseOutcome]:
    """Parse+analyze a batch of replays in a process pool.

    Returns path -> outcome (in the same order as paths): Analyzed on success,
    Skipped (untracked, missing metadata) or Failed (corrupt replay) otherwise.
    Calls on_parsed(path) as each file's parse completes, for progress
    reporting; that happens in completion order, but the returned dict is
    reordered back to match paths so write order stays deterministic
    regardless of which worker finishes first.
    """
    workers = max(1, (os.cpu_count() or 2) // 2)
    worker = functools.partial(_parse_and_analyze, tracked_players=tracked_players)
    completed: dict[Path, ParseOutcome] = {}
    with ProcessPoolExecutor(max_workers=workers) as pool:
        future_to_path = {pool.submit(worker, path): path for path in paths}
        for future in as_completed(future_to_path):
            path = future_to_path[future]
            completed[path] = future.result()
            if on_parsed is not None:
                on_parsed(path)
    return {path: completed[path] for path in paths}


def write_parsed_batch(
    conn: sqlite3.Connection,
    tracked_players: dict[PlayerIdentity, str],
    results: dict[Path, ParseOutcome],
) -> dict[str, FileOutcome]:
    """Write parsed results to conn, finalize the batch, and report per-file outcomes.

    Maps each Analyzed to a terminal Written once its match row commits.

    Acquires _batch_lock itself, so batch-commit semantics (one commit + all
    sentinels at the end, write_match's own savepoint isolating each file's
    write) are unchanged from before this was pulled out of process_batch.
    """
    outcomes: dict[str, FileOutcome] = {}
    with _batch_lock:
        resolved: list[tuple[Path, Written | Skipped]] = []
        for path, result in results.items():
            if isinstance(result, Analyzed):
                try:
                    write_match(conn, result.analysis)
                except Exception as exc:
                    logger.warning("Ingest failed for %s: %s", path.name, exc)
                    outcomes[path.name] = Failed(f"Ingest failed: {exc}")
                else:
                    written = Written()
                    resolved.append((path, written))
                    outcomes[path.name] = written
            elif isinstance(result, Skipped):
                resolved.append((path, result))
                outcomes[path.name] = result
            else:
                outcomes[path.name] = result
        _finalize_batch(conn, tracked_players, resolved)
    return outcomes


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

    results = parallel_parse(replay_paths, tracked_players)

    conn = open_write_conn(db_path)
    try:
        write_parsed_batch(conn, tracked_players, results)
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
    conn = open_write_conn(db_path)
    apply_migrations(conn)
    conn.close()

    tracked_players = load_tracked_players()
    process_unprocessed(db_path, replay_dir, tracked_players, force=args.force)
