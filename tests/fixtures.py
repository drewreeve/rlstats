import functools
import json
import sqlite3
from pathlib import Path
from typing import cast

from config import load_settings
from db import apply_migrations
from ingest import analyze_replay, write_match
from rrrocket_schema import ReplayJSON
from rrrocket_schema import parse as parse_replay

TEST_DATA_DIR = Path(__file__).parent / "data"

TRACKED_PLAYERS = load_settings(TEST_DATA_DIR).players


@functools.cache
def load_replay(name: str) -> ReplayJSON:
    path = TEST_DATA_DIR / name
    with open(path, "r", encoding="utf-8") as f:
        return cast(ReplayJSON, json.load(f))


def in_memory_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    apply_migrations(conn)
    return conn


@functools.cache
def _cached_ingested_db(replay_names: tuple[str, ...]) -> sqlite3.Connection:
    """Ingest replays once and cache the result."""
    conn = sqlite3.connect(":memory:")
    apply_migrations(conn)
    for name in replay_names:
        analysis = analyze_replay(parse_replay(load_replay(name)), TRACKED_PLAYERS)
        assert analysis is not None
        write_match(conn, analysis)
    conn.commit()
    return conn


def cached_db(*replay_names: str) -> sqlite3.Connection:
    """Return a fresh copy of a cached ingested DB."""
    source = _cached_ingested_db(tuple(replay_names))
    conn = sqlite3.connect(":memory:")
    source.backup(conn)
    return conn


def file_db(tmp_path: Path) -> Path:
    """Create a migrated file-based SQLite DB and return its path."""
    db_path = tmp_path / "test.sqlite"
    conn = sqlite3.connect(db_path)
    apply_migrations(conn)
    conn.close()
    return db_path
