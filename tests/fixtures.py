import functools
import json
import sqlite3
from pathlib import Path

from db import apply_migrations
from ingest import ingest_match

TEST_DATA_DIR = Path(__file__).parent / "data"


@functools.cache
def load_replay(name: str):
    path = TEST_DATA_DIR / name
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def in_memory_db():
    conn = sqlite3.connect(":memory:")
    apply_migrations(conn)
    return conn


@functools.cache
def _cached_ingested_db(replay_names: tuple[str, ...]) -> sqlite3.Connection:
    """Ingest replays once and cache the result."""
    conn = sqlite3.connect(":memory:")
    apply_migrations(conn)
    for name in replay_names:
        ingest_match(conn, load_replay(name))
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
