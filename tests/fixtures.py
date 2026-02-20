import json
import sqlite3
from pathlib import Path

from db import apply_migrations

TEST_DATA_DIR = Path(__file__).parent / "data"


def load_replay(name: str):
    path = TEST_DATA_DIR / name
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def in_memory_db():
    conn = sqlite3.connect(":memory:")
    apply_migrations(conn)
    return conn


def file_db(tmp_path: Path) -> Path:
    """Create a migrated file-based SQLite DB and return its path."""
    db_path = tmp_path / "test.sqlite"
    conn = sqlite3.connect(db_path)
    apply_migrations(conn)
    conn.close()
    return db_path
