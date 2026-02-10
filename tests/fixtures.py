import json
import sqlite3
from pathlib import Path

from ingest import ensure_schema, ensure_analytics_views

TEST_DATA_DIR = Path(__file__).parent / "data"


def load_replay(name: str):
    path = TEST_DATA_DIR / name
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def in_memory_db():
    conn = sqlite3.connect(":memory:")
    ensure_schema(conn)
    ensure_analytics_views(conn)
    return conn
