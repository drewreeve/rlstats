import sqlite3
from pathlib import Path

import aiosql

MIGRATIONS_DIR = Path(__file__).parent / "migrations"
SQL_DIR = Path(__file__).parent / "sql"

queries = aiosql.from_path(SQL_DIR, "sqlite3")


def apply_migrations(conn: sqlite3.Connection):
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        try:
            conn.executescript(path.read_text())
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e):
                raise
