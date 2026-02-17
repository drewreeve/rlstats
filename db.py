import sqlite3
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def apply_migrations(conn: sqlite3.Connection):
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        try:
            conn.executescript(path.read_text())
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e):
                raise
