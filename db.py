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
    conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER NOT NULL)")

    row = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
    current_version = row[0] or 0

    # Bootstrap: existing DB with no version tracking yet
    if current_version == 0:
        columns = [r[1] for r in conn.execute("PRAGMA table_info(players)").fetchall()]
        if "platform" in columns:
            current_version = 6
            conn.execute("INSERT INTO schema_migrations (version) VALUES (?)", (current_version,))
            conn.commit()

    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        migration_num = int(path.name.split("_", 1)[0])
        if migration_num <= current_version:
            continue
        conn.executescript(path.read_text())
        conn.execute("INSERT INTO schema_migrations (version) VALUES (?)", (migration_num,))
        conn.commit()
