import sqlite3
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, overload

import aiosql

MIGRATIONS_DIR = Path(__file__).parent / "migrations"
SQL_DIR = Path(__file__).parent / "sql"

sql: Any = aiosql.from_path(SQL_DIR, "sqlite3")  # pyright: ignore[reportUnknownMemberType]


@overload
def upsert(
    conn: sqlite3.Connection,
    table: str,
    conflict_columns: Sequence[str],
    row: Mapping[str, Any],
    returning: str,
) -> Any: ...
@overload
def upsert(
    conn: sqlite3.Connection,
    table: str,
    conflict_columns: Sequence[str],
    row: Mapping[str, Any],
    returning: None = None,
) -> None: ...
def upsert(
    conn: sqlite3.Connection,
    table: str,
    conflict_columns: Sequence[str],
    row: Mapping[str, Any],
    returning: str | None = None,
) -> Any | None:
    """Insert row into table, updating all non-conflict columns on conflict.

    Column names come from row.keys() and conflict_columns, both always
    caller-controlled literals — never user input — so f-string SQL is safe here.
    """
    columns = list(row)
    update_columns = [c for c in columns if c not in conflict_columns]
    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) "
        f"VALUES ({', '.join('?' for _ in columns)}) "
        f"ON CONFLICT({', '.join(conflict_columns)}) DO UPDATE SET "
        + ", ".join(f"{c} = excluded.{c}" for c in update_columns)
    )
    if returning:
        sql += f" RETURNING {returning}"
    cur = conn.execute(sql, list(row.values()))
    return cur.fetchone()[0] if returning else None


def apply_migrations(conn: sqlite3.Connection):
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER NOT NULL)"
    )

    row = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
    current_version = row[0] or 0

    # Bootstrap: existing DB with no version tracking yet
    if current_version == 0:
        columns = [r[1] for r in conn.execute("PRAGMA table_info(players)").fetchall()]
        if "platform" in columns:
            current_version = 6
            conn.execute(
                "INSERT INTO schema_migrations (version) VALUES (?)", (current_version,)
            )
            conn.commit()

    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        migration_num = int(path.name.split("_", 1)[0])
        if migration_num <= current_version:
            continue
        conn.executescript(path.read_text())
        conn.execute(
            "INSERT INTO schema_migrations (version) VALUES (?)", (migration_num,)
        )
        conn.commit()
