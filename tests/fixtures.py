import functools
import json
import sqlite3
import struct
from pathlib import Path
from typing import Any, cast

from config import load_settings
from db import apply_migrations
from ingest import Analyzed, analyze_replay, build_replay_context, write_match
from replay_frames import ReplayFrames, extract_replay_frames
from replay_view import ENVELOPE_HEADER_FORMAT, ENVELOPE_HEADER_SIZE
from rrrocket_schema import ReplayJSON
from rrrocket_schema import parse as parse_replay

TEST_DATA_DIR = Path(__file__).parent / "data"

TRACKED_PLAYERS = load_settings(TEST_DATA_DIR).players


@functools.cache
def load_replay(name: str) -> ReplayJSON:
    path = TEST_DATA_DIR / name
    with open(path, "r", encoding="utf-8") as f:
        return cast(ReplayJSON, json.load(f))


@functools.cache
def replay_frames_of(name: str) -> ReplayFrames:
    """``extract_replay_frames`` over a fixture replay, wired through the real
    Replay Context — the same four kwargs ``replay_view.build_replay_frames``
    passes. Cached; callers must treat the result as read-only.
    """
    replay = parse_replay(load_replay(name))
    context = build_replay_context(replay, TRACKED_PLAYERS)
    return extract_replay_frames(
        replay,
        tracked_team=context.perspective.team,
        tracked_identities=context.tracked_identities,
        player_names=context.player_names,
        game_mode=context.game_mode,
    )


def unpack_replay_envelope(content: bytes) -> tuple[bytes, bytes, Any]:
    """Split a ``replay_view.serialize_replay_envelope`` response back into
    ``(positions, boost, meta)`` — the one place tests decode the header
    rather than each hand-rolling the offset arithmetic."""
    positions_len, boost_len, meta_len = struct.unpack(
        ENVELOPE_HEADER_FORMAT, content[:ENVELOPE_HEADER_SIZE]
    )
    positions_start = ENVELOPE_HEADER_SIZE
    boost_start = positions_start + positions_len
    meta_start = boost_start + boost_len
    meta_end = meta_start + meta_len
    assert meta_end == len(content), (
        f"envelope header claims {meta_end} bytes, got {len(content)}"
    )
    positions = content[positions_start:boost_start]
    boost = content[boost_start:meta_start]
    meta = json.loads(content[meta_start:meta_end])
    return positions, boost, meta


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
        result = analyze_replay(
            parse_replay(load_replay(name)), TRACKED_PLAYERS, source_filename=name
        )
        assert isinstance(result, Analyzed)
        write_match(conn, result.analysis)
    conn.commit()
    return conn


def cached_db(*replay_names: str) -> sqlite3.Connection:
    """Return a fresh copy of a cached ingested DB."""
    source = _cached_ingested_db(tuple(replay_names))
    conn = sqlite3.connect(":memory:")
    source.backup(conn)
    return conn


def row_db(*replay_names: str) -> sqlite3.Connection:
    """cached_db() with a sqlite3.Row factory, which queries.py needs — its
    _rows/_one do dict(row)."""
    conn = cached_db(*replay_names)
    conn.row_factory = sqlite3.Row
    return conn


def empty_row_db() -> sqlite3.Connection:
    """Migrations-only DB with sqlite3.Row, for queries.py's empty-result paths."""
    conn = in_memory_db()
    conn.row_factory = sqlite3.Row
    return conn


def file_db(tmp_path: Path) -> Path:
    """Create a migrated file-based SQLite DB and return its path."""
    db_path = tmp_path / "test.sqlite"
    conn = sqlite3.connect(db_path)
    apply_migrations(conn)
    conn.close()
    return db_path
