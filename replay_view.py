"""Glue between the replay-viewer routes and ``replay_frames``.

Resolves a match to its stored ``.replay`` file and turns that file into a
``ReplayFrames`` — without the deleting ingest path (:func:`process.parse_replay`)
and without re-running ``analyze_frames``. See ``docs/adr/0004-browser-replay-viewer-design.md``.
"""

import json
import logging
import sqlite3
import struct
from pathlib import Path
from typing import Any, cast

from fastapi.encoders import jsonable_encoder

import queries
from ingest import build_replay_context
from player_identity import PlayerIdentity
from replay_frames import ReplayFrames, extract_replay_frames
from rrrocket import run_rrrocket

logger = logging.getLogger(__name__)


def replay_path_for(
    conn: sqlite3.Connection, replay_dir: Path, match_id: int
) -> Path | None:
    """The stored ``.replay`` file for a match, or ``None`` if the match is
    unknown, has no recorded filename, or the file is not on disk."""
    filename = queries.match_replay_filename(conn, match_id)
    if not filename:
        return None
    path = replay_dir / filename
    return path if path.is_file() else None


def build_replay_frames(
    replay_path: Path, tracked_players: dict[PlayerIdentity, str]
) -> ReplayFrames | None:
    """Parse a stored ``.replay`` and reshape it for playback.

    Returns ``None`` when rrrocket fails or the replay carries no usable network
    data. Never deletes the file.
    """
    replay, error = run_rrrocket(replay_path)
    if replay is None:
        logger.warning("replay viewer: %s (%s)", error, replay_path.name)
        return None

    context = build_replay_context(replay, tracked_players)
    frames = extract_replay_frames(
        replay,
        tracked_team=context.perspective.team,
        tracked_identities=context.tracked_identities,
        player_names=context.player_names,
        game_mode=context.game_mode,
    )
    return frames if frames.frame_times else None


#: Three little-endian uint32 lengths — positions, boost, meta — fixed at the
#: front of every envelope. Exported so tests can decode without re-typing the
#: format string or hardcoding its size.
ENVELOPE_HEADER_FORMAT = "<III"
ENVELOPE_HEADER_SIZE = struct.calcsize(ENVELOPE_HEADER_FORMAT)


def encode_replay_meta(frames: ReplayFrames) -> dict[str, Any]:
    """The exact jsonable dict served in the envelope's meta slice — the one
    place ``frames.meta_dict()`` gets its ``jsonable_encoder`` pass, so the
    route, ``tests/e2e/dump_fixture.py`` and ``test_replay_wire.py`` all agree
    on what "serialized like the route" means."""
    return cast(dict[str, Any], jsonable_encoder(frames.meta_dict()))


def encode_replay_meta_bytes(frames: ReplayFrames) -> bytes:
    """``encode_replay_meta`` as the exact UTF-8 bytes the envelope's meta
    slice carries — the one place the JSON-encoding flags are chosen, so the
    route and ``tests/e2e/dump_fixture.py`` (which writes the same bytes to
    ``tests/data/replay-viewer/meta.json``) can't drift apart on them.
    ``allow_nan=False`` matches the strictness of the ``JSONResponse`` this
    route used to return — a NaN now fails loudly server-side instead of
    shipping as the non-standard ``NaN`` token for the browser's
    ``JSON.parse`` to choke on.
    """
    return json.dumps(
        encode_replay_meta(frames),
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def serialize_replay_envelope(frames: ReplayFrames) -> bytes:
    """Pack a ``ReplayFrames`` into the ``/api/matches/{id}/replay`` wire
    envelope: a fixed-size header of three little-endian ``uint32`` lengths
    (positions, boost, meta — see ``ENVELOPE_HEADER_FORMAT``), then the
    positions buffer, the boost buffer, then the meta JSON bytes — in that
    order so the positions buffer (which a browser wants as a 4-byte-aligned
    ``Float32Array`` view) sits right after the fixed-size header, not after
    the meta JSON's unpredictable byte length. See
    docs/adr/0004-browser-replay-viewer-design.md's addendum.
    """
    meta_bytes = encode_replay_meta_bytes(frames)
    header = struct.pack(
        ENVELOPE_HEADER_FORMAT,
        len(frames.positions),
        len(frames.boost),
        len(meta_bytes),
    )
    return b"".join((header, frames.positions, frames.boost, meta_bytes))
