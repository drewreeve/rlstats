"""Glue between the replay-viewer routes and ``replay_frames``.

Resolves a match to its stored ``.replay`` file and turns that file into a
``ReplayFrames`` — without the deleting ingest path (:func:`process.parse_replay`)
and without re-running ``analyze_frames``. See ``docs/adr/0004-browser-replay-viewer-design.md``.
"""

import logging
import sqlite3
from pathlib import Path

import queries
from ingest import build_replay_context
from player_identity import PlayerIdentity
from process import run_rrrocket
from replay_frames import ReplayFrames, extract_replay_frames

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
