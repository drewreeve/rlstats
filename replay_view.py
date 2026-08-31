"""Glue between the replay-viewer routes and ``replay_frames``.

Resolves a match to its stored ``.replay`` file and turns that file into a
``ReplayFrames`` — without the deleting ingest path (:func:`process.parse_replay`)
and without re-running ``analyze_frames``. See ``docs/replay-viewer.md``.
"""

import logging
import sqlite3
from pathlib import Path

import queries
from ingest import build_player_stats, detect_game_mode, resolve_perspective
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

    props = replay.properties
    player_stats = build_player_stats(props)
    perspective = resolve_perspective(
        player_stats,
        tracked_players,
        props.get("Team0Score", 0),
        props.get("Team1Score", 0),
        props.get("WinningTeam"),
    )
    player_names: dict[PlayerIdentity, str] = {
        identity: tracked_players.get(identity) or entry.get("Name", "Unknown")
        for identity, entry in player_stats.items()
    }
    frames = extract_replay_frames(
        replay,
        tracked_team=perspective.team,
        tracked_identities=set(tracked_players),
        player_names=player_names,
        game_mode=detect_game_mode(props.get("TeamSize"), props.get("MapName")),
    )
    return frames if frames.frame_times else None
