"""Invoke the ``rrrocket`` binary and hand back a ``ParsedReplay``.

The one place a ``.replay`` file becomes structured data. The ingest path wraps
this in :func:`process.parse_replay`, which deletes a corrupt file; the
replay-viewer path deliberately does not (ADR-0004 §13). See :func:`run_rrrocket`.
"""

import logging
import subprocess
from pathlib import Path
from typing import cast

import orjson

from rrrocket_schema import ParsedReplay, ReplayJSON
from rrrocket_schema import parse as _parse_rrrocket

logger = logging.getLogger(__name__)


def run_rrrocket(replay_path: Path) -> tuple[ParsedReplay | None, str | None]:
    """Run ``rrrocket -n`` on a .replay file and parse its output.

    Returns ``(parsed, None)`` on success, ``(None, error_message)`` on a
    subprocess/timeout/exit failure. Never touches the file — callers that want
    a corrupt replay removed do that themselves (see :func:`process.parse_replay`).
    """
    try:
        result = subprocess.run(
            ["rrrocket", "-n", str(replay_path)],
            capture_output=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        logger.warning("rrrocket failed for %s: %s", replay_path.name, exc)
        return None, f"rrrocket failed: {exc}"

    if result.returncode != 0:
        stderr = result.stderr.decode(errors="replace").strip()
        logger.warning(
            "rrrocket failed for %s (exit %d): %s",
            replay_path.name,
            result.returncode,
            stderr,
        )
        return None, f"rrrocket failed (exit {result.returncode}): {stderr}"

    return _parse_rrrocket(cast(ReplayJSON, orjson.loads(result.stdout))), None
