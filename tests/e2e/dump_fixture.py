"""Regenerate the Node-test input fixture: tests/data/replay-viewer/{meta.json,
frames.bin,boost.bin}.

These are the server's replay-frames output for the one committed replay, used
by tests/js/replay-core.test.js as *input* to the property tests (not as a
golden output oracle). The JSON is built exactly as the ``/api/matches/{id}/
replay`` route builds it, so the fixture matches what the browser parses.

Rerun when replay_frames.py changes the meta shape or the packed-buffer format,
then eyeball ``mise run test-js``:

    uv run python tests/e2e/dump_fixture.py
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from config import load_settings  # noqa: E402
from replay_view import build_replay_frames, encode_replay_meta_bytes  # noqa: E402

DATA_DIR = REPO_ROOT / "tests" / "data"
OUT_DIR = DATA_DIR / "replay-viewer"
REPLAY_NAME = "BEC7EF8411F170E7DBCA41B0676B6A04.replay"


def main() -> None:
    settings = load_settings(DATA_DIR)
    frames = build_replay_frames(DATA_DIR / REPLAY_NAME, settings.players)
    if frames is None:
        raise SystemExit("build_replay_frames returned None (rrrocket on PATH?)")

    # The exact bytes the merged /api/matches/{id}/replay route's meta slice
    # carries.
    meta_bytes = encode_replay_meta_bytes(frames)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "meta.json").write_bytes(meta_bytes)
    (OUT_DIR / "frames.bin").write_bytes(frames.positions)
    (OUT_DIR / "boost.bin").write_bytes(frames.boost)
    print(
        f"wrote meta.json ({len(frames.slots)} slots, "
        f"{len(frames.frame_times)} frames) + frames.bin "
        f"({len(frames.positions)} bytes) + boost.bin "
        f"({len(frames.boost)} bytes) -> {OUT_DIR}"
    )


if __name__ == "__main__":
    main()
