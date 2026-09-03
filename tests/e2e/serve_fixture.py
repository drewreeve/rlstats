"""A throwaway web server for the Playwright suite.

Builds a temp SQLite DB from the single committed replay
(``tests/data/BEC7EF8411F170E7DBCA41B0676B6A04.replay``), then serves the real
FastAPI app against it. The ingested replay is deterministically **match 1**, so
the e2e specs can hard-code ``/match/1/replay`` without plumbing an id across the
language boundary.

Usage: ``python tests/e2e/serve_fixture.py --port 8787`` (Playwright's
``webServer`` starts this and waits for the port).
"""

import argparse
import sqlite3
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

import uvicorn  # noqa: E402

from config import load_settings  # noqa: E402
from db import apply_migrations  # noqa: E402
from process import process_unprocessed  # noqa: E402
from server import create_app  # noqa: E402

TEST_DATA_DIR = REPO_ROOT / "tests" / "data"
REPLAY_NAME = "BEC7EF8411F170E7DBCA41B0676B6A04.replay"


def build_app():
    settings = load_settings(TEST_DATA_DIR)
    workdir = Path(tempfile.mkdtemp(prefix="rlstats-e2e-"))

    replay_dir = workdir / "replays"
    replay_dir.mkdir()
    (replay_dir / REPLAY_NAME).write_bytes((TEST_DATA_DIR / REPLAY_NAME).read_bytes())

    db_path = workdir / "e2e.sqlite"
    conn = sqlite3.connect(db_path)
    apply_migrations(conn)
    conn.close()

    process_unprocessed(db_path, replay_dir, settings.players)
    return create_app(db_path, replay_dir=replay_dir, settings=settings)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()
    uvicorn.run(build_app(), host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
