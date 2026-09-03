"""A throwaway web server for the Playwright suite.

Builds a temp SQLite DB from the committed fixture replays, then serves the real
FastAPI app against it. They are ingested one at a time in a fixed order, so the
ids are deterministic and the e2e specs can hard-code them without plumbing an
id across the language boundary:

* **match 1** — ``BEC7EF8411F170E7DBCA41B0676B6A04.replay`` (standard soccar)
* **match 2** — ``D9C3347845961812E9817293F9886DDB.replay`` (2v2 hoops)

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
# In ingest order — index 0 is match 1, index 1 is match 2, …
FIXTURE_REPLAYS = (
    "BEC7EF8411F170E7DBCA41B0676B6A04.replay",  # standard soccar
    "D9C3347845961812E9817293F9886DDB.replay",  # 2v2 hoops
)


def build_app():
    settings = load_settings(TEST_DATA_DIR)
    workdir = Path(tempfile.mkdtemp(prefix="rlstats-e2e-"))

    replay_dir = workdir / "replays"
    replay_dir.mkdir()

    db_path = workdir / "e2e.sqlite"
    conn = sqlite3.connect(db_path)
    apply_migrations(conn)
    conn.close()

    # One at a time, in order: process_unprocessed skips files that already have
    # an .ingested sentinel, so each call ingests exactly the new replay and the
    # match ids come out in FIXTURE_REPLAYS order.
    for name in FIXTURE_REPLAYS:
        (replay_dir / name).write_bytes((TEST_DATA_DIR / name).read_bytes())
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
