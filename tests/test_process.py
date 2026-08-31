import json
import sqlite3
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import patch

from file_outcome import Failed, Skipped, SkipReason, Written, decode, sentinel_path
from ingest import Analyzed, analyze_replay
from process import (
    _parse_and_analyze,  # pyright: ignore[reportPrivateUsage]
    parse_replay,
    process_unprocessed,
    run_rrrocket,
    write_parsed_batch,
)
from rrrocket_schema import parse as parse_rrrocket
from tests.fixtures import (
    TEST_DATA_DIR,
    TRACKED_PLAYERS,
    file_db,
    in_memory_db,
    load_replay,
)


def _make_conn() -> sqlite3.Connection:
    conn = in_memory_db()
    conn.row_factory = None  # use tuples for simplicity
    return conn


def test_parse_replay_success(tmp_path: Path):
    """parse_replay returns parsed dict on success."""
    replay_data = load_replay("match.json")
    replay_path = tmp_path / "test.replay"
    replay_path.write_bytes(b"\x00" * 1024)

    def fake_rrrocket(args: Any, **kwargs: Any):
        stdout = json.dumps(replay_data).encode()
        return subprocess.CompletedProcess(args, 0, stdout=stdout)

    with patch("process.subprocess.run", side_effect=fake_rrrocket):
        result, error = parse_replay(replay_path)

    assert result == parse_rrrocket(replay_data)
    assert error is None
    # No .json sidecar should be written
    assert not (tmp_path / "test.replay.json").exists()


def test_parse_replay_failure(tmp_path: Path):
    """parse_replay removes .replay on rrrocket failure."""
    replay_path = tmp_path / "corrupt.replay"
    replay_path.write_bytes(b"\x00" * 1024)

    failed = subprocess.CompletedProcess(["rrrocket"], 1, stderr=b"parse error")

    with patch("process.subprocess.run", return_value=failed):
        result, error = parse_replay(replay_path)

    assert result is None
    assert error is not None
    assert "rrrocket failed" in error
    assert not replay_path.exists()


def test_run_rrrocket_leaves_the_file_on_failure(tmp_path: Path):
    """Unlike parse_replay, run_rrrocket never deletes — the replay viewer
    depends on a failed re-parse not destroying an already-ingested replay."""
    replay_path = tmp_path / "keep.replay"
    replay_path.write_bytes(b"\x00" * 1024)

    failed = subprocess.CompletedProcess(["rrrocket"], 1, stderr=b"boom")
    with patch("process.subprocess.run", return_value=failed):
        result, error = run_rrrocket(replay_path)

    assert result is None
    assert error is not None and "rrrocket failed" in error
    assert replay_path.exists()


def test_write_parsed_batch_commits(tmp_path: Path):
    """write_parsed_batch processes multiple files and commits once."""
    conn = _make_conn()
    replay_data = load_replay("match.json")

    files: list[Path] = []
    results: dict[Path, Any] = {}
    for i in range(3):
        p = tmp_path / f"match{i}.replay"
        p.write_bytes(b"\x00" * 1024)
        files.append(p)
        # Give each match a unique GUID so they don't collide
        data = json.loads(json.dumps(replay_data))
        data["properties"]["MatchGUID"] = f"GUID-{i}"
        results[p] = analyze_replay(parse_rrrocket(data), TRACKED_PLAYERS)

    outcomes = write_parsed_batch(conn, TRACKED_PLAYERS, results)

    row = conn.execute("SELECT COUNT(*) FROM matches").fetchone()
    assert row[0] == 3
    # Markers are written only after commit
    for p in files:
        assert (p.with_suffix(p.suffix + ".ingested")).exists()
        assert isinstance(outcomes[p.name], Written)


def test_write_parsed_batch_marks_skipped_files_ingested(tmp_path: Path):
    """A Skipped outcome (no tracked players / missing metadata) is reported as
    such and still gets a sentinel, so it isn't retried forever."""
    conn = _make_conn()
    replay_path = tmp_path / "untracked.replay"
    replay_path.write_bytes(b"\x00" * 1024)
    skip = Skipped(SkipReason.NO_TRACKED_PLAYERS)

    outcomes = write_parsed_batch(conn, TRACKED_PLAYERS, {replay_path: skip})

    assert outcomes[replay_path.name] == skip
    sentinel = sentinel_path(replay_path)
    assert sentinel.exists()
    assert decode(sentinel.read_text()) == skip
    row = conn.execute("SELECT COUNT(*) FROM matches").fetchone()
    assert row[0] == 0


def test_write_parsed_batch_rolls_back_partial_write_on_failure(tmp_path: Path):
    """A failure partway through write_match must not leave a half-written match row.

    write_match wraps its own savepoint (see _try_write_match), so a mid-write
    failure here must not get committed alongside the rest of the batch once
    _finalize_batch commits.
    """
    conn = _make_conn()
    replay_data = load_replay("match.json")
    replay_path = tmp_path / "bad.replay"
    replay_path.write_bytes(b"\x00" * 1024)
    analysis = analyze_replay(parse_rrrocket(replay_data), TRACKED_PLAYERS)

    with patch(
        "ingest._insert_match_players",
        side_effect=RuntimeError("simulated failure after match row inserted"),
    ):
        outcomes = write_parsed_batch(conn, TRACKED_PLAYERS, {replay_path: analysis})

    assert conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0] == 0
    assert not replay_path.with_suffix(replay_path.suffix + ".ingested").exists()
    assert replay_path.exists()
    assert isinstance(outcomes[replay_path.name], Failed)


def test_write_parsed_batch_isolates_failure_from_other_files_in_batch(
    tmp_path: Path,
):
    """A failing file's rolled-back savepoint must not affect other files in the
    same batch, before or after it, once they share one connection/transaction."""
    from ingest import (
        _insert_match_players as real_insert,  # type: ignore[reportPrivateUsage]
    )

    conn = _make_conn()
    replay_data = load_replay("match.json")

    files = [tmp_path / f"{name}.replay" for name in ("a", "bad", "c")]
    results: dict[Path, Any] = {}
    for p in files:
        p.write_bytes(b"\x00" * 1024)
        data = json.loads(json.dumps(replay_data))
        data["properties"]["MatchGUID"] = f"GUID-{p.stem}"
        results[p] = analyze_replay(parse_rrrocket(data), TRACKED_PLAYERS)

    call_count = 0

    def insert_or_fail(*args: Any, **kwargs: Any):
        nonlocal call_count
        call_count += 1
        if call_count == 2:  # the middle ("bad") file
            raise RuntimeError("simulated failure")
        return real_insert(*args, **kwargs)

    with patch("ingest._insert_match_players", side_effect=insert_or_fail):
        outcomes = write_parsed_batch(conn, TRACKED_PLAYERS, results)

    assert conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM match_players").fetchone()[0] > 0
    a, bad, c = files
    assert a.with_suffix(a.suffix + ".ingested").exists()
    assert not bad.with_suffix(bad.suffix + ".ingested").exists()
    assert c.with_suffix(c.suffix + ".ingested").exists()
    assert bad.exists()
    assert isinstance(outcomes[a.name], Written)
    assert isinstance(outcomes[bad.name], Failed)
    assert isinstance(outcomes[c.name], Written)


def test_write_parsed_batch_syncs_tracked_players(tmp_path: Path):
    """A player already in the DB as untracked gets flipped once they're in config.

    Regression test: get_or_create_player's ON CONFLICT doesn't update is_tracked,
    so this relies on write_parsed_batch also running sync_tracked_players.
    """
    conn = _make_conn()
    conn.execute(
        "INSERT INTO players (platform, platform_id, name, is_tracked)"
        " VALUES (?,?,?,0)",
        ("steam", "76561197969365901", "Drew"),
    )
    conn.commit()

    replay_path = tmp_path / "BEC7EF8411F170E7DBCA41B0676B6A04.replay"
    replay_path.write_bytes(
        (TEST_DATA_DIR / "BEC7EF8411F170E7DBCA41B0676B6A04.replay").read_bytes()
    )
    result = _parse_and_analyze(replay_path, TRACKED_PLAYERS)
    assert isinstance(result, Analyzed)

    write_parsed_batch(conn, TRACKED_PLAYERS, {replay_path: result})

    row = conn.execute(
        "SELECT is_tracked FROM players WHERE platform_id = ?",
        ("76561197969365901",),
    ).fetchone()
    assert row[0] == 1


def test_parse_replay_end_to_end():
    """parse_replay invokes the real rrrocket binary on a .replay file."""
    replay_path = TEST_DATA_DIR / "BEC7EF8411F170E7DBCA41B0676B6A04.replay"
    result, error = parse_replay(replay_path)
    assert error is None
    assert result is not None
    assert result.match_guid is not None


def test_process_unprocessed_end_to_end(tmp_path: Path):
    """process_unprocessed runs rrrocket + ingests real replays via the process pool."""
    db_path = file_db(tmp_path)
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    replay_path = replay_dir / "BEC7EF8411F170E7DBCA41B0676B6A04.replay"
    replay_path.write_bytes(
        (TEST_DATA_DIR / "BEC7EF8411F170E7DBCA41B0676B6A04.replay").read_bytes()
    )

    process_unprocessed(db_path, replay_dir, TRACKED_PLAYERS)

    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT COUNT(*), replay_filename FROM matches").fetchone()
    assert row[0] == 1
    assert row[1] == "BEC7EF8411F170E7DBCA41B0676B6A04.replay"
    assert replay_path.with_suffix(replay_path.suffix + ".ingested").exists()


def test_process_unprocessed_write_failure_does_not_abort_batch(tmp_path: Path):
    """One replay's write failure doesn't prevent the rest of the batch from ingesting."""
    db_path = file_db(tmp_path)
    replay_dir = tmp_path / "replays"
    replay_dir.mkdir()
    src = TEST_DATA_DIR / "BEC7EF8411F170E7DBCA41B0676B6A04.replay"
    bad = replay_dir / "bad.replay"
    good = replay_dir / "good.replay"
    bad.write_bytes(src.read_bytes())
    good.write_bytes(src.read_bytes())

    call_count = 0

    def fake_write_match(conn: Any, analysis: Any) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("simulated failure")

    with patch("process.write_match", side_effect=fake_write_match):
        process_unprocessed(db_path, replay_dir, TRACKED_PLAYERS)

    assert not bad.with_suffix(bad.suffix + ".ingested").exists()
    assert bad.exists()
    assert good.with_suffix(good.suffix + ".ingested").exists()
