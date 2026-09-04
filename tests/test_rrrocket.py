"""Tests for rrrocket.run_rrrocket — the binary-invocation contract.

The delete-on-failure policy is process.parse_replay's, tested in
tests/test_process.py; run_rrrocket itself never touches the file.
"""

import json
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import patch

from rrrocket import run_rrrocket
from rrrocket_schema import parse as parse_rrrocket
from tests.fixtures import TEST_DATA_DIR, load_replay


def test_run_rrrocket_parses_on_success(tmp_path: Path):
    replay_data = load_replay("match.json")
    replay_path = tmp_path / "test.replay"
    replay_path.write_bytes(b"\x00" * 1024)

    def fake_rrrocket(args: Any, **kwargs: Any):
        return subprocess.CompletedProcess(
            args, 0, stdout=json.dumps(replay_data).encode()
        )

    with patch("rrrocket.subprocess.run", side_effect=fake_rrrocket):
        result, error = run_rrrocket(replay_path)

    assert result == parse_rrrocket(replay_data)
    assert error is None


def test_run_rrrocket_leaves_the_file_on_failure(tmp_path: Path):
    """Unlike parse_replay, run_rrrocket never deletes — the replay viewer
    depends on a failed re-parse not destroying an already-ingested replay."""
    replay_path = tmp_path / "keep.replay"
    replay_path.write_bytes(b"\x00" * 1024)

    failed = subprocess.CompletedProcess(["rrrocket"], 1, stderr=b"boom")
    with patch("rrrocket.subprocess.run", return_value=failed):
        result, error = run_rrrocket(replay_path)

    assert result is None
    assert error is not None and "rrrocket failed" in error
    assert replay_path.exists()


def test_run_rrrocket_end_to_end():
    """Invokes the real rrrocket binary on a committed .replay."""
    result, error = run_rrrocket(
        TEST_DATA_DIR / "BEC7EF8411F170E7DBCA41B0676B6A04.replay"
    )
    assert error is None
    assert result is not None
    assert result.match_guid is not None
