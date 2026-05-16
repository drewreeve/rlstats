from pathlib import Path

import pytest

from config import load_settings
from player_identity import PlayerIdentity

_DATA_DIR = Path(__file__).parent / "data"


def test_loads_fixture_file():
    s = load_settings(_DATA_DIR)
    assert len(s.players) == 3
    assert s.players[PlayerIdentity("steam", "76561197969365901")] == "Drew"
    assert s.upload_password is None
    assert s.secret_key is None


def test_server_section_parsed(tmp_path: Path):
    (tmp_path / "settings.toml").write_text(
        '[server]\nupload_password = "secret"\nsecret_key = "key123"\n\n'
        '[[players]]\nplatform = "steam"\nplatform_id = "1"\nname = "A"\n'
    )
    s = load_settings(tmp_path)
    assert s.upload_password == "secret"
    assert s.secret_key == "key123"
    assert s.players == {PlayerIdentity("steam", "1"): "A"}


def test_empty_secret_key_normalized_to_none(tmp_path: Path):
    (tmp_path / "settings.toml").write_text('[server]\nsecret_key = ""\n')
    s = load_settings(tmp_path)
    assert s.secret_key is None


def test_missing_file_exits(tmp_path: Path):
    with pytest.raises(SystemExit):
        load_settings(tmp_path)
