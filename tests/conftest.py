import dataclasses
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from config import Settings, load_settings

_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture(autouse=True, scope="session")
def test_config():
    """Point CONFIG_DIR at tests/data so load_settings() uses the fixture file."""
    os.environ["CONFIG_DIR"] = str(_DATA_DIR)
    yield
    os.environ.pop("CONFIG_DIR", None)


@pytest.fixture(scope="session")
def base_settings() -> Settings:
    return load_settings()


@pytest.fixture(scope="session")
def make_settings(base_settings: Settings) -> Callable[..., Settings]:
    def _make(**overrides: Any) -> Settings:
        return dataclasses.replace(base_settings, **overrides)

    return _make
