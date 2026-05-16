import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

from player_identity import PlayerIdentity


@dataclass
class Settings:
    players: dict[PlayerIdentity, str]
    upload_password: str | None = None
    secret_key: str | None = None


def load_settings(config_dir: Path | None = None) -> Settings:
    if config_dir is None:
        config_dir = Path(os.environ.get("CONFIG_DIR", "config"))
    path = config_dir / "settings.toml"
    try:
        with open(path, "rb") as f:
            data = tomllib.load(f)
    except FileNotFoundError:
        raise SystemExit(
            f"Error: config file not found: {path}\n"
            f"Copy config/settings.example.toml to {path} and configure your settings."
        ) from None
    server = data.get("server", {})
    players = {
        PlayerIdentity(entry["platform"], entry["platform_id"]): entry["name"]
        for entry in data.get("players", [])
    }
    return Settings(
        players=players,
        upload_password=server.get("upload_password") or None,
        secret_key=server.get("secret_key") or None,
    )


def load_tracked_players(config_dir: Path | None = None) -> dict[PlayerIdentity, str]:
    return load_settings(config_dir).players
