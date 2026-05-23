"""Player Identity

A player identity is the stable (platform, platform_id) pair that uniquely
identifies a player across all data sources: the end-of-game PlayerStats blob
and the per-frame UniqueId network actor attributes.

See CONTEXT.md for the full definition.
"""

from typing import Any, NamedTuple, cast

from rrrocket_schema import PlayerStatEntry


class PlayerIdentity(NamedTuple):
    platform: str
    platform_id: str


class _PlatformSpec(NamedTuple):
    normalized: str
    player_stats_keys: tuple[str, ...]
    network_key: str


_PLATFORMS: tuple[_PlatformSpec, ...] = (
    _PlatformSpec("steam", ("OnlinePlatform_Steam",), "Steam"),
    _PlatformSpec("epic", ("OnlinePlatform_Epic",), "Epic"),
    _PlatformSpec("ps4", ("OnlinePlatform_PS4",), "PlayStation"),
    _PlatformSpec("switch", ("OnlinePlatform_Switch", "OnlinePlatform_NNX"), "PsyNet"),
    _PlatformSpec("xbox", ("OnlinePlatform_Xbox", "OnlinePlatform_Dingo"), "Xbox"),
)

_PLAYER_STATS_PLATFORM_MAP: dict[str, str] = {
    key: spec.normalized for spec in _PLATFORMS for key in spec.player_stats_keys
}
_NETWORK_PLATFORM_MAP: dict[str, str] = {
    spec.network_key: spec.normalized for spec in _PLATFORMS
}


def from_player_stats(player: PlayerStatEntry) -> PlayerIdentity | None:
    """Resolve a PlayerStats entry from the rrrocket JSON to a PlayerIdentity."""
    platform_value = player.get("Platform", {}).get("value", "")

    if platform_value == "OnlinePlatform_Epic":
        epic_id = player.get("PlayerID", {}).get("fields", {}).get("EpicAccountId", "")
        return PlayerIdentity("epic", epic_id) if epic_id else None

    platform = _PLAYER_STATS_PLATFORM_MAP.get(platform_value)
    if not platform:
        return None
    online_id = player.get("OnlineID", "0")
    return (
        PlayerIdentity(platform, online_id) if online_id and online_id != "0" else None
    )


def from_network_frame(uid: dict[str, Any]) -> PlayerIdentity | None:
    """Resolve a UniqueId attribute from network frames to a PlayerIdentity."""
    remote: Any = uid.get("remote_id", {})
    if not remote:
        return None
    platform_key = next(iter(remote))
    platform = _NETWORK_PLATFORM_MAP.get(platform_key)
    if not platform:
        return None
    value: Any = remote[platform_key]
    if isinstance(value, dict):
        platform_id = cast(dict[str, Any], value).get("online_id")
    else:
        platform_id = value
    if not platform_id:
        return None
    return PlayerIdentity(platform, str(platform_id))
