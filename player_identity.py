"""Player Identity

A player identity is the stable (platform, platform_id) pair that uniquely
identifies a player across all data sources: the end-of-game PlayerStats blob
and the per-frame UniqueId network actor attributes.

See CONTEXT.md for the full definition.
"""

from typing import Any, NamedTuple, cast


class PlayerIdentity(NamedTuple):
    platform: str
    platform_id: str


# Keys are the platform strings used in the rrrocket PlayerStats blob.
_PLAYER_STATS_PLATFORM_MAP = {
    "OnlinePlatform_Steam": "steam",
    "OnlinePlatform_Epic": "epic",
    "OnlinePlatform_PS4": "ps4",
    "OnlinePlatform_Switch": "switch",
    "OnlinePlatform_NNX": "switch",
    "OnlinePlatform_Xbox": "xbox",
    "OnlinePlatform_Dingo": "xbox",
}

# Keys are the platform strings used in the rrrocket network frame UniqueId attribute.
_NETWORK_PLATFORM_MAP = {
    "Steam": "steam",
    "Epic": "epic",
    "PlayStation": "ps4",
    "PsyNet": "switch",
    "Xbox": "xbox",
}


def from_player_stats(player: dict[str, Any]) -> PlayerIdentity | None:
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
