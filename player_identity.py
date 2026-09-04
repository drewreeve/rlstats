"""Player Identity

A player identity is the stable (platform, platform_id) pair that uniquely
identifies a player across all data sources: the end-of-game PlayerStats blob
and the per-frame UniqueId network actor attributes.

See CONTEXT.md for the full definition.
"""

from collections.abc import Set as AbstractSet
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


class IdentityResolver:
    """Owns the three-map identity chain and exposes typed resolution methods.

    Chain: car_actor_id → pri_actor_id → PlayerIdentity(platform, platform_id)
    Boost components add a fourth entry point: component_actor_id → car_actor_id.

    Shared by both frame walks — `frame_analysis.analyze_frames` (the aggregate
    stats pass) and `replay_frames.extract_replay_frames` (the viewer pass).
    The per-frame wiring that feeds it is deliberately not shared between the
    two walks (ADR-0003); this class is the reusable part.
    """

    def __init__(self) -> None:
        self._car_to_pri: dict[int, int] = {}
        self._pri_identity: dict[int, PlayerIdentity] = {}
        self._component_to_car: dict[int, int] = {}

    def link_car_to_pri(self, car_id: int, pri_id: int) -> None:
        self._car_to_pri[car_id] = pri_id

    def set_identity(self, pri_id: int, identity: PlayerIdentity) -> None:
        self._pri_identity[pri_id] = identity

    def link_component_to_car(self, comp_id: int, car_id: int) -> None:
        self._component_to_car[comp_id] = car_id

    def remove_actor(self, aid: int) -> None:
        self._car_to_pri.pop(aid, None)
        self._pri_identity.pop(aid, None)
        self._component_to_car.pop(aid, None)

    def resolve_car(self, car_id: int) -> PlayerIdentity | None:
        pri = self._car_to_pri.get(car_id)
        if pri is None:
            return None
        return self._pri_identity.get(pri)

    def resolve_pri(self, pri_id: int) -> PlayerIdentity | None:
        return self._pri_identity.get(pri_id)

    def resolve_component(self, comp_id: int) -> PlayerIdentity | None:
        car_id = self._component_to_car.get(comp_id)
        if car_id is None:
            return None
        return self.resolve_car(car_id)

    def find_pri_ids_for(self, identities: AbstractSet[PlayerIdentity]) -> list[int]:
        return [aid for aid, ident in self._pri_identity.items() if ident in identities]
