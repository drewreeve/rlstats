"""Frame Analysis Module

Processes rrrocket network frame data into aggregate match stats.  Each frame
carries three actor-lifecycle lists: new_actors (spawned), updated_actors
(attribute changed), and deleted_actors (despawned).

Identity resolution chain — the central indirection in this file:
  car_actor_id  →  pri_actor_id    (Engine.Pawn:PlayerReplicationInfo)
  pri_actor_id  →  (platform, id)  (Engine.PlayerReplicationInfo:UniqueId)
Cars get a new actor ID each life; PRIs persist the whole match; (platform, id)
is the stable key that joins to the `players` table.

Per-frame processing order enforced by analyze_frames:
  1. new_actors    — register car/ball/boost archetypes into FrameContext
  2. updated_actors — update shared FrameContext state, then dispatch to handlers
  3. deleted_actors — notify handlers BEFORE cleaning FrameContext so that
     identity resolution is still valid when a car is deleted mid-frame
"""

import math
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from itertools import pairwise
from typing import Any, NamedTuple, cast

NETWORK_PLATFORM_MAP = {
    "Steam": "steam",
    "Epic": "epic",
    "PlayStation": "ps4",
    "PsyNet": "switch",
    "Xbox": "xbox",
}

# Coordinates are taken from wiki.rlbot.org
# https://wiki.rlbot.org/v4/botmaking/useful-game-values/
# https://wiki.rlbot.org/v4/botmaking/hoops/
BIG_PAD_POSITIONS = {
    "standard": [
        (-3072, -4096),
        (3072, -4096),
        (-3584, 0),
        (3584, 0),
        (-3072, 4096),
        (3072, 4096),
    ],
    "hoops": [
        (-2176, -2944),
        (2176, -2944),
        (-2432, 0),
        (2432, 0),
        (-2176, 2944),
        (2176, 2944),
    ],
}
# Official hitbox radius is 208uu (wiki.rlbot.org); we use 400 to allow for
# car size and positional jitter in replay network frames.
BIG_PAD_RADIUS = 400
BIG_PAD_RADIUS_SQ = BIG_PAD_RADIUS**2


class IdentityResolver:
    """Owns the three-map identity chain and exposes typed resolution methods.

    Chain: car_actor_id → pri_actor_id → (platform, platform_id)
    Boost components add a fourth entry point: component_actor_id → car_actor_id.
    """

    def __init__(self) -> None:
        self._car_to_pri: dict[int, int] = {}
        self._pri_identity: dict[int, tuple[str, str]] = {}
        self._component_to_car: dict[int, int] = {}

    def link_car_to_pri(self, car_id: int, pri_id: int) -> None:
        self._car_to_pri[car_id] = pri_id

    def set_identity(self, pri_id: int, platform: str, platform_id: str) -> None:
        self._pri_identity[pri_id] = (platform, platform_id)

    def link_component_to_car(self, comp_id: int, car_id: int) -> None:
        self._component_to_car[comp_id] = car_id

    def remove_actor(self, aid: int) -> None:
        self._car_to_pri.pop(aid, None)
        self._pri_identity.pop(aid, None)
        self._component_to_car.pop(aid, None)

    def resolve_car(self, car_id: int) -> tuple[str, str] | None:
        pri = self._car_to_pri.get(car_id)
        if pri is None:
            return None
        return self._pri_identity.get(pri)

    def resolve_pri(self, pri_id: int) -> tuple[str, str] | None:
        return self._pri_identity.get(pri_id)

    def resolve_component(self, comp_id: int) -> tuple[str, str] | None:
        car_id = self._component_to_car.get(comp_id)
        if car_id is None:
            return None
        return self.resolve_car(car_id)

    def find_pri_ids_for(self, identities: set[tuple[str, str]]) -> list[int]:
        return [aid for aid, ident in self._pri_identity.items() if ident in identities]


class FrameAnalysis(NamedTuple):
    team_possession_seconds: float | None
    opponent_possession_seconds: float | None
    defensive_third_seconds: float | None
    neutral_third_seconds: float | None
    offensive_third_seconds: float | None
    demolitions: dict[tuple[str, str], int]
    team_boost_collected: int | None
    opponent_boost_collected: int | None
    team_boost_stolen: int | None
    opponent_boost_stolen: int | None
    movement_stats: dict[tuple[str, str], dict[str, float | int]]
    demos_received: dict[tuple[str, str], int]
    match_events: list[tuple[str, float, str, str, int]]


@dataclass
class FrameContext:
    """Shared state maintained by the orchestrator loop."""

    # Actor archetype sets
    car_actors: set[int] = field(default_factory=set[int])
    ball_actors: set[int] = field(default_factory=set[int])
    boost_comp_actors: set[int] = field(default_factory=set[int])

    # Identity resolution chain
    resolver: IdentityResolver = field(default_factory=IdentityResolver)

    # Per-actor state
    actor_team: dict[int, int] = field(default_factory=dict[int, int])
    actor_position: dict[int, tuple[float, float]] = field(
        default_factory=dict[int, tuple[float, float]]
    )

    # Game state
    is_playing: bool = False
    frame_time: float = 0.0


class FrameHandler(ABC):
    """Base class for frame-loop handlers.

    Lifecycle invariants enforced by the orchestrator (see ``analyze_frames``):

    - ``on_update``: called after shared ``FrameContext`` state for this frame
      has been updated (``is_playing``, ``actor_team``, ``actor_position``,
      archetype sets, resolver maps, etc.). Actor deletions for the frame have
      NOT yet been processed.
    - ``on_deleted_actor``: called before the actor is removed from
      ``car_actors``, ``ball_actors``, ``boost_comp_actors``, ``actor_team``,
      ``actor_position``, and the resolver's internal maps. Identity resolution
      via ``ctx.resolver.resolve_car(aid)`` is still valid here.
    - ``finalize``: called once after all frames. Any remaining live-actor
      state (boost components, speed samples) must be flushed into
      identity-keyed accumulators here.

    Subclasses must declare ``update_obj_ids`` (a frozenset of network object
    IDs to receive ``on_update`` notifications for) on the instance.
    """

    update_obj_ids: frozenset[int]

    @abstractmethod
    def on_update(self, ctx: FrameContext, actor: dict[str, Any]) -> None: ...

    def on_deleted_actor(self, ctx: FrameContext, aid: int) -> None:
        del ctx, aid

    @abstractmethod
    def finalize(self, ctx: FrameContext) -> Any: ...


def _resolve_network_identity(
    uid: dict[str, Any],
) -> tuple[str, str] | None:
    """Resolve a UniqueId attribute from network frames to (platform, platform_id)."""
    remote: Any = uid.get("remote_id", {})
    if not remote:
        return None
    platform_key = next(iter(remote))
    platform = NETWORK_PLATFORM_MAP.get(platform_key)
    if not platform:
        return None
    value: Any = remote[platform_key]
    if isinstance(value, dict):
        platform_id: Any = cast(dict[str, Any], value).get("online_id")
    else:
        platform_id = value
    if not platform_id:
        return None
    return (platform, str(platform_id))


def _parse_pickup(
    actor: dict[str, Any],
    last_pickup_state: dict[int, int],
    actor_team: dict[int, int],
    actor_position: dict[int, tuple[float, float]],
    big_pads: Sequence[tuple[float, float]],
) -> tuple[int, int, bool, bool] | None:
    """Parse a NewReplicatedPickupData actor update.

    Returns (instigator_actor_id, team, is_big, is_stolen) or None to skip.
    """
    aid = actor["actor_id"]
    pickup = actor.get("attribute", {}).get("PickupNew", {})
    picked_up_state = pickup.get("picked_up")
    if (
        picked_up_state is None or picked_up_state == 255
    ):  # 255 = pad respawning, no valid pickup
        return None
    if last_pickup_state.get(aid) == picked_up_state:
        return None
    last_pickup_state[aid] = picked_up_state

    instigator = pickup.get("instigator")
    if instigator is None:
        return None
    team = actor_team.get(instigator)
    pos = actor_position.get(instigator)
    if team is None or pos is None:
        return None

    x, y = pos
    is_big = any(
        (x - bx) ** 2 + (y - by) ** 2 <= BIG_PAD_RADIUS_SQ for bx, by in big_pads
    )
    # Stolen = picked up on the opponent's half (team 0 attacks positive-y)
    is_stolen = (team == 0 and y > 0) or (team == 1 and y < 0)
    return instigator, team, is_big, is_stolen


def _resolve_obj_ids(objects: list[str]) -> dict[str, int | None]:
    """Resolve all object names to IDs in a single pass."""
    names = {
        "TAGame.Ball_TA:HitTeamNum",
        "Archetypes.Ball.Ball_Default",
        "Archetypes.Car.Car_Default",
        "Archetypes.CarComponents.CarComponent_Boost",
        "TAGame.RBActor_TA:ReplicatedRBState",
        "TAGame.CarComponent_Boost_TA:ReplicatedBoost",
        "TAGame.CarComponent_TA:Vehicle",
        "Engine.Pawn:PlayerReplicationInfo",
        "Engine.PlayerReplicationInfo:UniqueId",
        "TAGame.GameEvent_Soccar_TA:ReplicatedScoredOnTeam",
        "TAGame.GameEvent_TA:ReplicatedRoundCountDownNumber",
        "TAGame.VehiclePickup_TA:NewReplicatedPickupData",
        "TAGame.Car_TA:TeamPaint",
        "TAGame.PRI_TA:MatchDemolishes",
        "TAGame.Car_TA:ReplicatedDemolishExtended",
        "TAGame.GameEvent_Soccar_TA:SecondsRemaining",
        "Engine.PlayerReplicationInfo:Team",
        "TAGame.PRI_TA:MatchGoals",
        "TAGame.PRI_TA:MatchShots",
        "TAGame.PRI_TA:MatchSaves",
        "TAGame.PRI_TA:MatchAssists",
    }
    index = {name: i for i, name in enumerate(objects) if name in names}
    return {name: index.get(name) for name in names}


# -- Handlers --


class PossessionHandler(FrameHandler):
    """Tracks possession seconds per team based on ball hit team transitions."""

    @classmethod
    def create(
        cls, obj_ids: dict[str, int | None], tracked_team: int | None
    ) -> "PossessionHandler | None":
        if tracked_team is None:
            return None
        hit_team_obj_id = obj_ids.get("TAGame.Ball_TA:HitTeamNum")
        if hit_team_obj_id is None:
            return None
        return cls(hit_team_obj_id, tracked_team)

    def __init__(self, hit_team_obj_id: int, tracked_team: int) -> None:
        self.update_obj_ids = frozenset({hit_team_obj_id})
        self.tracked_team = tracked_team
        self.touches: list[tuple[float, int]] = []

    def on_update(self, ctx: FrameContext, actor: dict[str, Any]) -> None:
        team_num = actor.get("attribute", {}).get("Byte")
        if team_num is not None:
            self.touches.append((ctx.frame_time, team_num))

    def finalize(self, ctx: FrameContext) -> tuple[float | None, float | None]:
        if not self.touches:
            return None, None

        possession = {0: 0.0, 1: 0.0}
        for (t_start, team_num), (t_end, _) in pairwise(self.touches):
            possession[team_num] += t_end - t_start

        last_time, last_team = self.touches[-1]
        possession[last_team] += ctx.frame_time - last_time

        team_poss = possession.get(self.tracked_team, 0.0)
        opp_poss = possession.get(1 - self.tracked_team, 0.0)
        return round(team_poss, 2), round(opp_poss, 2)


class BallThirdsHandler(FrameHandler):
    """Tracks time the ball spent in each third of the field."""

    @classmethod
    def create(
        cls, obj_ids: dict[str, int | None], tracked_team: int | None
    ) -> "BallThirdsHandler | None":
        if tracked_team is None:
            return None
        rb_obj_id = obj_ids.get("TAGame.RBActor_TA:ReplicatedRBState")
        ball_archetype = obj_ids.get("Archetypes.Ball.Ball_Default")
        if rb_obj_id is None or ball_archetype is None:
            return None
        return cls(rb_obj_id, tracked_team)

    def __init__(self, rb_obj_id: int, tracked_team: int) -> None:
        self.update_obj_ids = frozenset({rb_obj_id})
        self.tracked_team = tracked_team
        self.samples: list[tuple[float, float]] = []

    def on_update(self, ctx: FrameContext, actor: dict[str, Any]) -> None:
        if actor["actor_id"] not in ctx.ball_actors:
            return
        loc = actor.get("attribute", {}).get("RigidBody", {}).get("location")
        if loc and "y" in loc:
            self.samples.append((ctx.frame_time, loc["y"]))

    def finalize(
        self, ctx: FrameContext
    ) -> tuple[float | None, float | None, float | None]:
        if len(self.samples) < 2:
            return None, None, None

        THIRD_BOUNDARY = 1707  # field is ±5120 uu from center; one third ≈ 5120 / 3
        zones = {"negative": 0.0, "neutral": 0.0, "positive": 0.0}
        for (t_start, y), (t_end, _) in pairwise(self.samples):
            dt = t_end - t_start
            if dt <= 0:
                continue
            if y < -THIRD_BOUNDARY:
                zones["negative"] += dt
            elif y > THIRD_BOUNDARY:
                zones["positive"] += dt
            else:
                zones["neutral"] += dt

        if self.tracked_team == 0:
            defensive = zones["negative"]
            offensive = zones["positive"]
        else:
            defensive = zones["positive"]
            offensive = zones["negative"]

        return round(defensive, 2), round(zones["neutral"], 2), round(offensive, 2)


class DemolitionsHandler(FrameHandler):
    """Tracks per-player demolitions-dealt count via the PRI counter."""

    @classmethod
    def create(cls, obj_ids: dict[str, int | None]) -> "DemolitionsHandler | None":
        demo_obj_id = obj_ids.get("TAGame.PRI_TA:MatchDemolishes")
        if demo_obj_id is None:
            return None
        return cls(demo_obj_id)

    def __init__(self, demo_obj_id: int) -> None:
        self.update_obj_ids = frozenset({demo_obj_id})
        self.actor_demos: dict[int, int] = {}

    def on_update(self, ctx: FrameContext, actor: dict[str, Any]) -> None:
        val = actor.get("attribute", {}).get("Int", 0)
        aid = actor["actor_id"]
        self.actor_demos[aid] = max(self.actor_demos.get(aid, 0), val)

    def finalize(self, ctx: FrameContext) -> dict[tuple[str, str], int]:
        result: dict[tuple[str, str], int] = {}
        for aid, count in self.actor_demos.items():
            identity = ctx.resolver.resolve_pri(aid)
            if identity:
                result[identity] = count
        return result


class DemosReceivedHandler(FrameHandler):
    """Tracks per-player demolitions-received count via DemolishExtended events."""

    @classmethod
    def create(cls, obj_ids: dict[str, int | None]) -> "DemosReceivedHandler | None":
        demolish_obj_id = obj_ids.get("TAGame.Car_TA:ReplicatedDemolishExtended")
        if demolish_obj_id is None:
            return None
        return cls(demolish_obj_id)

    def __init__(self, demolish_obj_id: int) -> None:
        self.update_obj_ids = frozenset({demolish_obj_id})
        self.demos_received: dict[tuple[str, str], int] = {}
        self.demolish_last_active: dict[int, bool] = {}

    def on_deleted_actor(self, ctx: FrameContext, aid: int) -> None:
        self.demolish_last_active.pop(aid, None)

    def on_update(self, ctx: FrameContext, actor: dict[str, Any]) -> None:
        demolish = actor.get("attribute", {}).get("DemolishExtended", {})
        victim = demolish.get("victim", {})
        currently_active = bool(victim.get("active"))
        was_active = self.demolish_last_active.get(actor["actor_id"], False)
        self.demolish_last_active[actor["actor_id"]] = currently_active
        # Rising-edge only: skip unless transitioning from inactive → active
        if not currently_active or was_active:
            return
        if demolish.get("self_demolish"):
            return
        attacker = demolish.get("attacker", {})
        if not attacker.get("active"):
            return
        vid = victim["actor"]
        victim_identity = (
            ctx.resolver.resolve_car(vid) if vid in ctx.car_actors else None
        )
        if victim_identity:
            self.demos_received[victim_identity] = (
                self.demos_received.get(victim_identity, 0) + 1
            )

    def finalize(self, ctx: FrameContext) -> dict[tuple[str, str], int]:
        return self.demos_received


class BoostStatsHandler(FrameHandler):
    """Tracks team-level boost collected and stolen totals from pickup events."""

    @classmethod
    def create(
        cls,
        obj_ids: dict[str, int | None],
        tracked_team: int | None,
        big_pads: Sequence[tuple[float, float]],
    ) -> "BoostStatsHandler | None":
        if tracked_team is None:
            return None
        pickup_obj_id = obj_ids.get("TAGame.VehiclePickup_TA:NewReplicatedPickupData")
        if pickup_obj_id is None:
            return None
        return cls(pickup_obj_id, tracked_team, big_pads)

    def __init__(
        self,
        pickup_obj_id: int,
        tracked_team: int,
        big_pads: Sequence[tuple[float, float]],
    ) -> None:
        self.update_obj_ids = frozenset({pickup_obj_id})
        self.tracked_team = tracked_team
        self.big_pads = big_pads
        self.last_pickup_state: dict[int, int] = {}
        self.collected = {0: 0, 1: 0}
        self.stolen = {0: 0, 1: 0}

    def on_deleted_actor(self, ctx: FrameContext, aid: int) -> None:
        self.last_pickup_state.pop(aid, None)

    def on_update(self, ctx: FrameContext, actor: dict[str, Any]) -> None:
        result = _parse_pickup(
            actor,
            self.last_pickup_state,
            ctx.actor_team,
            ctx.actor_position,
            self.big_pads,
        )
        if result is None:
            return
        _, team, is_big, is_stolen = result
        boost_value = 100 if is_big else 12
        self.collected[team] += boost_value
        if is_stolen:
            self.stolen[team] += boost_value

    def finalize(
        self, ctx: FrameContext
    ) -> tuple[int | None, int | None, int | None, int | None]:
        if self.collected[0] == 0 and self.collected[1] == 0:
            return None, None, None, None

        team_collected = self.collected[self.tracked_team]
        opp_collected = self.collected[1 - self.tracked_team]
        team_stolen = self.stolen[self.tracked_team]
        opp_stolen = self.stolen[1 - self.tracked_team]
        return team_collected, opp_collected, team_stolen, opp_stolen


class MovementHandler(FrameHandler):
    """Tracks per-player movement stats (boost consumed, speed, pad pickups)."""

    @classmethod
    def create(
        cls,
        obj_ids: dict[str, int | None],
        duration: int | None,
        big_pads: Sequence[tuple[float, float]],
    ) -> "MovementHandler | None":
        if not duration or duration <= 0:
            return None
        rb_obj_id = obj_ids.get("TAGame.RBActor_TA:ReplicatedRBState")
        boost_obj_id = obj_ids.get("TAGame.CarComponent_Boost_TA:ReplicatedBoost")
        pickup_obj_id = obj_ids.get("TAGame.VehiclePickup_TA:NewReplicatedPickupData")
        if rb_obj_id is None or boost_obj_id is None or pickup_obj_id is None:
            return None
        return cls(rb_obj_id, boost_obj_id, pickup_obj_id, duration, big_pads)

    def __init__(
        self,
        rb_obj_id: int,
        boost_obj_id: int,
        pickup_obj_id: int,
        duration: int,
        big_pads: Sequence[tuple[float, float]],
    ) -> None:
        self.update_obj_ids = frozenset({rb_obj_id, boost_obj_id, pickup_obj_id})
        self.rb_obj_id = rb_obj_id
        self.boost_obj_id = boost_obj_id
        self.pickup_obj_id = pickup_obj_id
        self.duration = duration
        self.big_pads = big_pads

        # Per boost-component state
        self.comp_boost: dict[int, int] = {}
        self.comp_boost_consumed: dict[int, float] = {}
        self.car_speed_samples: dict[int, list[tuple[float, float]]] = {}
        self.identity_pads: dict[tuple[str, str], dict[str, int]] = {}
        self.last_pickup_state: dict[int, int] = {}

        self.identity_boost_consumed: dict[tuple[str, str], float] = {}
        self.identity_speeds: dict[tuple[str, str], list[tuple[float, float]]] = {}

    def _flush_boost_comp(
        self, ctx: FrameContext, comp_id: int, consumed: float
    ) -> None:
        identity = ctx.resolver.resolve_component(comp_id)
        if identity:
            self.identity_boost_consumed[identity] = (
                self.identity_boost_consumed.get(identity, 0.0) + consumed
            )

    def _flush_speed_samples(
        self, ctx: FrameContext, car_id: int, samples: list[tuple[float, float]]
    ) -> None:
        identity = ctx.resolver.resolve_car(car_id)
        if identity:
            self.identity_speeds.setdefault(identity, []).extend(samples)

    def on_deleted_actor(self, ctx: FrameContext, aid: int) -> None:
        self.comp_boost.pop(aid, None)
        self.last_pickup_state.pop(aid, None)
        consumed = self.comp_boost_consumed.pop(aid, None)
        if consumed:
            self._flush_boost_comp(ctx, aid, consumed)
        samples = self.car_speed_samples.pop(aid, None)
        if samples:
            self._flush_speed_samples(ctx, aid, samples)

    def on_update(self, ctx: FrameContext, actor: dict[str, Any]) -> None:
        oid = actor.get("object_id")
        aid = actor["actor_id"]

        if oid == self.boost_obj_id and ctx.is_playing and aid in ctx.boost_comp_actors:
            amount = (
                actor.get("attribute", {})
                .get("ReplicatedBoost", {})
                .get("boost_amount")
            )
            if amount is None:
                return
            prev = self.comp_boost.get(aid)
            self.comp_boost[aid] = amount
            if prev is not None and amount < prev:
                self.comp_boost_consumed[aid] = self.comp_boost_consumed.get(
                    aid, 0.0
                ) + (prev - amount)

        elif oid == self.rb_obj_id:
            if aid in ctx.car_actors and ctx.is_playing:
                lv = (
                    actor.get("attribute", {})
                    .get("RigidBody", {})
                    .get("linear_velocity")
                )
                if lv is not None and "x" in lv and "y" in lv and "z" in lv:
                    speed = math.sqrt(lv["x"] ** 2 + lv["y"] ** 2 + lv["z"] ** 2)
                    if aid not in self.car_speed_samples:
                        self.car_speed_samples[aid] = []
                    self.car_speed_samples[aid].append((ctx.frame_time, speed))

        elif oid == self.pickup_obj_id and ctx.is_playing:
            result = _parse_pickup(
                actor,
                self.last_pickup_state,
                ctx.actor_team,
                ctx.actor_position,
                self.big_pads,
            )
            if result is None:
                return
            instigator, _, is_big, is_stolen = result
            identity = ctx.resolver.resolve_car(instigator)
            if identity is None:
                return
            pads = self.identity_pads.setdefault(
                identity,
                {
                    "small_pads": 0,
                    "large_pads": 0,
                    "stolen_small_pads": 0,
                    "stolen_large_pads": 0,
                },
            )
            if is_big:
                pads["large_pads"] += 1
                if is_stolen:
                    pads["stolen_large_pads"] += 1
            else:
                pads["small_pads"] += 1
                if is_stolen:
                    pads["stolen_small_pads"] += 1

    def finalize(
        self, ctx: FrameContext
    ) -> dict[tuple[str, str], dict[str, float | int]]:
        # Deleted-actor data already accumulated; handle remaining live actors
        for comp_id, consumed in self.comp_boost_consumed.items():
            self._flush_boost_comp(ctx, comp_id, consumed)

        for car_id, samples in self.car_speed_samples.items():
            self._flush_speed_samples(ctx, car_id, samples)

        # Build results
        all_identities = (
            set(self.identity_boost_consumed.keys())
            | set(self.identity_speeds.keys())
            | set(self.identity_pads.keys())
        )
        result: dict[tuple[str, str], dict[str, float | int]] = {}

        for identity in all_identities:
            stats: dict[str, float] = {}

            consumed = self.identity_boost_consumed.get(identity, 0.0)
            stats["boost_per_minute"] = round(
                (consumed / 255 * 100) / (self.duration / 60), 1
            )

            speeds = self.identity_speeds.get(identity, [])
            if speeds:
                speeds.sort(key=lambda s: s[0])
                total_weight = 0.0
                weighted_sum = 0.0
                supersonic_time = 0.0
                for (t1, s1), (t2, _) in pairwise(speeds):
                    dt = t2 - t1
                    if (
                        0 < dt < 5
                    ):  # skip gaps > 5s (replay pauses, halftime) to avoid skewing averages
                        weighted_sum += s1 * dt
                        total_weight += dt
                        if s1 >= 2200:
                            supersonic_time += dt
                stats["avg_speed"] = (
                    round(weighted_sum / total_weight) if total_weight > 0 else 0
                )
                stats["time_supersonic_pct"] = round(
                    supersonic_time / self.duration * 100, 1
                )
            else:
                stats["avg_speed"] = 0
                stats["time_supersonic_pct"] = 0.0

            pad = self.identity_pads.get(identity, {})
            stats["small_pads"] = pad.get("small_pads", 0)
            stats["large_pads"] = pad.get("large_pads", 0)
            stats["stolen_small_pads"] = pad.get("stolen_small_pads", 0)
            stats["stolen_large_pads"] = pad.get("stolen_large_pads", 0)

            result[identity] = stats

        return result


class MatchEventsHandler(FrameHandler):
    """Extracts per-event match events (goals, shots, saves, demos, assists)
    with game-clock-anchored timestamps."""

    _COUNTER_NAMES = {
        "TAGame.PRI_TA:MatchGoals": "goal",
        "TAGame.PRI_TA:MatchShots": "shot",
        "TAGame.PRI_TA:MatchSaves": "save",
        "TAGame.PRI_TA:MatchDemolishes": "demo",
        "TAGame.PRI_TA:MatchAssists": "assist",
    }

    @classmethod
    def create(
        cls,
        obj_ids: dict[str, int | None],
        tracked_team: int | None,
        tracked_identities: set[tuple[str, str]],
    ) -> "MatchEventsHandler | None":
        if tracked_team is None:
            return None
        sr_obj_id = obj_ids.get("TAGame.GameEvent_Soccar_TA:SecondsRemaining")
        team_obj_id = obj_ids.get("Engine.PlayerReplicationInfo:Team")
        if sr_obj_id is None or team_obj_id is None:
            return None
        counter_obj_ids: dict[int, str] = {}
        for obj_name, event_type in cls._COUNTER_NAMES.items():
            oid = obj_ids.get(obj_name)
            if oid is not None:
                counter_obj_ids[oid] = event_type
        if not counter_obj_ids:
            return None
        return cls(
            sr_obj_id, team_obj_id, counter_obj_ids, tracked_team, tracked_identities
        )

    def __init__(
        self,
        sr_obj_id: int,
        team_obj_id: int,
        counter_obj_ids: dict[int, str],
        tracked_team: int,
        tracked_identities: set[tuple[str, str]],
    ) -> None:
        self.update_obj_ids = frozenset(
            {sr_obj_id, team_obj_id} | set(counter_obj_ids.keys())
        )
        self.sr_obj_id = sr_obj_id
        self.team_obj_id = team_obj_id
        self.counter_obj_ids = counter_obj_ids
        self.tracked_team = tracked_team
        self.tracked_identities = tracked_identities

        self.clock_updates: list[tuple[float, int]] = []
        self.actor_team_actor: dict[int, int] = {}
        self.actor_counters: dict[int, dict[str, int]] = {}
        self.raw_events: list[tuple[str, float, int]] = []

    def on_update(self, ctx: FrameContext, actor: dict[str, Any]) -> None:
        obj_id = actor.get("object_id")
        aid = actor["actor_id"]

        if obj_id == self.sr_obj_id:
            sr = actor.get("attribute", {}).get("Int")
            if sr is not None:
                self.clock_updates.append((ctx.frame_time, sr))
        elif obj_id == self.team_obj_id:
            team_actor = actor.get("attribute", {}).get("ActiveActor", {}).get("actor")
            if team_actor is not None:
                self.actor_team_actor[aid] = team_actor
        elif obj_id in self.counter_obj_ids:
            event_type = self.counter_obj_ids[obj_id]
            val = actor.get("attribute", {}).get("Int", 0)
            if aid not in self.actor_counters:
                self.actor_counters[aid] = {}
            prev = self.actor_counters[aid].get(event_type, 0)
            if val > prev:
                for _ in range(val - prev):
                    self.raw_events.append((event_type, ctx.frame_time, aid))
            self.actor_counters[aid][event_type] = val

    def finalize(self, ctx: FrameContext) -> list[tuple[str, float, str, str, int]]:
        if not self.clock_updates or not self.raw_events:
            return []

        game_start = self.clock_updates[0][1]

        # Regulation: clock counts down (game_start - sr → monotonically increasing game time).
        # Overtime: clock resets to 0 then counts up; seen_zero marks when regulation ended.
        seen_zero = False
        clock_game_seconds: list[tuple[float, float]] = []
        for c_ft, sr in self.clock_updates:
            if sr == 0:
                seen_zero = True
            if seen_zero and sr > 0:
                clock_game_seconds.append((c_ft, game_start + sr))
            else:
                clock_game_seconds.append((c_ft, game_start - sr))

        def frame_to_game_seconds(ft: float) -> float:
            _, best_gs = clock_game_seconds[0]
            for c_ft, c_gs in clock_game_seconds:
                if c_ft <= ft:
                    best_gs = c_gs
                else:
                    break
            return best_gs

        tracked_team_actor = None
        for aid in ctx.resolver.find_pri_ids_for(self.tracked_identities):
            if aid in self.actor_team_actor:
                tracked_team_actor = self.actor_team_actor[aid]
                break

        if tracked_team_actor is None:
            return []

        def resolve_team(aid: int) -> int | None:
            ta = self.actor_team_actor.get(aid)
            if ta is None:
                return None
            return (
                self.tracked_team
                if ta == tracked_team_actor
                else (1 - self.tracked_team)
            )

        events: list[tuple[str, float, str, str, int]] = []
        for event_type, ft, aid in self.raw_events:
            identity = ctx.resolver.resolve_pri(aid)
            team = resolve_team(aid)
            if identity is None or team is None:
                continue
            gs = frame_to_game_seconds(ft)
            events.append((event_type, gs, identity[0], identity[1], team))

        return events


# -- Orchestrator --


def analyze_frames(
    replay: dict[str, Any],
    tracked_team: int | None,
    tracked_identities: set[tuple[str, str]],
    duration: int | None,
    game_mode: str | None,
) -> FrameAnalysis:

    objects = replay.get("objects")
    frames = replay.get("network_frames", {}).get("frames")

    # Default empty result
    empty = FrameAnalysis(
        team_possession_seconds=None,
        opponent_possession_seconds=None,
        defensive_third_seconds=None,
        neutral_third_seconds=None,
        offensive_third_seconds=None,
        demolitions={},
        team_boost_collected=None,
        opponent_boost_collected=None,
        team_boost_stolen=None,
        opponent_boost_stolen=None,
        movement_stats={},
        demos_received={},
        match_events=[],
    )

    if not objects or not frames:
        return empty

    obj_ids = _resolve_obj_ids(objects)

    # Archetype IDs for new_actor processing
    car_archetype = obj_ids.get("Archetypes.Car.Car_Default")
    ball_archetype = obj_ids.get("Archetypes.Ball.Ball_Default")
    boost_comp_archetype = obj_ids.get("Archetypes.CarComponents.CarComponent_Boost")

    # Shared state object IDs
    scored_obj_id = obj_ids.get("TAGame.GameEvent_Soccar_TA:ReplicatedScoredOnTeam")
    countdown_obj_id = obj_ids.get("TAGame.GameEvent_TA:ReplicatedRoundCountDownNumber")
    vehicle_obj_id = obj_ids.get("TAGame.CarComponent_TA:Vehicle")
    pri_obj_id = obj_ids.get("Engine.Pawn:PlayerReplicationInfo")
    uid_obj_id = obj_ids.get("Engine.PlayerReplicationInfo:UniqueId")
    team_paint_obj_id = obj_ids.get("TAGame.Car_TA:TeamPaint")
    rb_obj_id = obj_ids.get("TAGame.RBActor_TA:ReplicatedRBState")

    big_pads = BIG_PAD_POSITIONS["hoops" if game_mode == "hoops" else "standard"]

    # Create handlers
    possession_h = PossessionHandler.create(obj_ids, tracked_team)
    ball_thirds_h = BallThirdsHandler.create(obj_ids, tracked_team)
    demolitions_h = DemolitionsHandler.create(obj_ids)
    boost_stats_h = BoostStatsHandler.create(obj_ids, tracked_team, big_pads)
    movement_h = MovementHandler.create(obj_ids, duration, big_pads)
    demos_received_h = DemosReceivedHandler.create(obj_ids)
    match_events_h = MatchEventsHandler.create(
        obj_ids, tracked_team, tracked_identities
    )

    handlers: list[FrameHandler] = [
        h
        for h in [
            possession_h,
            ball_thirds_h,
            demolitions_h,
            boost_stats_h,
            movement_h,
            demos_received_h,
            match_events_h,
        ]
        if h is not None
    ]

    # Build dispatch table: object_id -> list of handlers interested in that obj_id
    update_dispatch: dict[int, list[FrameHandler]] = {}
    for h in handlers:
        for oid in h.update_obj_ids:
            update_dispatch.setdefault(oid, []).append(h)

    # Only handlers that override on_deleted_actor need to be called on deletions
    deleted_actor_handlers = [
        h
        for h in handlers
        if type(h).on_deleted_actor is not FrameHandler.on_deleted_actor
    ]

    ctx = FrameContext()

    for frame in frames:
        ctx.frame_time = frame["time"]

        # 1. Process new_actors -> update shared archetype sets
        for new_actor in frame.get("new_actors", []):
            oid = new_actor.get("object_id")
            aid = new_actor["actor_id"]
            if oid == car_archetype:
                ctx.car_actors.add(aid)
            elif oid == ball_archetype:
                ctx.ball_actors.add(aid)
            elif oid == boost_comp_archetype:
                ctx.boost_comp_actors.add(aid)

        # 2. Process updated_actors -> shared state first, then handler dispatch
        #    Updates run before deletions so that handlers processing demolish
        #    notifications can still resolve victim identity via car_actors
        #    even when the victim's car is deleted in the same frame.
        for actor in frame.get("updated_actors", []):
            oid = actor.get("object_id")
            aid = actor["actor_id"]

            # Shared state updates (mutually exclusive obj_ids)
            if oid == scored_obj_id:
                team = actor.get("attribute", {}).get("Byte")
                if team in (0, 1):
                    ctx.is_playing = False
            elif oid == countdown_obj_id:
                val = actor.get("attribute", {}).get("Int")
                if val == 0:
                    ctx.is_playing = True
            elif oid == vehicle_obj_id:
                car_id = actor.get("attribute", {}).get("ActiveActor", {}).get("actor")
                if car_id is not None and car_id >= 0:
                    ctx.resolver.link_component_to_car(aid, car_id)
            elif oid == pri_obj_id:
                pri_actor = (
                    actor.get("attribute", {}).get("ActiveActor", {}).get("actor")
                )
                if pri_actor is not None and pri_actor >= 0 and aid in ctx.car_actors:
                    ctx.resolver.link_car_to_pri(aid, pri_actor)
            elif oid == uid_obj_id:
                uid = actor.get("attribute", {}).get("UniqueId", {})
                identity = _resolve_network_identity(uid)
                if identity:
                    ctx.resolver.set_identity(aid, *identity)
            elif oid == team_paint_obj_id:
                team = actor.get("attribute", {}).get("TeamPaint", {}).get("team")
                if team is not None:
                    ctx.actor_team[aid] = team

            # Position tracking (non-exclusive — runs for any RigidBody update)
            if oid == rb_obj_id:
                loc = actor.get("attribute", {}).get("RigidBody", {}).get("location")
                if loc and "x" in loc and "y" in loc:
                    ctx.actor_position[aid] = (loc["x"], loc["y"])

            # Handler dispatch
            subscribers = update_dispatch.get(oid)
            if subscribers:
                for h in subscribers:
                    h.on_update(ctx, actor)

        # 3. Process deleted_actors -> notify ALL handlers for ALL actors first,
        #    then clean shared state for all. This ensures that if a car and its
        #    boost component are deleted in the same frame, the boost component's
        #    handler can still resolve identity via the car mapping.
        deleted_actors = frame.get("deleted_actors", [])
        for aid in deleted_actors:
            for h in deleted_actor_handlers:
                h.on_deleted_actor(ctx, aid)
        for aid in deleted_actors:
            ctx.car_actors.discard(aid)
            ctx.ball_actors.discard(aid)
            ctx.boost_comp_actors.discard(aid)
            ctx.resolver.remove_actor(aid)
            ctx.actor_team.pop(aid, None)
            ctx.actor_position.pop(aid, None)

    # Finalize all handlers
    poss_result = possession_h.finalize(ctx) if possession_h else (None, None)
    thirds_result = ball_thirds_h.finalize(ctx) if ball_thirds_h else (None, None, None)
    demo_result: Any = demolitions_h.finalize(ctx) if demolitions_h else {}
    boost_result = (
        boost_stats_h.finalize(ctx) if boost_stats_h else (None, None, None, None)
    )
    movement_result: Any = movement_h.finalize(ctx) if movement_h else {}
    demos_recv_result: Any = demos_received_h.finalize(ctx) if demos_received_h else {}
    events_result: Any = match_events_h.finalize(ctx) if match_events_h else []

    return FrameAnalysis(
        team_possession_seconds=poss_result[0],
        opponent_possession_seconds=poss_result[1],
        defensive_third_seconds=thirds_result[0],
        neutral_third_seconds=thirds_result[1],
        offensive_third_seconds=thirds_result[2],
        demolitions=demo_result,
        team_boost_collected=boost_result[0],
        opponent_boost_collected=boost_result[1],
        team_boost_stolen=boost_result[2],
        opponent_boost_stolen=boost_result[3],
        movement_stats=movement_result,
        demos_received=demos_recv_result,
        match_events=events_result,
    )
