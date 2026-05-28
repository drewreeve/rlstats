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

from player_identity import PlayerIdentity, from_network_frame
from rrrocket_schema import FrameData, ParsedReplay, UpdatedActor

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


@dataclass(frozen=True)
class MatchEvent:
    event_type: str
    game_seconds: float
    platform: str
    platform_id: str
    team: int


@dataclass(frozen=True)
class PlayerMovementStats:
    boost_per_minute: float
    avg_speed: float
    time_supersonic_pct: float
    small_pads: int
    large_pads: int
    stolen_small_pads: int
    stolen_large_pads: int


@dataclass(frozen=True)
class PlayerZoneSeconds:
    defensive: float
    neutral: float
    offensive: float


@dataclass(frozen=True)
class PlayerMatchStats:
    """Per-player metrics computed from frame analysis. See CONTEXT.md: Player Match Stats."""

    demos: int = 0
    demos_received: int = 0
    movement: PlayerMovementStats | None = None
    zone_seconds: PlayerZoneSeconds | None = None


@dataclass
class FrameAnalysis:
    team_possession_seconds: float | None = None
    opponent_possession_seconds: float | None = None
    defensive_zone_seconds: float | None = None
    neutral_zone_seconds: float | None = None
    offensive_zone_seconds: float | None = None
    demolitions: dict[tuple[str, str], int] = field(
        default_factory=dict[tuple[str, str], int]
    )
    team_boost_collected: int | None = None
    opponent_boost_collected: int | None = None
    team_boost_stolen: int | None = None
    opponent_boost_stolen: int | None = None
    movement_stats: dict[tuple[str, str], PlayerMovementStats] = field(
        default_factory=dict[tuple[str, str], PlayerMovementStats]
    )
    demos_received: dict[tuple[str, str], int] = field(
        default_factory=dict[tuple[str, str], int]
    )
    match_events: list[MatchEvent] = field(default_factory=list[MatchEvent])
    player_zone_seconds: dict[tuple[str, str], PlayerZoneSeconds] = field(
        default_factory=dict[tuple[str, str], PlayerZoneSeconds]
    )

    def per_player(self) -> dict[PlayerIdentity, PlayerMatchStats]:
        """Assemble per-player match stats keyed by player identity.

        This is the intended interface for external callers (e.g. ingest.py).
        The raw per-player dicts above are populated by handlers and should not
        be accessed outside this module.
        """
        identities = (
            self.demolitions.keys()
            | self.demos_received.keys()
            | self.movement_stats.keys()
            | self.player_zone_seconds.keys()
        )
        return {
            PlayerIdentity(*identity): PlayerMatchStats(
                demos=self.demolitions.get(identity, 0),
                demos_received=self.demos_received.get(identity, 0),
                movement=self.movement_stats.get(identity),
                zone_seconds=self.player_zone_seconds.get(identity),
            )
            for identity in identities
        }


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
    def on_update(self, ctx: FrameContext, actor: UpdatedActor) -> None: ...

    def on_deleted_actor(self, ctx: FrameContext, aid: int) -> None:
        del ctx, aid

    @abstractmethod
    def finalize(self, ctx: FrameContext, result: FrameAnalysis) -> None: ...


def _parse_pickup(
    actor: UpdatedActor,
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


def _resolve_obj_ids(replay: ParsedReplay) -> dict[str, int | None]:
    """Resolve handler-relevant object names to IDs via the replay's object index."""
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
    return {name: replay.object_index.get(name) for name in names}


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

    def on_update(self, ctx: FrameContext, actor: UpdatedActor) -> None:
        team_num = actor.get("attribute", {}).get("Byte")
        if team_num is not None:
            self.touches.append((ctx.frame_time, team_num))

    def finalize(self, ctx: FrameContext, result: FrameAnalysis) -> None:
        if not self.touches:
            return

        possession = {0: 0.0, 1: 0.0}
        for (t_start, team_num), (t_end, _) in pairwise(self.touches):
            possession[team_num] += t_end - t_start

        last_time, last_team = self.touches[-1]
        possession[last_team] += ctx.frame_time - last_time

        team_poss = possession.get(self.tracked_team, 0.0)
        opp_poss = possession.get(1 - self.tracked_team, 0.0)
        result.team_possession_seconds = round(team_poss, 2)
        result.opponent_possession_seconds = round(opp_poss, 2)


_ZONE_BOUNDARY = 1707  # field is ±5120 uu from center; one zone ≈ 5120 / 3


def _accumulate_zone_seconds(
    samples: list[tuple[float, float]], tracked_team: int
) -> dict[str, float]:
    zones = {"defensive": 0.0, "neutral": 0.0, "offensive": 0.0}
    for (t_start, y), (t_end, _) in pairwise(samples):
        dt = t_end - t_start
        if not 0 < dt < 2.0:
            continue
        if tracked_team == 0:
            if y < -_ZONE_BOUNDARY:
                zones["defensive"] += dt
            elif y > _ZONE_BOUNDARY:
                zones["offensive"] += dt
            else:
                zones["neutral"] += dt
        else:
            if y > _ZONE_BOUNDARY:
                zones["defensive"] += dt
            elif y < -_ZONE_BOUNDARY:
                zones["offensive"] += dt
            else:
                zones["neutral"] += dt
    return zones


class BallZonesHandler(FrameHandler):
    """Tracks time the ball spent in each zone of the field."""

    @classmethod
    def create(
        cls, obj_ids: dict[str, int | None], tracked_team: int | None
    ) -> "BallZonesHandler | None":
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

    def on_update(self, ctx: FrameContext, actor: UpdatedActor) -> None:
        if not ctx.is_playing:
            return
        if actor["actor_id"] not in ctx.ball_actors:
            return
        loc = actor.get("attribute", {}).get("RigidBody", {}).get("location")
        if loc and "y" in loc:
            self.samples.append((ctx.frame_time, loc["y"]))

    def finalize(self, ctx: FrameContext, result: FrameAnalysis) -> None:
        if len(self.samples) < 2:
            return
        zones = _accumulate_zone_seconds(self.samples, self.tracked_team)
        result.defensive_zone_seconds = round(zones["defensive"], 2)
        result.neutral_zone_seconds = round(zones["neutral"], 2)
        result.offensive_zone_seconds = round(zones["offensive"], 2)


class PlayerZonesHandler(FrameHandler):
    """Tracks time each player spent in each zone of the field."""

    @classmethod
    def create(
        cls, obj_ids: dict[str, int | None], tracked_team: int | None
    ) -> "PlayerZonesHandler | None":
        if tracked_team is None:
            return None
        rb_obj_id = obj_ids.get("TAGame.RBActor_TA:ReplicatedRBState")
        if rb_obj_id is None:
            return None
        return cls(rb_obj_id, tracked_team)

    def __init__(self, rb_obj_id: int, tracked_team: int) -> None:
        self.update_obj_ids = frozenset({rb_obj_id})
        self.tracked_team = tracked_team
        self.car_samples: dict[int, list[tuple[float, float]]] = {}
        self.identity_zone_times: dict[tuple[str, str], dict[str, float]] = {}

    def _accumulate(
        self, identity: tuple[str, str], samples: list[tuple[float, float]]
    ) -> None:
        new_zones = _accumulate_zone_seconds(samples, self.tracked_team)
        existing = self.identity_zone_times.setdefault(
            identity, {"defensive": 0.0, "neutral": 0.0, "offensive": 0.0}
        )
        for k in new_zones:
            existing[k] += new_zones[k]

    def _flush_car(self, ctx: FrameContext, car_id: int) -> None:
        samples = self.car_samples.pop(car_id, None)
        if not samples:
            return
        identity = ctx.resolver.resolve_car(car_id)
        if identity:
            self._accumulate(identity, samples)

    def on_update(self, ctx: FrameContext, actor: UpdatedActor) -> None:
        if not ctx.is_playing:
            return
        aid = actor["actor_id"]
        if aid not in ctx.car_actors:
            return
        loc = actor.get("attribute", {}).get("RigidBody", {}).get("location")
        if loc and "y" in loc:
            self.car_samples.setdefault(aid, []).append((ctx.frame_time, loc["y"]))

    def on_deleted_actor(self, ctx: FrameContext, aid: int) -> None:
        self._flush_car(ctx, aid)

    def finalize(self, ctx: FrameContext, result: FrameAnalysis) -> None:
        for car_id in list(self.car_samples):
            self._flush_car(ctx, car_id)
        result.player_zone_seconds = {
            identity: PlayerZoneSeconds(
                defensive=round(zones["defensive"], 2),
                neutral=round(zones["neutral"], 2),
                offensive=round(zones["offensive"], 2),
            )
            for identity, zones in self.identity_zone_times.items()
        }


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

    def on_update(self, ctx: FrameContext, actor: UpdatedActor) -> None:
        val = actor.get("attribute", {}).get("Int", 0)
        aid = actor["actor_id"]
        self.actor_demos[aid] = max(self.actor_demos.get(aid, 0), val)

    def finalize(self, ctx: FrameContext, result: FrameAnalysis) -> None:
        for aid, count in self.actor_demos.items():
            identity = ctx.resolver.resolve_pri(aid)
            if identity:
                result.demolitions[identity] = count


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

    def on_update(self, ctx: FrameContext, actor: UpdatedActor) -> None:
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

    def finalize(self, ctx: FrameContext, result: FrameAnalysis) -> None:
        result.demos_received = self.demos_received


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

    def on_update(self, ctx: FrameContext, actor: UpdatedActor) -> None:
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

    def finalize(self, ctx: FrameContext, result: FrameAnalysis) -> None:
        if self.collected[0] == 0 and self.collected[1] == 0:
            return

        result.team_boost_collected = self.collected[self.tracked_team]
        result.opponent_boost_collected = self.collected[1 - self.tracked_team]
        result.team_boost_stolen = self.stolen[self.tracked_team]
        result.opponent_boost_stolen = self.stolen[1 - self.tracked_team]


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

    def on_update(self, ctx: FrameContext, actor: UpdatedActor) -> None:
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

    def finalize(self, ctx: FrameContext, result: FrameAnalysis) -> None:
        # Deleted-actor data already accumulated; handle remaining live actors
        for comp_id, consumed in self.comp_boost_consumed.items():
            self._flush_boost_comp(ctx, comp_id, consumed)

        for car_id, samples in self.car_speed_samples.items():
            self._flush_speed_samples(ctx, car_id, samples)

        all_identities = (
            set(self.identity_boost_consumed.keys())
            | set(self.identity_speeds.keys())
            | set(self.identity_pads.keys())
        )

        for identity in all_identities:
            consumed = self.identity_boost_consumed.get(identity, 0.0)
            boost_per_minute = round((consumed / 255 * 100) / (self.duration / 60), 1)

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
                avg_speed = (
                    round(weighted_sum / total_weight) if total_weight > 0 else 0
                )
                time_supersonic_pct = round(supersonic_time / self.duration * 100, 1)
            else:
                avg_speed = 0
                time_supersonic_pct = 0.0

            pad = self.identity_pads.get(identity, {})
            result.movement_stats[identity] = PlayerMovementStats(
                boost_per_minute=boost_per_minute,
                avg_speed=avg_speed,
                time_supersonic_pct=time_supersonic_pct,
                small_pads=pad.get("small_pads", 0),
                large_pads=pad.get("large_pads", 0),
                stolen_small_pads=pad.get("stolen_small_pads", 0),
                stolen_large_pads=pad.get("stolen_large_pads", 0),
            )


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

    def on_update(self, ctx: FrameContext, actor: UpdatedActor) -> None:
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

    def finalize(self, ctx: FrameContext, result: FrameAnalysis) -> None:
        if not self.clock_updates or not self.raw_events:
            return

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
            return

        def resolve_team(aid: int) -> int | None:
            ta = self.actor_team_actor.get(aid)
            if ta is None:
                return None
            return (
                self.tracked_team
                if ta == tracked_team_actor
                else (1 - self.tracked_team)
            )

        for event_type, ft, aid in self.raw_events:
            identity = ctx.resolver.resolve_pri(aid)
            team = resolve_team(aid)
            if identity is None or team is None:
                continue
            gs = frame_to_game_seconds(ft)
            result.match_events.append(
                MatchEvent(event_type, gs, identity[0], identity[1], team)
            )


# -- Orchestrator --


@dataclass(frozen=True)
class _FrameLoopObjectIds:
    """Object IDs resolved from the replay's objects list, used by _process_frame."""

    car_archetype: int | None
    ball_archetype: int | None
    boost_comp_archetype: int | None
    scored_obj_id: int | None
    countdown_obj_id: int | None
    vehicle_obj_id: int | None
    pri_obj_id: int | None
    uid_obj_id: int | None
    team_paint_obj_id: int | None
    rb_obj_id: int | None


def _process_frame(
    ctx: FrameContext,
    frame: FrameData,
    obj_ids: _FrameLoopObjectIds,
    update_dispatch: dict[int, list[FrameHandler]],
    deleted_actor_handlers: list[FrameHandler],
) -> None:
    """Apply one frame to ctx, enforcing the three-phase ordering contract:

    1. new_actors    — register archetypes into FrameContext
    2. updated_actors — shared state first, then handler dispatch
    3. deleted_actors — notify ALL handlers (two-pass), then purge ALL actor state
    """
    ctx.frame_time = frame["time"]

    # 1. Process new_actors -> update shared archetype sets
    for new_actor in frame.get("new_actors", []):
        oid = new_actor.get("object_id")
        aid = new_actor["actor_id"]
        if oid == obj_ids.car_archetype:
            ctx.car_actors.add(aid)
        elif oid == obj_ids.ball_archetype:
            ctx.ball_actors.add(aid)
        elif oid == obj_ids.boost_comp_archetype:
            ctx.boost_comp_actors.add(aid)

    # 2. Process updated_actors -> shared state first, then handler dispatch.
    #    Updates run before deletions so that handlers processing demolish
    #    notifications can still resolve victim identity via car_actors
    #    even when the victim's car is deleted in the same frame.
    for actor in frame.get("updated_actors", []):
        oid = actor.get("object_id")
        aid = actor["actor_id"]

        # Shared state updates (mutually exclusive obj_ids)
        if oid == obj_ids.scored_obj_id:
            team = actor.get("attribute", {}).get("Byte")
            if team in (0, 1):
                ctx.is_playing = False
        elif oid == obj_ids.countdown_obj_id:
            val = actor.get("attribute", {}).get("Int")
            if val == 0:
                ctx.is_playing = True
        elif oid == obj_ids.vehicle_obj_id:
            car_id = actor.get("attribute", {}).get("ActiveActor", {}).get("actor")
            if car_id is not None and car_id >= 0:
                ctx.resolver.link_component_to_car(aid, car_id)
        elif oid == obj_ids.pri_obj_id:
            pri_actor = actor.get("attribute", {}).get("ActiveActor", {}).get("actor")
            if pri_actor is not None and pri_actor >= 0 and aid in ctx.car_actors:
                ctx.resolver.link_car_to_pri(aid, pri_actor)
        elif oid == obj_ids.uid_obj_id:
            uid = actor.get("attribute", {}).get("UniqueId", {})
            identity = from_network_frame(uid)
            if identity:
                ctx.resolver.set_identity(aid, *identity)
        elif oid == obj_ids.team_paint_obj_id:
            team = actor.get("attribute", {}).get("TeamPaint", {}).get("team")
            if team is not None:
                ctx.actor_team[aid] = team

        # Position tracking (non-exclusive — runs for any RigidBody update)
        if oid == obj_ids.rb_obj_id:
            loc = actor.get("attribute", {}).get("RigidBody", {}).get("location")
            if loc and "x" in loc and "y" in loc:
                ctx.actor_position[aid] = (loc["x"], loc["y"])

        # Handler dispatch
        subscribers = update_dispatch.get(oid) if oid is not None else None
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


def analyze_frames(
    replay: ParsedReplay,
    tracked_team: int | None,
    tracked_identities: set[tuple[str, str]],
    duration: int | None,
    game_mode: str | None,
) -> FrameAnalysis:

    frames = replay.frames

    if not replay.object_index or not frames:
        return FrameAnalysis()

    obj_ids = _resolve_obj_ids(replay)

    loop_obj_ids = _FrameLoopObjectIds(
        car_archetype=obj_ids.get("Archetypes.Car.Car_Default"),
        ball_archetype=obj_ids.get("Archetypes.Ball.Ball_Default"),
        boost_comp_archetype=obj_ids.get("Archetypes.CarComponents.CarComponent_Boost"),
        scored_obj_id=obj_ids.get("TAGame.GameEvent_Soccar_TA:ReplicatedScoredOnTeam"),
        countdown_obj_id=obj_ids.get(
            "TAGame.GameEvent_TA:ReplicatedRoundCountDownNumber"
        ),
        vehicle_obj_id=obj_ids.get("TAGame.CarComponent_TA:Vehicle"),
        pri_obj_id=obj_ids.get("Engine.Pawn:PlayerReplicationInfo"),
        uid_obj_id=obj_ids.get("Engine.PlayerReplicationInfo:UniqueId"),
        team_paint_obj_id=obj_ids.get("TAGame.Car_TA:TeamPaint"),
        rb_obj_id=obj_ids.get("TAGame.RBActor_TA:ReplicatedRBState"),
    )

    big_pads = BIG_PAD_POSITIONS["hoops" if game_mode == "hoops" else "standard"]

    handlers: list[FrameHandler] = [
        h
        for h in [
            PossessionHandler.create(obj_ids, tracked_team),
            BallZonesHandler.create(obj_ids, tracked_team),
            PlayerZonesHandler.create(obj_ids, tracked_team),
            DemolitionsHandler.create(obj_ids),
            BoostStatsHandler.create(obj_ids, tracked_team, big_pads),
            MovementHandler.create(obj_ids, duration, big_pads),
            DemosReceivedHandler.create(obj_ids),
            MatchEventsHandler.create(obj_ids, tracked_team, tracked_identities),
        ]
        if h is not None
    ]

    # Build dispatch table: object_id -> list of handlers interested in that obj_id
    update_dispatch: dict[int, list[FrameHandler]] = {}
    for h in handlers:
        for obj_id in h.update_obj_ids:
            update_dispatch.setdefault(obj_id, []).append(h)

    # Only handlers that override on_deleted_actor need to be called on deletions
    deleted_actor_handlers = [
        h
        for h in handlers
        if type(h).on_deleted_actor is not FrameHandler.on_deleted_actor
    ]

    ctx = FrameContext()

    for frame in frames:
        _process_frame(
            ctx, frame, loop_obj_ids, update_dispatch, deleted_actor_handlers
        )

    fa = FrameAnalysis()
    for h in handlers:
        h.finalize(ctx, fa)
    return fa
