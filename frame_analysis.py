# Frame Analysis Module
# Extracts statistics from rrrocket network frame data

import math
from dataclasses import dataclass, field
from itertools import pairwise
from typing import Any, NamedTuple


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
    match_events: list[tuple[str, float, str, str, int]]


@dataclass
class FrameContext:
    """Shared state maintained by the orchestrator loop."""

    # Actor archetype sets
    car_actors: set[int] = field(default_factory=set)
    ball_actors: set[int] = field(default_factory=set)
    boost_comp_actors: set[int] = field(default_factory=set)

    # Identity resolution chain
    car_to_pri: dict[int, int] = field(default_factory=dict)
    pri_identity: dict[int, tuple[str, str]] = field(default_factory=dict)
    component_to_car: dict[int, int] = field(default_factory=dict)

    # Per-actor state
    actor_team: dict[int, int] = field(default_factory=dict)
    actor_position: dict[int, tuple[float, float]] = field(default_factory=dict)

    # Game state
    is_playing: bool = False
    frame_time: float = 0.0

    def resolve_car_identity(self, car_id: int) -> tuple[str, str] | None:
        pri = self.car_to_pri.get(car_id)
        if pri is None or pri < 0:
            return None
        return self.pri_identity.get(pri)


@dataclass
class Handler:
    """Lightweight handler registration."""

    update_obj_ids: set[int] = field(default_factory=set)
    on_update: Any = None  # (ctx, actor) -> None
    on_new_actor: Any = None  # (ctx, new_actor) -> None
    on_deleted_actor: Any = None  # (ctx, actor_id) -> None
    finalize: Any = None  # (ctx) -> result


def _resolve_network_identity(
    uid: dict[str, Any],
) -> tuple[str, str] | None:
    """Resolve a UniqueId attribute from network frames to (platform, platform_id)."""
    remote = uid.get("remote_id", {})
    if not remote:
        return None
    platform_key = next(iter(remote))
    platform = NETWORK_PLATFORM_MAP.get(platform_key)
    if not platform:
        return None
    value = remote[platform_key]
    if isinstance(value, dict):
        platform_id = value.get("online_id")
    else:
        platform_id = value
    if not platform_id:
        return None
    return (platform, str(platform_id))


def _parse_pickup(
    actor: dict,
    last_pickup_state: dict[int, int],
    actor_team: dict[int, int],
    actor_position: dict[int, tuple[float, float]],
    big_pads: list[tuple[float, float]],
) -> tuple[int, int, bool, bool] | None:
    """Parse a NewReplicatedPickupData actor update.

    Returns (instigator_actor_id, team, is_big, is_stolen) or None to skip.
    """
    aid = actor["actor_id"]
    pickup = actor.get("attribute", {}).get("PickupNew", {})
    picked_up_state = pickup.get("picked_up")
    if picked_up_state is None or picked_up_state == 255:
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
    }
    index = {name: i for i, name in enumerate(objects) if name in names}
    return {name: index.get(name) for name in names}


# -- Handler factories --


def _make_possession_handler(
    obj_ids: dict[str, int | None], tracked_team: int | None
) -> Handler | None:
    if tracked_team is None:
        return None
    hit_team_obj_id = obj_ids.get("TAGame.Ball_TA:HitTeamNum")
    if hit_team_obj_id is None:
        return None

    touches: list[tuple[float, int]] = []

    def on_update(ctx: FrameContext, actor: dict):
        team_num = actor.get("attribute", {}).get("Byte")
        if team_num is not None:
            touches.append((ctx.frame_time, team_num))

    def finalize(ctx: FrameContext, last_frame_time: float):
        if not touches:
            return None, None

        possession = {0: 0.0, 1: 0.0}
        for (t_start, team_num), (t_end, _) in pairwise(touches):
            possession[team_num] += t_end - t_start

        last_time, last_team = touches[-1]
        possession[last_team] += ctx.frame_time - last_time

        team_poss = possession.get(tracked_team, 0.0)
        opp_poss = possession.get(1 - tracked_team, 0.0)
        return round(team_poss, 2), round(opp_poss, 2)

    return Handler(
        update_obj_ids={hit_team_obj_id},
        on_update=on_update,
        finalize=finalize,
    )


def _make_ball_thirds_handler(
    obj_ids: dict[str, int | None], tracked_team: int | None
) -> Handler | None:
    if tracked_team is None:
        return None
    rb_obj_id = obj_ids.get("TAGame.RBActor_TA:ReplicatedRBState")
    ball_archetype = obj_ids.get("Archetypes.Ball.Ball_Default")
    if rb_obj_id is None or ball_archetype is None:
        return None

    samples: list[tuple[float, float]] = []

    def on_update(ctx: FrameContext, actor: dict):
        if actor["actor_id"] not in ctx.ball_actors:
            return
        loc = actor.get("attribute", {}).get("RigidBody", {}).get("location")
        if loc and "y" in loc:
            samples.append((ctx.frame_time, loc["y"]))

    def finalize(ctx: FrameContext, last_frame_time: float):
        if len(samples) < 2:
            return None, None, None

        THIRD_BOUNDARY = 1707
        zones = {"negative": 0.0, "neutral": 0.0, "positive": 0.0}
        for (t_start, y), (t_end, _) in pairwise(samples):
            dt = t_end - t_start
            if dt <= 0:
                continue
            if y < -THIRD_BOUNDARY:
                zones["negative"] += dt
            elif y > THIRD_BOUNDARY:
                zones["positive"] += dt
            else:
                zones["neutral"] += dt

        if tracked_team == 0:
            defensive = zones["negative"]
            offensive = zones["positive"]
        else:
            defensive = zones["positive"]
            offensive = zones["negative"]

        return round(defensive, 2), round(zones["neutral"], 2), round(offensive, 2)

    return Handler(
        update_obj_ids={rb_obj_id},
        on_update=on_update,
        finalize=finalize,
    )


def _make_demolitions_handler(
    obj_ids: dict[str, int | None],
) -> Handler | None:
    demo_obj_id = obj_ids.get("TAGame.PRI_TA:MatchDemolishes")
    if demo_obj_id is None:
        return None

    actor_demos: dict[int, int] = {}

    def on_update(ctx: FrameContext, actor: dict):
        val = actor.get("attribute", {}).get("Int", 0)
        aid = actor["actor_id"]
        actor_demos[aid] = max(actor_demos.get(aid, 0), val)

    def finalize(ctx: FrameContext, last_frame_time: float):
        result: dict[tuple[str, str], int] = {}
        for aid, count in actor_demos.items():
            identity = ctx.pri_identity.get(aid)
            if identity:
                result[identity] = count
        return result

    return Handler(
        update_obj_ids={demo_obj_id},
        on_update=on_update,
        finalize=finalize,
    )


def _make_boost_stats_handler(
    obj_ids: dict[str, int | None],
    tracked_team: int | None,
    big_pads: list[tuple[float, float]],
) -> Handler | None:
    if tracked_team is None:
        return None
    pickup_obj_id = obj_ids.get("TAGame.VehiclePickup_TA:NewReplicatedPickupData")
    if pickup_obj_id is None:
        return None

    last_pickup_state: dict[int, int] = {}
    collected = {0: 0, 1: 0}
    stolen = {0: 0, 1: 0}

    def on_deleted_actor(ctx: FrameContext, aid: int):
        last_pickup_state.pop(aid, None)

    def on_update(ctx: FrameContext, actor: dict):
        result = _parse_pickup(
            actor, last_pickup_state, ctx.actor_team, ctx.actor_position, big_pads
        )
        if result is None:
            return
        _, team, is_big, is_stolen = result
        boost_value = 100 if is_big else 12
        collected[team] += boost_value
        if is_stolen:
            stolen[team] += boost_value

    def finalize(ctx: FrameContext, last_frame_time: float):
        if collected[0] == 0 and collected[1] == 0:
            return None, None, None, None

        team_collected = collected[tracked_team]
        opp_collected = collected[1 - tracked_team]
        team_stolen = stolen[tracked_team]
        opp_stolen = stolen[1 - tracked_team]
        return team_collected, opp_collected, team_stolen, opp_stolen

    return Handler(
        update_obj_ids={pickup_obj_id},
        on_update=on_update,
        on_deleted_actor=on_deleted_actor,
        finalize=finalize,
    )


def _make_movement_handler(
    obj_ids: dict[str, int | None],
    duration: int | None,
    big_pads: list[tuple[float, float]],
) -> Handler | None:
    if not duration or duration <= 0:
        return None

    rb_obj_id = obj_ids.get("TAGame.RBActor_TA:ReplicatedRBState")
    boost_obj_id = obj_ids.get("TAGame.CarComponent_Boost_TA:ReplicatedBoost")
    pickup_obj_id = obj_ids.get("TAGame.VehiclePickup_TA:NewReplicatedPickupData")

    if rb_obj_id is None or boost_obj_id is None or pickup_obj_id is None:
        return None

    # Per boost-component state
    comp_boost: dict[int, int] = {}
    comp_boost_consumed: dict[int, float] = {}
    car_speed_samples: dict[int, list[tuple[float, float]]] = {}
    identity_pads: dict[tuple[str, str], dict[str, int]] = {}
    last_pickup_state: dict[int, int] = {}

    finalized_boost: list[tuple[tuple[str, str], float]] = []
    finalized_speeds: list[tuple[tuple[str, str], list[tuple[float, float]]]] = []

    update_ids = {rb_obj_id, boost_obj_id, pickup_obj_id}

    def on_deleted_actor(ctx: FrameContext, aid: int):
        comp_boost.pop(aid, None)
        last_pickup_state.pop(aid, None)
        consumed = comp_boost_consumed.pop(aid, None)
        if consumed:
            car_id = ctx.component_to_car.pop(aid, None)
            if car_id is not None:
                identity = ctx.resolve_car_identity(car_id)
                if identity:
                    finalized_boost.append((identity, consumed))
        samples = car_speed_samples.pop(aid, None)
        if samples:
            identity = ctx.resolve_car_identity(aid)
            if identity:
                finalized_speeds.append((identity, samples))

    def on_update(ctx: FrameContext, actor: dict):
        oid = actor.get("object_id")
        aid = actor["actor_id"]

        if oid == boost_obj_id and ctx.is_playing and aid in ctx.boost_comp_actors:
            amount = (
                actor.get("attribute", {})
                .get("ReplicatedBoost", {})
                .get("boost_amount")
            )
            if amount is None:
                return
            prev = comp_boost.get(aid)
            comp_boost[aid] = amount
            if prev is not None and amount < prev:
                comp_boost_consumed[aid] = comp_boost_consumed.get(aid, 0.0) + (
                    prev - amount
                )

        elif oid == rb_obj_id:
            if aid in ctx.car_actors and ctx.is_playing:
                lv = (
                    actor.get("attribute", {})
                    .get("RigidBody", {})
                    .get("linear_velocity")
                )
                if lv is not None and "x" in lv and "y" in lv and "z" in lv:
                    speed = math.sqrt(lv["x"] ** 2 + lv["y"] ** 2 + lv["z"] ** 2)
                    if aid not in car_speed_samples:
                        car_speed_samples[aid] = []
                    car_speed_samples[aid].append((ctx.frame_time, speed))

        elif oid == pickup_obj_id and ctx.is_playing:
            result = _parse_pickup(
                actor, last_pickup_state, ctx.actor_team, ctx.actor_position, big_pads
            )
            if result is None:
                return
            instigator, team, is_big, is_stolen = result
            identity = ctx.resolve_car_identity(instigator)
            if identity is None:
                return
            pads = identity_pads.setdefault(
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

    def finalize(ctx: FrameContext, last_frame_time: float):
        # Build car -> identity from car_to_pri + pri_identity
        car_identity: dict[int, tuple[str, str]] = {}
        for car_id, pri_actor in ctx.car_to_pri.items():
            if pri_actor >= 0:
                identity = ctx.pri_identity.get(pri_actor)
                if identity:
                    car_identity[car_id] = identity

        # Aggregate boost consumed per identity
        identity_boost_consumed: dict[tuple[str, str], float] = {}
        for identity, consumed in finalized_boost:
            identity_boost_consumed[identity] = (
                identity_boost_consumed.get(identity, 0.0) + consumed
            )
        for comp_id, consumed in comp_boost_consumed.items():
            car_id = ctx.component_to_car.get(comp_id)
            if car_id is not None:
                identity = car_identity.get(car_id)
                if identity:
                    identity_boost_consumed[identity] = (
                        identity_boost_consumed.get(identity, 0.0) + consumed
                    )

        # Aggregate speed samples per identity
        identity_speeds: dict[tuple[str, str], list[tuple[float, float]]] = {}
        for identity, samples in finalized_speeds:
            if identity not in identity_speeds:
                identity_speeds[identity] = []
            identity_speeds[identity].extend(samples)
        for car_id, samples in car_speed_samples.items():
            identity = car_identity.get(car_id)
            if identity:
                if identity not in identity_speeds:
                    identity_speeds[identity] = []
                identity_speeds[identity].extend(samples)

        # Build results
        all_identities = (
            set(identity_boost_consumed.keys())
            | set(identity_speeds.keys())
            | set(identity_pads.keys())
        )
        result: dict[tuple[str, str], dict[str, float | int]] = {}

        for identity in all_identities:
            stats: dict[str, float] = {}

            consumed = identity_boost_consumed.get(identity, 0.0)
            stats["boost_per_minute"] = round(
                (consumed / 255 * 100) / (duration / 60), 1
            )

            speeds = identity_speeds.get(identity, [])
            if speeds:
                speeds.sort(key=lambda s: s[0])
                total_weight = 0.0
                weighted_sum = 0.0
                supersonic_time = 0.0
                for (t1, s1), (t2, _) in pairwise(speeds):
                    dt = t2 - t1
                    if 0 < dt < 5:
                        weighted_sum += s1 * dt
                        total_weight += dt
                        if s1 >= 2200:
                            supersonic_time += dt
                stats["avg_speed"] = (
                    round(weighted_sum / total_weight) if total_weight > 0 else 0
                )
                stats["time_supersonic_pct"] = round(
                    supersonic_time / duration * 100, 1
                )
            else:
                stats["avg_speed"] = 0
                stats["time_supersonic_pct"] = 0.0

            pad = identity_pads.get(identity, {})
            stats["small_pads"] = pad.get("small_pads", 0)
            stats["large_pads"] = pad.get("large_pads", 0)
            stats["stolen_small_pads"] = pad.get("stolen_small_pads", 0)
            stats["stolen_large_pads"] = pad.get("stolen_large_pads", 0)

            result[identity] = stats

        return result

    return Handler(
        update_obj_ids=update_ids,
        on_update=on_update,
        on_deleted_actor=on_deleted_actor,
        finalize=finalize,
    )


def _make_match_events_handler(
    obj_ids: dict[str, int | None],
    tracked_team: int | None,
    tracked_identities: set[tuple[str, str]],
) -> Handler | None:
    if tracked_team is None:
        return None

    sr_obj_id = obj_ids.get("TAGame.GameEvent_Soccar_TA:SecondsRemaining")
    team_obj_id = obj_ids.get("Engine.PlayerReplicationInfo:Team")
    if sr_obj_id is None or team_obj_id is None:
        return None

    counter_names = {
        "TAGame.PRI_TA:MatchGoals": "goal",
        "TAGame.PRI_TA:MatchShots": "shot",
        "TAGame.PRI_TA:MatchSaves": "save",
        "TAGame.PRI_TA:MatchDemolishes": "demo",
    }
    counter_obj_ids: dict[int, str] = {}
    for obj_name, event_type in counter_names.items():
        oid = obj_ids.get(obj_name)
        if oid is not None:
            counter_obj_ids[oid] = event_type

    if not counter_obj_ids:
        return None

    clock_updates: list[tuple[float, int]] = []
    actor_team_actor: dict[int, int] = {}
    actor_counters: dict[int, dict[str, int]] = {}
    raw_events: list[tuple[str, float, int]] = []

    update_ids: set[int] = {sr_obj_id, team_obj_id}
    update_ids.update(counter_obj_ids.keys())

    def on_update(ctx: FrameContext, actor: dict):
        obj_id = actor.get("object_id")
        aid = actor["actor_id"]

        if obj_id == sr_obj_id:
            sr = actor.get("attribute", {}).get("Int")
            if sr is not None:
                clock_updates.append((ctx.frame_time, sr))
        elif obj_id == team_obj_id:
            team_actor = actor.get("attribute", {}).get("ActiveActor", {}).get("actor")
            if team_actor is not None:
                actor_team_actor[aid] = team_actor
        elif obj_id in counter_obj_ids:
            event_type = counter_obj_ids[obj_id]
            val = actor.get("attribute", {}).get("Int", 0)
            if aid not in actor_counters:
                actor_counters[aid] = {}
            prev = actor_counters[aid].get(event_type, 0)
            if val > prev:
                for _ in range(val - prev):
                    raw_events.append((event_type, ctx.frame_time, aid))
            actor_counters[aid][event_type] = val

    def finalize(ctx: FrameContext, last_frame_time: float):
        if not clock_updates or not raw_events:
            return []

        game_start = clock_updates[0][1]

        seen_zero = False
        clock_game_seconds: list[tuple[float, float]] = []
        for c_ft, sr in clock_updates:
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
        for aid, identity in ctx.pri_identity.items():
            if identity in tracked_identities and aid in actor_team_actor:
                tracked_team_actor = actor_team_actor[aid]
                break

        if tracked_team_actor is None:
            return []

        def resolve_team(aid: int) -> int | None:
            ta = actor_team_actor.get(aid)
            if ta is None:
                return None
            return tracked_team if ta == tracked_team_actor else (1 - tracked_team)

        events: list[tuple[str, float, str, str, int]] = []
        for event_type, ft, aid in raw_events:
            identity = ctx.pri_identity.get(aid)
            team = resolve_team(aid)
            if identity is None or team is None:
                continue
            gs = frame_to_game_seconds(ft)
            events.append((event_type, gs, identity[0], identity[1], team))

        return events

    return Handler(
        update_obj_ids=update_ids,
        on_update=on_update,
        finalize=finalize,
    )


# -- Orchestrator --


def analyze_frames(
    replay: dict,
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
    possession_h = _make_possession_handler(obj_ids, tracked_team)
    ball_thirds_h = _make_ball_thirds_handler(obj_ids, tracked_team)
    demolitions_h = _make_demolitions_handler(obj_ids)
    boost_stats_h = _make_boost_stats_handler(obj_ids, tracked_team, big_pads)
    movement_h = _make_movement_handler(obj_ids, duration, big_pads)
    match_events_h = _make_match_events_handler(
        obj_ids, tracked_team, tracked_identities
    )

    handlers = [
        h
        for h in [
            possession_h,
            ball_thirds_h,
            demolitions_h,
            boost_stats_h,
            movement_h,
            match_events_h,
        ]
        if h is not None
    ]

    # Build dispatch table: object_id -> list of handler on_update functions
    update_dispatch: dict[int, list] = {}
    for h in handlers:
        if h.on_update:
            for oid in h.update_obj_ids:
                if oid not in update_dispatch:
                    update_dispatch[oid] = []
                update_dispatch[oid].append(h.on_update)

    # Handlers with on_new_actor / on_deleted_actor callbacks
    new_actor_handlers = [h for h in handlers if h.on_new_actor]
    deleted_actor_handlers = [h for h in handlers if h.on_deleted_actor]

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
            for h_cb in new_actor_handlers:
                h_cb.on_new_actor(ctx, new_actor)

        # 2. Process deleted_actors -> notify handlers first, then clean shared state
        for aid in frame.get("deleted_actors", []):
            for h in deleted_actor_handlers:
                h.on_deleted_actor(ctx, aid)
            ctx.car_actors.discard(aid)
            ctx.ball_actors.discard(aid)
            ctx.boost_comp_actors.discard(aid)
            ctx.actor_team.pop(aid, None)
            ctx.actor_position.pop(aid, None)

        # 3. Process updated_actors -> shared state first, then handler dispatch
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
                    ctx.component_to_car[aid] = car_id
            elif oid == pri_obj_id:
                pri_actor = (
                    actor.get("attribute", {}).get("ActiveActor", {}).get("actor")
                )
                if pri_actor is not None and pri_actor >= 0 and aid in ctx.car_actors:
                    ctx.car_to_pri[aid] = pri_actor
            elif oid == uid_obj_id:
                uid = actor.get("attribute", {}).get("UniqueId", {})
                identity = _resolve_network_identity(uid)
                if identity:
                    ctx.pri_identity[aid] = identity
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
            callbacks = update_dispatch.get(oid)
            if callbacks:
                for cb in callbacks:
                    cb(ctx, actor)

    # Finalize all handlers
    poss_result = (
        possession_h.finalize(ctx, ctx.frame_time) if possession_h else (None, None)
    )
    thirds_result = (
        ball_thirds_h.finalize(ctx, ctx.frame_time)
        if ball_thirds_h
        else (None, None, None)
    )
    demo_result = demolitions_h.finalize(ctx, ctx.frame_time) if demolitions_h else {}
    boost_result = (
        boost_stats_h.finalize(ctx, ctx.frame_time)
        if boost_stats_h
        else (None, None, None, None)
    )
    movement_result = (
        movement_h.finalize(ctx, ctx.frame_time) if movement_h else {}
    )
    events_result = (
        match_events_h.finalize(ctx, ctx.frame_time) if match_events_h else []
    )

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
        match_events=events_result,
    )
