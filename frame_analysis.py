# Frame Analysis Module
# Extracts statistics from rrrocket network frame data

import math
from itertools import pairwise
from typing import Any


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


def _resolve_network_identity(
    uid: dict[str, Any],
) -> tuple[str, str] | None:
    """Resolve a UniqueId attribute from network frames to (platform, platform_id).

    Handles both string remote_ids (Steam, Epic) and dict remote_ids with
    an 'online_id' field (PlayStation, PsyNet).
    """
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


def _calculate_possession(
    replay: dict, tracked_team: int | None
) -> tuple[float | None, float | None]:
    """Calculate ball possession seconds per team from network frame data.

    Returns (team_possession_seconds, opponent_possession_seconds) or (None, None)
    if network data is unavailable.
    """
    if tracked_team is None:
        return None, None

    objects = replay.get("objects")
    frames = replay.get("network_frames", {}).get("frames")
    if not objects or not frames:
        return None, None

    try:
        hit_team_obj_id = objects.index("TAGame.Ball_TA:HitTeamNum")
    except ValueError:
        return None, None

    # Collect (time, team) for each HitTeamNum update
    touches: list[tuple[float, int]] = []
    for frame in frames:
        for actor in frame.get("updated_actors", []):
            if actor.get("object_id") == hit_team_obj_id:
                team_num = actor.get("attribute", {}).get("Byte")
                if team_num is not None:
                    touches.append((frame["time"], team_num))

    if not touches:
        return None, None

    # Calculate possession between consecutive touches
    possession = {0: 0.0, 1: 0.0}
    for (t_start, team_num), (t_end, _) in pairwise(touches):
        possession[team_num] += t_end - t_start

    # Last touch to end of match
    last_time, last_team = touches[-1]
    match_end = frames[-1]["time"]
    possession[last_team] += match_end - last_time

    team_poss = possession.get(tracked_team, 0.0)
    opp_poss = possession.get(1 - tracked_team, 0.0)
    return round(team_poss, 2), round(opp_poss, 2)


def _calculate_ball_thirds(
    replay: dict, tracked_team: int | None
) -> tuple[float | None, float | None, float | None]:
    """Calculate time the ball spends in each third of the field.

    Returns (defensive_seconds, neutral_seconds, offensive_seconds) from the
    tracked team's perspective, or (None, None, None) if data is unavailable.
    """
    if tracked_team is None:
        return None, None, None

    objects = replay.get("objects")
    frames = replay.get("network_frames", {}).get("frames")
    if not objects or not frames:
        return None, None, None

    # Find ball archetype to identify ball actors
    try:
        ball_archetype = objects.index("Archetypes.Ball.Ball_Default")
    except ValueError:
        return None, None, None

    # Find the RigidBody replication object ID
    try:
        rb_obj_id = objects.index("TAGame.RBActor_TA:ReplicatedRBState")
    except ValueError:
        return None, None, None

    # Track all ball actor IDs (ball is recreated after goals) and collect
    # (time, y) samples from position updates
    ball_actor_ids: set[int] = set()
    samples: list[tuple[float, float]] = []
    for frame in frames:
        for new_actor in frame.get("new_actors", []):
            if new_actor.get("object_id") == ball_archetype:
                ball_actor_ids.add(new_actor.get("actor_id"))
        for actor in frame.get("updated_actors", []):
            if (
                actor.get("actor_id") in ball_actor_ids
                and actor.get("object_id") == rb_obj_id
            ):
                loc = actor.get("attribute", {}).get("RigidBody", {}).get("location")
                if loc and "y" in loc:
                    samples.append((frame["time"], loc["y"]))

    if len(samples) < 2:
        return None, None, None

    # Y boundary for thirds: field is ~10240 units long, split at ±1707
    THIRD_BOUNDARY = 1707

    # Accumulate time in each zone
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

    # Map to team-relative: team 0 defends negative Y, team 1 defends positive Y
    if tracked_team == 0:
        defensive = zones["negative"]
        offensive = zones["positive"]
    else:
        defensive = zones["positive"]
        offensive = zones["negative"]

    return round(defensive, 2), round(zones["neutral"], 2), round(offensive, 2)


def _extract_demolitions(replay: dict) -> dict[tuple[str, str], int]:
    """Extract per-player demolition counts from network frame data.

    Returns a dict mapping (platform, platform_id) -> demo count,
    or {} if network data is unavailable.
    """
    objects = replay.get("objects")
    frames = replay.get("network_frames", {}).get("frames")
    if not objects or not frames:
        return {}

    try:
        demo_obj_id = objects.index("TAGame.PRI_TA:MatchDemolishes")
        uid_obj_id = objects.index("Engine.PlayerReplicationInfo:UniqueId")
    except ValueError:
        return {}

    actor_identity: dict[int, tuple[str, str]] = {}
    actor_demos: dict[int, int] = {}

    for frame in frames:
        for actor in frame.get("updated_actors", []):
            obj_id = actor.get("object_id")
            if obj_id == uid_obj_id:
                uid = actor.get("attribute", {}).get("UniqueId", {})
                identity = _resolve_network_identity(uid)
                if identity:
                    actor_identity[actor["actor_id"]] = identity
            elif obj_id == demo_obj_id:
                val = actor.get("attribute", {}).get("Int", 0)
                aid = actor["actor_id"]
                actor_demos[aid] = max(actor_demos.get(aid, 0), val)

    result: dict[tuple[str, str], int] = {}
    for aid, count in actor_demos.items():
        identity = actor_identity.get(aid)
        if identity:
            result[identity] = count
    return result


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
    pos = actor_position.get(
        instigator
    )  # pad actors don't emit RigidBody; use car position
    if team is None or pos is None:
        return None

    x, y = pos
    is_big = any(
        (x - bx) ** 2 + (y - by) ** 2 <= BIG_PAD_RADIUS_SQ for bx, by in big_pads
    )
    is_stolen = (team == 0 and y > 0) or (team == 1 and y < 0)
    return instigator, team, is_big, is_stolen


def _extract_boost_stats(
    replay: dict, tracked_team: int | None, game_mode: str | None
) -> tuple[int | None, int | None, int | None, int | None]:
    """Extract per-team boost collected and boost stolen from network frame data.

    Returns (team_collected, opp_collected, team_stolen, opp_stolen)
    or (None, None, None, None) if data is unavailable.
    """
    if tracked_team is None:
        return None, None, None, None

    objects = replay.get("objects")
    frames = replay.get("network_frames", {}).get("frames")
    if not objects or not frames:
        return None, None, None, None

    try:
        team_paint_obj_id = objects.index("TAGame.Car_TA:TeamPaint")
        rb_obj_id = objects.index("TAGame.RBActor_TA:ReplicatedRBState")
        pickup_obj_id = objects.index("TAGame.VehiclePickup_TA:NewReplicatedPickupData")
    except ValueError:
        return None, None, None, None

    map_key = "hoops" if game_mode == "hoops" else "standard"
    big_pads = BIG_PAD_POSITIONS[map_key]

    actor_team: dict[int, int] = {}
    actor_position: dict[int, tuple[float, float]] = {}
    last_pickup_state: dict[int, int] = {}
    collected = {0: 0, 1: 0}
    stolen = {0: 0, 1: 0}

    for frame in frames:
        # Clear stale state when actors are deleted and their IDs recycled
        for aid in frame.get("deleted_actors", []):
            actor_team.pop(aid, None)
            actor_position.pop(aid, None)
            last_pickup_state.pop(aid, None)

        for actor in frame.get("updated_actors", []):
            obj_id = actor.get("object_id")
            aid = actor["actor_id"]

            if obj_id == team_paint_obj_id:
                team = actor.get("attribute", {}).get("TeamPaint", {}).get("team")
                if team is not None:
                    actor_team[aid] = team

            elif obj_id == rb_obj_id:
                loc = actor.get("attribute", {}).get("RigidBody", {}).get("location")
                if loc and "x" in loc and "y" in loc:
                    actor_position[aid] = (loc["x"], loc["y"])

            elif obj_id == pickup_obj_id:
                result = _parse_pickup(
                    actor, last_pickup_state, actor_team, actor_position, big_pads
                )
                if result is None:
                    continue
                _, team, is_big, is_stolen = result
                boost_value = 100 if is_big else 12
                collected[team] += boost_value
                if is_stolen:
                    stolen[team] += boost_value

    if collected[0] == 0 and collected[1] == 0:
        return None, None, None, None

    team_collected = collected[tracked_team]
    opp_collected = collected[1 - tracked_team]
    team_stolen = stolen[tracked_team]
    opp_stolen = stolen[1 - tracked_team]
    return team_collected, opp_collected, team_stolen, opp_stolen


def _extract_player_movement_stats(
    replay: dict, duration: int | None, game_mode: str | None = None
) -> dict[tuple[str, str], dict[str, float | int]]:
    """Extract per-player boost consumption, average speed, supersonic time, and pad pickups.

    Returns dict mapping (platform, platform_id) -> {
        "boost_per_minute": float,
        "avg_speed": float,
        "time_supersonic_pct": float,
        "small_pads": int,
        "large_pads": int,
        "stolen_small_pads": int,
        "stolen_large_pads": int,
    }, or {} if network data is unavailable.
    """
    if not duration or duration <= 0:
        return {}

    objects = replay.get("objects")
    frames = replay.get("network_frames", {}).get("frames")
    if not objects or not frames:
        return {}

    try:
        car_archetype = objects.index("Archetypes.Car.Car_Default")
        ball_archetype = objects.index("Archetypes.Ball.Ball_Default")
        boost_comp_archetype = objects.index(
            "Archetypes.CarComponents.CarComponent_Boost"
        )
        rb_obj_id = objects.index("TAGame.RBActor_TA:ReplicatedRBState")
        boost_obj_id = objects.index("TAGame.CarComponent_Boost_TA:ReplicatedBoost")
        vehicle_obj_id = objects.index("TAGame.CarComponent_TA:Vehicle")
        pri_obj_id = objects.index("Engine.Pawn:PlayerReplicationInfo")
        uid_obj_id = objects.index("Engine.PlayerReplicationInfo:UniqueId")
        scored_obj_id = objects.index(
            "TAGame.GameEvent_Soccar_TA:ReplicatedScoredOnTeam"
        )
        countdown_obj_id = objects.index(
            "TAGame.GameEvent_TA:ReplicatedRoundCountDownNumber"
        )
        pickup_obj_id = objects.index("TAGame.VehiclePickup_TA:NewReplicatedPickupData")
        team_paint_obj_id = objects.index("TAGame.Car_TA:TeamPaint")
    except ValueError:
        return {}

    is_playing = False  # Starts False; game begins with a countdown
    car_actors: set[int] = set()
    ball_actors: set[int] = set()
    boost_comp_actors: set[int] = set()

    # Component -> car mapping (via Vehicle obj 85)
    component_to_car: dict[int, int] = {}
    # Car -> PRI actor mapping (via PRI obj 25)
    car_to_pri: dict[int, int] = {}
    # PRI actor -> identity mapping (via UniqueId obj 121)
    pri_identity: dict[int, tuple[str, str]] = {}

    # Per boost-component: last known boost_amount
    comp_boost: dict[int, int] = {}
    # Per boost-component: total consumed (in 0-255 scale)
    comp_boost_consumed: dict[int, float] = {}

    # Per car actor: list of (time, speed) samples
    car_speed_samples: dict[int, list[tuple[float, float]]] = {}

    # Pad pickup tracking
    actor_team: dict[int, int] = {}  # car actor -> team (from TeamPaint)
    actor_position: dict[int, tuple[float, float]] = {}  # any actor -> (x, y)
    last_pickup_state: dict[int, int] = {}  # dedup counter per pickup actor
    identity_pads: dict[tuple[str, str], dict[str, int]] = {}
    map_key = "hoops" if game_mode == "hoops" else "standard"
    big_pads = BIG_PAD_POSITIONS[map_key]

    # Finalized data snapshotted on actor deletion to handle actor-ID recycling.
    # Identity is resolved at deletion time to avoid misattribution when IDs are reused.
    finalized_boost: list[tuple[tuple[str, str], float]] = []
    finalized_speeds: list[tuple[tuple[str, str], list[tuple[float, float]]]] = []

    for frame in frames:
        ft = frame["time"]

        for new_actor in frame.get("new_actors", []):
            oid = new_actor.get("object_id")
            aid = new_actor["actor_id"]
            if oid == car_archetype:
                car_actors.add(aid)
            elif oid == ball_archetype:
                ball_actors.add(aid)
            elif oid == boost_comp_archetype:
                boost_comp_actors.add(aid)

        for aid in frame.get("deleted_actors", []):
            car_actors.discard(aid)
            ball_actors.discard(aid)
            boost_comp_actors.discard(aid)
            comp_boost.pop(aid, None)
            actor_team.pop(aid, None)
            actor_position.pop(aid, None)
            last_pickup_state.pop(aid, None)
            # Snapshot accumulated data before clearing so recycled actor IDs
            # don't merge data from different players.  Resolve identity now
            # (not post-hoc) since car_to_pri may be overwritten by recycling.
            consumed = comp_boost_consumed.pop(aid, None)
            if consumed:
                car_id = component_to_car.pop(aid, None)
                if car_id is not None:
                    pri = car_to_pri.get(car_id)
                    identity = (
                        pri_identity.get(pri) if pri is not None and pri >= 0 else None
                    )
                    if identity:
                        finalized_boost.append((identity, consumed))
            samples = car_speed_samples.pop(aid, None)
            if samples:
                pri = car_to_pri.get(aid)
                identity = (
                    pri_identity.get(pri) if pri is not None and pri >= 0 else None
                )
                if identity:
                    finalized_speeds.append((identity, samples))

        for actor in frame.get("updated_actors", []):
            oid = actor.get("object_id")
            aid = actor["actor_id"]

            if oid == scored_obj_id:
                team = actor.get("attribute", {}).get("Byte")
                if team in (0, 1):
                    is_playing = False
                continue
            elif oid == countdown_obj_id:
                val = actor.get("attribute", {}).get("Int")
                if val == 0:
                    is_playing = True
                continue

            if oid == vehicle_obj_id:
                car_id = actor.get("attribute", {}).get("ActiveActor", {}).get("actor")
                if car_id is not None and car_id >= 0:
                    component_to_car[aid] = car_id

            elif oid == pri_obj_id:
                pri_actor = (
                    actor.get("attribute", {}).get("ActiveActor", {}).get("actor")
                )
                if pri_actor is not None and pri_actor >= 0 and aid in car_actors:
                    car_to_pri[aid] = pri_actor

            elif oid == uid_obj_id:
                uid = actor.get("attribute", {}).get("UniqueId", {})
                identity = _resolve_network_identity(uid)
                if identity:
                    pri_identity[aid] = identity

            elif oid == boost_obj_id and is_playing and aid in boost_comp_actors:
                amount = (
                    actor.get("attribute", {})
                    .get("ReplicatedBoost", {})
                    .get("boost_amount")
                )
                if amount is None:
                    continue
                prev = comp_boost.get(aid)
                comp_boost[aid] = amount
                if prev is not None and amount < prev:
                    comp_boost_consumed[aid] = comp_boost_consumed.get(aid, 0.0) + (
                        prev - amount
                    )

            elif oid == rb_obj_id:
                rb = actor.get("attribute", {}).get("RigidBody", {})
                if aid in car_actors:
                    loc = rb.get("location")
                    if loc and "x" in loc and "y" in loc:
                        actor_position[aid] = (loc["x"], loc["y"])
                    if is_playing:
                        lv = rb.get("linear_velocity")
                        if lv is not None and "x" in lv and "y" in lv and "z" in lv:
                            speed = math.sqrt(
                                lv["x"] ** 2 + lv["y"] ** 2 + lv["z"] ** 2
                            )
                            if aid not in car_speed_samples:
                                car_speed_samples[aid] = []
                            car_speed_samples[aid].append((ft, speed))

            elif oid == team_paint_obj_id:
                team = actor.get("attribute", {}).get("TeamPaint", {}).get("team")
                if team is not None:
                    actor_team[aid] = team

            elif oid == pickup_obj_id and is_playing:
                result = _parse_pickup(
                    actor, last_pickup_state, actor_team, actor_position, big_pads
                )
                if result is None:
                    continue
                instigator, team, is_big, is_stolen = result
                pri = car_to_pri.get(instigator)
                identity = (
                    pri_identity.get(pri) if pri is not None and pri >= 0 else None
                )
                if identity is None:
                    continue
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

    # Post-process: resolve still-active data to player identities.
    # Build car -> identity from car_to_pri + pri_identity (accumulated over full replay)
    car_identity: dict[int, tuple[str, str]] = {}
    for car_id, pri_actor in car_to_pri.items():
        if pri_actor >= 0:
            identity = pri_identity.get(pri_actor)
            if identity:
                car_identity[car_id] = identity

    # Aggregate boost consumed per identity.
    # Finalized entries already have identity resolved; still-active use car_identity.
    identity_boost_consumed: dict[tuple[str, str], float] = {}
    for identity, consumed in finalized_boost:
        identity_boost_consumed[identity] = (
            identity_boost_consumed.get(identity, 0.0) + consumed
        )
    for comp_id, consumed in comp_boost_consumed.items():
        car_id = component_to_car.get(comp_id)
        if car_id is not None:
            identity = car_identity.get(car_id)
            if identity:
                identity_boost_consumed[identity] = (
                    identity_boost_consumed.get(identity, 0.0) + consumed
                )

    # Aggregate speed samples per identity (finalized + still-active).
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

        # Boost per minute: consumed is in 0-255 scale, convert to 0-100 scale
        consumed = identity_boost_consumed.get(identity, 0.0)
        stats["boost_per_minute"] = round((consumed / 255 * 100) / (duration / 60), 1)

        # Average speed
        speeds = identity_speeds.get(identity, [])
        if speeds:
            # Sort by time for supersonic calculation
            speeds.sort(key=lambda s: s[0])
            # Time-weighted average speed and supersonic time from consecutive samples
            total_weight = 0.0
            weighted_sum = 0.0
            supersonic_time = 0.0
            for (t1, s1), (t2, _) in pairwise(speeds):
                dt = t2 - t1
                if 0 < dt < 5:  # Skip unreasonable gaps (goal replays etc)
                    weighted_sum += s1 * dt
                    total_weight += dt
                    if s1 >= 2200:
                        supersonic_time += dt
            stats["avg_speed"] = (
                round(weighted_sum / total_weight) if total_weight > 0 else 0
            )
            stats["time_supersonic_pct"] = round(supersonic_time / duration * 100, 1)
        else:
            stats["avg_speed"] = 0
            stats["time_supersonic_pct"] = 0.0

        pads = identity_pads.get(identity, {})
        stats["small_pads"] = pads.get("small_pads", 0)
        stats["large_pads"] = pads.get("large_pads", 0)
        stats["stolen_small_pads"] = pads.get("stolen_small_pads", 0)
        stats["stolen_large_pads"] = pads.get("stolen_large_pads", 0)

        result[identity] = stats

    return result


def _extract_match_events(
    replay: dict, tracked_team: int | None, tracked_identities: set[tuple[str, str]]
) -> list[tuple[str, float, str, str, int]]:
    """Extract individual match events from network frame data.

    Returns list of (event_type, game_seconds, platform, platform_id, team) tuples.
    """
    if tracked_team is None:
        return []

    objects = replay.get("objects")
    frames = replay.get("network_frames", {}).get("frames")
    if not objects or not frames:
        return []

    # Resolve object IDs
    try:
        sr_obj_id = objects.index("TAGame.GameEvent_Soccar_TA:SecondsRemaining")
        uid_obj_id = objects.index("Engine.PlayerReplicationInfo:UniqueId")
        team_obj_id = objects.index("Engine.PlayerReplicationInfo:Team")
    except ValueError:
        return []

    counter_names = {
        "TAGame.PRI_TA:MatchGoals": "goal",
        "TAGame.PRI_TA:MatchShots": "shot",
        "TAGame.PRI_TA:MatchSaves": "save",
        "TAGame.PRI_TA:MatchDemolishes": "demo",
    }
    counter_obj_ids: dict[int, str] = {}
    for obj_name, event_type in counter_names.items():
        try:
            counter_obj_ids[objects.index(obj_name)] = event_type
        except ValueError:
            pass

    if not counter_obj_ids:
        return []

    # Build game clock: list of (frame_time, seconds_remaining)
    clock_updates: list[tuple[float, int]] = []
    # Build actor identity and team mappings, track counters — all in one pass
    actor_identity: dict[int, tuple[str, str]] = {}
    actor_team_actor: dict[int, int] = {}
    actor_counters: dict[int, dict[str, int]] = {}
    raw_events: list[tuple[str, float, int]] = []  # (event_type, frame_time, actor_id)

    for frame in frames:
        ft = frame["time"]
        for actor in frame.get("updated_actors", []):
            obj_id = actor.get("object_id")
            aid = actor["actor_id"]

            if obj_id == sr_obj_id:
                sr = actor.get("attribute", {}).get("Int")
                if sr is not None:
                    clock_updates.append((ft, sr))

            elif obj_id == uid_obj_id:
                uid = actor.get("attribute", {}).get("UniqueId", {})
                identity = _resolve_network_identity(uid)
                if identity:
                    actor_identity[aid] = identity

            elif obj_id == team_obj_id:
                team_actor = (
                    actor.get("attribute", {}).get("ActiveActor", {}).get("actor")
                )
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
                        raw_events.append((event_type, ft, aid))
                actor_counters[aid][event_type] = val

    if not clock_updates or not raw_events:
        return []

    # Determine game_start (first SecondsRemaining value, typically 300)
    game_start = clock_updates[0][1]

    # Pre-compute monotonic game_seconds from clock updates.
    # In overtime, SecondsRemaining counts up from 0 instead of down,
    # so we detect the transition and add to game_start instead of subtracting.
    seen_zero = False
    clock_game_seconds: list[tuple[float, float]] = []
    for c_ft, sr in clock_updates:
        if sr == 0:
            seen_zero = True
        if seen_zero and sr > 0:
            clock_game_seconds.append((c_ft, game_start + sr))
        else:
            clock_game_seconds.append((c_ft, game_start - sr))

    # Convert frame_time -> game_seconds using pre-computed values
    def frame_to_game_seconds(ft: float) -> float:
        _, best_gs = clock_game_seconds[0]
        for c_ft, c_gs in clock_game_seconds:
            if c_ft <= ft:
                best_gs = c_gs
            else:
                break
        return best_gs

    # Resolve team actor -> team number
    # Tracked players' actor IDs point to one team actor = tracked_team
    tracked_team_actor = None
    for aid, identity in actor_identity.items():
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

    # Build final events
    events: list[tuple[str, float, str, str, int]] = []
    for event_type, ft, aid in raw_events:
        identity = actor_identity.get(aid)
        team = resolve_team(aid)
        if identity is None or team is None:
            continue
        gs = frame_to_game_seconds(ft)
        events.append((event_type, gs, identity[0], identity[1], team))

    return events
