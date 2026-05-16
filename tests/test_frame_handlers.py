"""Unit tests for individual FrameHandler subclasses.

These exercise handlers in isolation: construct via __init__ with resolved
obj IDs, set up a minimal FrameContext with only the fields the handler
reads, drive on_update/on_deleted_actor directly, and assert finalize output.

The integration tests in test_ingest.py exercise the orchestrator and
frame-loop ordering invariants; these tests cover handler logic.
"""

from typing import Any

from frame_analysis import (
    BallThirdsHandler,
    BoostStatsHandler,
    DemolitionsHandler,
    DemosReceivedHandler,
    FrameContext,
    IdentityResolver,
    MatchEventsHandler,
    MovementHandler,
    PossessionHandler,
)

HIT_TEAM_OID = 100
RB_OID = 101
BOOST_OID = 102
PICKUP_OID = 103
DEMO_OID = 104
DEMOLISH_OID = 105
SR_OID = 106
TEAM_OID = 107
GOALS_OID = 108

BIG_PADS = [(-3072.0, -4096.0), (3072.0, 4096.0)]


def _hit(team_num: int) -> dict[str, Any]:
    return {"actor_id": 1, "object_id": HIT_TEAM_OID, "attribute": {"Byte": team_num}}


# -- IdentityResolver --


def test_resolve_car_follows_full_chain():
    r = IdentityResolver()
    r.link_car_to_pri(7, 100)
    r.set_identity(100, "steam", "abc123")
    assert r.resolve_car(7) == ("steam", "abc123")


def test_resolve_car_returns_none_when_pri_not_linked():
    r = IdentityResolver()
    assert r.resolve_car(7) is None


def test_resolve_car_returns_none_when_identity_not_yet_set():
    r = IdentityResolver()
    r.link_car_to_pri(7, 100)
    assert r.resolve_car(7) is None


def test_resolve_pri_returns_identity_directly():
    r = IdentityResolver()
    r.set_identity(100, "epic", "xyz")
    assert r.resolve_pri(100) == ("epic", "xyz")
    assert r.resolve_pri(999) is None


def test_resolve_component_follows_three_hops():
    r = IdentityResolver()
    r.link_component_to_car(10, 7)
    r.link_car_to_pri(7, 100)
    r.set_identity(100, "steam", "abc123")
    assert r.resolve_component(10) == ("steam", "abc123")


def test_resolve_component_returns_none_when_car_missing():
    r = IdentityResolver()
    r.link_component_to_car(10, 7)
    assert r.resolve_component(10) is None


def test_remove_actor_clears_car_entry():
    r = IdentityResolver()
    r.link_car_to_pri(7, 100)
    r.set_identity(100, "steam", "abc")
    r.remove_actor(7)
    assert r.resolve_car(7) is None


def test_remove_actor_clears_pri_entry():
    r = IdentityResolver()
    r.set_identity(100, "steam", "abc")
    r.remove_actor(100)
    assert r.resolve_pri(100) is None


def test_remove_actor_is_silent_for_unknown_actor():
    r = IdentityResolver()
    r.remove_actor(999)  # must not raise


def test_find_pri_ids_for_returns_matching_ids():
    r = IdentityResolver()
    r.set_identity(5, "steam", "TRACKED")
    r.set_identity(6, "steam", "OTHER")
    result = r.find_pri_ids_for({("steam", "TRACKED")})
    assert result == [5]


# -- PossessionHandler --


def test_possession_handler_splits_time_by_last_hit_team():
    h = PossessionHandler(HIT_TEAM_OID, tracked_team=0)
    ctx = FrameContext()

    ctx.frame_time = 0.0
    h.on_update(ctx, _hit(0))
    ctx.frame_time = 10.0
    h.on_update(ctx, _hit(1))
    ctx.frame_time = 20.0

    team, opp = h.finalize(ctx)
    assert team == 10.0
    assert opp == 10.0


def test_possession_handler_no_touches_returns_none():
    h = PossessionHandler(HIT_TEAM_OID, tracked_team=0)
    ctx = FrameContext(frame_time=30.0)
    assert h.finalize(ctx) == (None, None)


def test_possession_handler_inverts_for_team_1():
    h = PossessionHandler(HIT_TEAM_OID, tracked_team=1)
    ctx = FrameContext()
    ctx.frame_time = 0.0
    h.on_update(ctx, _hit(0))
    ctx.frame_time = 6.0
    h.on_update(ctx, _hit(1))
    ctx.frame_time = 10.0

    team, opp = h.finalize(ctx)
    assert team == 4.0  # team 1
    assert opp == 6.0  # team 0


# -- BallThirdsHandler --


def _ball_update(actor_id: int, y: float) -> dict[str, Any]:
    return {
        "actor_id": actor_id,
        "object_id": RB_OID,
        "attribute": {"RigidBody": {"location": {"x": 0.0, "y": y, "z": 0.0}}},
    }


def test_ball_thirds_handler_ignores_non_ball_actors():
    h = BallThirdsHandler(RB_OID, tracked_team=0)
    ctx = FrameContext()
    ctx.ball_actors.add(99)

    ctx.frame_time = 0.0
    h.on_update(ctx, _ball_update(actor_id=42, y=-2500.0))  # not in ball_actors
    ctx.frame_time = 5.0
    h.on_update(ctx, _ball_update(actor_id=42, y=2500.0))

    assert h.finalize(ctx) == (None, None, None)


def test_ball_thirds_handler_buckets_time_by_zone():
    h = BallThirdsHandler(RB_OID, tracked_team=0)
    ctx = FrameContext()
    ctx.ball_actors.add(7)

    # 0.0 .. 3.0 in defensive third (y < -1707 for team 0)
    ctx.frame_time = 0.0
    h.on_update(ctx, _ball_update(7, -2500.0))
    ctx.frame_time = 3.0
    h.on_update(ctx, _ball_update(7, 0.0))  # neutral
    ctx.frame_time = 5.0
    h.on_update(ctx, _ball_update(7, 2500.0))  # offensive
    ctx.frame_time = 9.0
    h.on_update(ctx, _ball_update(7, 2500.0))

    defensive, neutral, offensive = h.finalize(ctx)
    assert defensive == 3.0
    assert neutral == 2.0
    assert offensive == 4.0


# -- DemolitionsHandler --


def test_demolitions_handler_maps_counter_max_to_identity():
    h = DemolitionsHandler(DEMO_OID)
    ctx = FrameContext()
    ctx.resolver.set_identity(5, "steam", "AAA")
    ctx.resolver.set_identity(6, "steam", "BBB")

    h.on_update(ctx, {"actor_id": 5, "object_id": DEMO_OID, "attribute": {"Int": 2}})
    h.on_update(
        ctx, {"actor_id": 5, "object_id": DEMO_OID, "attribute": {"Int": 1}}
    )  # not max
    h.on_update(ctx, {"actor_id": 6, "object_id": DEMO_OID, "attribute": {"Int": 3}})

    assert h.finalize(ctx) == {("steam", "AAA"): 2, ("steam", "BBB"): 3}


def test_demolitions_handler_skips_unknown_identities():
    h = DemolitionsHandler(DEMO_OID)
    ctx = FrameContext()
    # No pri_identity for actor 9
    h.on_update(ctx, {"actor_id": 9, "object_id": DEMO_OID, "attribute": {"Int": 4}})
    assert h.finalize(ctx) == {}


# -- DemosReceivedHandler --


def _demolish(
    actor_id: int,
    victim_active: bool,
    victim_actor: int,
    attacker_active: bool = True,
    self_demolish: bool = False,
) -> dict[str, Any]:
    return {
        "actor_id": actor_id,
        "object_id": DEMOLISH_OID,
        "attribute": {
            "DemolishExtended": {
                "victim": {"active": victim_active, "actor": victim_actor},
                "attacker": {"active": attacker_active, "actor": 0},
                "self_demolish": self_demolish,
            }
        },
    }


def test_demos_received_handler_counts_active_transitions():
    h = DemosReceivedHandler(DEMOLISH_OID)
    ctx = FrameContext()
    ctx.car_actors.add(7)
    ctx.resolver.link_car_to_pri(7, 100)
    ctx.resolver.set_identity(100, "steam", "VICTIM")

    h.on_update(ctx, _demolish(actor_id=7, victim_active=True, victim_actor=7))
    # Still active — not a new transition
    h.on_update(ctx, _demolish(actor_id=7, victim_active=True, victim_actor=7))
    # Reset
    h.on_update(ctx, _demolish(actor_id=7, victim_active=False, victim_actor=7))
    # Second hit
    h.on_update(ctx, _demolish(actor_id=7, victim_active=True, victim_actor=7))

    assert h.finalize(ctx) == {("steam", "VICTIM"): 2}


def test_demos_received_handler_skips_self_demolish():
    h = DemosReceivedHandler(DEMOLISH_OID)
    ctx = FrameContext()
    ctx.car_actors.add(7)
    ctx.resolver.link_car_to_pri(7, 100)
    ctx.resolver.set_identity(100, "steam", "VICTIM")

    h.on_update(
        ctx,
        _demolish(actor_id=7, victim_active=True, victim_actor=7, self_demolish=True),
    )
    assert h.finalize(ctx) == {}


def test_demos_received_handler_skips_when_attacker_inactive():
    h = DemosReceivedHandler(DEMOLISH_OID)
    ctx = FrameContext()
    ctx.car_actors.add(7)
    ctx.resolver.link_car_to_pri(7, 100)
    ctx.resolver.set_identity(100, "steam", "VICTIM")

    h.on_update(
        ctx,
        _demolish(
            actor_id=7, victim_active=True, victim_actor=7, attacker_active=False
        ),
    )
    assert h.finalize(ctx) == {}


def test_demos_received_handler_cleans_up_on_delete():
    h = DemosReceivedHandler(DEMOLISH_OID)
    ctx = FrameContext()
    ctx.car_actors.add(7)
    ctx.resolver.link_car_to_pri(7, 100)
    ctx.resolver.set_identity(100, "steam", "VICTIM")

    h.on_update(ctx, _demolish(actor_id=7, victim_active=True, victim_actor=7))
    h.on_deleted_actor(ctx, 7)
    # After deletion, the next "active=True" should count as a NEW hit
    # (previous active state was cleared)
    h.on_update(ctx, _demolish(actor_id=7, victim_active=True, victim_actor=7))
    assert h.finalize(ctx) == {("steam", "VICTIM"): 2}


# -- BoostStatsHandler --


def _pickup(
    pickup_actor_id: int,
    instigator: int,
    picked_up_state: int,
) -> dict[str, Any]:
    return {
        "actor_id": pickup_actor_id,
        "object_id": PICKUP_OID,
        "attribute": {
            "PickupNew": {"picked_up": picked_up_state, "instigator": instigator}
        },
    }


def test_boost_stats_handler_attributes_big_pad_to_team():
    h = BoostStatsHandler(PICKUP_OID, tracked_team=0, big_pads=BIG_PADS)
    ctx = FrameContext()
    ctx.actor_team[1] = 0  # team 0 player
    ctx.actor_position[1] = (-3072.0, -4096.0)  # on a big pad, defensive half

    h.on_update(ctx, _pickup(pickup_actor_id=50, instigator=1, picked_up_state=1))

    team, opp, team_stolen, opp_stolen = h.finalize(ctx)
    assert team == 100
    assert opp == 0
    assert team_stolen == 0
    assert opp_stolen == 0


def test_boost_stats_handler_detects_stolen():
    h = BoostStatsHandler(PICKUP_OID, tracked_team=0, big_pads=BIG_PADS)
    ctx = FrameContext()
    ctx.actor_team[1] = 0
    ctx.actor_position[1] = (3072.0, 4096.0)  # on a big pad in opponent half (y > 0)

    h.on_update(ctx, _pickup(50, instigator=1, picked_up_state=1))

    team, opp, team_stolen, opp_stolen = h.finalize(ctx)
    assert team == 100
    assert team_stolen == 100
    assert opp == 0
    assert opp_stolen == 0


def test_boost_stats_handler_small_pad_when_far_from_big():
    h = BoostStatsHandler(PICKUP_OID, tracked_team=0, big_pads=BIG_PADS)
    ctx = FrameContext()
    ctx.actor_team[1] = 0
    ctx.actor_position[1] = (0.0, -1000.0)  # not near any big pad

    h.on_update(ctx, _pickup(50, instigator=1, picked_up_state=1))

    team, _, _, _ = h.finalize(ctx)
    assert team == 12


def test_boost_stats_handler_dedupes_same_pickup_state():
    h = BoostStatsHandler(PICKUP_OID, tracked_team=0, big_pads=BIG_PADS)
    ctx = FrameContext()
    ctx.actor_team[1] = 0
    ctx.actor_position[1] = (-3072.0, -4096.0)

    h.on_update(ctx, _pickup(50, instigator=1, picked_up_state=1))
    h.on_update(
        ctx, _pickup(50, instigator=1, picked_up_state=1)
    )  # same state -> ignored

    team, _, _, _ = h.finalize(ctx)
    assert team == 100


def test_boost_stats_handler_returns_none_when_nothing_collected():
    h = BoostStatsHandler(PICKUP_OID, tracked_team=0, big_pads=BIG_PADS)
    ctx = FrameContext()
    assert h.finalize(ctx) == (None, None, None, None)


# -- MovementHandler --


def _boost_amount(comp_id: int, amount: int) -> dict[str, Any]:
    return {
        "actor_id": comp_id,
        "object_id": BOOST_OID,
        "attribute": {"ReplicatedBoost": {"boost_amount": amount}},
    }


def _car_velocity(
    car_id: int, x: float = 0.0, y: float = 0.0, z: float = 0.0
) -> dict[str, Any]:
    return {
        "actor_id": car_id,
        "object_id": RB_OID,
        "attribute": {"RigidBody": {"linear_velocity": {"x": x, "y": y, "z": z}}},
    }


def test_movement_handler_attributes_boost_consumption_on_delete():
    h = MovementHandler(
        rb_obj_id=RB_OID,
        boost_obj_id=BOOST_OID,
        pickup_obj_id=PICKUP_OID,
        duration=300,
        big_pads=BIG_PADS,
    )
    ctx = FrameContext(is_playing=True)
    ctx.boost_comp_actors.add(10)
    ctx.resolver.link_component_to_car(10, 1)
    ctx.resolver.link_car_to_pri(1, 100)
    ctx.resolver.set_identity(100, "steam", "PLAYER")

    h.on_update(ctx, _boost_amount(10, 255))
    h.on_update(ctx, _boost_amount(10, 200))  # 55 consumed
    h.on_deleted_actor(ctx, 10)

    result = h.finalize(ctx)
    bpm = result[("steam", "PLAYER")]["boost_per_minute"]
    # 55 / 255 * 100 / (300 / 60) = ~4.3
    assert bpm == 4.3


def test_movement_handler_skips_boost_when_not_playing():
    h = MovementHandler(RB_OID, BOOST_OID, PICKUP_OID, duration=300, big_pads=BIG_PADS)
    ctx = FrameContext(is_playing=False)
    ctx.boost_comp_actors.add(10)

    h.on_update(ctx, _boost_amount(10, 255))
    h.on_update(ctx, _boost_amount(10, 200))

    assert h.finalize(ctx) == {}


def test_movement_handler_accumulates_speed_samples():
    h = MovementHandler(RB_OID, BOOST_OID, PICKUP_OID, duration=60, big_pads=BIG_PADS)
    ctx = FrameContext(is_playing=True)
    ctx.car_actors.add(1)
    ctx.resolver.link_car_to_pri(1, 100)
    ctx.resolver.set_identity(100, "steam", "PLAYER")

    ctx.frame_time = 0.0
    h.on_update(ctx, _car_velocity(1, x=2300.0))  # supersonic
    ctx.frame_time = 1.0
    h.on_update(ctx, _car_velocity(1, x=2300.0))
    ctx.frame_time = 2.0
    h.on_update(ctx, _car_velocity(1, x=1000.0))

    result = h.finalize(ctx)
    stats = result[("steam", "PLAYER")]
    assert stats["avg_speed"] > 0
    # 1 second of supersonic out of 60s duration = ~1.7%
    assert stats["time_supersonic_pct"] > 0


# -- MatchEventsHandler --


def test_match_events_handler_emits_goal_event_with_game_time():
    h = MatchEventsHandler(
        sr_obj_id=SR_OID,
        team_obj_id=TEAM_OID,
        counter_obj_ids={GOALS_OID: "goal"},
        tracked_team=0,
        tracked_identities={("steam", "TRACKED")},
    )
    ctx = FrameContext()
    ctx.resolver.set_identity(5, "steam", "TRACKED")

    # Establish clock: starts at 300s, ticks down
    h.on_update(ctx, {"actor_id": 0, "object_id": SR_OID, "attribute": {"Int": 300}})
    # Team actor assignment for tracked player (PRI 5 -> team actor 200)
    h.on_update(
        ctx,
        {
            "actor_id": 5,
            "object_id": TEAM_OID,
            "attribute": {"ActiveActor": {"actor": 200}},
        },
    )
    # Player scores
    ctx.frame_time = 10.0
    h.on_update(ctx, {"actor_id": 5, "object_id": GOALS_OID, "attribute": {"Int": 1}})

    events = h.finalize(ctx)
    assert len(events) == 1
    event_type, _gs, platform, platform_id, team = events[0]
    assert event_type == "goal"
    assert platform == "steam"
    assert platform_id == "TRACKED"
    assert team == 0


def test_match_events_handler_emits_nothing_without_clock():
    h = MatchEventsHandler(
        sr_obj_id=SR_OID,
        team_obj_id=TEAM_OID,
        counter_obj_ids={GOALS_OID: "goal"},
        tracked_team=0,
        tracked_identities={("steam", "TRACKED")},
    )
    ctx = FrameContext()
    ctx.resolver.set_identity(5, "steam", "TRACKED")
    h.on_update(ctx, {"actor_id": 5, "object_id": GOALS_OID, "attribute": {"Int": 1}})

    assert h.finalize(ctx) == []


def test_match_events_handler_emits_multiple_when_counter_jumps():
    h = MatchEventsHandler(
        sr_obj_id=SR_OID,
        team_obj_id=TEAM_OID,
        counter_obj_ids={GOALS_OID: "goal"},
        tracked_team=0,
        tracked_identities={("steam", "TRACKED")},
    )
    ctx = FrameContext()
    ctx.resolver.set_identity(5, "steam", "TRACKED")
    h.on_update(ctx, {"actor_id": 0, "object_id": SR_OID, "attribute": {"Int": 300}})
    h.on_update(
        ctx,
        {
            "actor_id": 5,
            "object_id": TEAM_OID,
            "attribute": {"ActiveActor": {"actor": 200}},
        },
    )
    ctx.frame_time = 10.0
    h.on_update(ctx, {"actor_id": 5, "object_id": GOALS_OID, "attribute": {"Int": 3}})
    events = h.finalize(ctx)
    assert len(events) == 3
    assert all(e[0] == "goal" for e in events)
