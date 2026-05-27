"""Tests for the frame-loop ordering contract enforced by _process_frame.

Each test uses a _SpyHandler and synthetic frame dicts — no replay JSON or
objects list required. The goal is to pin the three-phase ordering invariant
so that a refactor of _process_frame fails here before it fails in fixtures.
"""

from typing import Any, cast

from frame_analysis import (
    FrameAnalysis,
    FrameContext,
    FrameHandler,
    _FrameLoopObjectIds,  # type: ignore[reportPrivateUsage]
    _process_frame,  # type: ignore[reportPrivateUsage]
)
from rrrocket_schema import FrameData, UpdatedActor

_EMPTY_OBJ_IDS = _FrameLoopObjectIds(
    car_archetype=None,
    ball_archetype=None,
    boost_comp_archetype=None,
    scored_obj_id=None,
    countdown_obj_id=None,
    vehicle_obj_id=None,
    pri_obj_id=None,
    uid_obj_id=None,
    team_paint_obj_id=None,
    rb_obj_id=None,
)


class _SpyHandler(FrameHandler):
    def __init__(self, watch_obj_id: int | None = None) -> None:
        self.update_obj_ids = frozenset({watch_obj_id}) if watch_obj_id else frozenset()
        self.calls: list[Any] = []

    def on_update(self, ctx: FrameContext, actor: UpdatedActor) -> None:
        self.calls.append(("on_update", actor["actor_id"]))

    def on_deleted_actor(self, ctx: FrameContext, aid: int) -> None:
        self.calls.append(("on_deleted_actor", aid, ctx.resolver.resolve_car(aid)))

    def finalize(self, ctx: FrameContext, result: FrameAnalysis) -> None:
        pass


def test_deleted_actor_sees_identity_before_resolver_cleanup() -> None:
    """on_deleted_actor fires before ctx.resolver.remove_actor(), so identity is still valid."""
    ctx = FrameContext()
    ctx.car_actors.add(10)
    ctx.resolver.link_car_to_pri(10, 20)
    ctx.resolver.set_identity(20, "steam", "abc123")

    spy = _SpyHandler()
    frame = cast(FrameData, {"time": 1.0, "deleted_actors": [10]})
    _process_frame(ctx, frame, _EMPTY_OBJ_IDS, {}, [spy])

    assert spy.calls == [("on_deleted_actor", 10, ("steam", "abc123"))]
    assert ctx.resolver.resolve_car(10) is None


def test_two_pass_deletion_second_actor_sees_first_in_same_frame() -> None:
    """All on_deleted_actor callbacks fire before any actor state is purged.

    When a car (10) and its boost component (11) are both deleted in the same
    frame, the boost component's handler must still see car 10 in ctx.car_actors
    when it fires — even if car 10 appears first in deleted_actors.
    """
    ctx = FrameContext()
    ctx.car_actors.add(10)
    ctx.boost_comp_actors.add(11)
    ctx.resolver.link_component_to_car(11, 10)
    ctx.resolver.link_car_to_pri(10, 20)
    ctx.resolver.set_identity(20, "steam", "abc123")

    car_present_when_boost_comp_notified: list[bool] = []

    class BoostCompSpy(_SpyHandler):
        def on_deleted_actor(self, ctx: FrameContext, aid: int) -> None:
            if aid == 11:
                car_present_when_boost_comp_notified.append(10 in ctx.car_actors)

    spy = BoostCompSpy()
    frame = cast(FrameData, {"time": 1.0, "deleted_actors": [10, 11]})
    _process_frame(ctx, frame, _EMPTY_OBJ_IDS, {}, [spy])

    assert car_present_when_boost_comp_notified == [True]
    assert 10 not in ctx.car_actors
    assert 11 not in ctx.boost_comp_actors


def test_updated_actors_processed_before_deleted_actors() -> None:
    """on_update fires before on_deleted_actor when an actor appears in both lists."""
    ctx = FrameContext()
    ctx.car_actors.add(10)

    ORDER_OID = 99
    spy = _SpyHandler(watch_obj_id=ORDER_OID)
    frame = cast(
        FrameData,
        {
            "time": 1.0,
            "updated_actors": [
                {"actor_id": 10, "object_id": ORDER_OID, "attribute": {}}
            ],
            "deleted_actors": [10],
        },
    )
    _process_frame(ctx, frame, _EMPTY_OBJ_IDS, {ORDER_OID: [spy]}, [spy])

    assert [c[0] for c in spy.calls] == ["on_update", "on_deleted_actor"]
