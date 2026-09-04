"""Tests for replay_frames.extract_replay_frames.

Two layers:

* Synthetic single-frame tests that pin the frame-walk contract (phase order,
  identity resolution, rotation carry-forward) with hand-built ParsedReplay
  objects — no fixture JSON needed.
* One integration pass over tests/data/team_size_2.json (a real 2v2) checking
  the shape of the whole output.
"""

import math
import struct
from typing import cast

import pytest

from replay_frames import (
    FLOATS_PER_POSE,
    GoalMarker,
    ReplayFrames,
    _dead_periods,  # type: ignore[reportPrivateUsage]
    _scan_countdowns,  # type: ignore[reportPrivateUsage]
    _scan_goals,  # type: ignore[reportPrivateUsage]
    _walk,  # type: ignore[reportPrivateUsage]
    extract_replay_frames,
    packed_buffer_bytes,
    pose_offset,
)
from rrrocket_schema import FrameData, ParsedReplay
from rrrocket_schema import parse as parse_replay
from tests.fixtures import load_replay, replay_frames_of

_OBJECTS = [
    "Archetypes.Car.Car_Default",
    "Archetypes.Ball.Ball_Default",
    "TAGame.RBActor_TA:ReplicatedRBState",
    "Engine.Pawn:PlayerReplicationInfo",
    "Engine.PlayerReplicationInfo:UniqueId",
    "TAGame.Car_TA:TeamPaint",
    "TAGame.GameEvent_Soccar_TA:ReplicatedScoredOnTeam",
    "TAGame.GameEvent_TA:ReplicatedRoundCountDownNumber",
    "TAGame.VehiclePickup_TA:NewReplicatedPickupData",
    "stadium_p.TheWorld:PersistentLevel.VehiclePickup_Boost_TA_0",
    "stadium_p.TheWorld:PersistentLevel.VehiclePickup_Boost_TA_1",
]
_CAR, _BALL, _RB, _PRI, _UID, _PAINT, _SCORED, _COUNTDOWN, _PICKUP, _PAD_A, _PAD_B = (
    range(len(_OBJECTS))
)


def _replay(frames: list[FrameData]) -> ParsedReplay:
    return ParsedReplay(
        match_guid="TEST",
        played_at=None,
        properties={},
        object_index={name: i for i, name in enumerate(_OBJECTS)},
        frames=frames,
        debug_info=[],
    )


def _rb(loc: tuple[float, float, float], rot: tuple[float, float, float, float] | None):
    body: dict[str, object] = {"location": {"x": loc[0], "y": loc[1], "z": loc[2]}}
    if rot is not None:
        body["rotation"] = {"x": rot[0], "y": rot[1], "z": rot[2], "w": rot[3]}
    return {"RigidBody": body}


def _rb_update(
    aid: int,
    loc: tuple[float, float, float],
    rot: tuple[float, float, float, float] | None = None,
) -> dict[str, object]:
    return {"actor_id": aid, "object_id": _RB, "attribute": _rb(loc, rot)}


def _frame(
    time: float,
    *,
    new: list[dict[str, object]] | None = None,
    updated: list[dict[str, object]] | None = None,
    deleted: list[int] | None = None,
) -> FrameData:
    f: dict[str, object] = {"time": time}
    if new is not None:
        f["new_actors"] = new
    if updated is not None:
        f["updated_actors"] = updated
    if deleted is not None:
        f["deleted_actors"] = deleted
    return cast(FrameData, f)


def _pose(rf: ReplayFrames, frame: int, slot: int) -> tuple[float, ...]:
    off = pose_offset(len(rf.slots), frame, slot) * 4  # * 4: float32 -> bytes
    return struct.unpack_from("<7f", rf.positions, off)


def _extract(rf_replay: ParsedReplay, **kw: object) -> ReplayFrames:
    base: dict[str, object] = {
        "tracked_team": None,
        "tracked_identities": set(),
        "player_names": {},
        "game_mode": None,
    }
    base.update(kw)
    return extract_replay_frames(rf_replay, **base)  # type: ignore[arg-type]


# --- packed-buffer geometry ---


def test_pose_offset_matches_the_row_major_layout() -> None:
    # The .bin is row-major [frame][slot][x,y,z,qx,qy,qz,qw]; pose_offset is the
    # only Python expression of that geometry — verbatim twin of poseOffset() in
    # static/replay-core.js. Hand-checked points.
    assert FLOATS_PER_POSE == 7
    assert pose_offset(4, 0, 0) == 0
    assert pose_offset(4, 0, 3) == 21
    assert pose_offset(4, 1, 0) == 28
    assert pose_offset(4, 2, 1) == 63
    assert pose_offset(1, 5, 0) == 35


def test_packed_buffer_bytes_is_four_bytes_per_float() -> None:
    assert packed_buffer_bytes(0, 5) == 0
    assert packed_buffer_bytes(10, 3) == 4 * 10 * 3 * 7


# --- synthetic frame-walk contract ---


def test_car_spawned_and_linked_in_one_frame_keeps_identity() -> None:
    """new_actor -> PRI link -> UniqueId -> RigidBody, all in the same frame.

    The PRI link is only applied when the car is already in car_actors, so this
    fails if phase 1 (new_actors) does not run before phase 2 (updated_actors).
    """
    frame = cast(
        FrameData,
        {
            "time": 0.0,
            "new_actors": [{"actor_id": 1, "object_id": _CAR}],
            "updated_actors": [
                {
                    "actor_id": 1,
                    "object_id": _PRI,
                    "attribute": {"ActiveActor": {"actor": 5}},
                },
                {
                    "actor_id": 5,
                    "object_id": _UID,
                    "attribute": {
                        "UniqueId": {"remote_id": {"Steam": "76561197960287930"}}
                    },
                },
                {
                    "actor_id": 1,
                    "object_id": _PAINT,
                    "attribute": {"TeamPaint": {"team": 0}},
                },
                {
                    "actor_id": 1,
                    "object_id": _RB,
                    "attribute": _rb((100.0, 200.0, 17.0), (0.0, 0.0, 0.0, 1.0)),
                },
            ],
        },
    )
    rf = _extract(_replay([frame]))

    cars = [s for s in rf.slots if s.kind == "car"]
    assert len(cars) == 1
    assert cars[0].identity == ("steam", "76561197960287930")
    assert cars[0].team == 0
    assert cars[0].segments == [(0, 0)]
    assert _pose(rf, 0, rf.slots.index(cars[0]))[:3] == (100.0, 200.0, 17.0)


def test_car_without_uniqueid_becomes_anonymous_slot() -> None:
    frame = cast(
        FrameData,
        {
            "time": 0.0,
            "new_actors": [{"actor_id": 1, "object_id": _CAR}],
            "updated_actors": [
                {
                    "actor_id": 1,
                    "object_id": _RB,
                    "attribute": _rb((1.0, 2.0, 3.0), (0.0, 0.0, 0.0, 1.0)),
                }
            ],
        },
    )
    rf = _extract(_replay([frame]))

    cars = [s for s in rf.slots if s.kind == "car"]
    assert len(cars) == 1
    assert cars[0].identity is None
    assert cars[0].name == "Player 1"
    assert _pose(rf, 0, rf.slots.index(cars[0]))[:3] == (1.0, 2.0, 3.0)


def test_rotation_absent_from_update_carries_forward() -> None:
    frames = [
        cast(
            FrameData,
            {
                "time": 0.0,
                "new_actors": [{"actor_id": 9, "object_id": _BALL}],
                "updated_actors": [
                    {
                        "actor_id": 9,
                        "object_id": _RB,
                        "attribute": _rb((0.0, 0.0, 93.0), (0.1, 0.2, 0.3, 0.9)),
                    }
                ],
            },
        ),
        cast(
            FrameData,
            {
                "time": 0.033,
                "updated_actors": [
                    {
                        "actor_id": 9,
                        "object_id": _RB,
                        "attribute": _rb((0.0, 10.0, 93.0), None),
                    }
                ],
            },
        ),
    ]
    rf = _extract(_replay(frames))

    ball = next(s for s in rf.slots if s.kind == "ball")
    idx = rf.slots.index(ball)
    assert _pose(rf, 0, idx)[3:] == pytest.approx((0.1, 0.2, 0.3, 0.9))  # pyright: ignore[reportUnknownMemberType]
    # frame 1 moved but sent no rotation -> quaternion is unchanged
    assert _pose(rf, 1, idx) == pytest.approx((0.0, 10.0, 93.0, 0.1, 0.2, 0.3, 0.9))  # pyright: ignore[reportUnknownMemberType]


def test_deleted_actor_ends_segment_and_leaves_gap() -> None:
    frames = [
        cast(
            FrameData,
            {
                "time": 0.0,
                "new_actors": [{"actor_id": 1, "object_id": _CAR}],
                "updated_actors": [
                    {
                        "actor_id": 1,
                        "object_id": _RB,
                        "attribute": _rb((5.0, 5.0, 5.0), (0.0, 0.0, 0.0, 1.0)),
                    }
                ],
            },
        ),
        cast(FrameData, {"time": 0.1, "deleted_actors": [1]}),
        cast(FrameData, {"time": 0.2}),
    ]
    rf = _extract(_replay(frames))

    car = next(s for s in rf.slots if s.kind == "car")
    assert car.segments == [(0, 1)]
    # frame 2 is outside the segment -> all zeros
    assert _pose(rf, 2, rf.slots.index(car)) == (0.0,) * 7


def test_empty_when_no_network_data() -> None:
    rf = _extract(_replay([]))
    assert rf == ReplayFrames([], [], b"", None, None)


# --- held-frame interpolation (_densify) ---


def _ball_idx(rf: ReplayFrames) -> int:
    return rf.slots.index(next(s for s in rf.slots if s.kind == "ball"))


def _car_idx(rf: ReplayFrames) -> int:
    return rf.slots.index(next(s for s in rf.slots if s.kind == "car"))


def test_held_frame_is_the_wallclock_linear_interpolant() -> None:
    # samples at frames 0 and 2; frame 1 has none. Non-uniform times, so the
    # wall-clock interpolant (x=75) differs from a frame-index midpoint (x=50).
    rf = _extract(
        _replay(
            [
                _frame(
                    0.0,
                    new=[{"actor_id": 9, "object_id": _BALL}],
                    updated=[_rb_update(9, (0.0, 0.0, 0.0), (0.0, 0.0, 0.0, 1.0))],
                ),
                _frame(0.15),
                _frame(
                    0.2,
                    updated=[_rb_update(9, (100.0, 0.0, 0.0), (0.0, 0.0, 0.0, 1.0))],
                ),
            ]
        )
    )
    assert _pose(rf, 1, _ball_idx(rf))[:3] == pytest.approx((75.0, 0.0, 0.0))  # pyright: ignore[reportUnknownMemberType]


def test_held_frame_rotation_is_slerped() -> None:
    th = math.pi / 2  # 90° about Z between frames 0 and 2
    q2 = (0.0, 0.0, math.sin(th / 2), math.cos(th / 2))
    rf = _extract(
        _replay(
            [
                _frame(
                    0.0,
                    new=[{"actor_id": 9, "object_id": _BALL}],
                    updated=[_rb_update(9, (0.0, 0.0, 0.0), (0.0, 0.0, 0.0, 1.0))],
                ),
                _frame(0.1),
                _frame(0.2, updated=[_rb_update(9, (0.0, 0.0, 0.0), q2)]),
            ]
        )
    )
    half = (0.0, 0.0, math.sin(th / 4), math.cos(th / 4))  # 45° about Z
    assert _pose(rf, 1, _ball_idx(rf))[3:] == pytest.approx(half)  # pyright: ignore[reportUnknownMemberType]


def test_rotation_before_first_sample_is_held_back_position_lerps_from_seed() -> None:
    q = (0.0, 0.0, math.sin(math.pi / 8), math.cos(math.pi / 8))  # 45° about Z
    rf = _extract(
        _replay(
            [
                _frame(
                    0.0,
                    new=[
                        {
                            "actor_id": 1,
                            "object_id": _CAR,
                            "initial_trajectory": {
                                "location": {"x": 10.0, "y": 20.0, "z": 30.0}
                            },
                        }
                    ],
                ),
                _frame(0.1),
                _frame(0.2, updated=[_rb_update(1, (10.0, 20.0, 40.0), q)]),
            ]
        )
    )
    idx = _car_idx(rf)
    # no usable seed rotation -> both pre-sample frames carry the first real quat
    assert _pose(rf, 0, idx) == pytest.approx((10.0, 20.0, 30.0, *q))  # pyright: ignore[reportUnknownMemberType]
    assert _pose(rf, 1, idx) == pytest.approx((10.0, 20.0, 35.0, *q))  # pyright: ignore[reportUnknownMemberType]


def test_single_sample_segment_holds_its_pose() -> None:
    rf = _extract(
        _replay(
            [
                _frame(
                    0.0,
                    new=[{"actor_id": 1, "object_id": _CAR}],
                    updated=[_rb_update(1, (5.0, 5.0, 5.0), (0.0, 0.0, 0.0, 1.0))],
                ),
                _frame(0.1),
                _frame(0.2),
            ]
        )
    )
    idx = _car_idx(rf)
    for f in (0, 1, 2):
        assert _pose(rf, f, idx) == (5.0, 5.0, 5.0, 0.0, 0.0, 0.0, 1.0)


def test_kickoff_reannounce_is_a_cut_not_a_glide() -> None:
    # ball live at (1000, 2000, 100), re-announced at centre spawn on frame 3
    # with no intervening delete. Frames 1-2 must hold, not drift toward spawn.
    rf = _extract(
        _replay(
            [
                _frame(
                    0.0,
                    new=[{"actor_id": 9, "object_id": _BALL}],
                    updated=[
                        _rb_update(9, (1000.0, 2000.0, 100.0), (0.0, 0.0, 0.0, 1.0))
                    ],
                ),
                _frame(0.1),
                _frame(0.2),
                _frame(
                    0.3,
                    new=[
                        {
                            "actor_id": 9,
                            "object_id": _BALL,
                            "initial_trajectory": {
                                "location": {"x": 0.0, "y": 0.0, "z": 93.0}
                            },
                        }
                    ],
                ),
                _frame(0.4),
            ]
        )
    )
    ball = next(s for s in rf.slots if s.kind == "ball")
    idx = rf.slots.index(ball)
    assert ball.segments == [(0, 4)]  # one lane, no gap
    assert _pose(rf, 1, idx)[:3] == (1000.0, 2000.0, 100.0)
    assert _pose(rf, 2, idx)[:3] == (1000.0, 2000.0, 100.0)
    assert _pose(rf, 3, idx)[:3] == (0.0, 0.0, 93.0)  # clean cut to spawn


# --- goal markers ---


def _scored(byte: int):
    return cast(
        FrameData,
        {
            "time": 0.0,
            "updated_actors": [
                {"actor_id": 2, "object_id": _SCORED, "attribute": {"Byte": byte}}
            ],
        },
    )


def test_scan_goals_counts_rising_edges_into_0_or_1() -> None:
    # team 0 scored on, re-sent, reset; then team 1 scored on
    frames = [
        _scored(0),
        _scored(0),  # re-send, not a new goal
        _scored(255),  # reset
        _scored(1),
        _scored(255),
    ]
    goals = _scan_goals(_replay(frames), _SCORED)
    assert [(g.frame, g.team) for g in goals] == [(0, 1), (3, 0)]


def test_scan_goals_empty_without_the_object() -> None:
    assert _scan_goals(_replay([_scored(0)]), None) == []


def test_scan_goals_on_a_real_replay_matches_the_scoreline() -> None:
    replay = parse_replay(load_replay("match.json"))
    goals = _scan_goals(
        replay,
        replay.object_index.get("TAGame.GameEvent_Soccar_TA:ReplicatedScoredOnTeam"),
    )
    team0 = replay.properties.get("Team0Score", 0)
    team1 = replay.properties.get("Team1Score", 0)
    assert len(goals) == team0 + team1
    assert sum(g.team == 0 for g in goals) == team0
    assert sum(g.team == 1 for g in goals) == team1
    frames = [g.frame for g in goals]
    assert frames == sorted(frames)


# --- kickoff countdowns + dead periods ---


def _countdown(n: int) -> FrameData:
    return cast(
        FrameData,
        {
            "time": 0.0,
            "updated_actors": [
                {"actor_id": 2, "object_id": _COUNTDOWN, "attribute": {"Int": n}}
            ],
        },
    )


def test_scan_countdowns_returns_every_tick_in_frame_order() -> None:
    frames = [
        _countdown(3),
        _countdown(2),
        _countdown(1),
        _countdown(0),
        _frame(0.0),
        _countdown(3),
        _countdown(2),
        _countdown(1),
        _countdown(0),
    ]
    ticks = _scan_countdowns(_replay(frames), _COUNTDOWN)
    assert ticks == [(0, 3), (1, 2), (2, 1), (3, 0), (5, 3), (6, 2), (7, 1), (8, 0)]


def test_scan_countdowns_empty_without_the_object() -> None:
    assert _scan_countdowns(_replay([_countdown(3)]), None) == []


def test_dead_periods_pair_each_goal_with_the_next_countdown_start() -> None:
    goals = [GoalMarker(frame=300, team=0), GoalMarker(frame=2000, team=1)]
    countdowns = [
        (20, 3), (40, 2), (60, 1), (80, 0),  # pre-match
        (400, 3), (420, 2), (440, 1), (460, 0),  # after goal 1
        (2100, 3), (2120, 2), (2140, 1), (2160, 0),  # after goal 2
    ]  # fmt: skip
    assert _dead_periods(goals, countdowns) == [(0, 19), (300, 399), (2000, 2099)]


def test_dead_periods_skip_a_goal_with_no_following_countdown() -> None:
    # a golden goal: the last countdown sequence (the OT kickoff) is before it
    goals = [GoalMarker(frame=300, team=0), GoalMarker(frame=5000, team=1)]
    countdowns = [(20, 3), (80, 0), (400, 3), (460, 0)]
    assert _dead_periods(goals, countdowns) == [(0, 19), (300, 399)]


def test_dead_periods_no_prematch_span_when_first_countdown_is_frame_zero() -> None:
    assert _dead_periods([], [(0, 3), (10, 0)]) == []


def test_dead_periods_empty_without_countdowns() -> None:
    assert _dead_periods([GoalMarker(frame=5, team=0)], []) == []


def test_overtime_replay_golden_goal_gets_no_dead_period() -> None:
    replay = parse_replay(load_replay("overtime.json"))
    goals = _scan_goals(
        replay,
        replay.object_index.get("TAGame.GameEvent_Soccar_TA:ReplicatedScoredOnTeam"),
    )
    countdowns = _scan_countdowns(
        replay,
        replay.object_index.get("TAGame.GameEvent_TA:ReplicatedRoundCountDownNumber"),
    )
    periods = _dead_periods(goals, countdowns)
    assert goals and countdowns
    # the golden goal ends the match — nothing to trim after it
    assert all(start != goals[-1].frame for start, _ in periods)
    # the earlier goals (and the pre-match warmup) still trim
    assert 2 <= len(periods) <= len(goals)
    assert periods[0][0] == 0


# --- boost pad pickups ---


def _pickup(
    aid: int, picked_up: int, instigator: int | None = None
) -> dict[str, object]:
    return {
        "actor_id": aid,
        "object_id": _PICKUP,
        "attribute": {"PickupNew": {"instigator": instigator, "picked_up": picked_up}},
    }


def _walk_pads(frames: list[FrameData]) -> list[tuple[int, int, bool, int | None]]:
    _segments, pickups = _walk(
        _replay(frames),
        _CAR,
        _BALL,
        _RB,
        _PRI,
        _UID,
        _PAINT,
        _PICKUP,
        frozenset({_PAD_A, _PAD_B}),
    )
    return pickups


def test_walk_emits_a_pickup_only_when_the_pad_state_flips() -> None:
    frames = [
        _frame(
            0.0,
            new=[
                {"actor_id": 50, "object_id": _PAD_A},
                {"actor_id": 51, "object_id": _PAD_B},
            ],
        ),
        # pads spawn available, so the opening 255 announce emits nothing
        _frame(0.1, updated=[_pickup(50, 255), _pickup(51, 255)]),
        _frame(0.2, updated=[_pickup(51, 7, instigator=10)]),  # PAD_B collected
        _frame(0.3, updated=[_pickup(51, 7, instigator=10)]),  # re-send, no row
        _frame(0.4, updated=[_pickup(51, 255)]),  # PAD_B available again
    ]
    assert _walk_pads(frames) == [
        (2, _PAD_B, True, 10),
        (4, _PAD_B, False, None),
    ]


def test_walk_rebinds_a_recycled_pickup_actor_id_to_its_new_pad() -> None:
    frames = [
        _frame(0.0, new=[{"actor_id": 51, "object_id": _PAD_B}]),
        _frame(0.1, updated=[_pickup(51, 5, instigator=10)]),  # PAD_B collected
        _frame(0.2, deleted=[51]),
        _frame(0.3, new=[{"actor_id": 51, "object_id": _PAD_A}]),  # id reused
        _frame(0.4, updated=[_pickup(51, 9, instigator=11)]),  # -> PAD_A
    ]
    assert _walk_pads(frames) == [
        (1, _PAD_B, True, 10),
        (4, _PAD_A, True, 11),
    ]


def test_walk_ignores_pickup_updates_for_an_unbound_actor() -> None:
    frames = [_frame(0.0, updated=[_pickup(99, 7, instigator=10)])]
    assert _walk_pads(frames) == []


def _pad_events(rf: ReplayFrames) -> dict[int, list[tuple[int, int]]]:
    """Per pad index, its ``(frame, collected)`` rows in frame order."""
    per_pad: dict[int, list[tuple[int, int]]] = {}
    for frame, p, collected, _x, _y in rf.boost_pads:
        per_pad.setdefault(p, []).append((frame, collected))
    return per_pad


@pytest.mark.parametrize(
    ("name", "pad_count"), [("match.json", 34), ("hoops.json", 20)]
)
def test_real_replay_boost_pads_cover_every_pad_and_alternate(
    name: str, pad_count: int
) -> None:
    rf = replay_frames_of(name)
    rows = rf.boost_pads
    assert rows

    # frame order, like goals / countdowns
    assert [f for f, *_ in rows] == sorted(f for f, *_ in rows)
    # a dense index over exactly the mode's pad layout
    assert sorted({p for _, p, *_ in rows}) == list(range(pad_count))

    last = len(rf.frame_times) - 1
    for frame, _p, collected, x, y in rows:
        assert 0 <= frame <= last
        assert collected in (0, 1)
        if collected == 0:  # respawn rows carry no instigator position
            assert (x, y) == (0.0, 0.0)

    # every pad strictly alternates collected <-> available, collect first
    for states in ([c for _, c in evs] for evs in _pad_events(rf).values()):
        assert states and states[0] == 1
        assert all(a != b for a, b in zip(states, states[1:], strict=False))


@pytest.mark.parametrize("name", ["match.json", "hoops.json"])
def test_real_replay_boost_pad_respawns_match_canonical_timers(name: str) -> None:
    rf = replay_frames_of(name)
    ft = rf.frame_times
    deltas = [
        ft[f1] - ft[f0]
        for evs in _pad_events(rf).values()
        for (f0, c0), (f1, c1) in zip(evs, evs[1:], strict=False)
        if c0 == 1 and c1 == 0
    ]
    # small pads respawn in ~4 s, big pads in ~10 s (canonical RL). Most gaps
    # sit near one of the two, and both classes show up.
    assert deltas
    near = [d for d in deltas if abs(d - 4) < 1.5 or abs(d - 10) < 2.0]
    assert len(near) / len(deltas) > 0.8
    assert any(abs(d - 4) < 1.5 for d in deltas)
    assert any(abs(d - 10) < 2.0 for d in deltas)


@pytest.mark.parametrize("name", ["match.json", "hoops.json"])
def test_real_replay_boost_pads_all_available_at_every_kickoff(name: str) -> None:
    """The raw pickup stream self-heals — no pad reads collected when a kickoff
    countdown fires, so the viewer needs no forced reset (ADR 0004)."""
    rf = replay_frames_of(name)
    per_pad = _pad_events(rf)

    def collected_at(pad: int, f: int) -> int:
        state = 0
        for row_f, row_c in per_pad[pad]:
            if row_f > f:
                break
            state = row_c
        return state

    kickoff_frames = [f for f, n in rf.countdowns if n in (0, 3)]
    assert kickoff_frames
    for f in kickoff_frames:
        assert not [p for p in per_pad if collected_at(p, f)]


def test_real_replay_boost_pad_collects_are_located_inside_the_arena() -> None:
    rf = replay_frames_of("match.json")
    collects = [(x, y) for _, _, c, x, y in rf.boost_pads if c == 1]
    located = [p for p in collects if p != (0.0, 0.0)]
    assert len(located) / len(collects) > 0.9
    for x, y in located:  # the instigating car is on the pad, inside the field
        assert abs(x) < 4300 and abs(y) < 5300


# --- integration: a real 2v2 replay ---


def _real() -> ReplayFrames:
    return replay_frames_of("team_size_2.json")


def test_real_replay_top_level_shape() -> None:
    rf = _real()
    assert len(rf.frame_times) == 9113
    assert rf.frame_times == sorted(rf.frame_times)
    assert rf.tracked_team == 1
    assert rf.game_mode == "2v2"
    assert len(rf.positions) == packed_buffer_bytes(len(rf.frame_times), len(rf.slots))


def test_real_replay_has_one_ball_and_four_cars() -> None:
    rf = _real()
    assert sum(s.kind == "ball" for s in rf.slots) == 1
    assert sum(s.kind == "car" for s in rf.slots) == 4


def test_real_replay_identifies_the_tracked_pair() -> None:
    rf = _real()
    tracked = [s for s in rf.slots if s.is_tracked]
    assert {s.name for s in tracked} == {"Drew", "Steve"}
    assert {s.team for s in tracked} == {1}
    assert all(s.identity is not None for s in tracked)


def test_real_replay_car_slots_are_two_per_team() -> None:
    rf = _real()
    cars = [s for s in rf.slots if s.kind == "car"]
    teams = [s.team for s in cars]
    assert None not in teams
    assert sorted(t for t in teams if t is not None) == [0, 0, 1, 1]
    ball = next(s for s in rf.slots if s.kind == "ball")
    assert ball.team is None


def test_real_replay_segments_are_sane() -> None:
    rf = _real()
    last = len(rf.frame_times) - 1
    for slot in rf.slots:
        assert slot.segments == sorted(slot.segments)
        for start, end in slot.segments:
            assert 0 <= start <= end <= last


def test_real_replay_countdowns_and_dead_periods() -> None:
    rf = _real()
    last = len(rf.frame_times) - 1

    assert rf.countdowns
    assert all(0 <= n <= 3 for _, n in rf.countdowns)
    assert [f for f, _ in rf.countdowns] == sorted(f for f, _ in rf.countdowns)
    assert all(0 <= f <= last for f, _ in rf.countdowns)
    # one clean 3 -> ... -> 0 run per kickoff: pre-match + one per goal (holds
    # for overtime too — the OT-start countdown offsets the missing one after
    # the golden goal)
    assert sum(1 for _, n in rf.countdowns if n == 0) == len(rf.goals) + 1
    assert sum(1 for _, n in rf.countdowns if n == 3) == len(rf.goals) + 1

    assert all(0 <= g.frame <= last for g in rf.goals)

    assert rf.dead_periods
    assert rf.dead_periods[0][0] == 0  # pre-match warmup is always trimmed
    goal_frames = {g.frame for g in rf.goals}
    prev_end = -1
    for start, end in rf.dead_periods:
        assert 0 <= start <= end <= last
        assert start > prev_end  # non-overlapping, ascending
        prev_end = end
    # every span past the pre-match one starts on a goal frame
    assert all(start in goal_frames for start, _ in rf.dead_periods[1:])
    assert len(rf.dead_periods) <= len(rf.goals) + 1


def test_dead_period_count_is_exact_on_a_no_overtime_replay() -> None:
    # match.json ends in regulation: exactly one trimmed span per kickoff —
    # pre-match + one per goal. Overtime relaxes this to <= (a golden goal has
    # no following countdown; see test_overtime_replay_golden_goal_gets_no_dead_period).
    rf = replay_frames_of("match.json")
    assert len(rf.dead_periods) == len(rf.goals) + 1 == 10


def test_real_replay_poses_decode_to_plausible_values() -> None:
    rf = _real()
    mid = len(rf.frame_times) // 2
    for slot_idx, slot in enumerate(rf.slots):
        if not any(start <= mid <= end for start, end in slot.segments):
            continue
        x, y, z, qx, qy, qz, qw = _pose(rf, mid, slot_idx)
        assert all(math.isfinite(v) for v in (x, y, z, qx, qy, qz, qw))
        # generously inside the arena (half-extents ~4096 x 5120 x 2044 uu)
        assert abs(x) < 6000 and abs(y) < 7000 and -100 < z < 2500
        # rotation is a unit quaternion
        assert abs(math.sqrt(qx * qx + qy * qy + qz * qz + qw * qw) - 1.0) < 0.05


# --- integration: a real hoops replay ---
#
# Hoops names its ball actor Ball_BasketBall, not Ball_Default, so extract_
# replay_frames used to bail out with an empty ReplayFrames. These pin that the
# archetype fallback keeps a hoops replay walkable.


def _hoops() -> ReplayFrames:
    return replay_frames_of("hoops.json")


def test_hoops_replay_walks_to_a_full_buffer() -> None:
    rf = _hoops()
    assert rf.game_mode == "hoops"
    assert len(rf.frame_times) == 11184
    assert rf.frame_times == sorted(rf.frame_times)
    assert len(rf.positions) == packed_buffer_bytes(len(rf.frame_times), len(rf.slots))


def test_hoops_replay_has_one_ball_and_four_cars() -> None:
    rf = _hoops()
    assert sum(s.kind == "ball" for s in rf.slots) == 1
    assert sum(s.kind == "car" for s in rf.slots) == 4


def test_hoops_replay_identifies_the_tracked_pair() -> None:
    rf = _hoops()
    tracked = [s for s in rf.slots if s.is_tracked]
    assert {s.name for s in tracked} == {"Drew", "Jeff"}
    assert all(s.identity is not None for s in tracked)


def test_hoops_replay_carries_goals_and_countdowns() -> None:
    rf = _hoops()
    assert rf.goals
    assert rf.countdowns
    assert rf.dead_periods and rf.dead_periods[0][0] == 0


def test_hoops_replay_poses_sit_inside_the_hoops_footprint() -> None:
    rf = _hoops()
    mid = len(rf.frame_times) // 2
    for slot_idx, slot in enumerate(rf.slots):
        if not any(start <= mid <= end for start, end in slot.segments):
            continue
        x, y, z, qx, qy, qz, qw = _pose(rf, mid, slot_idx)
        assert all(math.isfinite(v) for v in (x, y, z, qx, qy, qz, qw))
        # hoops half-extents ~2967 x 3581 x 1820 uu — allow slack for wall climbs
        assert abs(x) < 4200 and abs(y) < 4600 and -100 < z < 2200
        assert abs(math.sqrt(qx * qx + qy * qy + qz * qz + qw * qw) - 1.0) < 0.05
