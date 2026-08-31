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

from ingest import build_replay_context
from replay_frames import (
    ReplayFrames,
    _scan_goals,  # type: ignore[reportPrivateUsage]
    extract_replay_frames,
)
from rrrocket_schema import FrameData, ParsedReplay
from rrrocket_schema import parse as parse_replay
from tests.fixtures import TRACKED_PLAYERS, load_replay

_OBJECTS = [
    "Archetypes.Car.Car_Default",
    "Archetypes.Ball.Ball_Default",
    "TAGame.RBActor_TA:ReplicatedRBState",
    "Engine.Pawn:PlayerReplicationInfo",
    "Engine.PlayerReplicationInfo:UniqueId",
    "TAGame.Car_TA:TeamPaint",
    "TAGame.GameEvent_Soccar_TA:ReplicatedScoredOnTeam",
]
_CAR, _BALL, _RB, _PRI, _UID, _PAINT, _SCORED = range(7)


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


def _pose(rf: ReplayFrames, frame: int, slot: int) -> tuple[float, ...]:
    n = len(rf.slots)
    off = (frame * n + slot) * 7 * 4
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
    assert rf == ReplayFrames([], [], b"", None, None, [])


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


# --- integration: a real 2v2 replay ---


def _real() -> ReplayFrames:
    replay = parse_replay(load_replay("team_size_2.json"))
    context = build_replay_context(replay, TRACKED_PLAYERS)
    return extract_replay_frames(
        replay,
        tracked_team=context.perspective.team,
        tracked_identities=context.tracked_identities,
        player_names=context.player_names,
        game_mode=context.game_mode,
    )


def test_real_replay_top_level_shape() -> None:
    rf = _real()
    assert len(rf.frame_times) == 9113
    assert rf.frame_times == sorted(rf.frame_times)
    assert rf.tracked_team == 1
    assert rf.game_mode == "2v2"
    assert len(rf.positions) == 4 * len(rf.frame_times) * len(rf.slots) * 7


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
