"""Replay Frames — reshape rrrocket network data into a playback buffer.

Feeds the browser replay viewer (see ``docs/adr/0004-browser-replay-viewer-design.md``). Where
``frame_analysis.py`` walks the same frames to compute aggregate match stats and
throws the poses away, this module keeps them: a dense ``Float32`` position
buffer plus the metadata needed to play it back in the browser.

The core is :func:`extract_replay_frames`, a pure function over a
``ParsedReplay`` whose signature mirrors ``analyze_frames``. The rrrocket
subprocess glue (a non-deleting variant of ``process.parse_replay``) lives with
the route layer, not here.

Divergences from the ``frame_analysis`` handlers, both deliberate:

* **No ``is_playing`` gate.** The viewer shows the whole stream — warmup,
  countdowns, celebrations — and lets the user scrub past dead time.
* **Poses are kept per frame.** rrrocket's ~30 Hz samples are densified to one
  pose per frame per lane, interpolating (lerp / slerp) across the frames a lane
  did not update so the client can play the buffer back straight.
"""

import math
import sys
from array import array
from collections.abc import Iterator
from collections.abc import Set as AbstractSet
from dataclasses import dataclass, field
from typing import Any

from player_identity import IdentityResolver, PlayerIdentity, from_network_frame
from rrrocket_schema import NetObj, ParsedReplay

# x, y, z, qx, qy, qz, qw. Mirrors FLOATS_PER_POSE in static/replay-core.js —
# keep in lockstep (structurally frozen: a position vec3 + an orientation
# quaternion; there is no build step binding the two).
FLOATS_PER_POSE = 7
_BYTES_PER_FLOAT = 4


def pose_offset(slot_count: int, frame: int, slot: int) -> int:
    """Float index of ``(frame, slot)``'s pose in the row-major
    ``[frame][slot][x, y, z, qx, qy, qz, qw]`` position buffer.

    Verbatim twin of ``poseOffset()`` in ``static/replay-core.js`` — keep in
    lockstep. See CONTEXT.md "Replay Wire".
    """
    return (frame * slot_count + slot) * FLOATS_PER_POSE


def packed_buffer_bytes(frame_count: int, slot_count: int) -> int:
    """Byte length of a full :attr:`ReplayFrames.positions` buffer."""
    return _BYTES_PER_FLOAT * frame_count * slot_count * FLOATS_PER_POSE


# Boost pads are per-map level actors; every one's object name carries this
# ("<map>.TheWorld:PersistentLevel.VehiclePickup_Boost_TA_<n>"). The name is the
# pad's stable identity — the network updates that report pickups all share one
# NewReplicatedPickupData object id, so only new_actors ties an actor to its pad.
_BOOST_PAD_MARKER = "PersistentLevel.VehiclePickup_Boost_TA"

# The ball actor's archetype name is mode-specific. The frame walk keys on a
# single archetype oid, so resolve whichever of these the replay carries.
# (frame_analysis.py has the same Ball_Default assumption for its positional
# stats — out of scope here; hoops match stats come from the PlayerStats blob,
# not the frame walk.)
_BALL_ARCHETYPE_NAMES = (
    NetObj.BALL_ARCHETYPE.value,  # soccar
    NetObj.BALL_ARCHETYPE_HOOPS.value,  # hoops
)


def _ball_archetype_oid(object_index: dict[str, int]) -> int | None:
    """The object id of this replay's ball archetype, or ``None`` if it carries
    no archetype this module knows how to walk."""
    for name in _BALL_ARCHETYPE_NAMES:
        oid = object_index.get(name)
        if oid is not None:
            return oid
    return None


# (frame, pad_object_id, collected, instigator_actor_id) — one raw pad state
# flip from the walk, before pad ids are densified and the instigator located.
_RawPickup = tuple[int, int, bool, int | None]

# (frame_idx, x, y, z, qx, qy, qz, qw)
_Sample = tuple[int, float, float, float, float, float, float, float]
# (x, y, z, qx, qy, qz, qw) — one row of the position buffer
_Pose = tuple[float, float, float, float, float, float, float]

_IDENTITY_POSE: _Pose = (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0)


@dataclass(frozen=True)
class ActorSlot:
    """One playback lane in the position buffer.

    A car slot gathers every network actor a single player drove across the
    match (cars get a fresh actor id each life); ``segments`` says which frame
    ranges the lane is live, and the client hides its mesh outside them.
    """

    identity: PlayerIdentity | None  # None for cars whose PRI never resolved
    name: str  # display name > in-game name > "Player N"
    team: int | None  # 0 | 1 for cars (from TeamPaint); None for the ball
    is_tracked: bool
    kind: str  # "car" | "ball"
    segments: list[tuple[int, int]]  # (start_frame, end_frame) inclusive, sorted


@dataclass(frozen=True)
class GoalMarker:
    frame: int  # frame index the goal was scored on
    team: int  # the team that scored (0 or 1)


@dataclass(frozen=True)
class ReplayFrames:
    frame_times: list[
        float
    ]  # length F, wall-clock seconds from replay start, non-uniform
    slots: list[ActorSlot]  # ball slot (if any) + one car slot per player
    positions: bytes  # F * N * 7 little-endian float32, row-major [frame][slot][x,y,z,qx,qy,qz,qw]
    tracked_team: int | None
    game_mode: str | None
    # all three in frame order; default empty for the no-network-data case
    goals: list[GoalMarker] = field(default_factory=list[GoalMarker])
    # (frame_index, n) per kickoff tick
    countdowns: list[tuple[int, int]] = field(default_factory=list[tuple[int, int]])
    # (start, end) inclusive frame indices, non-overlapping, ascending
    dead_periods: list[tuple[int, int]] = field(default_factory=list[tuple[int, int]])
    # (frame, pad, collected, x, y) per boost-pad state flip, in frame order.
    # `pad` is a dense 0-based index (ascending pad object id); `collected` is 1
    # when the pad was picked up, 0 when it became available again. `x, y` are
    # the instigating car's position at that frame — only meaningful when
    # `collected == 1` (0.0, 0.0 on a respawn row or an unlocatable car).
    boost_pads: list[tuple[int, int, int, float, float]] = field(
        default_factory=list[tuple[int, int, int, float, float]]
    )

    def meta_dict(self) -> dict[str, Any]:
        """The ``/api/matches/{id}/replay`` JSON body — every field except
        ``positions`` (served raw by the ``.bin`` route).

        The sole serialization entry point: ``server.py``'s route and
        ``tests/e2e/dump_fixture`` both call this rather than assembling the dict
        themselves. It is *not* the wire-shape declaration — that is the
        ``WIRE_*`` manifest below. See CONTEXT.md "Replay Wire".
        """
        return {
            "frame_times": self.frame_times,
            "tracked_team": self.tracked_team,
            "game_mode": self.game_mode,
            "slots": self.slots,
            "goals": self.goals,
            "countdowns": self.countdowns,
            "dead_periods": self.dead_periods,
            "boost_pads": self.boost_pads,
        }


# --- the replay wire contract (see CONTEXT.md "Replay Wire") ---
#
# These sets are stated here INDEPENDENTLY of the dataclasses on purpose: they
# are the half of the drift check that must not move when a field is renamed. A
# set derived from ``fields(ActorSlot)`` would follow a rename automatically and
# never catch it. ``tests/test_replay_wire.py`` checks the emitted JSON against
# these, and checks these against the dataclasses in the *reverse* direction (no
# dataclass field missing from the manifest).

WIRE_META_KEYS = frozenset(
    {
        "frame_times",
        "tracked_team",
        "game_mode",
        "slots",
        "goals",
        "countdowns",
        "dead_periods",
        "boost_pads",
    }
)

# Read by static/replay-core.js and static/replay.js — grep `slot\.` in both
# before changing a name. `identity` and `is_tracked` ride the wire but no JS
# path reads them today (they are there for a future per-player view); a rename
# is still a breaking change once that view exists.
WIRE_SLOT_FIELDS = frozenset(
    {"identity", "name", "team", "is_tracked", "kind", "segments"}
)

WIRE_GOAL_FIELDS = frozenset({"frame", "team"})

# Positionally-packed rows on the wire — width is the contract (the client
# destructures by index). `segments` is nested one level down, inside each slot.
WIRE_TUPLE_WIDTHS = {
    "segments": 2,  # (start, end), inclusive frame indices
    "countdowns": 2,  # (frame, n)
    "dead_periods": 2,  # (start, end), inclusive frame indices
    "boost_pads": 5,  # (frame, pad, collected, x, y)
}


@dataclass(eq=False)
class _Segment:
    kind: str
    actor_id: int
    start: int
    end: int  # -1 while the actor is still live
    seed: tuple[float, float, float] | None  # initial_trajectory location, if given
    identity: PlayerIdentity | None = None  # resolved when the segment closes
    team: int | None = None
    samples: list[_Sample] = field(default_factory=list[_Sample])
    resets: set[int] = field(default_factory=set[int])  # kickoff re-announce frames
    slot: int = -1


def _actor_updates(
    replay: ParsedReplay, oid: int | None
) -> Iterator[tuple[int, dict[str, Any]]]:
    """``(frame_index, attribute)`` for every ``updated_actors`` entry on ``oid``.

    The shared skeleton behind the game-event scans (``_walk`` has its own, richer
    pass). Yields nothing when ``oid`` is ``None`` (the object isn't in the
    replay's index).
    """
    if oid is None:
        return
    for fidx, frame in enumerate(replay.frames):
        for ua in frame.get("updated_actors", []):
            if ua.get("object_id") == oid:
                yield fidx, ua.get("attribute", {})


def _scan_goals(replay: ParsedReplay, scored_oid: int | None) -> list[GoalMarker]:
    """Goal frames from ``ReplicatedScoredOnTeam`` rising edges.

    The attribute carries the team that was *scored on* (0 or 1); it is re-sent a
    few times per goal and reset to 255 in between, so a goal is each transition
    into {0, 1} from anything else.
    """
    goals: list[GoalMarker] = []
    last = -1
    for fidx, attr in _actor_updates(replay, scored_oid):
        byte = attr.get("Byte")
        if byte in (0, 1) and last not in (0, 1):
            goals.append(GoalMarker(frame=fidx, team=1 - byte))
        if byte is not None:
            last = byte
    return goals


def _scan_countdowns(
    replay: ParsedReplay, countdown_oid: int | None
) -> list[tuple[int, int]]:
    """Every ``ReplicatedRoundCountDownNumber`` tick as ``(frame_index, n)``.

    The attribute is an ``Int`` that counts ``3 -> 2 -> 1 -> 0`` once per kickoff
    — pre-match, after every goal, and for an overtime kickoff. ``0`` is the
    frame live play resumes (``frame_analysis`` flips ``is_playing`` there).
    """
    return [
        (fidx, int(n))
        for fidx, attr in _actor_updates(replay, countdown_oid)
        if (n := attr.get("Int")) is not None
    ]


def _dead_periods(
    goals: list[GoalMarker], countdowns: list[tuple[int, int]]
) -> list[tuple[int, int]]:
    """Frame spans where play is stopped, for the viewer to collapse.

    Each runs ``[start, end]`` inclusive: ``start`` is a goal frame (or ``0`` for
    the pre-match warmup) and ``end`` is the frame before that kickoff's
    countdown begins — so the goal replay, the actor reset and the
    frozen-at-spawn wait are trimmed while the 3-2-1 countdown itself stays in
    the timeline. A goal with no following countdown (an overtime golden goal, or
    one as the clock expires) gets no span.

    Every kickoff countdown opens with an ``n == 3`` tick (verified across the
    sample replays); each is consumed by at most one stop frame, so the returned
    spans are non-overlapping and in ascending frame order — the viewer relies on
    that.
    """
    starts = [f for f, n in countdowns if n == 3]
    periods: list[tuple[int, int]] = []
    si = 0
    for stop in (-1, *(g.frame for g in goals)):
        while si < len(starts) and starts[si] <= stop:
            si += 1
        if si < len(starts) and starts[si] > max(stop, 0):
            periods.append((max(stop, 0), starts[si] - 1))
            si += 1
    return periods


def _first_team(segments: list[_Segment]) -> int | None:
    for seg in segments:
        if seg.team is not None:
            return seg.team
    return None


def _last_quat(samples: list[_Sample]) -> tuple[float, float, float, float]:
    """The most recent rotation for a lane, or the identity quaternion."""
    if samples:
        s = samples[-1]
        return s[4], s[5], s[6], s[7]
    return 0.0, 0.0, 0.0, 1.0


def _walk(
    replay: ParsedReplay,
    car_arch: int,
    ball_arch: int,
    rb_oid: int,
    pri_oid: int | None,
    uid_oid: int | None,
    paint_oid: int | None,
    pickup_oid: int | None,
    pad_oids: AbstractSet[int],
) -> tuple[list[_Segment], list[_RawPickup]]:
    """Single pass over the frames, producing one ``_Segment`` per actor life
    plus the boost-pad state flips (``_RawPickup`` rows, in frame order).

    Preserves ``_process_frame``'s phase order (new -> updated -> deleted) so a
    car spawned and PRI-linked in the same frame keeps its link. Boost pads ride
    the same pass: ``new_actors`` binds an actor id to its pad (they recycle, so
    the bind is cleared on delete and rebuilt on the pad's next announce), and a
    ``NewReplicatedPickupData`` update is emitted only when it flips the pad's
    collected/available state — the game re-sends the current state periodically.
    """
    segments: list[_Segment] = []
    open_seg: dict[int, int] = {}  # actor_id -> index into segments
    car_actors: set[int] = set()
    ball_actors: set[int] = set()
    resolver = IdentityResolver()
    actor_team: dict[int, int] = {}

    pickups: list[_RawPickup] = []
    actor_pad: dict[int, int] = {}  # actor_id -> pad object id (recycles)
    pad_collected: dict[int, bool] = {}  # pad object id -> last emitted state

    frames = replay.frames
    last_frame = len(frames) - 1

    for fidx, frame in enumerate(frames):
        # 1. new_actors. Replays re-announce a live car/ball at every kickoff,
        #    each with a fresh initial_trajectory and no preceding delete — that
        #    is a reset, not a new life. Only an aid that is not already open
        #    starts a segment; a re-announcement just snaps the lane to the new
        #    spawn point.
        for na in frame.get("new_actors", []):
            oid = na.get("object_id")
            if oid == car_arch:
                kind = "car"
            elif oid == ball_arch:
                kind = "ball"
            elif oid is not None and oid in pad_oids:
                actor_pad[na["actor_id"]] = oid
                pad_collected.setdefault(oid, False)  # pads spawn available
                continue
            else:
                continue
            aid = na["actor_id"]
            loc = (na.get("initial_trajectory", {})).get("location", {})
            have_loc = {"x", "y", "z"} <= loc.keys()
            si = open_seg.get(aid)
            if si is not None:
                if have_loc:
                    seg = segments[si]
                    qx, qy, qz, qw = _last_quat(seg.samples)
                    seg.samples.append(
                        (
                            fidx,
                            float(loc["x"]),
                            float(loc["y"]),
                            float(loc["z"]),
                            qx,
                            qy,
                            qz,
                            qw,
                        )
                    )
                    # A kickoff reset is a cut: the fill holds the pre-kickoff
                    # pose up to here rather than gliding the lane to spawn.
                    seg.resets.add(fidx)
                continue
            seed = (
                (float(loc["x"]), float(loc["y"]), float(loc["z"]))
                if have_loc
                else None
            )
            open_seg[aid] = len(segments)
            segments.append(
                _Segment(kind=kind, actor_id=aid, start=fidx, end=-1, seed=seed)
            )
            (car_actors if kind == "car" else ball_actors).add(aid)

        # 2. updated_actors — resolve identity/team, record RigidBody samples
        for ua in frame.get("updated_actors", []):
            attr = ua.get("attribute", {})
            if not attr:
                continue
            oid = ua.get("object_id")
            aid = ua["actor_id"]

            if oid == pri_oid:
                pri = attr.get("ActiveActor", {}).get("actor")
                if pri is not None and pri >= 0 and aid in car_actors:
                    resolver.link_car_to_pri(aid, pri)
            elif oid == uid_oid:
                ident = from_network_frame(attr.get("UniqueId", {}))
                if ident:
                    resolver.set_identity(aid, ident)
            elif oid == paint_oid:
                team = attr.get("TeamPaint", {}).get("team")
                if team is not None:
                    actor_team[aid] = team
            elif oid == rb_oid:
                if aid not in car_actors and aid not in ball_actors:
                    continue
                si = open_seg.get(aid)
                if si is None:
                    continue
                rb = attr.get("RigidBody", {})
                loc = rb.get("location", {})
                if "x" not in loc:
                    continue
                seg = segments[si]
                # rotation and velocities can be absent from an update; carry the
                # last quaternion forward, or identity if none seen yet.
                lqx, lqy, lqz, lqw = _last_quat(seg.samples)
                rot: dict[str, float] = rb.get("rotation") or {}
                seg.samples.append(
                    (
                        fidx,
                        float(loc["x"]),
                        float(loc.get("y", 0.0)),
                        float(loc.get("z", 0.0)),
                        float(rot.get("x", lqx)),
                        float(rot.get("y", lqy)),
                        float(rot.get("z", lqz)),
                        float(rot.get("w", lqw)),
                    )
                )

            elif oid == pickup_oid:
                pad_oid = actor_pad.get(aid)
                if pad_oid is None:
                    continue
                # picked_up: 255 = available/respawning, else a rolling sequence
                # number that changes on each grab. instigator is the grabbing
                # car (absent once the pad is empty again).
                pickup = attr.get("PickupNew")
                if not pickup:
                    continue
                state = pickup.get("picked_up")
                if state is None:
                    continue
                collected = state != 255
                if pad_collected.get(pad_oid) == collected:
                    continue
                pad_collected[pad_oid] = collected
                instigator = pickup.get("instigator") if collected else None
                pickups.append((fidx, pad_oid, collected, instigator))

        # 3. deleted_actors — close segments (resolving identity first), then purge
        deleted = frame.get("deleted_actors", [])
        for aid in deleted:
            si = open_seg.pop(aid, None)
            if si is None:
                continue
            seg = segments[si]
            if seg.kind == "car":
                seg.identity = resolver.resolve_car(aid)
            seg.team = actor_team.get(aid)
            seg.end = fidx
        for aid in deleted:
            car_actors.discard(aid)
            ball_actors.discard(aid)
            resolver.remove_actor(aid)
            actor_team.pop(aid, None)
            actor_pad.pop(aid, None)  # the pad rebinds this id on its next announce

    # Close whatever is still live at the final frame.
    for aid, si in open_seg.items():
        seg = segments[si]
        if seg.kind == "car":
            seg.identity = resolver.resolve_car(aid)
        seg.team = actor_team.get(aid)
        seg.end = last_frame

    return segments, pickups


def _build_slots(
    segments: list[_Segment],
    tracked_identities: AbstractSet[PlayerIdentity],
    player_names: dict[PlayerIdentity, str],
) -> list[ActorSlot]:
    """Group segments into playback lanes and assign each segment its slot index."""
    ball_segs: list[_Segment] = []
    car_by_identity: dict[PlayerIdentity, list[_Segment]] = {}
    anon_car_segs: list[_Segment] = []
    for seg in segments:
        if seg.kind == "ball":
            ball_segs.append(seg)
        elif seg.identity is not None:
            car_by_identity.setdefault(seg.identity, []).append(seg)
        else:
            anon_car_segs.append(seg)

    slots: list[ActorSlot] = []
    anon_n = 0

    if ball_segs:
        for s in ball_segs:
            s.slot = len(slots)
        slots.append(
            ActorSlot(
                identity=None,
                name="Ball",
                team=None,
                is_tracked=False,
                kind="ball",
                segments=sorted((s.start, s.end) for s in ball_segs),
            )
        )

    # Deterministic order: by team, then display name.
    ordered = sorted(
        car_by_identity.items(),
        key=lambda kv: (
            _first_team(kv[1]) if _first_team(kv[1]) is not None else 9,
            player_names.get(kv[0], ""),
        ),
    )
    for identity, segs in ordered:
        for s in segs:
            s.slot = len(slots)
        name = player_names.get(identity)
        if not name:
            anon_n += 1
            name = f"Player {anon_n}"
        slots.append(
            ActorSlot(
                identity=identity,
                name=name,
                team=_first_team(segs),
                is_tracked=identity in tracked_identities,
                kind="car",
                segments=sorted((s.start, s.end) for s in segs),
            )
        )

    # A car whose PRI never resolved (bots, odd platforms) still gets a lane, one
    # per life — without identity its lives cannot be merged.
    for seg in anon_car_segs:
        seg.slot = len(slots)
        anon_n += 1
        slots.append(
            ActorSlot(
                identity=None,
                name=f"Player {anon_n}",
                team=seg.team,
                is_tracked=False,
                kind="car",
                segments=[(seg.start, seg.end)],
            )
        )

    return slots


# The near-parallel cutoff for _slerp's fast path, == JS Number.EPSILON.
_EPSILON = sys.float_info.epsilon


def _slerp(
    a: tuple[float, float, float, float],
    b: tuple[float, float, float, float],
    t: float,
) -> tuple[float, float, float, float]:
    """Spherical linear interpolation between two unit quaternions.

    A verbatim port of ``slerpQuat()`` in ``static/replay-core.js`` — itself a
    port of ``THREE.Quaternion.slerp`` (ADR-0004 §15). The browser re-interpolates
    between the frames this bakes, so the two must agree; keep them in lockstep.
    ``tests/test_replay_frames.py`` and ``tests/js/replay-core.test.js`` check
    both against the same quaternion cases, and the e2e suite pins ``slerpQuat``
    to THREE — so ``_slerp`` ← analytic cases → ``slerpQuat`` ← e2e → THREE.
    """
    ax, ay, az, aw = a
    bx, by, bz, bw = b
    if t == 0.0:
        return ax, ay, az, aw
    if t == 1.0:
        return bx, by, bz, bw

    cos_half_theta = aw * bw + ax * bx + ay * by + az * bz
    if cos_half_theta < 0.0:  # take the shorter arc
        bx, by, bz, bw = -bx, -by, -bz, -bw
        cos_half_theta = -cos_half_theta
    if cos_half_theta >= 1.0:
        return ax, ay, az, aw

    sqr_sin_half_theta = 1.0 - cos_half_theta * cos_half_theta
    if sqr_sin_half_theta <= _EPSILON:  # almost parallel — nlerp dodges sinθ→0
        s = 1.0 - t
        rx, ry, rz, rw = (
            s * ax + t * bx,
            s * ay + t * by,
            s * az + t * bz,
            s * aw + t * bw,
        )
        n = math.sqrt(rx * rx + ry * ry + rz * rz + rw * rw)
        if n == 0.0:
            return 0.0, 0.0, 0.0, 1.0
        return rx / n, ry / n, rz / n, rw / n

    sin_half_theta = math.sqrt(sqr_sin_half_theta)
    half_theta = math.atan2(sin_half_theta, cos_half_theta)
    ratio_a = math.sin((1.0 - t) * half_theta) / sin_half_theta
    ratio_b = math.sin(t * half_theta) / sin_half_theta
    return (
        ax * ratio_a + bx * ratio_b,
        ay * ratio_a + by * ratio_b,
        az * ratio_a + bz * ratio_b,
        aw * ratio_a + bw * ratio_b,
    )


def _lerp_pose(a: _Pose, b: _Pose, w: float) -> _Pose:
    """Linear on position, slerp on rotation; ``w`` clamped to [0, 1]."""
    w = 0.0 if w < 0.0 else 1.0 if w > 1.0 else w
    qx, qy, qz, qw = _slerp((a[3], a[4], a[5], a[6]), (b[3], b[4], b[5], b[6]), w)
    return (
        a[0] + (b[0] - a[0]) * w,
        a[1] + (b[1] - a[1]) * w,
        a[2] + (b[2] - a[2]) * w,
        qx,
        qy,
        qz,
        qw,
    )


def _sample_pose(s: _Sample) -> _Pose:
    """A sample's 7-float pose, without its leading frame index."""
    return s[1], s[2], s[3], s[4], s[5], s[6], s[7]


def _keyframes(seg: _Segment) -> list[tuple[int, _Pose]]:
    """The poses the fill interpolates between: every real sample, always
    preceded by a keyframe at ``seg.start`` so the list spans the whole segment.

    When the first real sample lands after ``seg.start``, that leading keyframe
    takes its position from ``initial_trajectory`` if there is one (it carries no
    usable rotation) and otherwise holds the first sample's position back; its
    rotation is the first sample's, held back rather than slerped up from
    identity.
    """
    kf: list[tuple[int, _Pose]] = [(s[0], _sample_pose(s)) for s in seg.samples]
    if not kf:
        s = seg.seed
        pose: _Pose = (
            (s[0], s[1], s[2], 0.0, 0.0, 0.0, 1.0) if s is not None else _IDENTITY_POSE
        )
        return [(seg.start, pose)]
    if kf[0][0] > seg.start:
        p = kf[0][1]
        x, y, z = seg.seed if seg.seed is not None else (p[0], p[1], p[2])
        kf.insert(0, (seg.start, (x, y, z, p[3], p[4], p[5], p[6])))
    return kf


def _fill(
    buf: "array[float]",
    lane_off: int,
    stride: int,
    lo: int,
    hi: int,
    row: "array[float]",
) -> None:
    """Write the 7-float ``row`` into one lane's frames ``[lo, hi)``."""
    for fidx in range(lo, hi):
        off = fidx * stride + lane_off
        buf[off : off + FLOATS_PER_POSE] = row


def _densify(
    segments: list[_Segment], frame_times: list[float], slot_count: int
) -> "array[float]":
    """Fill a dense F*N*7 float32 buffer (``.tobytes()`` at the call boundary).

    Per lane, ``_keyframes`` gives the segment's real samples spanning
    ``[start, end]``. Between consecutive keyframes a frame with no sample of its
    own is interpolated — linear on position, slerp on rotation, weighted by
    wall-clock time — so a held actor glides rather than freezing then lurching
    to the next sample. A gap whose closing keyframe is a kickoff re-announcement
    (``seg.resets`` — a cut, not motion) holds the earlier pose instead, as does
    the tail past the final sample. Frames outside every segment stay zero; the
    client hides the lane there.
    """
    row_stride = slot_count * FLOATS_PER_POSE
    buf = array("f", bytes(packed_buffer_bytes(len(frame_times), slot_count)))

    for seg in segments:
        if seg.slot < 0:
            continue
        kf = _keyframes(seg)
        lane_off = seg.slot * FLOATS_PER_POSE

        for (fa, pa), (fb, pb) in zip(kf, kf[1:], strict=False):
            ta = frame_times[fa]
            span = frame_times[fb] - ta
            if span <= 0.0 or fb in seg.resets:
                _fill(buf, lane_off, row_stride, fa, fb, array("f", pa))
                continue
            for fidx in range(fa, fb):
                w = (frame_times[fidx] - ta) / span
                off = fidx * row_stride + lane_off
                buf[off : off + FLOATS_PER_POSE] = array("f", _lerp_pose(pa, pb, w))

        last_f, last_p = kf[-1]
        _fill(buf, lane_off, row_stride, last_f, seg.end + 1, array("f", last_p))

    return buf


def _resolve_pickups(
    raw: list[_RawPickup],
    segments: list[_Segment],
    positions: "array[float]",
    slot_count: int,
) -> list[tuple[int, int, int, float, float]]:
    """Turn raw ``(frame, pad_oid, collected, instigator)`` rows into the wire
    form ``(frame, pad, collected, x, y)``.

    ``pad`` is a dense 0-based index assigned in ascending pad-object-id order.
    On a collect row ``x, y`` is the instigating car's position that frame, read
    from the densified pose buffer via the car segment live then; a respawn row,
    or a car that can't be located, gets ``0.0, 0.0``. Row order (frame order)
    is preserved from the walk.
    """
    pad_index = {oid: i for i, oid in enumerate(sorted({r[1] for r in raw}))}
    car_segs: dict[int, list[_Segment]] = {}
    for seg in segments:
        if seg.kind == "car" and seg.slot >= 0:
            car_segs.setdefault(seg.actor_id, []).append(seg)

    rows: list[tuple[int, int, int, float, float]] = []
    for frame, pad_oid, collected, instigator in raw:
        x = y = 0.0
        if instigator is not None:
            for seg in car_segs.get(instigator, ()):
                if seg.start <= frame <= seg.end:
                    off = pose_offset(slot_count, frame, seg.slot)
                    x, y = positions[off], positions[off + 1]
                    break
        rows.append((frame, pad_index[pad_oid], int(collected), x, y))
    return rows


def extract_replay_frames(
    replay: ParsedReplay,
    *,
    tracked_team: int | None,
    tracked_identities: AbstractSet[PlayerIdentity],
    player_names: dict[PlayerIdentity, str],
    game_mode: str | None,
) -> ReplayFrames:
    """Reshape a parsed replay's network frames into a playback buffer.

    ``player_names`` maps every known player identity to its preferred display
    string (a tracked player's configured name where set, the in-game ``Name``
    from the ``PlayerStats`` blob otherwise). Identities absent from it, and
    cars whose PRI never resolves, fall back to ``"Player N"``.

    Returns an empty :class:`ReplayFrames` when the replay carries no usable
    network data (mirrors ``analyze_frames``).
    """
    obj = replay.object_index
    car_arch = obj.get(NetObj.CAR_ARCHETYPE)
    ball_arch = _ball_archetype_oid(obj)
    rb_oid = obj.get(NetObj.RB_STATE)

    if not replay.frames or car_arch is None or ball_arch is None or rb_oid is None:
        return ReplayFrames([], [], b"", tracked_team, game_mode)

    pad_oids = frozenset(oid for name, oid in obj.items() if _BOOST_PAD_MARKER in name)
    segments, raw_pickups = _walk(
        replay,
        car_arch,
        ball_arch,
        rb_oid,
        obj.get(NetObj.PAWN_PRI),
        obj.get(NetObj.PRI_UNIQUE_ID),
        obj.get(NetObj.TEAM_PAINT),
        obj.get(NetObj.PICKUP_DATA),
        pad_oids,
    )
    slots = _build_slots(segments, tracked_identities, player_names)
    frame_times = [float(f["time"]) for f in replay.frames]
    buf = _densify(segments, frame_times, len(slots))
    goals = _scan_goals(replay, obj.get(NetObj.SCORED_ON_TEAM))
    countdowns = _scan_countdowns(replay, obj.get(NetObj.COUNTDOWN))
    dead_periods = _dead_periods(goals, countdowns)
    boost_pads = _resolve_pickups(raw_pickups, segments, buf, len(slots))

    return ReplayFrames(
        frame_times=frame_times,
        slots=slots,
        positions=buf.tobytes(),
        tracked_team=tracked_team,
        game_mode=game_mode,
        goals=goals,
        countdowns=countdowns,
        dead_periods=dead_periods,
        boost_pads=boost_pads,
    )
