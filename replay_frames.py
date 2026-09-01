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
* **Poses are kept per frame.** Carried forward into a dense buffer so the
  client can lerp/slerp between rrrocket's ~30 Hz samples.
"""

import math
from array import array
from collections.abc import Set as AbstractSet
from dataclasses import dataclass, field

from frame_analysis import IdentityResolver
from player_identity import PlayerIdentity, from_network_frame
from rrrocket_schema import NetObj, ParsedReplay

_FLOATS_PER_POSE = 7  # x, y, z, qx, qy, qz, qw
_IDENTITY_POSE: tuple[float, ...] = (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0)

# (frame_idx, x, y, z, qx, qy, qz, qw)
_Sample = tuple[int, float, float, float, float, float, float, float]


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
    goals: list[GoalMarker]  # in frame order


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


def _scan_goals(replay: ParsedReplay, scored_oid: int | None) -> list[GoalMarker]:
    """Goal frames from ``ReplicatedScoredOnTeam`` rising edges.

    The attribute carries the team that was *scored on* (0 or 1); it is re-sent a
    few times per goal and reset to 255 in between, so a goal is each transition
    into {0, 1} from anything else.
    """
    if scored_oid is None:
        return []
    goals: list[GoalMarker] = []
    last = -1
    for fidx, frame in enumerate(replay.frames):
        for ua in frame.get("updated_actors", []):
            if ua.get("object_id") != scored_oid:
                continue
            byte = ua.get("attribute", {}).get("Byte")
            if byte in (0, 1) and last not in (0, 1):
                goals.append(GoalMarker(frame=fidx, team=1 - byte))
            if byte is not None:
                last = byte
    return goals


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


def _resolve_identity(resolver: IdentityResolver, aid: int) -> PlayerIdentity | None:
    ident = resolver.resolve_car(aid)
    return PlayerIdentity(*ident) if ident is not None else None


def _walk(
    replay: ParsedReplay,
    car_arch: int,
    ball_arch: int,
    rb_oid: int,
    pri_oid: int | None,
    uid_oid: int | None,
    paint_oid: int | None,
) -> list[_Segment]:
    """Single pass over the frames, producing one ``_Segment`` per actor life.

    Preserves ``_process_frame``'s phase order (new -> updated -> deleted) so a
    car spawned and PRI-linked in the same frame keeps its link.
    """
    segments: list[_Segment] = []
    open_seg: dict[int, int] = {}  # actor_id -> index into segments
    car_actors: set[int] = set()
    ball_actors: set[int] = set()
    resolver = IdentityResolver()
    actor_team: dict[int, int] = {}

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
                    resolver.set_identity(aid, *ident)
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

        # 3. deleted_actors — close segments (resolving identity first), then purge
        deleted = frame.get("deleted_actors", [])
        for aid in deleted:
            si = open_seg.pop(aid, None)
            if si is None:
                continue
            seg = segments[si]
            if seg.kind == "car":
                seg.identity = _resolve_identity(resolver, aid)
            seg.team = actor_team.get(aid)
            seg.end = fidx
        for aid in deleted:
            car_actors.discard(aid)
            ball_actors.discard(aid)
            resolver.remove_actor(aid)
            actor_team.pop(aid, None)

    # Close whatever is still live at the final frame.
    for aid, si in open_seg.items():
        seg = segments[si]
        if seg.kind == "car":
            seg.identity = _resolve_identity(resolver, aid)
        seg.team = actor_team.get(aid)
        seg.end = last_frame

    return segments


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


def _slerp(
    a: tuple[float, float, float, float],
    b: tuple[float, float, float, float],
    t: float,
) -> tuple[float, float, float, float]:
    """Spherical linear interpolation between two unit quaternions."""
    ax, ay, az, aw = a
    bx, by, bz, bw = b
    dot = ax * bx + ay * by + az * bz + aw * bw
    if dot < 0.0:  # take the shorter arc
        bx, by, bz, bw, dot = -bx, -by, -bz, -bw, -dot
    if dot > 0.9995:  # almost parallel — normalised lerp dodges sin(θ₀)→0
        rx, ry, rz, rw = (
            ax + t * (bx - ax),
            ay + t * (by - ay),
            az + t * (bz - az),
            aw + t * (bw - aw),
        )
        n = math.sqrt(rx * rx + ry * ry + rz * rz + rw * rw) or 1.0
        return rx / n, ry / n, rz / n, rw / n
    theta0 = math.acos(dot)
    s0 = math.sin(theta0 - t * theta0) / math.sin(theta0)
    s1 = math.sin(t * theta0) / math.sin(theta0)
    return (
        ax * s0 + bx * s1,
        ay * s0 + by * s1,
        az * s0 + bz * s1,
        aw * s0 + bw * s1,
    )


def _lerp_pose(
    a: tuple[float, ...], b: tuple[float, ...], w: float
) -> tuple[float, ...]:
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


def _keyframes(seg: _Segment) -> list[tuple[int, tuple[float, ...]]]:
    """The poses the fill interpolates between: every real sample, preceded by
    a seed keyframe at ``seg.start`` when the first sample lands later.

    ``initial_trajectory`` never carries a usable rotation, so the seed
    keyframe borrows the first sample's quaternion — held back to the segment
    start rather than slerped up from identity.
    """
    samples = seg.samples
    kf: list[tuple[int, tuple[float, ...]]] = [(s[0], s[1:]) for s in samples]
    if seg.seed is not None and (not kf or kf[0][0] > seg.start):
        rot = (
            (samples[0][4], samples[0][5], samples[0][6], samples[0][7])
            if samples
            else (0.0, 0.0, 0.0, 1.0)
        )
        kf.insert(0, (seg.start, (*seg.seed, *rot)))
    return kf or [(seg.start, _IDENTITY_POSE)]


def _densify(
    segments: list[_Segment], frame_times: list[float], slot_count: int
) -> bytes:
    """Fill a dense F*N*7 float32 buffer.

    Within a lane's segment, a frame with no ``RigidBody`` sample is filled by
    interpolating between the two real samples bracketing it — linear on
    position, slerp on rotation, weighted by wall-clock time — so a held actor
    glides instead of freezing then lurching to the next sample. Two runs hold
    the previous pose instead: up to a kickoff re-announcement (``seg.resets``,
    a cut not motion) and after a lane's final sample. Frames outside every
    segment stay zero; the client hides the lane there.
    """
    frame_count = len(frame_times)
    row_stride = slot_count * _FLOATS_PER_POSE

    buf = array("f", bytes(4 * frame_count * row_stride))

    for seg in segments:
        if seg.slot < 0:
            continue
        kf = _keyframes(seg)
        off0 = seg.slot * _FLOATS_PER_POSE
        ki = 0
        for fidx in range(seg.start, seg.end + 1):
            while ki + 1 < len(kf) and kf[ki + 1][0] <= fidx:
                ki += 1
            fa, pa = kf[ki]
            if ki + 1 < len(kf) and kf[ki + 1][0] not in seg.resets:
                fb, pb = kf[ki + 1]
                ta, tb = frame_times[fa], frame_times[fb]
                w = (frame_times[fidx] - ta) / (tb - ta) if tb > ta else 0.0
                pose = _lerp_pose(pa, pb, w)
            else:
                pose = pa  # trailing hold, or the run up to a reset cut
            base = fidx * row_stride + off0
            buf[base : base + _FLOATS_PER_POSE] = array("f", pose)

    return buf.tobytes()


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
    ball_arch = obj.get(NetObj.BALL_ARCHETYPE)
    rb_oid = obj.get(NetObj.RB_STATE)

    if not replay.frames or car_arch is None or ball_arch is None or rb_oid is None:
        return ReplayFrames([], [], b"", tracked_team, game_mode, [])

    segments = _walk(
        replay,
        car_arch,
        ball_arch,
        rb_oid,
        obj.get(NetObj.PAWN_PRI),
        obj.get(NetObj.PRI_UNIQUE_ID),
        obj.get(NetObj.TEAM_PAINT),
    )
    slots = _build_slots(segments, tracked_identities, player_names)
    frame_times = [float(f["time"]) for f in replay.frames]
    positions = _densify(segments, frame_times, len(slots))
    goals = _scan_goals(replay, obj.get(NetObj.SCORED_ON_TEAM))

    return ReplayFrames(
        frame_times=frame_times,
        slots=slots,
        positions=positions,
        tracked_team=tracked_team,
        game_mode=game_mode,
        goals=goals,
    )
