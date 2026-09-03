// Pure playback math for the browser replay viewer — no THREE, no DOM, no
// imports. Extracted from replay.js so the interpolation + timeline logic can be
// exercised directly by tests/js/*.test.js.
//
// replay.js is the only production consumer: createPlayback()'s applyPoses()
// calls writePoses() here and does nothing but copy the result onto meshes, and
// its transport delegates to createTransport(). One code path, so the tested
// code is the shipped code. Keep this file dependency-free.

export const FLOATS_PER_POSE = 7; // x, y, z, qx, qy, qz, qw
export const TRAIL_FRAMES = 45; // ~1.5 s of motion tail at rrrocket's ~30 Hz
export const TRAIL_POINTS = TRAIL_FRAMES + 1; // head vertex + one per walked-back frame

// Standard Rocket League field, unreal units (see replay.js for the full
// geometry notes) — the raw numbers behind STANDARD_SPEC. arenaSpec() below is
// the structured, per-mode form the viewer's arena builders consume.
const FIELD_X = 8192;
const FIELD_Y = 10240;
const FIELD_Z = 2044;
const CORNER = 1152;

// ── Arena specs ───────────────────────────────────────────────────────────
// Per-mode field geometry: dimensions, chamfer, boost-pad layout and goal
// shape. Pure data — no THREE, no DOM — so it is unit-tested here and consumed
// by static/replay.js (buildArena / buildGoals / buildHalfTint / buildBoostPads,
// the ball radius, the overview camera). All lengths unreal units; X = wall to
// wall, Y = goal to goal, Z = floor to ceiling; `corner` is the 45° cut on each
// of the four corners. Coordinates from wiki.rlbot.org (the same source as
// frame_analysis.BIG_PAD_POSITIONS). See
// docs/adr/0005-hoops-arena-for-the-replay-viewer.md.

// Chamfered-octagon outline in the XY plane, CCW from +x/+y — shared by the
// arena wireframe, the floor-grid clip and the half-pitch tint.
function chamferedOutline(halfX, halfY, corner) {
  return [
    [halfX, halfY - corner],
    [halfX - corner, halfY],
    [-(halfX - corner), halfY],
    [-halfX, halfY - corner],
    [-halfX, -(halfY - corner)],
    [-(halfX - corner), -halfY],
    [halfX - corner, -halfY],
    [halfX, -(halfY - corner)],
  ];
}

const STANDARD_SPEC = {
  mode: "standard",
  halfX: FIELD_X / 2, // 4096
  halfY: FIELD_Y / 2, // 5120
  ceiling: FIELD_Z, // 2044
  corner: CORNER, // 1152 (|x| + |y| = 8064 along each diagonal)
  ballRadius: 91.25,
  goalClearance: 880, // goal box depth past the back wall
  grid: { cols: 3, rows: 4 }, // floor guide: 3 wall-to-wall, 4 goal-to-goal
  bigPads: [
    [-3584, 0], [3584, 0],
    [-3072, -4096], [3072, -4096],
    [-3072, 4096], [3072, 4096],
  ],
  smallPads: [
    [0, -4240], [-1792, -4184], [1792, -4184], [-940, -3308], [940, -3308],
    [0, -2816], [-3584, -2484], [3584, -2484], [-1788, -2300], [1788, -2300],
    [-2048, -1036], [0, -1024], [2048, -1036], [-1024, 0], [1024, 0],
    [-2048, 1036], [0, 1024], [2048, 1036], [-1788, 2300], [1788, 2300],
    [-3584, 2484], [3584, 2484], [0, 2816], [-940, 3308], [940, 3308],
    [-1792, 4184], [1792, 4184], [0, 4240],
  ],
  goal: { kind: "box", halfWidth: 893, height: 643, depth: 880, fxScale: 643 },
};

const HOOPS_SPEC = {
  mode: "hoops",
  halfX: 2966.67,
  halfY: 3581,
  ceiling: 1820,
  corner: 765.67, // 2966.67 + 3581 − 5782 (wiki diagonal-wall intersection)
  ballRadius: 98.38,
  goalClearance: 0, // the rim sits inside the field
  grid: { cols: 2, rows: 2 },
  bigPads: [
    [-2176, -2944], [2176, -2944],
    [-2432, 0], [2432, 0],
    [-2176, 2944], [2176, 2944],
  ],
  smallPads: [
    [0, -2816], [-1280, -2304], [1280, -2304], [-1536, -1024], [1536, -1024],
    [512, -512], [-512, -512], [512, 512], [-512, 512],
    [-1536, 1024], [1536, 1024], [-1280, 2304], [1280, 2304], [0, 2816],
  ],
  // Basketball-style hoop: a semicircle rim (curved side to the pitch) with a
  // shallow basket sweep and short arms back to the wall. Outline only, in the
  // defending team's tint — nothing fills the opening (cars drive under it).
  goal: {
    kind: "ring",
    centreY: 2969, // |y| of the rim centre
    z: 364, // rim height
    radius: 655,
    basketDrop: 175, // basket arc sits this far below the rim
    basketInset: 120, // …and this far toward the wall
    basketRadius: 390,
    fxScale: 500,
  },
};

// Non-standard arenas keyed by game_mode. Add dropshot/snowday here (each with
// its own ball archetype — see replay_frames.py); everything else — "3v3",
// "2v2", an unknown or missing mode — falls through to STANDARD_SPEC.
const ARENA_SPECS = { hoops: HOOPS_SPEC };

for (const s of [STANDARD_SPEC, ...Object.values(ARENA_SPECS)]) {
  s.outline = chamferedOutline(s.halfX, s.halfY, s.corner);
  Object.freeze(s.goal);
  Object.freeze(s);
}

// The field geometry for a match's game_mode.
export function arenaSpec(gameMode) {
  return ARENA_SPECS[gameMode] ?? STANDARD_SPEC;
}

export const TEAM_OURS = 0x00e5ff;
export const TEAM_THEIRS = 0xff5a5a;
export const TEAM_UNKNOWN = 0x8585a0;

// Byte offset (in floats) of slot `slot`'s pose at frame `frame` in a packed
// [frame][slot][x,y,z,qx,qy,qz,qw] buffer.
export function poseOffset(slotCount, frame, slot) {
  return (frame * slotCount + slot) * FLOATS_PER_POSE;
}

// ours (tracked) vs theirs, keyed on RL team not screen side — the field flip
// then puts "ours" on the same side of the screen every match.
export function teamTint(team, trackedTeam) {
  if (team == null || trackedTeam == null) return TEAM_UNKNOWN;
  return team === trackedTeam ? TEAM_OURS : TEAM_THEIRS;
}

export function carColor(slot, trackedTeam) {
  return teamTint(slot.team, trackedTeam);
}

// Is this slot's actor live at frame index `frame`? Segments are inclusive
// [start, end] ranges; between them (demolitions) the buffer holds zeros.
export function slotLiveAt(slot, frame) {
  const segs = slot.segments;
  for (let k = 0; k < segs.length; k++) {
    if (frame >= segs[k][0] && frame <= segs[k][1]) return true;
  }
  return false;
}

export function formatClock(seconds) {
  const s = Math.max(0, Math.round(seconds));
  return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`;
}

// Ticks sit ~1 s apart; hold each numeral a touch longer so a dropped or aborted
// sequence clears itself rather than sticking on screen.
export const COUNTDOWN_TICK_HOLD = 1.4; // s
export const COUNTDOWN_GO_HOLD = 0.6; // s — keep in sync with the
// `replay-countdown-go` animation duration in replay.css.

// The kickoff countdown text to show at replay-time `t` ("3" / "2" / "1" /
// "GO!"), or null when no countdown is active. `countdowns` is [[frame, n], …]
// in frame order (server: replay_frames._scan_countdowns) — one 3→2→1→0 run per
// kickoff.
export function countdownLabelAt(times, countdowns, t) {
  const tick = countdowns.findLast(([f]) => times[f] <= t);
  if (!tick) return null;
  const [frame, n] = tick;
  const dt = t - times[frame];
  if (n === 0) return dt >= 0 && dt < COUNTDOWN_GO_HOLD ? "GO!" : null;
  return dt < COUNTDOWN_TICK_HOLD ? String(n) : null;
}

// Largest i with times[i] <= t, its successor j, and the [0,1] blend between.
export function bracket(times, t) {
  const n = times.length;
  if (n < 2 || t <= times[0]) return [0, 0, 0];
  if (t >= times[n - 1]) return [n - 1, n - 1, 0];
  let lo = 0;
  let hi = n - 1;
  while (hi - lo > 1) {
    const mid = (lo + hi) >> 1;
    if (times[mid] <= t) lo = mid;
    else hi = mid;
  }
  const span = times[hi] - times[lo];
  return [lo, hi, span > 0 ? (t - times[lo]) / span : 0];
}

// How far the flat wall reaches on the perpendicular axis before the chamfer
// starts, for the arena `spec` (default: standard). Used to clip the floor grid
// to the octagon.
export function outlineHalfWidth(x, y, spec = STANDARD_SPEC) {
  const { halfX, halfY, corner } = spec;
  const diag = halfX + halfY - corner;
  return {
    x: Math.abs(y) > halfY - corner ? diag - Math.abs(y) : halfX,
    y: Math.abs(x) > halfX - corner ? diag - Math.abs(x) : halfY,
  };
}

// THREE.Quaternion.slerp (three@0.170.0, src/math/Quaternion.js) ported verbatim
// as a pure function, so the extracted pose math is byte-for-byte what the
// viewer renders — including the sign flip, the cosHalfTheta >= 1 early return,
// and the linear-with-renormalize fallback for near-antipodal inputs. The parity
// check against the real THREE build lives in tests/e2e/replay.spec.js.
// `a` is the start quaternion, `b` the end; writes [x, y, z, w] into `out`.
export function slerpQuat(ax, ay, az, aw, bx, by, bz, bw, t, out) {
  out = out || [0, 0, 0, 0];
  if (t === 0) {
    out[0] = ax;
    out[1] = ay;
    out[2] = az;
    out[3] = aw;
    return out;
  }
  if (t === 1) {
    out[0] = bx;
    out[1] = by;
    out[2] = bz;
    out[3] = bw;
    return out;
  }

  const x = ax;
  const y = ay;
  const z = az;
  const w = aw;

  let cosHalfTheta = w * bw + x * bx + y * by + z * bz;
  let rx;
  let ry;
  let rz;
  let rw;
  if (cosHalfTheta < 0) {
    rw = -bw;
    rx = -bx;
    ry = -by;
    rz = -bz;
    cosHalfTheta = -cosHalfTheta;
  } else {
    rw = bw;
    rx = bx;
    ry = by;
    rz = bz;
  }

  if (cosHalfTheta >= 1.0) {
    out[0] = x;
    out[1] = y;
    out[2] = z;
    out[3] = w;
    return out;
  }

  const sqrSinHalfTheta = 1.0 - cosHalfTheta * cosHalfTheta;
  if (sqrSinHalfTheta <= Number.EPSILON) {
    const s = 1 - t;
    let nw = s * w + t * rw;
    let nx = s * x + t * rx;
    let ny = s * y + t * ry;
    let nz = s * z + t * rz;
    let len = Math.sqrt(nx * nx + ny * ny + nz * nz + nw * nw);
    if (len === 0) {
      nx = 0;
      ny = 0;
      nz = 0;
      nw = 1;
    } else {
      len = 1 / len;
      nx *= len;
      ny *= len;
      nz *= len;
      nw *= len;
    }
    out[0] = nx;
    out[1] = ny;
    out[2] = nz;
    out[3] = nw;
    return out;
  }

  const sinHalfTheta = Math.sqrt(sqrSinHalfTheta);
  const halfTheta = Math.atan2(sinHalfTheta, cosHalfTheta);
  const ratioA = Math.sin((1 - t) * halfTheta) / sinHalfTheta;
  const ratioB = Math.sin(t * halfTheta) / sinHalfTheta;

  out[0] = x * ratioA + rx * ratioB;
  out[1] = y * ratioA + ry * ratioB;
  out[2] = z * ratioA + rz * ratioB;
  out[3] = w * ratioA + rw * ratioB;
  return out;
}

// The compressed-time transport. The playback clock runs in real `frame_times`
// seconds (so bracket()/writePoses are untouched), but the scrub bar, clock
// readout and goal ticks run on a *compressed* axis with every dead_periods span
// — goal replay, actor reset, frozen-at-spawn wait — removed.
//
// `dead` holds those spans as real intervals [a, b) plus `len` and `c`, the
// span's coordinate on the compressed axis (`c === toCompressed(a)`). The server
// (replay_frames._dead_periods) guarantees them ascending and non-overlapping.
export function createTransport(meta) {
  const times = meta.frame_times;
  const t0 = times[0];
  const tN = times[times.length - 1];

  const dead = (meta.dead_periods || [])
    .map(([sf, ef]) => ({ a: times[sf], b: times[ef + 1] ?? tN }))
    .filter((d) => d.b > d.a);
  let cut = 0;
  for (const d of dead) {
    d.len = d.b - d.a;
    d.c = d.a - t0 - cut;
    cut += d.len;
  }

  // real seconds -> compressed seconds (0 at t0, dead spans removed)
  function toCompressed(t) {
    let c = t - t0;
    for (const d of dead) {
      if (t <= d.a) break;
      c -= Math.min(t, d.b) - d.a;
    }
    return c;
  }

  // compressed seconds -> real seconds. A `c` on a dead span's edge resolves to
  // the span's END (the resume frame), so nothing lands inside a gap.
  function toReal(c) {
    let t = c + t0;
    for (const d of dead) {
      if (c < d.c) break;
      t += d.len;
    }
    return t;
  }

  // advance()'s dead-span snap: a raw real time inside a span jumps to its end.
  function snapForward(t) {
    for (const d of dead) if (t >= d.a && t < d.b) return d.b;
    return t;
  }

  const compressedEnd = toCompressed(tN);
  const tStart = toReal(0); // first kept instant — past any pre-match warmup

  // real seconds -> [0,1] position on the compressed axis (scrub bar, goal ticks)
  const fractionAt = (t) =>
    compressedEnd > 0 ? toCompressed(t) / compressedEnd : 0;

  return {
    t0,
    tN,
    tStart,
    compressedEnd,
    dead,
    toCompressed,
    toReal,
    snapForward,
    fractionAt,
  };
}

// Reusable output buffers for writePoses — allocate once per match, reuse every
// frame. `position`/`quaternion` are f64 (the viewer keeps mesh transforms at
// full precision); `trail` is f32 and laid out exactly like the viewer's THREE
// trail geometry (slot s occupies [s*TRAIL_POINTS*3 .. +TRAIL_POINTS*3)).
export function makePoseBuffers(slotCount) {
  return {
    position: new Float64Array(slotCount * 3),
    quaternion: new Float64Array(slotCount * 4),
    visible: new Uint8Array(slotCount),
    trail: new Float32Array(slotCount * TRAIL_POINTS * 3),
    trailCount: new Int32Array(slotCount),
  };
}

const _q = [0, 0, 0, 0];

// Fill `out` (from makePoseBuffers) with every slot's pose at wall-clock time
// `t`: lerp'd position, slerp'd quaternion, a visibility flag, and the motion
// trail. This is replay.js's former applyPoses() with the mesh writes stripped
// out — the one place interpolation happens.
//
// A slot is hidden when neither bracket end is live (a demolition gap). When one
// end is live and the other isn't, the blend snaps to the live end rather than
// lerping toward a zero pose. The trail's head is the live position; the rest
// walk backward through the buffer while the slot stays live, never across a gap.
export function writePoses(meta, positions, t, out) {
  const times = meta.frame_times;
  const slots = meta.slots;
  const slotCount = slots.length;
  const [i, j, f] = bracket(times, t);

  for (let s = 0; s < slotCount; s++) {
    const slot = slots[s];
    const liveI = slotLiveAt(slot, i);
    const liveJ = slotLiveAt(slot, j);
    if (!liveI && !liveJ) {
      out.visible[s] = 0;
      out.trailCount[s] = 0;
      continue;
    }
    out.visible[s] = 1;
    const ff = !liveI ? 1 : !liveJ ? 0 : f;

    const a = poseOffset(slotCount, i, s);
    const b = poseOffset(slotCount, j, s);
    const px = positions[a] + (positions[b] - positions[a]) * ff;
    const py = positions[a + 1] + (positions[b + 1] - positions[a + 1]) * ff;
    const pz = positions[a + 2] + (positions[b + 2] - positions[a + 2]) * ff;
    out.position[s * 3] = px;
    out.position[s * 3 + 1] = py;
    out.position[s * 3 + 2] = pz;

    slerpQuat(
      positions[a + 3],
      positions[a + 4],
      positions[a + 5],
      positions[a + 6],
      positions[b + 3],
      positions[b + 4],
      positions[b + 5],
      positions[b + 6],
      ff,
      _q,
    );
    out.quaternion[s * 4] = _q[0];
    out.quaternion[s * 4 + 1] = _q[1];
    out.quaternion[s * 4 + 2] = _q[2];
    out.quaternion[s * 4 + 3] = _q[3];

    const tp = out.trail;
    const base = s * TRAIL_POINTS * 3;
    tp[base] = px;
    tp[base + 1] = py;
    tp[base + 2] = pz;
    let count = 1;
    for (let k = i; k >= 0 && k > i - TRAIL_FRAMES && slotLiveAt(slot, k); k--) {
      const o = poseOffset(slotCount, k, s);
      tp[base + count * 3] = positions[o];
      tp[base + count * 3 + 1] = positions[o + 1];
      tp[base + count * 3 + 2] = positions[o + 2];
      count++;
    }
    out.trailCount[s] = count;
  }
  return out;
}
