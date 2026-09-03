// Unit + property tests for static/replay-core.js — the THREE-free, DOM-free
// playback math the replay viewer ships. replay.js's applyPoses() calls
// writePoses() and only copies the result onto meshes; its transport delegates
// to createTransport(). So exercising the core here exercises what ships. The
// e2e parity test (tests/e2e/replay.spec.js) guards that that stays true.
//
// Run: node --test tests/js/   (or `mise run test-js`)

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

import {
  arenaSpec,
  bracket,
  countdownLabelAt,
  createTransport,
  formatClock,
  makePoseBuffers,
  outlineHalfWidth,
  poseOffset,
  slerpQuat,
  slotLiveAt,
  teamTint,
  TEAM_OURS,
  TEAM_THEIRS,
  TEAM_UNKNOWN,
  TRAIL_POINTS,
  writePoses,
} from "../../static/replay-core.js";

const HERE = dirname(fileURLToPath(import.meta.url));
const FIX = resolve(HERE, "..", "data", "replay-viewer");

// The fixture is real server output (meta.json + the packed position buffer) for
// the one committed replay — used here as *input* to the property tests, never
// as a golden output oracle. Regenerate with tests/e2e/dump_fixture.py when the
// server's frame format changes.
function loadFixture() {
  const meta = JSON.parse(readFileSync(resolve(FIX, "meta.json"), "utf8"));
  const buf = readFileSync(resolve(FIX, "frames.bin"));
  // A Node Buffer is a view into a larger pooled ArrayBuffer — pass
  // byteOffset/length or the Float32Array reads neighbouring bytes.
  const positions = new Float32Array(buf.buffer, buf.byteOffset, buf.byteLength / 4);
  return { meta, positions };
}

// ---------------------------------------------------------------------------
// bracket
// ---------------------------------------------------------------------------

test("bracket: degenerate and boundary inputs", () => {
  assert.deepEqual(bracket([], 5), [0, 0, 0]);
  assert.deepEqual(bracket([7], 7), [0, 0, 0]);
  assert.deepEqual(bracket([7], 100), [0, 0, 0]);

  const ts = [0, 1, 2, 3, 4];
  assert.deepEqual(bracket(ts, -1), [0, 0, 0]); // before start -> clamp low
  assert.deepEqual(bracket(ts, 0), [0, 0, 0]); // exactly start
  assert.deepEqual(bracket(ts, 4), [4, 4, 0]); // exactly end
  assert.deepEqual(bracket(ts, 9), [4, 4, 0]); // past end -> clamp high

  const [i, j, f] = bracket(ts, 2.25);
  assert.equal(i, 2);
  assert.equal(j, 3);
  assert.ok(Math.abs(f - 0.25) < 1e-12);

  assert.deepEqual(bracket(ts, 3), [3, 4, 0]); // exactly on an interior sample
});

test("bracket: non-uniform spacing gives the wall-clock fraction", () => {
  const [i, j, f] = bracket([0, 0.1, 10], 5);
  assert.equal(i, 1);
  assert.equal(j, 2);
  assert.ok(Math.abs(f - (5 - 0.1) / (10 - 0.1)) < 1e-12);
});

test("bracket: duplicate timestamps keep the blend finite", () => {
  const [i, j, f] = bracket([0, 2, 2, 5], 2);
  assert.equal(i, 2); // lo walks to the last of the equal run
  assert.equal(j, 3);
  assert.equal(f, 0);
});

// ---------------------------------------------------------------------------
// slotLiveAt / poseOffset / teamTint / formatClock / countdownLabelAt / outline
// ---------------------------------------------------------------------------

test("slotLiveAt: inclusive ranges, gaps, multiple segments", () => {
  const slot = { segments: [[0, 10], [20, 25]] };
  assert.equal(slotLiveAt(slot, 0), true);
  assert.equal(slotLiveAt(slot, 10), true); // inclusive end
  assert.equal(slotLiveAt(slot, 11), false); // in the gap
  assert.equal(slotLiveAt(slot, 19), false);
  assert.equal(slotLiveAt(slot, 20), true); // inclusive start of 2nd
  assert.equal(slotLiveAt(slot, 25), true);
  assert.equal(slotLiveAt(slot, 26), false);
  assert.equal(slotLiveAt({ segments: [] }, 0), false);
});

test("poseOffset: float index into a [frame][slot][7] buffer", () => {
  assert.equal(poseOffset(3, 0, 0), 0);
  assert.equal(poseOffset(3, 0, 2), 14);
  assert.equal(poseOffset(3, 1, 0), 21);
  assert.equal(poseOffset(7, 10, 4), (10 * 7 + 4) * 7);
});

test("teamTint: tracked -> ours, other -> theirs, unknown -> neutral", () => {
  assert.equal(teamTint(1, 1), TEAM_OURS);
  assert.equal(teamTint(0, 1), TEAM_THEIRS);
  assert.equal(teamTint(null, 1), TEAM_UNKNOWN);
  assert.equal(teamTint(0, null), TEAM_UNKNOWN);
});

test("formatClock: mm:ss, zero-padded, negatives clamped, rounds", () => {
  assert.equal(formatClock(0), "0:00");
  assert.equal(formatClock(9), "0:09");
  assert.equal(formatClock(69), "1:09");
  assert.equal(formatClock(600), "10:00");
  assert.equal(formatClock(-5), "0:00");
  assert.equal(formatClock(59.6), "1:00");
});

test("countdownLabelAt: 3/2/1 hold, GO! flashes then clears, nothing before", () => {
  const times = [0, 1, 2, 3, 4, 5];
  const cds = [[1, 3], [2, 2], [3, 1], [4, 0]];
  assert.equal(countdownLabelAt(times, cds, 0.5), null); // before the run
  assert.equal(countdownLabelAt(times, cds, 1.0), "3");
  assert.equal(countdownLabelAt(times, cds, 2.2), "2");
  assert.equal(countdownLabelAt(times, cds, 3.9), "1"); // still within TICK_HOLD (1.4)
  assert.equal(countdownLabelAt(times, cds, 4.2), "GO!"); // within GO_HOLD (0.6)
  assert.equal(countdownLabelAt(times, cds, 4.8), null); // GO! has cleared
  assert.equal(countdownLabelAt(times, [], 3), null);
});

test("outlineHalfWidth: flat wall vs chamfered corner", () => {
  // Near the middle of a back wall the limit is the full half-length.
  assert.equal(outlineHalfWidth(0, 0).x, 8192 / 2);
  assert.equal(outlineHalfWidth(0, 0).y, 10240 / 2);
  // Deep in a corner the chamfer line pulls both limits in.
  const deep = outlineHalfWidth(8192 / 2 - 10, 10240 / 2 - 10);
  assert.ok(deep.x < 8192 / 2);
  assert.ok(deep.y < 10240 / 2);
});

test("outlineHalfWidth: an explicit spec overrides the standard default", () => {
  const hoops = arenaSpec("hoops");
  // Mid-wall: the full half-extents of the hoops footprint.
  assert.ok(Math.abs(outlineHalfWidth(0, 0, hoops).x - hoops.halfX) < 1e-9);
  assert.ok(Math.abs(outlineHalfWidth(0, 0, hoops).y - hoops.halfY) < 1e-9);
  // Deep in a hoops corner both limits pull in, and short of the standard ones.
  const deep = outlineHalfWidth(hoops.halfX - 10, hoops.halfY - 10, hoops);
  assert.ok(deep.x < hoops.halfX && deep.x < 8192 / 2);
  assert.ok(deep.y < hoops.halfY && deep.y < 10240 / 2);
  // No spec arg still means standard.
  assert.equal(outlineHalfWidth(0, 0).x, 8192 / 2);
});

// ---------------------------------------------------------------------------
// arenaSpec — per-mode field geometry (docs/adr/0005)
// ---------------------------------------------------------------------------

test("arenaSpec: only 'hoops' diverges; every other mode is the standard arena", () => {
  const std = arenaSpec("3v3");
  assert.equal(std.mode, "standard");
  assert.equal(std.goal.kind, "box");
  // Same frozen singleton for 2v2, unknown, and missing modes.
  for (const m of ["2v2", "3v3", "dropshot", "", null, undefined]) {
    assert.equal(arenaSpec(m), std);
  }
  assert.notEqual(arenaSpec("hoops"), std);
});

test("arenaSpec: specs (and their goal sub-objects) are frozen", () => {
  for (const m of ["3v3", "hoops"]) {
    const s = arenaSpec(m);
    assert.ok(Object.isFrozen(s), `${m} spec frozen`);
    assert.ok(Object.isFrozen(s.goal), `${m} goal frozen`);
  }
});

test("arenaSpec: hoops footprint, ceiling, chamfer and ball radius", () => {
  const h = arenaSpec("hoops");
  assert.equal(h.mode, "hoops");
  assert.ok(Math.abs(h.halfX - 2966.67) < 1e-9);
  assert.equal(h.halfY, 3581);
  assert.equal(h.ceiling, 1820);
  assert.equal(h.ballRadius, 98.38);
  // corner = halfX + halfY − (wiki diagonal-wall intersection, 5782).
  assert.ok(Math.abs(h.halfX + h.halfY - h.corner - 5782) < 1e-9);
});

test("arenaSpec: hoops goal is an elevated ring, standard goal is a box", () => {
  const h = arenaSpec("hoops").goal;
  assert.equal(h.kind, "ring");
  assert.equal(h.radius, 655);
  assert.equal(h.z, 364);
  assert.equal(h.centreY, 2969);
  // The rim's back edge meets the back wall, so it reads as wall-mounted.
  assert.ok(h.centreY + h.radius >= arenaSpec("hoops").halfY);

  const s = arenaSpec("3v3").goal;
  assert.equal(s.kind, "box");
  assert.equal(s.depth, arenaSpec("3v3").goalClearance);
});

test("arenaSpec: boost-pad layouts — 34 standard, 20 hoops", () => {
  const key = ([x, y]) => `${x},${y}`;
  const std = arenaSpec("3v3");
  assert.equal(std.bigPads.length, 6);
  assert.equal(std.smallPads.length, 28);

  const h = arenaSpec("hoops");
  assert.equal(h.bigPads.length, 6);
  assert.equal(h.smallPads.length, 14);
  assert.equal(h.bigPads.length + h.smallPads.length, 20);

  // Hoops big pads must match frame_analysis.BIG_PAD_POSITIONS["hoops"] (both
  // cite wiki.rlbot.org — keep them in lockstep).
  assert.deepEqual(
    new Set(h.bigPads.map(key)),
    new Set([
      [-2176, -2944], [2176, -2944],
      [-2432, 0], [2432, 0],
      [-2176, 2944], [2176, 2944],
    ].map(key)),
  );

  // No pad sits outside its arena footprint.
  for (const spec of [std, h]) {
    for (const [x, y] of [...spec.bigPads, ...spec.smallPads]) {
      assert.ok(Math.abs(x) <= spec.halfX && Math.abs(y) <= spec.halfY);
    }
  }
});

test("arenaSpec: outline is a chamfered octagon around the footprint", () => {
  for (const m of ["3v3", "hoops"]) {
    const s = arenaSpec(m);
    assert.equal(s.outline.length, 8);
    assert.deepEqual(s.outline[0], [s.halfX, s.halfY - s.corner]);
    // Every vertex is on the footprint boundary.
    for (const [x, y] of s.outline) {
      assert.ok(Math.abs(x) <= s.halfX + 1e-9 && Math.abs(y) <= s.halfY + 1e-9);
    }
  }
});

// ---------------------------------------------------------------------------
// createTransport — the compressed <-> real time mapping
// ---------------------------------------------------------------------------

// frame_times [0..6] @ 1 Hz; one dead span over frames [2,4] -> real [2, 5).
const T_META = {
  frame_times: [0, 1, 2, 3, 4, 5, 6],
  dead_periods: [[2, 4]],
  slots: [],
};

test("createTransport: compressedEnd drops the dead span's length", () => {
  const tr = createTransport(T_META);
  assert.equal(tr.tN, 6);
  assert.equal(tr.compressedEnd, 3); // 6 real seconds - 3 dead
  assert.equal(tr.fractionAt(tr.t0), 0);
  assert.ok(Math.abs(tr.fractionAt(tr.tN) - 1) < 1e-12);
});

test("createTransport: a compressed instant on a dead-span edge resolves to the resume frame", () => {
  const tr = createTransport(T_META);
  // compressed 2 is the span's coordinate; toReal must land on b (5), not inside.
  assert.equal(tr.toReal(2), 5);
  assert.equal(tr.toReal(tr.compressedEnd), 6);
});

test("createTransport: snapForward jumps a raw real time inside a span to its end", () => {
  const tr = createTransport(T_META);
  assert.equal(tr.snapForward(1), 1); // before the span — untouched
  assert.equal(tr.snapForward(2), 5); // on the leading edge — jumps
  assert.equal(tr.snapForward(3.5), 5); // inside — jumps
  assert.equal(tr.snapForward(5), 5); // trailing edge is exclusive — untouched
  assert.equal(tr.snapForward(6), 6);
});

test("createTransport: a leading dead span becomes the start offset (warmup skipped)", () => {
  const tr = createTransport({
    frame_times: [0, 1, 2, 3],
    dead_periods: [[0, 1]], // real [0, 2)
    slots: [],
  });
  assert.equal(tr.tStart, 2); // first kept instant is the resume frame
  assert.equal(tr.toCompressed(0), 0);
  assert.equal(tr.compressedEnd, 1);
});

test("createTransport: no dead spans is an identity mapping", () => {
  const tr = createTransport({ frame_times: [0, 1, 2, 3, 4], slots: [] });
  assert.equal(tr.compressedEnd, 4);
  assert.equal(tr.tStart, 0);
  for (const t of [0, 1.3, 4]) assert.equal(tr.toReal(tr.toCompressed(t)), t);
});

test("createTransport: a dead span running to the last frame uses the tN fallback", () => {
  // dead_periods [[2,3]] over frame_times [0,1,2,3] -> times[3+1] is undefined,
  // so the span's end falls back to tN.
  const tr = createTransport({
    frame_times: [0, 1, 2, 3],
    dead_periods: [[2, 3]],
    slots: [],
  });
  assert.equal(tr.compressedEnd, 2); // [2, 3] removed from a 3 s span
  assert.equal(tr.toReal(tr.compressedEnd), tr.tN);
  assert.equal(tr.snapForward(2.5), 3);
});

test("createTransport: two adjacent dead spans both drop out", () => {
  const tr = createTransport({
    frame_times: [0, 1, 2, 3, 4, 5, 6],
    dead_periods: [[1, 1], [3, 4]], // real [1,2) and [3,5)
    slots: [],
  });
  assert.equal(tr.compressedEnd, 6 - 1 - 2);
  assert.equal(tr.toReal(1), 2); // first span edge -> its resume frame
  assert.equal(tr.toReal(2), 5); // second span edge -> its resume frame
  for (const t of [0.5, 2.5, 5.5]) {
    assert.ok(Math.abs(tr.toReal(tr.toCompressed(t)) - t) < 1e-9, `t=${t}`);
  }
});

// ---------------------------------------------------------------------------
// writePoses — the one place interpolation happens
// ---------------------------------------------------------------------------

// Build a packed [frame][slot][x,y,z,qx,qy,qz,qw] buffer from per-frame,
// per-slot pose arrays.
function packBuffer(frames) {
  const slotCount = frames[0].length;
  const buf = new Float32Array(frames.length * slotCount * 7);
  frames.forEach((slots, fi) => {
    slots.forEach((p, si) => buf.set(p, (fi * slotCount + si) * 7));
  });
  return buf;
}

const IDENT_Q = [0, 0, 0, 1];

test("writePoses: position is the linear blend of the bracketing samples", () => {
  const meta = { frame_times: [0, 1], slots: [{ segments: [[0, 1]] }] };
  const positions = packBuffer([
    [[0, 0, 0, ...IDENT_Q]],
    [[10, 20, 30, ...IDENT_Q]],
  ]);
  const out = makePoseBuffers(1);

  writePoses(meta, positions, 0.5, out);
  assert.deepEqual([...out.position], [5, 10, 15]);
  assert.equal(out.visible[0], 1);

  writePoses(meta, positions, 0.25, out);
  assert.deepEqual([...out.position], [2.5, 5, 7.5]);
});

test("writePoses: quaternion is slerp'd (constant angular speed, not nlerp)", () => {
  const meta = { frame_times: [0, 1], slots: [{ segments: [[0, 1]] }] };
  // 0deg -> 90deg about Z: qa = identity, qb = (0,0,sin45,cos45).
  const s = Math.SQRT1_2;
  const positions = packBuffer([
    [[0, 0, 0, 0, 0, 0, 1]],
    [[0, 0, 0, 0, 0, s, s]],
  ]);
  const out = makePoseBuffers(1);
  // Sample at t=0.25 — NOT 0.5, where nlerp and slerp coincide by symmetry.
  // slerp gives a constant-speed 22.5deg about Z: (0,0,sin(pi/16),cos(pi/16));
  // nlerp(0.25) would give qz ~= 0.1874, well outside tolerance.
  writePoses(meta, positions, 0.25, out);
  assert.ok(
    Math.abs(out.quaternion[2] - Math.sin(Math.PI / 16)) < 1e-6,
    `qz ${out.quaternion[2]} (slerp ${Math.sin(Math.PI / 16)}, nlerp ~0.18737)`,
  );
  assert.ok(Math.abs(out.quaternion[3] - Math.cos(Math.PI / 16)) < 1e-6);
});

// ---------------------------------------------------------------------------
// slerpQuat — the verbatim THREE.Quaternion.slerp port. Pinned here against
// intent, independent of THREE; the e2e suite checks it against the real build.
// ---------------------------------------------------------------------------

const norm4 = (q) => {
  const l = Math.hypot(...q);
  return q.map((v) => v / l);
};

const close4 = (got, want, tol = 1e-12) => {
  for (let c = 0; c < 4; c++) {
    assert.ok(Math.abs(got[c] - want[c]) <= tol, `comp ${c}: ${got[c]} vs ${want[c]}`);
  }
};

test("slerpQuat: endpoints are returned exactly", () => {
  const a = norm4([0.1, -0.3, 0.5, 0.8]);
  const b = norm4([0.6, 0.2, -0.1, 0.7]);
  assert.deepEqual(slerpQuat(...a, ...b, 0, [0, 0, 0, 0]), a);
  assert.deepEqual(slerpQuat(...a, ...b, 1, [0, 0, 0, 0]), b);
});

test("slerpQuat: identical inputs stay put; output is unit-norm", () => {
  const a = norm4([0.2, 0.4, -0.4, 0.8]);
  close4(slerpQuat(...a, ...a, 0.37, [0, 0, 0, 0]), a);
  const r = slerpQuat(
    ...norm4([1, 2, 3, 4]),
    ...norm4([-2, 1, 0.5, 3]),
    0.42,
    [0, 0, 0, 0],
  );
  assert.ok(Math.abs(Math.hypot(...r) - 1) < 1e-12, `norm ${Math.hypot(...r)}`);
});

test("slerpQuat: takes the shortest path — slerp(a, b) == slerp(a, -b)", () => {
  const a = norm4([0.3, 0.1, -0.2, 0.9]);
  const b = norm4([-0.5, 0.4, 0.3, 0.6]);
  const neg = b.map((v) => -v);
  for (const t of [0.2, 0.5, 0.8]) {
    const p = slerpQuat(...a, ...b, t, [0, 0, 0, 0]);
    const q = slerpQuat(...a, ...neg, t, [0, 0, 0, 0]);
    for (let c = 0; c < 4; c++) {
      assert.ok(Math.abs(p[c] - q[c]) < 1e-12, `t=${t} comp ${c}: ${p[c]} vs ${q[c]}`);
    }
  }
});

test("slerpQuat: near-antipodal inputs hit the linear-renormalize fallback and stay finite + unit", () => {
  // sqrSinHalfTheta === Number.EPSILON exactly -> the sqrSinHalfTheta <= EPSILON branch.
  const a = [0, 0, 0, 1];
  const b = [0, 0, 0, 1 - Number.EPSILON / 2];
  const r = slerpQuat(...a, ...b, 0.5, [0, 0, 0, 0]);
  assert.ok(r.every(Number.isFinite));
  assert.ok(Math.abs(Math.hypot(...r) - 1) < 1e-9, `norm ${Math.hypot(...r)}`);
});

test("writePoses: both bracket ends dead -> slot hidden, no trail", () => {
  const meta = {
    frame_times: [0, 1, 2, 3],
    slots: [{ segments: [[0, 0], [3, 3]] }], // dead at frames 1 and 2
  };
  const positions = packBuffer([
    [[1, 1, 1, ...IDENT_Q]],
    [[0, 0, 0, ...IDENT_Q]],
    [[0, 0, 0, ...IDENT_Q]],
    [[9, 9, 9, ...IDENT_Q]],
  ]);
  const out = makePoseBuffers(1);
  writePoses(meta, positions, 1.5, out);
  assert.equal(out.visible[0], 0);
  assert.equal(out.trailCount[0], 0);
});

test("writePoses: one end dead -> snap to the live end, never blend toward a zero pose", () => {
  const meta = {
    frame_times: [0, 1, 2],
    slots: [{ segments: [[0, 0], [2, 2]] }], // dead at frame 1
  };
  const positions = packBuffer([
    [[10, 10, 10, ...IDENT_Q]],
    [[0, 0, 0, ...IDENT_Q]], // dead frame — zeros in the buffer
    [[20, 20, 20, ...IDENT_Q]],
  ]);
  const out = makePoseBuffers(1);

  writePoses(meta, positions, 0.5, out); // bracket [0,1]: i live, j dead -> snap to i
  assert.equal(out.visible[0], 1);
  assert.deepEqual([...out.position], [10, 10, 10]);

  writePoses(meta, positions, 1.5, out); // bracket [1,2]: i dead, j live -> snap to j
  assert.deepEqual([...out.position], [20, 20, 20]);
});

test("writePoses: trail head is the live position, rest walk back, stopping at a segment edge", () => {
  const seg = [[5, 20]];
  const frame_times = Array.from({ length: 25 }, (_, k) => k);
  const meta = { frame_times, slots: [{ segments: seg }] };
  const positions = packBuffer(
    frame_times.map((_, k) => [[k, 0, 0, ...IDENT_Q]]),
  );
  const out = makePoseBuffers(1);

  writePoses(meta, positions, 8, out); // exactly on frame 8
  // head + frames 8,7,6,5 ; frame 4 is before the segment -> stop.
  assert.equal(out.trailCount[0], 5);
  assert.deepEqual([...out.trail.slice(0, 3)], [8, 0, 0]); // head = live pos
  assert.deepEqual([...out.trail.slice(3, 6)], [8, 0, 0]); // frame 8
  assert.deepEqual([...out.trail.slice(12, 15)], [5, 0, 0]); // frame 5, last before the edge
});

test("writePoses: trail is capped at TRAIL_FRAMES look-back", () => {
  const frame_times = Array.from({ length: 200 }, (_, k) => k);
  const meta = { frame_times, slots: [{ segments: [[0, 199]] }] };
  const positions = packBuffer(
    frame_times.map((_, k) => [[k, 0, 0, ...IDENT_Q]]),
  );
  const out = makePoseBuffers(1);
  writePoses(meta, positions, 120, out);
  assert.equal(out.trailCount[0], TRAIL_POINTS); // 1 head + TRAIL_FRAMES
});

// ---------------------------------------------------------------------------
// Property tests over the real replay buffer (inputs, not a golden oracle)
// ---------------------------------------------------------------------------

const { meta, positions } = loadFixture();

function grid(n) {
  const ft = meta.frame_times;
  const t0 = ft[0];
  const tN = ft[ft.length - 1];
  return Array.from({ length: n + 1 }, (_, k) => t0 + ((tN - t0) * k) / n);
}

test("real buffer: poses stay finite, quaternions unit-norm, no teleports", () => {
  const out = makePoseBuffers(meta.slots.length);
  const g = grid(4000);
  const dt = g[1] - g[0];
  const MAX_SPEED = 10000; // uu/s — a hard shot is ~6000

  // Segment boundaries are deliberate hard cuts (kickoff snap, post-goal
  // re-announce), so skip any interval straddling one of a slot's own edges.
  const edges = meta.slots.map((slot) => {
    const set = new Set();
    for (const [a, b] of slot.segments) {
      set.add(a);
      set.add(b);
    }
    return set;
  });
  const straddlesEdge = (s, f0, f1) => {
    for (let f = f0; f <= f1 + 1; f++) if (edges[s].has(f)) return true;
    return false;
  };

  const prev = new Map();
  let lastT = g[0];
  for (const t of g) {
    const [fPrev] = bracket(meta.frame_times, lastT);
    const [, fNow] = bracket(meta.frame_times, t);
    writePoses(meta, positions, t, out);
    for (let s = 0; s < meta.slots.length; s++) {
      if (!out.visible[s]) {
        prev.delete(s);
        continue;
      }
      const p = [out.position[s * 3], out.position[s * 3 + 1], out.position[s * 3 + 2]];
      const q = [
        out.quaternion[s * 4],
        out.quaternion[s * 4 + 1],
        out.quaternion[s * 4 + 2],
        out.quaternion[s * 4 + 3],
      ];
      assert.ok(p.every(Number.isFinite), `t=${t} slot ${s} non-finite position`);
      assert.ok(q.every(Number.isFinite), `t=${t} slot ${s} non-finite quaternion`);
      assert.ok(Math.abs(Math.hypot(...q) - 1) < 1e-3, `t=${t} slot ${s} quat norm`);
      const before = prev.get(s);
      if (before && !straddlesEdge(s, fPrev, fNow)) {
        const step = Math.hypot(p[0] - before[0], p[1] - before[1], p[2] - before[2]);
        assert.ok(
          step <= MAX_SPEED * dt,
          `t=${t} slot ${s} jumped ${step.toFixed(0)} uu in ${dt.toFixed(3)} s`,
        );
      }
      prev.set(s, p);
    }
    lastT = t;
  }
});

test("real buffer: a slot is visible iff some bracket end is live", () => {
  const out = makePoseBuffers(meta.slots.length);
  for (const t of grid(2000)) {
    const [i, j] = bracket(meta.frame_times, t);
    writePoses(meta, positions, t, out);
    for (let s = 0; s < meta.slots.length; s++) {
      const live = slotLiveAt(meta.slots[s], i) || slotLiveAt(meta.slots[s], j);
      assert.equal(out.visible[s] === 1, live, `t=${t} slot ${s}`);
    }
  }
});

test("real buffer: trail head equals the live position, length within bounds", () => {
  const out = makePoseBuffers(meta.slots.length);
  for (const t of grid(1500)) {
    writePoses(meta, positions, t, out);
    for (let s = 0; s < meta.slots.length; s++) {
      if (!out.visible[s]) continue;
      assert.ok(out.trailCount[s] >= 1 && out.trailCount[s] <= TRAIL_POINTS);
      const base = s * TRAIL_POINTS * 3;
      for (let c = 0; c < 3; c++) {
        const want = out.position[s * 3 + c];
        assert.ok(
          Math.abs(out.trail[base + c] - want) <= 1e-3 + Math.abs(want) * 1e-6,
          `slot ${s} trail head[${c}] ${out.trail[base + c]} vs ${want}`,
        );
      }
    }
  }
});

test("real buffer: createTransport round-trips the whole real timeline", () => {
  const tr = createTransport(meta);
  for (const t of grid(3000)) {
    const rt = tr.toReal(tr.toCompressed(t));
    // Inside a dead span toReal snaps to the resume frame, so only assert the
    // round-trip on kept instants.
    if (tr.snapForward(t) === t) {
      assert.ok(Math.abs(rt - t) < 1e-6, `t=${t} -> ${rt}`);
    }
  }
  assert.ok(tr.compressedEnd > 0 && tr.compressedEnd < tr.tN - tr.t0);
});
