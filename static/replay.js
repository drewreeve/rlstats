// Browser replay viewer — see docs/adr/0004-browser-replay-viewer-design.md
//
// Steps 3–9: load a match's metadata + packed position buffer, build a Three.js
// scene (wireframe soccar arena + box cars + sphere ball + name labels + motion
// trails), and play it back on a real-time clock — play/pause, scrub, 0.5×–4×
// speed, goal ticks on the scrub bar and a running scoreboard. Poses are
// lerp/slerp'd between rrrocket's ~30 Hz samples using the real (non-uniform)
// frame deltas. A slot's mesh is hidden while its actor is between segments
// (demolitions). When the tracked team is team 1 the field is flipped 180° so
// "our" half is always the same side of the screen. Orthographic camera with
// drag-orbit/zoom and BROADCAST / TOP presets.

import * as THREE from "https://cdn.jsdelivr.net/npm/three@0.170.0/+esm";
import { OrbitControls } from "https://cdn.jsdelivr.net/npm/three@0.170.0/examples/jsm/controls/OrbitControls.js/+esm";

// Rocket League field, unreal units. X = wall to wall, Y = goal to goal,
// Z = floor to ceiling. The world group is Z-up (RL); Three.js is Y-up.
const FIELD_X = 8192;
const FIELD_Y = 10240;
const FIELD_Z = 2044;
const CORNER = 1152; // 45° chamfer span on each axis at the four field corners
const HX = FIELD_X / 2;
const HY = FIELD_Y / 2;

// Chamfered soccar footprint in the RL XY plane: the rectangle with all four
// corners cut at 45° (|x| + |y| = 8064 along each diagonal). CCW from +x/+y.
// Shared by the arena wireframe, the floor grid clip, and the half-pitch tint.
const ARENA_OUTLINE = [
  [HX, HY - CORNER],
  [HX - CORNER, HY],
  [-(HX - CORNER), HY],
  [-HX, HY - CORNER],
  [-HX, -(HY - CORNER)],
  [-(HX - CORNER), -HY],
  [HX - CORNER, -HY],
  [HX, -(HY - CORNER)],
];

const CAR_SIZE = [118, 84, 36]; // Octane hitbox, RL local axes (X fwd, Y left, Z up)
const BALL_RADIUS = 91.25;
const FLOATS_PER_POSE = 7; // x, y, z, qx, qy, qz, qw

// Soccar goal: 1786 uu mouth width, 643 uu tall, 880 uu deep behind the back
// wall. Team 0 defends the −y goal, team 1 the +y goal (before the field flip).
const GOAL_HW = 893;
const GOAL_H = 643;
const GOAL_DEPTH = 880;
const GOAL_LINE = 0xaab8d8; // brighter than the arena edges so the frame reads

// Standard soccar boost pads: 6 big (full boost) + 28 small, positions in the
// RL XY plane (unreal units) from wiki.rlbot.org — the same source as
// frame_analysis.BIG_PAD_POSITIONS, whose 6 big coords these match. Drawn as
// flat discs just above the floor: static field furniture, no pickup/respawn
// state (that stays the deferred "pad respawn timers" item).
const BOOST_PADS_BIG = [
  [-3584, 0], [3584, 0],
  [-3072, -4096], [3072, -4096],
  [-3072, 4096], [3072, 4096],
];
const BOOST_PADS_SMALL = [
  [0, -4240], [-1792, -4184], [1792, -4184], [-940, -3308], [940, -3308],
  [0, -2816], [-3584, -2484], [3584, -2484], [-1788, -2300], [1788, -2300],
  [-2048, -1036], [0, -1024], [2048, -1036], [-1024, 0], [1024, 0],
  [-2048, 1036], [0, 1024], [2048, 1036], [-1788, 2300], [1788, 2300],
  [-3584, 2484], [3584, 2484], [0, 2816], [-940, 3308], [940, 3308],
  [-1792, 4184], [1792, 4184], [0, 4240],
];
const BOOST_PAD_BIG_R = 100; // big pads read at ~2x the small ones
const BOOST_PAD_SMALL_R = 50;
const BOOST_PAD_COLOR = 0x8f8062; // dim grey-gold, deliberately not boost-yellow
const BOOST_PAD_Z = 0.6; // above the floor grid (0.5), below the half tint (1)

const TEAM_OURS = 0x00e5ff;
const TEAM_THEIRS = 0xff5a5a;
const TEAM_UNKNOWN = 0x8585a0;
const SEEK_STEP = 5; // seconds, for arrow-key seeking
const LABEL_HEIGHT = 150; // uu above a car's centre for its name label
const TRAIL_FRAMES = 45; // ~1.5 s of motion tail at rrrocket's ~30 Hz

// Orthographic camera presets (world coords, Y-up). `size` is the frustum
// height in uu; `resize()` derives the width from the viewport aspect.
const CAM_PRESETS = {
  broadcast: {
    // 3/4 isometric-ish corner view (ballchasing style), not straight behind
    // the goal: offset toward +x and the tracked team's end, ~33° elevation.
    pos: [10500, 9600, 13000],
    target: [0, 700, 0],
    up: [0, 1, 0],
    size: 10200,
  },
  top: {
    pos: [0, 20000, 0],
    target: [0, 0, 0],
    up: [0, 0, -1], // screen-up = far goal
    size: 12800, // field + both goals (y spans ±6000)
  },
};

let viewSize = CAM_PRESETS.broadcast.size;

const stage =
  document.querySelector('[data-role="stage"]') ||
  document.querySelector(".replay-stage");
const canvas = document.querySelector('[data-role="scene"]');
const messageEl = document.querySelector('[data-role="message"]');
const controlsEl = document.querySelector('[data-role="controls"]');
const playBtn = document.querySelector('[data-role="playpause"]');
const scrubEl = document.querySelector('[data-role="scrub"]');
const clockEl = document.querySelector('[data-role="clock"]');
const speedsEl = document.querySelector('[data-role="speeds"]');
const camEl = document.querySelector('[data-role="cam"]');
const scoreEl = document.querySelector('[data-role="score"]');
const marksEl = document.querySelector('[data-role="marks"]');
const countdownEl = document.querySelector('[data-role="countdown"]');

const matchId = location.pathname.split("/").filter(Boolean)[1];
const backEl = document.querySelector('[data-role="back"]');
if (backEl) backEl.href = `/match/${matchId}`;

const _qa = new THREE.Quaternion();
const _qb = new THREE.Quaternion();

let renderer;
let camera;

function showMessage(text) {
  if (canvas) canvas.hidden = true;
  if (messageEl) {
    messageEl.textContent = text;
    messageEl.hidden = false;
  }
}

function poseOffset(slotCount, frame, slot) {
  return (frame * slotCount + slot) * FLOATS_PER_POSE;
}

// ours (tracked) vs theirs, keyed on RL team not screen side — the field flip
// then puts "ours" on the same side of the screen every match.
function teamTint(team, trackedTeam) {
  if (team == null || trackedTeam == null) return TEAM_UNKNOWN;
  return team === trackedTeam ? TEAM_OURS : TEAM_THEIRS;
}

function carColor(slot, trackedTeam) {
  return teamTint(slot.team, trackedTeam);
}

// Is this slot's actor live at frame index `frame`? Segments are inclusive
// [start, end] ranges; between them (demolitions) the buffer holds zeros.
function slotLiveAt(slot, frame) {
  const segs = slot.segments;
  for (let k = 0; k < segs.length; k++) {
    if (frame >= segs[k][0] && frame <= segs[k][1]) return true;
  }
  return false;
}

// A camera-facing name tag drawn to a canvas texture. Sprites ignore parent
// rotation, so the text reads normally even inside the flipped field group.
function makeLabelSprite(text, cssColor) {
  const font = "600 40px 'DM Mono', ui-monospace, monospace";
  const measure = document.createElement("canvas").getContext("2d");
  measure.font = font;
  const padX = 12;
  const w = Math.ceil(measure.measureText(text).width) + padX * 2;
  const h = 56;
  const c = document.createElement("canvas");
  c.width = w;
  c.height = h;
  const ctx = c.getContext("2d");
  ctx.font = font;
  ctx.textBaseline = "middle";
  ctx.fillStyle = "rgba(8, 10, 18, 0.7)";
  ctx.fillRect(0, 0, w, h);
  ctx.fillStyle = cssColor;
  ctx.fillText(text, padX, h / 2 + 2);

  const texture = new THREE.CanvasTexture(c);
  texture.anisotropy = 4;
  const sprite = new THREE.Sprite(
    new THREE.SpriteMaterial({
      map: texture,
      transparent: true,
      depthTest: false,
      depthWrite: false,
    }),
  );
  const height = 260; // uu
  sprite.scale.set((w / h) * height, height, 1);
  sprite.renderOrder = 10;
  return sprite;
}

// A polyline motion tail. Head vertex is the live position; the rest walk
// backward through the position buffer. Colour is baked once, head → background;
// per frame only the vertex positions and draw range change.
function makeTrail(colorHex) {
  const n = TRAIL_FRAMES + 1;
  const geo = new THREE.BufferGeometry();
  geo.setAttribute(
    "position",
    new THREE.BufferAttribute(new Float32Array(n * 3), 3),
  );
  const colors = new Float32Array(n * 3);
  const head = new THREE.Color(colorHex);
  const tail = new THREE.Color(0x0b0d15);
  const c = new THREE.Color();
  for (let k = 0; k < n; k++) {
    c.copy(head).lerp(tail, k / (n - 1));
    colors[k * 3] = c.r;
    colors[k * 3 + 1] = c.g;
    colors[k * 3 + 2] = c.b;
  }
  geo.setAttribute("color", new THREE.BufferAttribute(colors, 3));
  geo.setDrawRange(0, 0);

  const line = new THREE.Line(
    geo,
    new THREE.LineBasicMaterial({
      vertexColors: true,
      transparent: true,
      opacity: 0.85,
    }),
  );
  line.frustumCulled = false; // vertices move every frame
  return line;
}

function formatClock(seconds) {
  const s = Math.max(0, Math.round(seconds));
  return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`;
}

// Ticks sit ~1 s apart; hold each numeral a touch longer so a dropped or
// aborted sequence clears itself rather than sticking on screen.
const COUNTDOWN_TICK_HOLD = 1.4; // s
const COUNTDOWN_GO_HOLD = 0.6; // s — "GO!" flashes, then clears. Keep in sync with
// the `replay-countdown-go` animation duration in replay.css.

// The kickoff countdown text to show at replay-time `t` ("3" / "2" / "1" /
// "GO!"), or null when no countdown is active. `countdowns` is [[frame, n], …]
// in frame order (server: replay_frames._scan_countdowns) — one 3→2→1→0 run per
// kickoff.
function countdownLabelAt(times, countdowns, t) {
  const tick = countdowns.findLast(([f]) => times[f] <= t);
  if (!tick) return null;
  const [frame, n] = tick;
  const dt = t - times[frame];
  if (n === 0) return dt >= 0 && dt < COUNTDOWN_GO_HOLD ? "GO!" : null;
  return dt < COUNTDOWN_TICK_HOLD ? String(n) : null;
}

// Largest i with times[i] <= t, its successor j, and the [0,1] blend between.
function bracket(times, t) {
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
// starts. Used to clip the floor grid to the octagon.
function outlineHalfWidth(x, y) {
  // On the flat back walls (|x| small) the limit is HY; into a corner it is the
  // chamfer line |x| + |y| = HX + HY - CORNER. Mirror for the side walls.
  return {
    x: Math.abs(y) > HY - CORNER ? HX + HY - CORNER - Math.abs(y) : HX,
    y: Math.abs(x) > HX - CORNER ? HX + HY - CORNER - Math.abs(x) : HY,
  };
}

function buildArena(parent) {
  const edgeMat = new THREE.LineBasicMaterial({ color: 0x4a5d82 });

  // Chamfered octagon: floor loop (z = 0), ceiling loop (z = FIELD_Z), and a
  // vertical edge at each of the eight corners.
  const floor = ARENA_OUTLINE.map(([x, y]) => new THREE.Vector3(x, y, 0));
  const ceil = ARENA_OUTLINE.map(([x, y]) => new THREE.Vector3(x, y, FIELD_Z));
  parent.add(
    new THREE.LineLoop(new THREE.BufferGeometry().setFromPoints(floor), edgeMat),
  );
  parent.add(
    new THREE.LineLoop(new THREE.BufferGeometry().setFromPoints(ceil), edgeMat),
  );
  const verticals = [];
  for (const [x, y] of ARENA_OUTLINE) {
    verticals.push(new THREE.Vector3(x, y, 0), new THREE.Vector3(x, y, FIELD_Z));
  }
  parent.add(
    new THREE.LineSegments(
      new THREE.BufferGeometry().setFromPoints(verticals),
      edgeMat,
    ),
  );

  // Floor grid, ~1024 uu spacing, clipped to the octagon so nothing overhangs.
  const gridPts = [];
  for (let x = -HX; x <= HX + 1; x += 1024) {
    const lim = outlineHalfWidth(x, 0).y;
    gridPts.push(new THREE.Vector3(x, -lim, 0.5), new THREE.Vector3(x, lim, 0.5));
  }
  for (let y = -HY; y <= HY + 1; y += 1024) {
    const lim = outlineHalfWidth(0, y).x;
    gridPts.push(new THREE.Vector3(-lim, y, 0.5), new THREE.Vector3(lim, y, 0.5));
  }
  parent.add(
    new THREE.LineSegments(
      new THREE.BufferGeometry().setFromPoints(gridPts),
      new THREE.LineBasicMaterial({ color: 0x1e2842 }),
    ),
  );

  // Centre line (wall to wall at y = 0, just above the floor).
  parent.add(
    new THREE.LineSegments(
      new THREE.BufferGeometry().setFromPoints([
        new THREE.Vector3(-HX, 0, 2),
        new THREE.Vector3(HX, 0, 2),
      ]),
      edgeMat,
    ),
  );
}

// An open wireframe box at each end: goal mouth on the back wall, matching
// frame at full depth, four edges joining them. The mouth is filled with a
// translucent plane in the defending team's colour — the clearest "which end
// is whose" cue.
function goalFrameSegments(gy, by, hw, ht) {
  const P = (x, y, z) => new THREE.Vector3(x, y, z);
  return [
    P(-hw, gy, 0), P(hw, gy, 0), // mouth
    P(-hw, gy, ht), P(hw, gy, ht),
    P(-hw, gy, 0), P(-hw, gy, ht),
    P(hw, gy, 0), P(hw, gy, ht),
    P(-hw, by, 0), P(hw, by, 0), // back frame
    P(-hw, by, ht), P(hw, by, ht),
    P(-hw, by, 0), P(-hw, by, ht),
    P(hw, by, 0), P(hw, by, ht),
    P(-hw, gy, 0), P(-hw, by, 0), // depth edges
    P(hw, gy, 0), P(hw, by, 0),
    P(-hw, gy, ht), P(-hw, by, ht),
    P(hw, gy, ht), P(hw, by, ht),
  ];
}

function buildGoals(parent, trackedTeam) {
  const frameMat = new THREE.LineBasicMaterial({ color: GOAL_LINE });
  for (const sign of [-1, 1]) {
    const team = sign < 0 ? 0 : 1; // team 0 defends −y, team 1 defends +y
    const gy = sign * HY;
    const by = sign * (HY + GOAL_DEPTH);

    parent.add(
      new THREE.LineSegments(
        new THREE.BufferGeometry().setFromPoints(
          goalFrameSegments(gy, by, GOAL_HW, GOAL_H),
        ),
        frameMat,
      ),
    );

    const fill = new THREE.Mesh(
      new THREE.PlaneGeometry(2 * GOAL_HW, GOAL_H),
      new THREE.MeshBasicMaterial({
        color: teamTint(team, trackedTeam),
        transparent: true,
        opacity: 0.35,
        depthWrite: false,
        side: THREE.DoubleSide,
      }),
    );
    fill.rotation.x = Math.PI / 2; // stand it up in the x–z plane
    fill.position.set(0, gy - sign * 2, GOAL_H / 2); // inset 2 uu off the wall
    parent.add(fill);
  }
}

// A faint colour wash over each half of the pitch, in the defending team's
// colour, so the two ends read at a glance (and from straight overhead, where
// the vertical goal fills are edge-on). Same chamfered footprint as the arena,
// split at y = 0.
function buildHalfTint(parent, trackedTeam) {
  const halves = [
    {
      team: 1,
      ring: [
        [HX, 0],
        [HX, HY - CORNER],
        [HX - CORNER, HY],
        [-(HX - CORNER), HY],
        [-HX, HY - CORNER],
        [-HX, 0],
      ],
    },
    {
      team: 0,
      ring: [
        [-HX, 0],
        [-HX, -(HY - CORNER)],
        [-(HX - CORNER), -HY],
        [HX - CORNER, -HY],
        [HX, -(HY - CORNER)],
        [HX, 0],
      ],
    },
  ];
  for (const { team, ring } of halves) {
    const shape = new THREE.Shape(
      ring.map(([x, y]) => new THREE.Vector2(x, y)),
    );
    const mesh = new THREE.Mesh(
      new THREE.ShapeGeometry(shape),
      new THREE.MeshBasicMaterial({
        color: teamTint(team, trackedTeam),
        transparent: true,
        opacity: 0.1,
        depthWrite: false,
        side: THREE.DoubleSide,
      }),
    );
    mesh.position.z = 1; // above the floor grid (0.5), below the centre line (2)
    parent.add(mesh);
  }
}

// Static boost-pad markers: one flat disc per pad, big pads at twice the
// radius, lying just above the floor grid. Furniture only — no collect /
// respawn animation. Parented to `field` so it rides the orientation flip
// with the rest of the arena geometry (a visual no-op: the layout is
// symmetric under the 180° spin, but consistent — wrinkle 7).
function buildBoostPads(parent) {
  const mat = new THREE.MeshBasicMaterial({
    color: BOOST_PAD_COLOR,
    transparent: true,
    opacity: 0.5,
    depthWrite: false,
    side: THREE.DoubleSide,
  });
  for (const [pads, r] of [
    [BOOST_PADS_BIG, BOOST_PAD_BIG_R],
    [BOOST_PADS_SMALL, BOOST_PAD_SMALL_R],
  ]) {
    const geo = new THREE.CircleGeometry(r, 24); // in the x–y plane, faces +z
    for (const [x, y] of pads) {
      const disc = new THREE.Mesh(geo, mat);
      disc.position.set(x, y, BOOST_PAD_Z);
      parent.add(disc);
    }
  }
}

function createActorMeshes(field, meta) {
  return meta.slots.map((slot) => {
    const isBall = slot.kind === "ball";
    const color = isBall ? 0xf0f0f4 : carColor(slot, meta.tracked_team);

    const mesh = new THREE.Mesh(
      isBall
        ? new THREE.SphereGeometry(BALL_RADIUS, 24, 16)
        : new THREE.BoxGeometry(...CAR_SIZE),
      new THREE.MeshLambertMaterial({ color }),
    );
    field.add(mesh);

    const trail = makeTrail(color);
    mesh.userData.trail = trail;
    field.add(trail);

    if (!isBall) {
      const label = makeLabelSprite(
        slot.name,
        "#" + color.toString(16).padStart(6, "0"),
      );
      mesh.userData.label = label;
      field.add(label);
    }
    return mesh;
  });
}

// The playback clock + per-frame pose application, over one match's data.
function createPlayback(meta, positions, meshes) {
  const times = meta.frame_times;
  const slotCount = meta.slots.length;
  const t0 = times[0];
  const tN = times[times.length - 1];

  // Timeline remap: the clock still runs in real `frame_times` seconds (so
  // bracket()/applyPoses are untouched), but the transport UI runs on a
  // *compressed* axis with every dead_periods span — goal replay, actor reset,
  // frozen-at-spawn wait — removed. `dead` is those spans as real intervals
  // [a, b) plus `len` and `c`, the span's coordinate on the compressed axis
  // (`c === toCompressed(a)`). The server (replay_frames._dead_periods)
  // guarantees them ascending and non-overlapping.
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

  const compressedEnd = toCompressed(tN);
  const tStart = toReal(0); // first kept instant — past any pre-match warmup

  const state = { t: tStart, playing: false, speed: 1 };

  function applyPoses() {
    const [i, j, f] = bracket(times, state.t);
    for (let s = 0; s < meshes.length; s++) {
      const slot = meta.slots[s];
      const label = meshes[s].userData.label;
      const trail = meshes[s].userData.trail;
      const liveI = slotLiveAt(slot, i);
      const liveJ = slotLiveAt(slot, j);
      if (!liveI && !liveJ) {
        meshes[s].visible = false;
        if (label) label.visible = false;
        trail.visible = false;
        continue;
      }
      meshes[s].visible = true;
      // Don't lerp toward the zero-pose of a frame the actor isn't live in:
      // snap to whichever end is live when a segment boundary falls in [i, j].
      const ff = !liveI ? 1 : !liveJ ? 0 : f;

      const a = poseOffset(slotCount, i, s);
      const b = poseOffset(slotCount, j, s);
      meshes[s].position.set(
        positions[a] + (positions[b] - positions[a]) * ff,
        positions[a + 1] + (positions[b + 1] - positions[a + 1]) * ff,
        positions[a + 2] + (positions[b + 2] - positions[a + 2]) * ff,
      );
      _qa.set(
        positions[a + 3],
        positions[a + 4],
        positions[a + 5],
        positions[a + 6],
      );
      _qb.set(
        positions[b + 3],
        positions[b + 4],
        positions[b + 5],
        positions[b + 6],
      );
      meshes[s].quaternion.copy(_qa.slerp(_qb, ff));

      if (label) {
        label.visible = true;
        label.position.set(
          meshes[s].position.x,
          meshes[s].position.y,
          meshes[s].position.z + LABEL_HEIGHT,
        );
      }

      // Trail: head = live position, then walk back through the buffer while
      // this slot stays live (never across a demolition gap).
      const tp = trail.geometry.attributes.position.array;
      tp[0] = meshes[s].position.x;
      tp[1] = meshes[s].position.y;
      tp[2] = meshes[s].position.z;
      let count = 1;
      for (
        let k = i;
        k >= 0 && k > i - TRAIL_FRAMES && slotLiveAt(slot, k);
        k--
      ) {
        const o = poseOffset(slotCount, k, s);
        tp[count * 3] = positions[o];
        tp[count * 3 + 1] = positions[o + 1];
        tp[count * 3 + 2] = positions[o + 2];
        count++;
      }
      trail.visible = count > 1;
      trail.geometry.setDrawRange(0, count);
      trail.geometry.attributes.position.needsUpdate = true;
    }
  }

  // Transport is compressed-axis: `seek`/`nudge` take compressed seconds, and
  // `toReal` already resolves a seam to the resume frame so seeks never land in
  // a gap. Only `advance` steps raw real time, so only it needs the snap.
  function seek(c) {
    state.t = toReal(Math.min(compressedEnd, Math.max(0, c)));
  }

  function nudge(deltaCompressed) {
    seek(toCompressed(state.t) + deltaCompressed);
  }

  function advance(dt) {
    if (!state.playing) return;
    let t = state.t + dt * state.speed;
    for (const d of dead) if (t >= d.a && t < d.b) { t = d.b; break; }
    if (t >= tN) {
      t = tN;
      state.playing = false;
    }
    state.t = t;
  }

  // real seconds -> [0,1] position on the compressed axis (scrub bar, goal ticks)
  const fractionAt = (t) => (compressedEnd > 0 ? toCompressed(t) / compressedEnd : 0);

  return {
    state,
    tN,
    applyPoses,
    seek,
    nudge,
    advance,
    fractionAt,
    elapsed: () => toCompressed(state.t),
    duration: () => compressedEnd,
    progress: () => fractionAt(state.t),
    atEnd: () => state.t >= tN,
  };
}

// Goal ticks on the scrub bar, positioned by compressed-time fraction (a goal
// sits at the leading edge of its dead span, so its tick lands on the seam).
function renderScrubMarks(meta, playback) {
  if (!marksEl) return;
  marksEl.replaceChildren();
  for (const g of meta.goals) {
    const el = document.createElement("span");
    el.style.left = `${playback.fractionAt(meta.frame_times[g.frame]) * 100}%`;
    el.style.background =
      g.team === meta.tracked_team ? "#00e5ff" : "#ff5a5a";
    marksEl.appendChild(el);
  }
}

function wireControls(playback, meta) {
  let scrubbing = false;
  const goals = meta.goals.map((g) => ({
    t: meta.frame_times[g.frame],
    ours: g.team === meta.tracked_team,
  }));

  function setPlaying(on) {
    if (on && playback.atEnd()) playback.seek(0);
    playback.state.playing = on;
    playBtn.textContent = on ? "⏸" : "▶";
  }

  playBtn.addEventListener("click", () => setPlaying(!playback.state.playing));

  scrubEl.addEventListener("pointerdown", () => {
    scrubbing = true;
  });
  window.addEventListener("pointerup", () => {
    scrubbing = false;
  });
  scrubEl.addEventListener("input", () => {
    playback.seek((scrubEl.valueAsNumber / 1000) * playback.duration());
  });

  speedsEl.addEventListener("click", (e) => {
    const btn = e.target.closest("button[data-speed]");
    if (!btn) return;
    playback.state.speed = Number(btn.dataset.speed);
    for (const b of speedsEl.querySelectorAll("button")) {
      b.classList.toggle("is-active", b === btn);
    }
  });

  window.addEventListener("keydown", (e) => {
    if (e.key === " ") {
      e.preventDefault();
      setPlaying(!playback.state.playing);
    } else if (e.key === "ArrowLeft") {
      playback.nudge(-SEEK_STEP);
    } else if (e.key === "ArrowRight") {
      playback.nudge(SEEK_STEP);
    }
  });

  const countdowns = meta.countdowns || [];
  let shownCountdown; // last label written, so the DOM is only touched on change

  function showCountdown(label) {
    if (label === shownCountdown) return;
    shownCountdown = label;
    countdownEl.hidden = label == null;
    if (label == null) return;
    countdownEl.textContent = label;
    countdownEl.classList.toggle("is-go", label === "GO!");
    countdownEl.classList.remove("is-anim");
    void countdownEl.offsetWidth; // reflow so the pop / fade restarts
    countdownEl.classList.add("is-anim");
  }

  // Reflect clock state back into the DOM each frame.
  return function syncUI() {
    if (countdownEl) {
      showCountdown(
        countdownLabelAt(meta.frame_times, countdowns, playback.state.t),
      );
    }

    if (!scrubbing) scrubEl.value = String(Math.round(playback.progress() * 1000));
    clockEl.textContent = `${formatClock(playback.elapsed())} / ${formatClock(
      playback.duration(),
    )}`;
    if (!playback.state.playing && playBtn.textContent !== "▶") {
      playBtn.textContent = "▶";
    }
    if (scoreEl) {
      let ours = 0;
      let theirs = 0;
      for (const g of goals) {
        if (g.t > playback.state.t) continue;
        if (g.ours) ours++;
        else theirs++;
      }
      scoreEl.innerHTML =
        `<span class="ours">${ours}</span>` +
        ` &ndash; ` +
        `<span class="theirs">${theirs}</span>`;
    }
  };
}

// mean / p95 / max over the first `n` entries of a numeric buffer.
function windowStats(buf, n) {
  let sum = 0;
  for (let k = 0; k < n; k++) sum += buf[k];
  const sorted = Array.prototype.slice.call(buf, 0, n).sort((a, b) => a - b);
  return {
    mean: sum / n,
    p95: sorted[Math.min(n - 1, Math.floor(n * 0.95))],
    max: sorted[n - 1],
  };
}

// A ?debug-gated HUD: rolling rAF frame time, time spent inside frameLoop's JS,
// and a motion-continuity readout. If JS time is a couple of ms and frame time
// sits near the display interval, the main thread is idle and the stutter is
// interpolation, not scheduling. `freeze` is the fraction of played window
// frames where some visible actor stalled for a frame between two moving frames
// (the carry-forward freeze-then-lurch); `maxjump` is the worst single-frame
// actor step. Updates its own DOM on a 250 ms timer — a per-rAF textContent
// write would be exactly the kind of main-thread churn this is here to detect.
function createDebugHud(meshes) {
  const N = 120; // ~2 s at 60 Hz
  const EPS = 1; // uu; a moving actor clears this every frame, a held one is at 0
  const frameMs = new Float32Array(N);
  const jsMs = new Float32Array(N);
  const freezeFlag = new Float32Array(N); // 1 = a stall was seen this frame
  const evalFlag = new Float32Array(N); // 1 = frame was playing + evaluable
  const jumpUu = new Float32Array(N);
  const trail = meshes.map(() => []); // per slot: up to the last 4 positions
  let head = 0;
  let filled = 0;

  const el = document.createElement("div");
  el.className = "replay-hud";
  stage.appendChild(el);

  setInterval(() => {
    if (!filled) return;
    const f = windowStats(frameMs, filled);
    const j = windowStats(jsMs, filled);
    let stalls = 0;
    let evald = 0;
    let jmax = 0;
    for (let k = 0; k < filled; k++) {
      stalls += freezeFlag[k];
      evald += evalFlag[k];
      if (jumpUu[k] > jmax) jmax = jumpUu[k];
    }
    const freezePct = evald ? (100 * stalls) / evald : 0;
    el.textContent =
      `fps     ${(1000 / f.mean).toFixed(0)}\n` +
      `frame   ${f.mean.toFixed(1)} mean · ${f.p95.toFixed(1)} p95 · ${f.max.toFixed(1)} max ms\n` +
      `js      ${j.mean.toFixed(2)} mean · ${j.p95.toFixed(2)} p95 ms\n` +
      `freeze  ${freezePct.toFixed(1)}%  ·  maxjump ${jmax.toFixed(0)} uu`;
  }, 250);

  return function record(frameDelta, jsDelta, playing) {
    frameMs[head] = frameDelta;
    jsMs[head] = jsDelta;

    let stalled = 0;
    let evaluable = 0;
    let jump = 0;
    for (let s = 0; s < meshes.length; s++) {
      const h = trail[s];
      if (!(playing && meshes[s].visible)) {
        h.length = 0; // don't measure across a pause or a lifecycle gap
        continue;
      }
      h.push(meshes[s].position.clone());
      if (h.length > 4) h.shift();
      if (h.length < 4) continue;
      evaluable = 1;
      const dAB = h[0].distanceTo(h[1]);
      const dBC = h[1].distanceTo(h[2]);
      const dCD = h[2].distanceTo(h[3]);
      if (dAB > EPS && dBC < EPS && dCD > EPS) stalled = 1; // moving, held, moving
      if (dCD > jump) jump = dCD;
    }
    freezeFlag[head] = stalled;
    evalFlag[head] = evaluable;
    jumpUu[head] = jump;

    head = (head + 1) % N;
    if (filled < N) filled++;
  };
}

function buildScene(meta, positions) {
  const scene = new THREE.Scene();
  scene.background = new THREE.Color(0x0b0d15);
  scene.add(new THREE.AmbientLight(0xffffff, 0.75));
  const key = new THREE.DirectionalLight(0xffffff, 1.1);
  key.position.set(4000, 10000, 6000);
  scene.add(key);
  const fill = new THREE.DirectionalLight(0x88aaff, 0.35);
  fill.position.set(-6000, 3000, -4000);
  scene.add(fill);

  // RL is Z-up; rotate the whole world so RL Z maps to Three's Y.
  const world = new THREE.Group();
  world.rotation.x = -Math.PI / 2;
  scene.add(world);

  // Orientation normalisation (decision 12): when the tracked team is team 1,
  // spin the field 180° in RL's horizontal plane so "our" half of the pitch is
  // always on the same side of the screen. Label sprites stay camera-facing, so
  // their text is not mirrored.
  const field = new THREE.Group();
  if (meta.tracked_team === 1) field.rotation.z = Math.PI;
  world.add(field);

  buildArena(field);
  buildHalfTint(field, meta.tracked_team);
  buildBoostPads(field);
  buildGoals(field, meta.tracked_team);
  const meshes = createActorMeshes(field, meta);

  renderer = new THREE.WebGLRenderer({ canvas, antialias: true });
  renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));

  camera = new THREE.OrthographicCamera(-1, 1, 1, -1, 0.1, 200000);

  const controls = new OrbitControls(camera, renderer.domElement);
  controls.enableDamping = true;

  applyCamPreset("broadcast", controls);
  wireCamera(controls);
  window.addEventListener("resize", resize);

  const playback = createPlayback(meta, positions, meshes);
  const syncUI = wireControls(playback, meta);
  renderScrubMarks(meta, playback);
  playback.applyPoses();
  syncUI();
  controlsEl.hidden = false;

  const debug = location.search.includes("debug");
  const hud = debug ? createDebugHud(meshes) : null;
  if (debug) {
    window.__replay = {
      playback,
      meshes,
      camera,
      controls,
      renderer,
      scene,
      THREE,
      meta,
      countdownLabelAt,
    };
  }

  let lastNow = null;
  function frameLoop(now) {
    requestAnimationFrame(frameLoop);
    const dt = lastNow == null ? 0 : (now - lastNow) / 1000;
    lastNow = now;
    playback.advance(dt);
    playback.applyPoses();
    syncUI();
    controls.update();
    renderer.render(scene, camera);
    if (hud && dt > 0) {
      hud(dt * 1000, performance.now() - now, playback.state.playing);
    }
  }
  requestAnimationFrame(frameLoop);

  const metaEl = document.querySelector('[data-role="meta"]');
  if (metaEl) {
    metaEl.textContent = [meta.game_mode, formatClock(playback.duration())]
      .filter(Boolean)
      .join("  ·  ");
  }
}

function resize() {
  if (!renderer || !camera) return;
  const w = stage.clientWidth;
  const h = stage.clientHeight;
  renderer.setSize(w, h, false);
  const aspect = w / h || 1;
  camera.left = (-viewSize * aspect) / 2;
  camera.right = (viewSize * aspect) / 2;
  camera.top = viewSize / 2;
  camera.bottom = -viewSize / 2;
  camera.updateProjectionMatrix();
}

function applyCamPreset(name, controls) {
  const p = CAM_PRESETS[name];
  if (!p) return;
  camera.up.set(...p.up);
  camera.position.set(...p.pos);
  controls.target.set(...p.target);
  viewSize = p.size;
  resize();
  controls.update();
}

// Camera preset buttons, and clearing the active state once the user orbits.
function wireCamera(controls) {
  if (!camEl) return;
  camEl.addEventListener("click", (e) => {
    const btn = e.target.closest("button[data-view]");
    if (!btn) return;
    applyCamPreset(btn.dataset.view, controls);
    for (const b of camEl.querySelectorAll("button")) {
      b.classList.toggle("is-active", b === btn);
    }
  });
  controls.addEventListener("start", () => {
    for (const b of camEl.querySelectorAll("button")) b.classList.remove("is-active");
  });
}

async function main() {
  if (!canvas || !matchId) {
    showMessage("This replay could not be loaded.");
    return;
  }
  try {
    const metaRes = await fetch(`/api/matches/${matchId}/replay`);
    if (!metaRes.ok) {
      showMessage(
        metaRes.status === 404
          ? "No replay file for this match."
          : "This replay could not be loaded.",
      );
      return;
    }
    const meta = await metaRes.json();

    const binRes = await fetch(`/api/matches/${matchId}/replay-frames.bin`);
    if (!binRes.ok) {
      showMessage("This replay could not be loaded.");
      return;
    }
    const positions = new Float32Array(await binRes.arrayBuffer());

    await document.fonts.ready; // so name labels render in DM Mono, not fallback
    buildScene(meta, positions);
  } catch (err) {
    console.error(err);
    showMessage("This replay could not be loaded.");
  }
}

main();
