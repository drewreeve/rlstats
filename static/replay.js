// Browser replay viewer — see docs/adr/0004-browser-replay-viewer-design.md
//
// Steps 3–9: load a match's metadata + packed position buffer, build a Three.js
// scene (wireframe soccar arena + low-poly cars + seamed ball + name labels +
// motion trails + a goal-scored particle burst), and play it back on a real-time
// clock — play/pause, scrub, 0.5×–4× speed, goal ticks on the scrub bar and a
// running scoreboard. Poses are lerp/slerp'd between rrrocket's ~30 Hz samples
// using the real (non-uniform) frame deltas. A slot's mesh is hidden while its
// actor is between segments
// (demolitions). When the tracked team is team 1 the field is flipped 180° so
// "our" half is always the same side of the screen. Orthographic camera with
// drag-orbit/zoom and BROADCAST / TOP presets.

import * as THREE from "https://cdn.jsdelivr.net/npm/three@0.170.0/+esm";
import { OrbitControls } from "https://cdn.jsdelivr.net/npm/three@0.170.0/examples/jsm/controls/OrbitControls.js/+esm";
import { RoomEnvironment } from "https://cdn.jsdelivr.net/npm/three@0.170.0/examples/jsm/environments/RoomEnvironment.js/+esm";

// Field geometry, team colours, the countdown text and all playback/timeline
// math live in replay-core.js (THREE- and DOM-free, unit-tested by
// tests/js/replay-core.test.js). This file is the viewer shell: scene graph,
// DOM wiring, camera, and the rAF loop.
import {
  arenaSpec,
  carColor,
  countdownLabelAt,
  createTransport,
  formatClock,
  makePoseBuffers,
  outlineHalfWidth,
  poseOffset,
  teamTint,
  TRAIL_POINTS,
  writePoses,
} from "./replay-core.js";

// X = wall to wall, Y = goal to goal, Z = floor to ceiling. The world group is
// Z-up (RL); Three.js is Y-up. Per-mode field geometry — footprint, chamfer,
// ceiling, goal shape, boost-pad layout, ball radius — comes from
// arenaSpec(meta.game_mode) (replay-core.js) and is threaded into the arena
// builders and the overview camera below. See docs/adr/0005.

// Battle-car model (createCar / buildCarModel). Inlined from a Claude-designed
// three.js model (battle-car.js): an extruded curved hull + tinted canopy,
// graphite aero (splitter, diffuser, skirts, wing), chrome boost nozzles with
// emissive cores, emissive headlights, and four spoked wheels with body-colour
// fender flares. Authored in its own basis (+X forward, +Y up, +Z lateral;
// ~metres; wheels on y = 0). `buildCarModel` rotates that basis into RL local
// axes (+90° about X: +Y up → +Z up), scales it and drops the wheels onto the
// floor. Only recolour: the paint (hull + flares) takes the team tint; the
// designed graphite / glass / chrome / rubber are kept.
//
// CAR_SCALE is pinned on WIDTH — the wheel track is ~2.15 in model units, so
// 2.15 × 39 ≈ 84 uu ≈ the 84-wide hitbox. Length then lands ~1.1× and roof
// ~1.2× the hitbox — the slight overhang a real RL body has over its box.
const CAR_SCALE = 39;
const CAR_DROP = 17; // uu the wheels sit below the pose origin (grounded car)
const CAR_NOSE_BIAS = 4; // uu forward — rrrocket's origin is a touch aft of hull centre

// Goal frame colour — brighter than the arena edges so the frame reads. The
// goal geometry itself (box mouth/height/depth for soccar, elevated ring for
// hoops) comes from spec.goal; team 0 defends the −y goal, team 1 the +y goal
// (before the field flip).
const GOAL_LINE = 0xaab8d8;

// Boost-pad markers. Layouts (6 big + 28 small soccar, 6 + 14 hoops) come from
// spec.bigPads / spec.smallPads — coords from wiki.rlbot.org, the same source
// as frame_analysis.BIG_PAD_POSITIONS. Drawn as flat discs just above the
// floor: static field furniture, no pickup/respawn state.
const BOOST_PAD_BIG_R = 100; // big pads read at ~2x the small ones
const BOOST_PAD_SMALL_R = 50;
const BOOST_PAD_COLOR = 0x8f8062; // dim grey-gold, deliberately not boost-yellow
const BOOST_PAD_Z = 0.6; // above the floor grid (0.5), below the half tint (1)

const SEEK_STEP = 5; // seconds, for arrow-key seeking
// Name-label placement. The tag is bottom-anchored and screen-constant in size:
// its pill is LABEL_VIEW_FRAC of the viewport height, and its bottom edge sits
// LABEL_CLEAR_UU (world uu, always enough to clear the roof + spoiler) plus
// LABEL_GAP_FRAC of the viewport above the pose origin, so the gap breathes with
// zoom without ever letting the tag drop onto the car.
const LABEL_VIEW_FRAC = 0.025;
const LABEL_CLEAR_UU = 100;
const LABEL_GAP_FRAC = 0.006;

// Goal celebration: a glowy particle burst at the ball's entry point. The goal
// instant is trimmed from the timeline (advance() snaps over the dead span), so
// this is a wall-clock overlay fired on the forward crossing, not a playback
// state. Spread is scaled off the goal mouth so the burst reads about goal-sized.
const GOAL_FX_COUNT = 170;
const GOAL_FX_LIFETIME = 1.3; // s, particle burst
const GOAL_FX_CORE_LIFETIME = 0.32; // s, the central flash sprite
const GOAL_FX_POINT_SIZE = 22; // world uu (sizeAttenuation)
const GOAL_FX_DRAG = 0.06; // velocity retained per second (strong ease-out)
const GOAL_FX_GRAVITY = 900; // uu/s², pulls the up-thrown particles back

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

// The presets above are tuned to the standard footprint. Other arenas scale
// `pos` / `target` / `size` by whichever axis is largest relative to standard's
// same axis, so the field frames the same; `applyCamPreset` applies it. 1 for
// standard.
let camScale = 1;
const STD_SPEC = arenaSpec(null);
const arenaCamScale = (spec) =>
  Math.max(
    spec.halfX / STD_SPEC.halfX,
    (spec.halfY + spec.goalClearance) /
      (STD_SPEC.halfY + STD_SPEC.goalClearance),
  );

// Player name labels start on; the NAMES button / `n` key flip this.
let showNames = true;

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
const namesBtn = document.querySelector('[data-role="names"]');
const scoreEl = document.querySelector('[data-role="score"]');
const marksEl = document.querySelector('[data-role="marks"]');
const countdownEl = document.querySelector('[data-role="countdown"]');

const matchId = location.pathname.split("/").filter(Boolean)[1];
const backEl = document.querySelector('[data-role="back"]');
if (backEl) backEl.href = `/match/${matchId}`;

let renderer;
let camera;

function showMessage(text) {
  if (canvas) canvas.hidden = true;
  if (messageEl) {
    messageEl.textContent = text;
    messageEl.hidden = false;
  }
}

// A camera-facing name tag drawn to a canvas texture. Sprites ignore parent
// rotation, so the text reads normally even inside the flipped field group.
// Rendered at LABEL_SS x its on-screen size so a close orbit stays crisp. The
// world scale is (re)set every frame in applyPoses to stay screen-constant,
// keyed off the texture aspect stashed on userData.
const LABEL_SS = 2; // texture supersample factor
function makeLabelSprite(text, cssColor) {
  const font = `600 ${40 * LABEL_SS}px 'DM Mono', ui-monospace, monospace`;
  const measure = document.createElement("canvas").getContext("2d");
  measure.font = font;
  const padX = 12 * LABEL_SS;
  const w = Math.ceil(measure.measureText(text).width) + padX * 2;
  const h = 56 * LABEL_SS;
  const c = document.createElement("canvas");
  c.width = w;
  c.height = h;
  const ctx = c.getContext("2d");
  const baselineY = h / 2 + 2 * LABEL_SS;
  ctx.font = font;
  ctx.textBaseline = "middle";
  ctx.fillStyle = "rgba(8, 10, 18, 0.82)";
  ctx.fillRect(0, 0, w, h);
  ctx.lineJoin = "round";
  ctx.lineWidth = 4 * LABEL_SS;
  ctx.strokeStyle = "rgba(8, 10, 18, 0.9)"; // carries the tag over a bright wireframe line
  ctx.strokeText(text, padX, baselineY);
  ctx.fillStyle = cssColor;
  ctx.fillText(text, padX, baselineY);

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
  sprite.userData.aspect = w / h; // applyPoses multiplies the pill height by this
  sprite.center.set(0.5, 0); // bottom-anchored, so the tag sits above the car at any size
  sprite.renderOrder = 10;
  return sprite;
}

// A polyline motion tail. Head vertex is the live position; the rest walk
// backward through the position buffer. Colour is baked once, head → background;
// per frame only the vertex positions and draw range change.
function makeTrail(colorHex) {
  const n = TRAIL_POINTS; // must match the core's per-slot trail stride
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

// A hard little dot — solid white core, a thin feathered rim, then a faint
// halo — so a burst reads as sparks rather than a soft cloud.
function makeDotTexture() {
  const s = 64;
  const c = document.createElement("canvas");
  c.width = c.height = s;
  const ctx = c.getContext("2d");
  const g = ctx.createRadialGradient(s / 2, s / 2, 0, s / 2, s / 2, s / 2);
  g.addColorStop(0, "rgba(255,255,255,1)");
  g.addColorStop(0.4, "rgba(255,255,255,1)");
  g.addColorStop(0.55, "rgba(255,255,255,0.35)");
  g.addColorStop(0.8, "rgba(255,255,255,0.08)");
  g.addColorStop(1, "rgba(255,255,255,0)");
  ctx.fillStyle = g;
  ctx.fillRect(0, 0, s, s);
  const tex = new THREE.CanvasTexture(c);
  return tex;
}

// The goal-celebration burst. `trigger(origin, colorHex)` seeds GOAL_FX_COUNT
// additive points at `origin` (field-local, so the team-1 flip is already
// applied) with velocities biased toward the pitch centre and upward, and a
// fixed per-spark brightness for twinkle; `update` integrates them with heavy
// drag + light gravity and fades the whole burst out via material.opacity. A
// larger core sprite sells the flash for the first fraction of a second. Idle
// cost is nil — both objects are hidden.
function createGoalFx(field, fxScale) {
  const dot = makeDotTexture();

  const geo = new THREE.BufferGeometry();
  const pos = new Float32Array(GOAL_FX_COUNT * 3);
  const col = new Float32Array(GOAL_FX_COUNT * 3);
  geo.setAttribute("position", new THREE.BufferAttribute(pos, 3));
  geo.setAttribute("color", new THREE.BufferAttribute(col, 3));
  const points = new THREE.Points(
    geo,
    new THREE.PointsMaterial({
      size: GOAL_FX_POINT_SIZE,
      map: dot,
      vertexColors: true,
      transparent: true,
      depthWrite: false,
      blending: THREE.AdditiveBlending,
      sizeAttenuation: true,
    }),
  );
  points.frustumCulled = false;
  points.visible = false;
  field.add(points);

  const core = new THREE.Sprite(
    new THREE.SpriteMaterial({
      map: dot,
      transparent: true,
      depthWrite: false,
      depthTest: false,
      blending: THREE.AdditiveBlending,
      opacity: 0,
    }),
  );
  core.visible = false;
  field.add(core);

  const vel = new Float32Array(GOAL_FX_COUNT * 3);
  const bright = new Float32Array(GOAL_FX_COUNT); // per-spark brightness, for twinkle
  const base = new THREE.Color();
  let age = Infinity; // ≥ lifetime ⇒ inactive

  function trigger(origin, colorHex) {
    base.set(colorHex);
    const gy = Math.sign(origin.y) || 1; // toward-pitch is −gy on the goal axis
    for (let i = 0; i < GOAL_FX_COUNT; i++) {
      pos[i * 3] = origin.x;
      pos[i * 3 + 1] = origin.y;
      pos[i * 3 + 2] = origin.z;
      // random direction, biased into the pitch and upward
      const dx = Math.random() * 2 - 1;
      const dy = Math.random() * 2 - 1 - gy * 0.55;
      const dz = Math.abs(Math.random() * 2 - 1) * 0.7 + 0.45;
      const len = Math.hypot(dx, dy, dz) || 1;
      // rand² weighting ⇒ more slow particles ⇒ density packed toward the centre
      const speed = 380 + 2400 * Math.random() ** 2;
      vel[i * 3] = (dx / len) * speed;
      vel[i * 3 + 1] = (dy / len) * speed;
      vel[i * 3 + 2] = (dz / len) * speed;
      bright[i] = 0.55 + Math.random() * 0.9; // some sparks pop brighter (>1 under additive)
      col[i * 3] = base.r * bright[i];
      col[i * 3 + 1] = base.g * bright[i];
      col[i * 3 + 2] = base.b * bright[i];
    }
    geo.attributes.position.needsUpdate = true;
    geo.attributes.color.needsUpdate = true;
    points.material.opacity = 1;
    points.visible = true;

    core.position.copy(origin);
    core.scale.setScalar(fxScale * 0.6);
    core.material.color.set(colorHex);
    core.material.opacity = 1;
    core.visible = true;
    age = 0;
  }

  function update(dt) {
    if (age >= GOAL_FX_LIFETIME) return;
    age += dt;
    const dragT = Math.pow(GOAL_FX_DRAG, dt);
    const gdt = GOAL_FX_GRAVITY * dt;
    for (let i = 0; i < GOAL_FX_COUNT; i++) {
      vel[i * 3] *= dragT;
      vel[i * 3 + 1] *= dragT;
      vel[i * 3 + 2] = vel[i * 3 + 2] * dragT - gdt;
      pos[i * 3] += vel[i * 3] * dt;
      pos[i * 3 + 1] += vel[i * 3 + 1] * dt;
      pos[i * 3 + 2] += vel[i * 3 + 2] * dt;
    }
    geo.attributes.position.needsUpdate = true;

    // fade the whole burst out (per-spark brightness is baked into the colours)
    const k = Math.max(0, 1 - age / GOAL_FX_LIFETIME);
    points.material.opacity = k * k;

    const ck = Math.max(0, 1 - age / GOAL_FX_CORE_LIFETIME);
    core.material.opacity = ck * ck;
    core.visible = ck > 0;

    if (age >= GOAL_FX_LIFETIME) points.visible = false;
  }

  return { trigger, update };
}

// Fires goalFx once per goal on the forward crossing of its frame time. The
// `prevT < gt && t >= gt` test is already single-shot — `prevT` is set to `t`
// every call, so a fired goal can only cross again if the clock is taken back
// before it (scrub/rewatch), which re-arms it for free.
function makeGoalWatcher(meta, positions, playback, goalFx) {
  const slotCount = meta.slots.length;
  const ballSlot = meta.slots.findIndex((s) => s.kind === "ball");
  const goalTimes = meta.goals.map((g) => meta.frame_times[g.frame]);
  const origin = new THREE.Vector3();
  let prevT = playback.state.t;
  return function watchGoals() {
    const t = playback.state.t;
    if (ballSlot >= 0 && playback.state.playing && t > prevT) {
      for (let gi = 0; gi < goalTimes.length; gi++) {
        const gt = goalTimes[gi];
        if (prevT < gt && t >= gt) {
          const o = poseOffset(slotCount, meta.goals[gi].frame, ballSlot);
          origin.set(positions[o], positions[o + 1], positions[o + 2]);
          goalFx.trigger(origin, teamTint(meta.goals[gi].team, meta.tracked_team));
        }
      }
    }
    prevT = t;
  };
}

function buildArena(parent, spec) {
  const edgeMat = new THREE.LineBasicMaterial({ color: 0x4a5d82 });
  const { halfX: hx, halfY: hy, ceiling } = spec;

  // Chamfered octagon: floor loop (z = 0), ceiling loop (z = ceiling), and a
  // vertical edge at each of the eight corners.
  const floor = spec.outline.map(([x, y]) => new THREE.Vector3(x, y, 0));
  const ceil = spec.outline.map(([x, y]) => new THREE.Vector3(x, y, ceiling));
  parent.add(
    new THREE.LineLoop(new THREE.BufferGeometry().setFromPoints(floor), edgeMat),
  );
  parent.add(
    new THREE.LineLoop(new THREE.BufferGeometry().setFromPoints(ceil), edgeMat),
  );
  const verticals = [];
  for (const [x, y] of spec.outline) {
    verticals.push(new THREE.Vector3(x, y, 0), new THREE.Vector3(x, y, ceiling));
  }
  parent.add(
    new THREE.LineSegments(
      new THREE.BufferGeometry().setFromPoints(verticals),
      edgeMat,
    ),
  );

  // Floor guide: an interior grid — spec.grid.cols wall to wall, spec.grid.rows
  // goal to goal — the in-game reference for rotation spacing / avoiding double
  // commits (soccar 3×4; hoops a lighter 2×2). Interior dividers only (the
  // arena outline is the perimeter), clipped to the octagon so nothing
  // overhangs. The brighter centre line is drawn separately below.
  const gridPts = [];
  for (let i = 1; i < spec.grid.cols; i++) {
    const x = -hx + i * ((2 * hx) / spec.grid.cols);
    const lim = outlineHalfWidth(x, 0, spec).y;
    gridPts.push(new THREE.Vector3(x, -lim, 0.5), new THREE.Vector3(x, lim, 0.5));
  }
  for (let i = 1; i < spec.grid.rows; i++) {
    const y = -hy + i * ((2 * hy) / spec.grid.rows);
    const lim = outlineHalfWidth(0, y, spec).x;
    gridPts.push(new THREE.Vector3(-lim, y, 0.5), new THREE.Vector3(lim, y, 0.5));
  }
  if (gridPts.length) {
    parent.add(
      new THREE.LineSegments(
        new THREE.BufferGeometry().setFromPoints(gridPts),
        new THREE.LineBasicMaterial({ color: 0x1e2842 }),
      ),
    );
  }

  // Centre line (wall to wall at y = 0, just above the floor).
  parent.add(
    new THREE.LineSegments(
      new THREE.BufferGeometry().setFromPoints([
        new THREE.Vector3(-hx, 0, 2),
        new THREE.Vector3(hx, 0, 2),
      ]),
      edgeMat,
    ),
  );
}

// An open wireframe box: goal mouth on the back wall (y = gy), matching frame
// at full depth (y = by), four edges joining them.
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

function buildGoals(parent, spec, trackedTeam) {
  for (const sign of [-1, 1]) {
    const team = sign < 0 ? 0 : 1; // team 0 defends −y, team 1 defends +y
    const tint = teamTint(team, trackedTeam);
    if (spec.goal.kind === "ring") {
      buildHoopGoal(parent, spec, sign, tint);
    } else {
      buildBoxGoal(parent, spec, sign, tint);
    }
  }
}

// Soccar: an open wireframe box on the back wall, its mouth filled with a
// translucent plane in the defending team's colour — the clearest "which end
// is whose" cue.
function buildBoxGoal(parent, spec, sign, tint) {
  const { halfWidth, height } = spec.goal;
  const gy = sign * spec.halfY;
  const by = sign * (spec.halfY + spec.goal.depth);

  parent.add(
    new THREE.LineSegments(
      new THREE.BufferGeometry().setFromPoints(
        goalFrameSegments(gy, by, halfWidth, height),
      ),
      new THREE.LineBasicMaterial({ color: GOAL_LINE }),
    ),
  );

  const fill = new THREE.Mesh(
    new THREE.PlaneGeometry(2 * halfWidth, height),
    new THREE.MeshBasicMaterial({
      color: tint,
      transparent: true,
      opacity: 0.35,
      depthWrite: false,
      side: THREE.DoubleSide,
    }),
  );
  fill.rotation.x = Math.PI / 2; // stand it up in the x–z plane
  fill.position.set(0, gy - sign * 2, height / 2); // inset 2 uu off the wall
  parent.add(fill);
}

// Hoops: a horizontal semicircle rim (curved side facing the pitch) at rim
// height, a chord across its flat side with short arms back to the wall so it
// reads as wall-mounted, and a shallow basket sweep below. Outline only, all in
// the defending team's tint — nothing spans the opening, since cars pass under
// it constantly (ADR-0005).
function buildHoopGoal(parent, spec, sign, tint) {
  const { centreY, z, radius, basketDrop, basketInset, basketRadius } = spec.goal;
  const cy = sign * centreY;
  const wallY = sign * spec.halfY;
  // Arc from the wall-facing chord (angle 0 / π at y = cy) bulging toward the
  // pitch (−sign in y).
  const arc = (r, y0, z0, n) => {
    const pts = [];
    for (let i = 0; i <= n; i++) {
      const a = Math.PI * (i / n);
      pts.push(new THREE.Vector3(Math.cos(a) * r, y0 - sign * Math.sin(a) * r, z0));
    }
    return pts;
  };

  const rimMat = new THREE.LineBasicMaterial({ color: tint });
  const softMat = new THREE.LineBasicMaterial({
    color: tint,
    transparent: true,
    opacity: 0.5,
  });

  // Rim + its chord.
  parent.add(
    new THREE.Line(
      new THREE.BufferGeometry().setFromPoints(arc(radius, cy, z, 32)),
      rimMat,
    ),
  );
  parent.add(
    new THREE.LineSegments(
      new THREE.BufferGeometry().setFromPoints([
        new THREE.Vector3(-radius, cy, z),
        new THREE.Vector3(radius, cy, z),
      ]),
      rimMat,
    ),
  );

  // Short arms from the chord ends back to the wall.
  parent.add(
    new THREE.LineSegments(
      new THREE.BufferGeometry().setFromPoints([
        new THREE.Vector3(-radius, cy, z), new THREE.Vector3(-radius, wallY, z),
        new THREE.Vector3(radius, cy, z), new THREE.Vector3(radius, wallY, z),
      ]),
      softMat,
    ),
  );

  // Basket: a smaller arc dropped below and pulled toward the wall, tied to the
  // rim by a few struts.
  const bz = z - basketDrop;
  const by = cy + sign * basketInset;
  const rimArc = arc(radius, cy, z, 8);
  const basketArc = arc(basketRadius, by, bz, 8);
  parent.add(
    new THREE.Line(
      new THREE.BufferGeometry().setFromPoints(arc(basketRadius, by, bz, 24)),
      softMat,
    ),
  );
  const struts = [];
  for (let i = 0; i < rimArc.length; i += 2) {
    struts.push(rimArc[i], basketArc[i]);
  }
  parent.add(
    new THREE.LineSegments(
      new THREE.BufferGeometry().setFromPoints(struts),
      softMat,
    ),
  );
}

// A faint colour wash over each half of the pitch, in the defending team's
// colour, so the two ends read at a glance (and from straight overhead, where
// the vertical goal fills are edge-on). Same chamfered footprint as the arena,
// split at y = 0.
function buildHalfTint(parent, spec, trackedTeam) {
  // Each half is a closer at y = 0 plus the arena outline's +y or −y run
  // (chamferedOutline is CCW from +x/+y, so 0–3 is the +y half, 4–7 the −y).
  const hx = spec.halfX;
  const o = spec.outline;
  const halves = [
    { team: 1, ring: [[hx, 0], ...o.slice(0, 4), [-hx, 0]] },
    { team: 0, ring: [[-hx, 0], ...o.slice(4, 8), [hx, 0]] },
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
function buildBoostPads(parent, spec) {
  const mat = new THREE.MeshBasicMaterial({
    color: BOOST_PAD_COLOR,
    transparent: true,
    opacity: 0.5,
    depthWrite: false,
    side: THREE.DoubleSide,
  });
  for (const [pads, r] of [
    [spec.bigPads, BOOST_PAD_BIG_R],
    [spec.smallPads, BOOST_PAD_SMALL_R],
  ]) {
    const geo = new THREE.CircleGeometry(r, 24); // in the x–y plane, faces +z
    for (const [x, y] of pads) {
      const disc = new THREE.Mesh(geo, mat);
      disc.position.set(x, y, BOOST_PAD_Z);
      parent.add(disc);
    }
  }
}

// ── Battle-car model ────────────────────────────────────────────────────
// Inlined from battle-car.js (a Claude-designed three.js model), kept close to
// the source so a re-export stays easy to diff. Changes from the source: the
// `<three-d-stage>` harness and trailing `car.rotation.y` / `stage.setObject`
// are gone (buildCarModel does the orientation), `paint` is the team tint,
// and the no-op shadow flags / dead spoke `holder` group are dropped. Own
// basis: +X forward, +Y up, +Z lateral; wheels on y = 0. `buildScene` bakes a
// PMREM environment so the standard materials don't render dark.
function createCar(bodyColor) {
  const M = {
    paint: new THREE.MeshStandardMaterial({ color: bodyColor, roughness: 0.32, metalness: 0.25 }),
    trim: new THREE.MeshStandardMaterial({ color: 0x2a2d33, roughness: 0.55, metalness: 0.2 }),
    chrome: new THREE.MeshStandardMaterial({ color: 0xc7ccd4, roughness: 0.22, metalness: 0.38 }),
    glass: new THREE.MeshStandardMaterial({ color: 0x22303c, roughness: 0.1, metalness: 0.3, transparent: true, opacity: 0.68 }),
    rubber: new THREE.MeshStandardMaterial({ color: 0x16171a, roughness: 0.9, metalness: 0.0 }),
    glow: new THREE.MeshStandardMaterial({ color: 0xff8a3c, emissive: 0xff6a12, emissiveIntensity: 1.4, roughness: 0.4 }),
    lamp: new THREE.MeshStandardMaterial({ color: 0xfff2d8, emissive: 0xffd9a0, emissiveIntensity: 0.5, roughness: 0.3 }),
  };

  const car = new THREE.Group();
  car.name = "battle_car";

  const mesh = (geo, mat, name) => {
    const m = new THREE.Mesh(geo, mat);
    m.name = name;
    return m;
  };

  // ---- main body: side profile extruded across the width ----
  const bodyShape = new THREE.Shape();
  bodyShape.moveTo(1.50, 0.40);
  bodyShape.quadraticCurveTo(1.56, 0.22, 1.34, 0.18);
  bodyShape.lineTo(-1.34, 0.18);
  bodyShape.quadraticCurveTo(-1.56, 0.18, -1.54, 0.42);
  bodyShape.lineTo(-1.52, 0.60);
  bodyShape.quadraticCurveTo(-1.52, 0.72, -1.34, 0.71);
  bodyShape.lineTo(0.30, 0.67);
  bodyShape.quadraticCurveTo(0.95, 0.62, 1.30, 0.50);
  bodyShape.quadraticCurveTo(1.50, 0.46, 1.50, 0.40);
  const bodyGeo = new THREE.ExtrudeGeometry(bodyShape, {
    depth: 1.30, bevelEnabled: true, bevelThickness: 0.07, bevelSize: 0.07, bevelSegments: 4, curveSegments: 24,
  });
  bodyGeo.translate(0, 0, -0.65);
  car.add(mesh(bodyGeo, M.paint, "hull"));

  // ---- canopy ----
  const canopyShape = new THREE.Shape();
  canopyShape.moveTo(0.54, 0.62);
  canopyShape.quadraticCurveTo(0.30, 0.99, 0.10, 1.00);
  canopyShape.lineTo(-0.78, 1.00);
  canopyShape.quadraticCurveTo(-1.02, 0.98, -1.06, 0.62);
  canopyShape.lineTo(0.54, 0.62);
  const canopyGeo = new THREE.ExtrudeGeometry(canopyShape, {
    depth: 1.10, bevelEnabled: true, bevelThickness: 0.05, bevelSize: 0.05, bevelSegments: 3, curveSegments: 18,
  });
  canopyGeo.translate(0, 0, -0.55);
  car.add(mesh(canopyGeo, M.glass, "canopy"));

  // ---- aero / trim ----
  const splitter = mesh(new THREE.BoxGeometry(0.40, 0.06, 1.56), M.trim, "front_splitter");
  splitter.position.set(1.40, 0.165, 0);
  car.add(splitter);

  const diffuser = mesh(new THREE.BoxGeometry(0.46, 0.14, 1.34), M.trim, "rear_diffuser");
  diffuser.position.set(-1.42, 0.24, 0);
  car.add(diffuser);

  for (const s of [-1, 1]) {
    const side = s > 0 ? "l" : "r";

    const skirt = mesh(new THREE.BoxGeometry(1.90, 0.11, 0.10), M.trim, `side_skirt_${side}`);
    skirt.position.set(-0.05, 0.215, s * 0.70);
    car.add(skirt);

    const strut = mesh(new THREE.BoxGeometry(0.10, 0.30, 0.07), M.trim, `wing_strut_${side}`);
    strut.position.set(-1.36, 0.84, s * 0.46);
    car.add(strut);

    const nozzle = mesh(new THREE.CylinderGeometry(0.14, 0.17, 0.30, 28, 1, true), M.chrome, `boost_nozzle_${side}`);
    nozzle.rotation.z = Math.PI / 2;
    nozzle.position.set(-1.70, 0.44, s * 0.34);
    car.add(nozzle);

    const core = mesh(new THREE.CircleGeometry(0.135, 28), M.glow, `boost_core_${side}`);
    core.rotation.y = -Math.PI / 2;
    core.position.set(-1.74, 0.44, s * 0.34);
    car.add(core);

    const lamp = mesh(new THREE.BoxGeometry(0.06, 0.10, 0.30), M.lamp, `headlight_${side}`);
    lamp.position.set(1.545, 0.44, s * 0.42);
    car.add(lamp);

    const vent = mesh(new THREE.BoxGeometry(0.44, 0.07, 0.20), M.trim, `hood_vent_${side}`);
    vent.position.set(0.72, 0.685, s * 0.28);
    vent.rotation.z = -0.04;
    car.add(vent);
  }

  const wing = mesh(new THREE.BoxGeometry(0.34, 0.055, 1.24), M.trim, "rear_wing");
  wing.position.set(-1.40, 1.00, 0);
  wing.rotation.z = 0.10;
  car.add(wing);

  const intake = mesh(new THREE.BoxGeometry(0.40, 0.12, 0.52), M.chrome, "roof_intake");
  intake.position.set(-0.30, 1.02, 0);
  car.add(intake);

  // ---- wheels ----
  const wheel = (radius, width, name) => {
    const g = new THREE.Group();
    g.name = name;

    const tire = mesh(new THREE.CylinderGeometry(radius, radius, width, 40), M.rubber, `${name}_tire`);
    tire.rotation.x = Math.PI / 2;
    g.add(tire);

    const rim = mesh(new THREE.CylinderGeometry(radius * 0.58, radius * 0.58, width + 0.012, 32), M.chrome, `${name}_rim`);
    rim.rotation.x = Math.PI / 2;
    g.add(rim);

    for (let i = 0; i < 6; i++) {
      const spoke = mesh(new THREE.BoxGeometry(radius * 1.02, 0.05, 0.035), M.chrome, `${name}_spoke_${i}`);
      spoke.geometry.translate(radius * 0.28, 0, 0);
      spoke.position.z = width / 2 + 0.015;
      spoke.rotation.z = (i / 6) * Math.PI * 2;
      g.add(spoke);
    }
    return g;
  };

  const wheelSpec = [
    ["wheel_fl", 1.02, 0.86, 0.40, 0.34],
    ["wheel_fr", 1.02, -0.86, 0.40, 0.34],
    ["wheel_rl", -1.04, 0.88, 0.44, 0.38],
    ["wheel_rr", -1.04, -0.88, 0.44, 0.38],
  ];
  for (const [name, x, z, r, w] of wheelSpec) {
    const g = wheel(r, w, name);
    g.position.set(x, r, z);
    car.add(g);
  }

  // fender flares over each wheel
  for (const [name, x, z, r] of wheelSpec) {
    const flare = mesh(new THREE.TorusGeometry(r + 0.05, 0.075, 12, 26, Math.PI), M.paint, name.replace("wheel", "flare"));
    flare.position.set(x, r, z > 0 ? 0.70 : -0.70);
    car.add(flare);
  }

  return car;
}

// Wrap createCar in a group whose local axes are RL's (X fwd, Y left, Z up).
// The model is authored +X forward / +Y up / +Z lateral, so a +90° turn about
// X drops +Y onto RL's +Z (up) and leaves the nose on +X. The model is
// mirror-symmetric, so the L/R suffixes in the part names may come out swapped
// — invisible. Then scale to the hitbox and drop the wheels CAR_DROP below the
// pose origin, nudged slightly forward.
function buildCarModel(bodyColor) {
  const group = new THREE.Group();
  const model = createCar(bodyColor);
  model.scale.setScalar(CAR_SCALE);
  model.rotation.x = Math.PI / 2;
  model.position.set(CAR_NOSE_BIAS, 0, -CAR_DROP);
  group.add(model);
  return group;
}

const BALL_COLOR = 0xeceef2;

// A smooth shaded ball. Its rotation (the poses carry the real RigidBody
// quaternion) reads off the charcoal great-circle seam sweeping and the single
// pole pip dropping in and out of view — a bare great circle is symmetric about
// its own axis, so spin around it would otherwise be invisible. Seam/pip are
// children of the mesh, so they inherit its per-frame pose.
function buildBall(color, radius) {
  const ball = new THREE.Mesh(
    new THREE.SphereGeometry(radius, 32, 24),
    new THREE.MeshStandardMaterial({ color, roughness: 0.5, metalness: 0 }),
  );

  const ink = new THREE.MeshStandardMaterial({ color: 0x1a1c22, roughness: 0.6, metalness: 0 });
  const seam = new THREE.Mesh(
    new THREE.TorusGeometry(radius * 1.005, 2.5, 6, 40),
    ink,
  );
  ball.add(seam); // ring in the mesh's local XY plane, axis = local Z

  const pip = new THREE.Mesh(new THREE.SphereGeometry(4.5, 12, 8), ink);
  pip.position.set(0, 0, radius); // on the seam's axis
  ball.add(pip);

  return ball;
}

function createActorMeshes(field, meta, spec) {
  return meta.slots.map((slot) => {
    const isBall = slot.kind === "ball";
    const color = isBall ? BALL_COLOR : carColor(slot, meta.tracked_team);

    const obj = isBall
      ? buildBall(color, spec.ballRadius)
      : buildCarModel(color);
    field.add(obj);

    const trail = makeTrail(color);
    obj.userData.trail = trail;
    field.add(trail);

    if (!isBall) {
      const label = makeLabelSprite(
        slot.name,
        "#" + color.toString(16).padStart(6, "0"),
      );
      obj.userData.label = label;
      field.add(label);
    }
    return obj;
  });
}

// The playback clock + per-frame pose application, over one match's data. All
// the interpolation and timeline math is in replay-core.js: writePoses() fills a
// scratch buffer, createTransport() maps real <-> compressed seconds. applyPoses()
// here does nothing but copy that buffer onto the THREE meshes and size labels.
function createPlayback(meta, positions, meshes) {
  const slotCount = meta.slots.length;
  const transport = createTransport(meta);
  const { tN, compressedEnd } = transport;
  const pose = makePoseBuffers(slotCount);

  const state = { t: transport.tStart, playing: false, speed: 1 };

  function applyPoses() {
    writePoses(meta, positions, state.t, pose);
    // Screen-constant label sizing — camera-only, so hoisted out of the per-slot loop.
    const frustumH = (camera.top - camera.bottom) / camera.zoom;
    const pillH = LABEL_VIEW_FRAC * frustumH;
    const labelBase = LABEL_CLEAR_UU + LABEL_GAP_FRAC * frustumH;
    for (let s = 0; s < meshes.length; s++) {
      const mesh = meshes[s];
      const label = mesh.userData.label;
      const trail = mesh.userData.trail;
      if (!pose.visible[s]) {
        mesh.visible = false;
        if (label) label.visible = false;
        trail.visible = false;
        continue;
      }
      mesh.visible = true;
      mesh.position.set(
        pose.position[s * 3],
        pose.position[s * 3 + 1],
        pose.position[s * 3 + 2],
      );
      mesh.quaternion.set(
        pose.quaternion[s * 4],
        pose.quaternion[s * 4 + 1],
        pose.quaternion[s * 4 + 2],
        pose.quaternion[s * 4 + 3],
      );

      if (label) {
        label.visible = showNames;
        label.scale.set(pillH * label.userData.aspect, pillH, 1);
        label.position.set(
          mesh.position.x,
          mesh.position.y,
          mesh.position.z + labelBase,
        );
      }

      const count = pose.trailCount[s];
      const tp = trail.geometry.attributes.position.array;
      tp.set(
        pose.trail.subarray(s * TRAIL_POINTS * 3, s * TRAIL_POINTS * 3 + count * 3),
      );
      trail.visible = count > 1;
      trail.geometry.setDrawRange(0, count);
      trail.geometry.attributes.position.needsUpdate = true;
    }
  }

  // Transport is compressed-axis: `seek`/`nudge` take compressed seconds, and
  // `toReal` already resolves a seam to the resume frame so seeks never land in
  // a gap. Only `advance` steps raw real time, so only it needs the snap.
  function seek(c) {
    state.t = transport.toReal(Math.min(compressedEnd, Math.max(0, c)));
  }

  function nudge(deltaCompressed) {
    seek(transport.toCompressed(state.t) + deltaCompressed);
  }

  function advance(dt) {
    if (!state.playing) return;
    let t = transport.snapForward(state.t + dt * state.speed);
    if (t >= tN) {
      t = tN;
      state.playing = false;
    }
    state.t = t;
  }

  return {
    state,
    tN,
    applyPoses,
    seek,
    nudge,
    advance,
    fractionAt: transport.fractionAt,
    elapsed: () => transport.toCompressed(state.t),
    duration: () => compressedEnd,
    progress: () => transport.fractionAt(state.t),
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

  function setNames(on) {
    showNames = on;
    if (namesBtn) {
      namesBtn.classList.toggle("is-active", on);
      namesBtn.setAttribute("aria-pressed", String(on));
    }
  }
  if (namesBtn) namesBtn.addEventListener("click", () => setNames(!showNames));

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
    } else if (e.key === "n" || e.key === "N") {
      setNames(!showNames);
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

  const spec = arenaSpec(meta.game_mode);
  camScale = arenaCamScale(spec);

  buildArena(field, spec);
  buildHalfTint(field, spec, meta.tracked_team);
  buildBoostPads(field, spec);
  buildGoals(field, spec, meta.tracked_team);
  const meshes = createActorMeshes(field, meta, spec);
  const goalFx = createGoalFx(field, spec.goal.fxScale);

  renderer = new THREE.WebGLRenderer({ canvas, antialias: true });
  renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));

  // Image-based lighting for the MeshStandardMaterial cars and ball — without an
  // environment, metalness renders near-black. A one-off PMREM bake of three's
  // stock studio room; the line arena ignores `scene.environment`.
  const pmrem = new THREE.PMREMGenerator(renderer);
  scene.environment = pmrem.fromScene(new RoomEnvironment(), 0.04).texture;
  scene.environmentIntensity = 0.6; // studio room is bright; dial it into our dark scene
  pmrem.dispose();

  camera = new THREE.OrthographicCamera(-1, 1, 1, -1, 0.1, 200000);

  const controls = new OrbitControls(camera, renderer.domElement);
  controls.enableDamping = true;

  applyCamPreset("broadcast", controls);
  wireCamera(controls);
  window.addEventListener("resize", resize);

  const playback = createPlayback(meta, positions, meshes);
  const watchGoals = makeGoalWatcher(meta, positions, playback, goalFx);
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
      goalFx,
    };
  }

  let lastNow = null;
  function frameLoop(now) {
    requestAnimationFrame(frameLoop);
    const dt = lastNow == null ? 0 : (now - lastNow) / 1000;
    lastNow = now;
    // Order matters: watchGoals() reads state.t after advance(); goalFx.update()
    // must run after watchGoals() or a fresh burst loses its first frame.
    playback.advance(dt);
    watchGoals();
    playback.applyPoses();
    goalFx.update(dt);
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
  camera.up.set(...p.up); // a direction — never scaled
  camera.position.set(...p.pos.map((v) => v * camScale));
  controls.target.set(...p.target.map((v) => v * camScale));
  viewSize = p.size * camScale;
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
