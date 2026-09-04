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
  boostColor,
  boostPadStateAt,
  buildBoostPadTimeline,
  carColor,
  countdownLabelAt,
  createTransport,
  decodeReplayEnvelope,
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

// Boost pads. Layouts (6 big + 28 small soccar, 6 + 14 hoops) come from
// spec.bigPads / spec.smallPads — coords from wiki.rlbot.org, the same source
// as frame_analysis.BIG_PAD_POSITIONS. Drawn like the in-game pickup: a small
// amber orb (core sphere + additive halo) hovering over the pad spot.
// createBoostPads() hides each while it is collected and pops it back on
// respawn, driven by meta.boost_pads.
const BOOST_ORB_BIG_R = 26; // core sphere radius; big pads read a touch larger
const BOOST_ORB_SMALL_R = 17;
const BOOST_ORB_Z = 55; // uu the orb floats above the floor
const BOOST_ORB_HALO = 6; // additive glow sprite scale, ×core radius
const BOOST_ORB_CORE = 0xffd36b; // warm gold, brighter than the arena
const BOOST_ORB_GLOW = 0xffab3d; // amber halo
const BOOST_PAD_SNAP_MAX = 500; // uu — reject a pad-index → orb match farther than this
const BOOST_ORB_POP = 0.15; // s — a respawned orb scales up over this window
const BOOST_ORB_POP_MIN = 0.35; // scale it starts the pop-in at

const SEEK_STEP = 5; // seconds, for arrow-key seeking
// Name-label placement. The tag is bottom-anchored and screen-constant in size:
// its pill is LABEL_VIEW_FRAC of the viewport height, and its bottom edge sits
// LABEL_CLEAR_UU (world uu, always enough to clear the roof + spoiler) plus
// LABEL_GAP_FRAC of the viewport above the pose origin, so the gap breathes with
// zoom without ever letting the tag drop onto the car.
const LABEL_VIEW_FRAC = 0.025;
const LABEL_CLEAR_UU = 100;
const LABEL_GAP_FRAC = 0.006;

// Boost-bar placement: same width as the nameplate above it (label.userData.aspect
// * pillH), a thin height, sitting BOOST_BAR_GAP_FRAC of the viewport below the
// label's bottom edge. Track + fill are left-anchored sprites (center (0,1),
// top-left) sharing one anchor point, so the fill grows rightward from a fixed
// edge as boost rises rather than shrinking about the centre.
const BOOST_BAR_HEIGHT_FRAC = 0.006;
const BOOST_BAR_GAP_FRAC = 0; // flush against the label — reads as one card
const BOOST_BAR_TRACK_COLOR = 0x14161f;
const BOOST_BAR_TRACK_OPACITY = 0.55;
const BOOST_BAR_TRACK_TINT = 0.35; // how far the track leans toward the team colour

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

// The presets above are tuned to the standard footprint. Other arenas scale
// `pos` / `target` / `size` by whichever axis is largest relative to standard's
// same axis, so the field frames the same; `createCameraRig`'s `applyPreset`
// applies it. 1 for standard.
const STD_SPEC = arenaSpec(null);
const arenaCamScale = (spec) =>
  Math.max(
    spec.halfX / STD_SPEC.halfX,
    (spec.halfY + spec.goalClearance) /
      (STD_SPEC.halfY + STD_SPEC.goalClearance),
  );

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
const boostBtn = document.querySelector('[data-role="boost"]');
const scoreEl = document.querySelector('[data-role="score"]');
const marksEl = document.querySelector('[data-role="marks"]');
const countdownEl = document.querySelector('[data-role="countdown"]');

const matchId = location.pathname.split("/").filter(Boolean)[1];
const backEl = document.querySelector('[data-role="back"]');
if (backEl) backEl.href = `/match/${matchId}`;

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

// A live boost meter: a fixed-width track, faintly tinted toward the car's own
// team colour, plus a fill on top graded red/amber/green by level (boostColor,
// replay-core.js) — both solid-colour sprites (no canvas texture — nothing to
// redraw as the value changes every frame). Both are left-top anchored and,
// each frame, given the same world-space anchor point (applyPoses) so the
// fill's left edge stays flush with the track's regardless of camera orbit —
// only its width (and the fill's colour) changes. The track's colour is fixed
// at construction (the team tint never changes mid-match); the fill's is reset
// every frame in applyPoses.
function makeBoostBarSprites(teamColor) {
  const trackColor = new THREE.Color(BOOST_BAR_TRACK_COLOR).lerp(
    new THREE.Color(teamColor),
    BOOST_BAR_TRACK_TINT,
  );
  const track = new THREE.Sprite(
    new THREE.SpriteMaterial({
      color: trackColor,
      transparent: true,
      opacity: BOOST_BAR_TRACK_OPACITY,
      depthTest: false,
      depthWrite: false,
    }),
  );
  const fill = new THREE.Sprite(
    new THREE.SpriteMaterial({
      depthTest: false,
      depthWrite: false,
    }),
  );
  track.center.set(0, 1);
  fill.center.set(0, 1);
  track.renderOrder = 10;
  fill.renderOrder = 11; // over the track
  return { track, fill };
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

// Hoops: a U-shaped rim (semicircle bulging toward the pitch, radius 655 at
// centre y ±2969) with its two ends running straight back to the back wall, and
// a mesh net filling that whole D-shaped footprint — draping down at the front
// curve and running all the way back to the wall. Outline only, all in the
// defending team's tint — nothing spans the opening, since cars pass under it
// constantly (ADR-0005).
function buildHoopGoal(parent, spec, sign, tint) {
  const { centreY, z, radius: r, netDrop } = spec.goal;
  const cy = sign * centreY;
  const wallY = sign * spec.halfY;

  // The rim/net footprint is an *open* "D": +x back corner on the wall → +x side
  // run → +x U-tip → semicircle front (bulging toward the pitch, −sign in y) →
  // −x U-tip → −x side run → −x back corner on the wall. It is not closed along
  // the wall — the net has no mesh on the face against the back wall. `rr` is the
  // footprint radius, which grows toward the floor so the net bells out like the
  // real one. `dPath` returns the outline as a dense polyline; `resample` walks
  // it by arc length so N points land evenly along the front curve and sides.
  const flareR = (t) => r * (1 + 0.26 * t ** 1.4); // rim width → ~1.26× at the floor
  const dPath = (zed, rr = r) => {
    const pts = [
      new THREE.Vector3(rr, wallY, zed),
      new THREE.Vector3(rr, cy, zed),
    ];
    for (let i = 1; i < 48; i++) {
      const a = Math.PI * (i / 48);
      pts.push(
        new THREE.Vector3(Math.cos(a) * rr, cy - sign * Math.sin(a) * rr, zed),
      );
    }
    pts.push(
      new THREE.Vector3(-rr, cy, zed),
      new THREE.Vector3(-rr, wallY, zed),
    );
    return pts;
  };
  // `n` points evenly spaced by arc length along the *open* path, both endpoints
  // included — so the strand at each wall-side corner matches. `target` only
  // grows, so a single forward cursor walks the segment table.
  const resample = (path, n) => {
    const acc = [0];
    for (let i = 1; i < path.length; i++) {
      acc.push(acc[i - 1] + path[i].distanceTo(path[i - 1]));
    }
    const total = acc[acc.length - 1];
    const out = [];
    let si = 0;
    for (let k = 0; k < n; k++) {
      const target = (total * k) / (n - 1);
      while (si < acc.length - 2 && acc[si + 1] < target) si++;
      const span = acc[si + 1] - acc[si];
      const f = span > 0 ? (target - acc[si]) / span : 0;
      out.push(path[si].clone().lerp(path[si + 1], f));
    }
    return out;
  };

  // BANDS + 1 horizontal slices from the rim (t = 0) to the floor (t = 1): the
  // raw D-outline polyline at each, plus its arc-length resampling for the
  // strands. Each level's `dPath` is built once and shared by the rim tube, the
  // strands and the per-slice outline.
  const BANDS = 7;
  const levels = Array.from({ length: BANDS + 1 }, (_, b) => {
    const t = b / BANDS;
    const raw = dPath(z - netDrop * t, flareR(t));
    return { t, raw, ring: resample(raw, 36) };
  });

  // Rim: the top D-outline drawn as a thin tube (WebGL ignores line width) — a
  // little heft on the one part that marks the goal.
  parent.add(
    new THREE.Mesh(
      new THREE.TubeGeometry(
        new THREE.CatmullRomCurve3(levels[0].raw, false, "centripetal"),
        96,
        11,
        8,
        false,
      ),
      new THREE.MeshBasicMaterial({ color: tint }),
    ),
  );

  // Net: a mesh along the front curve and both sides (nothing across the wall
  // face), fading from ~0.4 opacity at the rim to ~0.07 at the floor.
  const netMat = (t) =>
    new THREE.LineBasicMaterial({
      color: tint,
      transparent: true,
      opacity: 0.4 - 0.33 * t,
    });
  for (let b = 1; b <= BANDS; b++) {
    const upper = levels[b - 1].ring;
    const lower = levels[b].ring;
    const strandSegs = [];
    for (let i = 0; i < upper.length; i++) strandSegs.push(upper[i], lower[i]);
    parent.add(
      new THREE.LineSegments(
        new THREE.BufferGeometry().setFromPoints(strandSegs),
        netMat((b - 0.5) / BANDS),
      ),
    );
    parent.add(
      new THREE.Line(
        new THREE.BufferGeometry().setFromPoints(levels[b].raw),
        netMat(levels[b].t),
      ),
    );
  }
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

// Per pad: a glowing orb (bright core sphere + additive halo sprite) hovering
// BOOST_ORB_Z above the pad spot. The orb is a Group — the collectible;
// createBoostPads hides/pops it. Parented to `field` so it rides the
// orientation flip (a visual no-op — the layout is symmetric under the 180°
// spin — but consistent, wrinkle 7).
// Returns `{ mesh: orbGroup, x, y }` per pad (field-local coords).
function buildBoostPads(parent, spec) {
  // Size-independent — one instance shared across every pad.
  const halo = makeDotTexture();
  const coreMat = new THREE.MeshBasicMaterial({ color: BOOST_ORB_CORE });
  const glowMat = new THREE.SpriteMaterial({
    map: halo,
    color: BOOST_ORB_GLOW,
    transparent: true,
    depthWrite: false,
    blending: THREE.AdditiveBlending,
    opacity: 0.6,
  });

  const orbs = [];
  for (const [pads, r, size] of [
    [spec.bigPads, BOOST_ORB_BIG_R, "big"],
    [spec.smallPads, BOOST_ORB_SMALL_R, "small"],
  ]) {
    const coreGeo = new THREE.SphereGeometry(r, 16, 12);

    for (const [x, y] of pads) {
      const grp = new THREE.Group();
      grp.position.set(x, y, BOOST_ORB_Z);
      const core = new THREE.Mesh(coreGeo, coreMat);
      core.name = `boost_orb_${size}`;
      grp.add(core);
      const glow = new THREE.Sprite(glowMat);
      glow.scale.setScalar(r * BOOST_ORB_HALO);
      grp.add(glow);
      parent.add(grp);
      orbs.push({ mesh: grp, x, y });
    }
  }
  return orbs;
}

// Drive the boost-pad orbs from meta.boost_pads: hide a pad while it is
// collected, pop it back to full size over BOOST_ORB_POP seconds on respawn.
// `pad` in the meta is a dense index whose physical location is not given, so
// each is snapped to the nearest orb by the instigating car's position on its
// first collect (server: replay_frames._resolve_pickups). A pad never collected,
// or with no orb within BOOST_PAD_SNAP_MAX, is left untouched (visible).
// apply(t) is a pure function of t — scrubbing restores pad state.
function createBoostPads(meta, orbs) {
  const timeline = buildBoostPadTimeline(meta.frame_times, meta.boost_pads || []);
  const bound = new Map(); // pad index -> orb group

  // Claim the globally-closest (pad, orb) pairs first, so the binding doesn't
  // depend on pad-index order (which is unrelated to the physical layout).
  const maxSq = BOOST_PAD_SNAP_MAX * BOOST_PAD_SNAP_MAX;
  const pairs = [];
  for (let pad = 0; pad < timeline.length; pad++) {
    const entry = timeline[pad];
    if (!entry || !entry.snap) continue;
    const [px, py] = entry.snap;
    for (const o of orbs) {
      const dSq = (o.x - px) ** 2 + (o.y - py) ** 2;
      if (dSq < maxSq) pairs.push([dSq, pad, o]);
    }
  }
  pairs.sort((a, b) => a[0] - b[0]);
  const taken = new Set();
  for (const [, pad, o] of pairs) {
    if (bound.has(pad) || taken.has(o)) continue;
    bound.set(pad, o.mesh);
    taken.add(o);
  }

  let lastT = NaN;
  function apply(t) {
    if (t === lastT) return; // pure in t — skip the redundant paused/dup frames
    lastT = t;
    for (const [pad, mesh] of bound) {
      const { collected, since } = boostPadStateAt(timeline[pad], t);
      mesh.visible = !collected;
      if (collected) continue;
      // pop the orb back in over the moment after its respawn
      const age = t - since; // Infinity before the first transition -> full
      const k = age >= 0 && age < BOOST_ORB_POP ? age / BOOST_ORB_POP : 1;
      mesh.scale.setScalar(BOOST_ORB_POP_MIN + (1 - BOOST_ORB_POP_MIN) * k);
    }
  }

  return { apply, bound, timeline };
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

      // No bar at all when the replay carries no boost data for this slot —
      // an empty-looking bar would read as "0 boost", not "unknown".
      if (slot.has_boost) {
        const bar = makeBoostBarSprites(color);
        obj.userData.boostBar = bar;
        field.add(bar.track);
        field.add(bar.fill);
      }
    }
    return obj;
  });
}

// Reusable scratch vectors for the boost bar's per-frame placement — allocated
// once at module scope rather than per slot per frame.
const _boostRight = new THREE.Vector3();
const _boostAnchor = new THREE.Vector3();

// The playback clock + per-frame pose application, over one match's data. All
// the interpolation and timeline math is in replay-core.js: writePoses() fills a
// scratch buffer, createTransport() maps real <-> compressed seconds. applyPoses()
// here copies that buffer onto the THREE meshes and sizes labels — its declared
// dependencies are `camera` (screen-constant label sizing) and `names` / `boostOn`
// (the shared, mutable show/hide boxes wireControls flips), all taken once here
// rather than read from module scope.
function createPlayback(
  meta,
  positions,
  boost,
  meshes,
  camera,
  names,
  boostOn,
  fieldWorldInverse,
) {
  const slotCount = meta.slots.length;
  const transport = createTransport(meta);
  const { tN, compressedEnd } = transport;
  const pose = makePoseBuffers(slotCount);

  const state = { t: transport.tStart, playing: false, speed: 1 };

  function applyPoses() {
    writePoses(meta, positions, state.t, pose, boost);
    // Screen-constant label sizing — camera-only, so hoisted out of the per-slot loop.
    const frustumH = (camera.top - camera.bottom) / camera.zoom;
    const pillH = LABEL_VIEW_FRAC * frustumH;
    const labelBase = LABEL_CLEAR_UU + LABEL_GAP_FRAC * frustumH;
    const barHeight = BOOST_BAR_HEIGHT_FRAC * frustumH;
    const barGap = BOOST_BAR_GAP_FRAC * frustumH;
    // THREE recomputes each sprite's screen-space rectangle from `center` +
    // this world point fresh every render, using the CURRENT camera basis — so
    // reading the camera's right vector once here (a frame behind the pending
    // controls.update(), same as frustumH above) is enough to left-anchor every
    // bar correctly regardless of orbit, with no per-slot camera math. The
    // vector comes out of camera.matrixWorld in world space, but mesh/label/bar
    // positions below are field-local (field is rotated 180° for half the
    // matches — decision 12) — transformDirection(fieldWorldInverse) converts
    // it, rotation only, no translation.
    _boostRight
      .setFromMatrixColumn(camera.matrixWorld, 0)
      .transformDirection(fieldWorldInverse);
    for (let s = 0; s < meshes.length; s++) {
      const mesh = meshes[s];
      const label = mesh.userData.label;
      const trail = mesh.userData.trail;
      const bar = mesh.userData.boostBar;
      if (!pose.visible[s]) {
        mesh.visible = false;
        if (label) label.visible = false;
        if (bar) {
          bar.track.visible = false;
          bar.fill.visible = false;
        }
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
        label.visible = names.on;
        label.scale.set(pillH * label.userData.aspect, pillH, 1);
        label.position.set(
          mesh.position.x,
          mesh.position.y,
          mesh.position.z + labelBase,
        );
      }

      if (bar) {
        if (boostOn.on) {
          const trackWidth = pillH * label.userData.aspect; // matches the nameplate
          _boostAnchor.copy(mesh.position);
          _boostAnchor.z += labelBase - barGap;
          _boostAnchor.addScaledVector(_boostRight, -trackWidth / 2);
          bar.track.position.copy(_boostAnchor);
          bar.fill.position.copy(_boostAnchor);
          bar.track.scale.set(trackWidth, barHeight, 1);
          const frac = pose.boost[s] / 255;
          bar.fill.scale.set(Math.max(trackWidth * frac, 0.01), barHeight, 1);
          bar.fill.material.color.setHex(boostColor(frac));
          bar.track.visible = true;
          bar.fill.visible = true;
        } else {
          bar.track.visible = false;
          bar.fill.visible = false;
        }
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

function wireControls(playback, meta, names, boostOn) {
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
    names.on = on;
    if (namesBtn) {
      namesBtn.classList.toggle("is-active", on);
      namesBtn.setAttribute("aria-pressed", String(on));
    }
  }
  if (namesBtn) namesBtn.addEventListener("click", () => setNames(!names.on));

  function setBoost(on) {
    boostOn.on = on;
    if (boostBtn) {
      boostBtn.classList.toggle("is-active", on);
      boostBtn.setAttribute("aria-pressed", String(on));
    }
  }
  if (boostBtn) boostBtn.addEventListener("click", () => setBoost(!boostOn.on));

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
      setNames(!names.on);
    } else if (e.key === "b" || e.key === "B") {
      setBoost(!boostOn.on);
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

// Owns the camera, renderer, and orbit controls together — the one piece of
// scene state that, unlike playback/boostPads/goalFx below, never got its own
// factory and instead lived as module-level `let`s reached into by a bare
// `resize()` / `applyCamPreset()`. `resize`/`applyPreset` close over the rig's
// own `camera`/`renderer`/`viewSize` instead. `camScale` (from arenaCamScale,
// fixed for the match's arena) is a constructor argument, not a per-call one.
function createCameraRig(canvas, camScale) {
  const renderer = new THREE.WebGLRenderer({ canvas, antialias: true });
  renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));

  const camera = new THREE.OrthographicCamera(-1, 1, 1, -1, 0.1, 200000);
  const controls = new OrbitControls(camera, renderer.domElement);
  controls.enableDamping = true;

  let viewSize = CAM_PRESETS.broadcast.size;

  function resize() {
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

  function applyPreset(name) {
    const p = CAM_PRESETS[name];
    if (!p) return;
    camera.up.set(...p.up); // a direction — never scaled
    camera.position.set(...p.pos.map((v) => v * camScale));
    controls.target.set(...p.target.map((v) => v * camScale));
    viewSize = p.size * camScale;
    resize();
    controls.update();
  }

  return { camera, renderer, controls, resize, applyPreset };
}

function buildScene(meta, positions, boost) {
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
  // field's world rotation is fixed for the whole match (only the camera
  // orbits, never field/world), so this inverse is captured once rather than
  // recomputed every frame — applyPoses uses it to turn the camera's
  // world-space right vector into a direction it can add to a field-local
  // position (mesh/label/bar positions all live in field-local space).
  field.updateWorldMatrix(true, false);
  const fieldWorldInverse = new THREE.Matrix4().copy(field.matrixWorld).invert();

  const spec = arenaSpec(meta.game_mode);
  const camScale = arenaCamScale(spec);

  buildArena(field, spec);
  buildHalfTint(field, spec, meta.tracked_team);
  const boostPads = createBoostPads(meta, buildBoostPads(field, spec));
  buildGoals(field, spec, meta.tracked_team);
  const meshes = createActorMeshes(field, meta, spec);
  const goalFx = createGoalFx(field, spec.goal.fxScale);

  const rig = createCameraRig(canvas, camScale);
  const { camera, renderer, controls } = rig;

  // Image-based lighting for the MeshStandardMaterial cars and ball — without an
  // environment, metalness renders near-black. A one-off PMREM bake of three's
  // stock studio room; the line arena ignores `scene.environment`.
  const pmrem = new THREE.PMREMGenerator(renderer);
  scene.environment = pmrem.fromScene(new RoomEnvironment(), 0.04).texture;
  scene.environmentIntensity = 0.6; // studio room is bright; dial it into our dark scene
  pmrem.dispose();

  rig.applyPreset("broadcast");
  wireCamera(rig);
  window.addEventListener("resize", rig.resize);

  // Player name labels and boost bars start on; the NAMES/BOOST buttons and
  // `n`/`b` keys flip these. Shared mutable boxes (not plain booleans) so
  // createPlayback and wireControls can both hold the one reference —
  // wireControls flips them, applyPoses reads them, constructed here before
  // either needs them.
  const names = { on: true };
  const boostOn = { on: true };
  const playback = createPlayback(
    meta,
    positions,
    boost,
    meshes,
    camera,
    names,
    boostOn,
    fieldWorldInverse,
  );
  const watchGoals = makeGoalWatcher(meta, positions, playback, goalFx);
  const syncUI = wireControls(playback, meta, names, boostOn);
  renderScrubMarks(meta, playback);
  playback.applyPoses();
  boostPads.apply(playback.state.t);
  syncUI();
  controlsEl.hidden = false;

  // The viewer's real interface, always built — `?debug` only decides whether
  // it's also mirrored onto `window` for interactive poking (and the e2e suite,
  // which navigates with `?debug` for exactly that reason).
  const handle = {
    playback,
    meshes,
    camera,
    controls,
    renderer,
    scene,
    THREE,
    meta,
    positions,
    boost,
    goalFx,
    boostPads,
  };

  const debug = location.search.includes("debug");
  const hud = debug ? createDebugHud(meshes) : null;
  if (debug) window.__replay = handle;

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
    boostPads.apply(playback.state.t);
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

  return handle;
}

// Camera preset buttons, and clearing the active state once the user orbits.
function wireCamera(rig) {
  if (!camEl) return;
  camEl.addEventListener("click", (e) => {
    const btn = e.target.closest("button[data-view]");
    if (!btn) return;
    rig.applyPreset(btn.dataset.view);
    for (const b of camEl.querySelectorAll("button")) {
      b.classList.toggle("is-active", b === btn);
    }
  });
  rig.controls.addEventListener("start", () => {
    for (const b of camEl.querySelectorAll("button")) b.classList.remove("is-active");
  });
}

async function main() {
  if (!canvas || !matchId) {
    showMessage("This replay could not be loaded.");
    return;
  }
  try {
    const res = await fetch(`/api/matches/${matchId}/replay`);
    if (!res.ok) {
      showMessage(
        res.status === 404
          ? "No replay file for this match."
          : "This replay could not be loaded.",
      );
      return;
    }
    const { meta, positions, boost } = decodeReplayEnvelope(await res.arrayBuffer());

    await document.fonts.ready; // so name labels render in DM Mono, not fallback
    buildScene(meta, positions, boost);
  } catch (err) {
    console.error(err);
    showMessage("This replay could not be loaded.");
  }
}

main();
