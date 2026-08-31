// Browser replay viewer — see docs/replay-viewer.md
//
// Steps 3–7: load a match's metadata + packed position buffer, build a Three.js
// scene (wireframe soccar arena + box cars + sphere ball + name labels + motion
// trails), and play it back on a real-time clock — play/pause, scrub, 0.5×–4×
// speed. Poses are lerp/slerp'd between rrrocket's ~30 Hz samples using the real
// (non-uniform) frame deltas. A slot's mesh is hidden while its actor is between
// segments (demolitions). When the tracked team is team 1 the field is flipped
// 180° so "our" half is always the same side of the screen.

import * as THREE from "https://cdn.jsdelivr.net/npm/three@0.170.0/+esm";
import { OrbitControls } from "https://cdn.jsdelivr.net/npm/three@0.170.0/examples/jsm/controls/OrbitControls.js/+esm";

// Rocket League field, unreal units. X = wall to wall, Y = goal to goal,
// Z = floor to ceiling. The world group is Z-up (RL); Three.js is Y-up.
const FIELD_X = 8192;
const FIELD_Y = 10240;
const FIELD_Z = 2044;

const CAR_SIZE = [118, 84, 36]; // Octane hitbox, RL local axes (X fwd, Y left, Z up)
const BALL_RADIUS = 91.25;
const FLOATS_PER_POSE = 7; // x, y, z, qx, qy, qz, qw

const TEAM_OURS = 0x00e5ff;
const TEAM_THEIRS = 0xff5a5a;
const TEAM_UNKNOWN = 0x8585a0;
const VIEW_SIZE = 9800; // orthographic frustum height, uu — frames the field
const SEEK_STEP = 5; // seconds, for arrow-key seeking
const LABEL_HEIGHT = 150; // uu above a car's centre for its name label
const TRAIL_FRAMES = 45; // ~1.5 s of motion tail at rrrocket's ~30 Hz

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

function carColor(slot, trackedTeam) {
  if (slot.team == null) return TEAM_UNKNOWN;
  return slot.team === trackedTeam ? TEAM_OURS : TEAM_THEIRS;
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

function buildArena(parent) {
  const box = new THREE.BoxGeometry(FIELD_X, FIELD_Y, FIELD_Z);
  const edges = new THREE.LineSegments(
    new THREE.EdgesGeometry(box),
    new THREE.LineBasicMaterial({ color: 0x4a5d82 }),
  );
  edges.position.set(0, 0, FIELD_Z / 2); // floor at z = 0
  parent.add(edges);

  const grid = new THREE.GridHelper(FIELD_Y, 20, 0x2e3d5c, 0x1e2842);
  grid.rotation.x = Math.PI / 2; // GridHelper lies in XZ; rotate onto the RL floor (XY)
  parent.add(grid);

  const halfLine = new THREE.LineSegments(
    new THREE.BufferGeometry().setFromPoints([
      new THREE.Vector3(-FIELD_X / 2, 0, 2),
      new THREE.Vector3(FIELD_X / 2, 0, 2),
    ]),
    new THREE.LineBasicMaterial({ color: 0x4a5d82 }),
  );
  parent.add(halfLine);
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

  const state = { t: t0, playing: false, speed: 1 };

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

  function seek(t) {
    state.t = Math.min(tN, Math.max(t0, t));
  }

  function advance(dt) {
    if (!state.playing) return;
    state.t += dt * state.speed;
    if (state.t >= tN) {
      state.t = tN;
      state.playing = false;
    }
  }

  return {
    state,
    t0,
    tN,
    applyPoses,
    seek,
    advance,
    elapsed: () => state.t - t0,
    duration: () => tN - t0,
    progress: () => (tN > t0 ? (state.t - t0) / (tN - t0) : 0),
    atEnd: () => state.t >= tN,
  };
}

function wireControls(playback) {
  let scrubbing = false;

  function setPlaying(on) {
    if (on && playback.atEnd()) playback.seek(playback.t0);
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
    playback.seek(playback.t0 + (scrubEl.valueAsNumber / 1000) * playback.duration());
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
      playback.seek(playback.state.t - SEEK_STEP);
    } else if (e.key === "ArrowRight") {
      playback.seek(playback.state.t + SEEK_STEP);
    }
  });

  // Reflect clock state back into the DOM each frame.
  return function syncUI() {
    if (!scrubbing) scrubEl.value = String(Math.round(playback.progress() * 1000));
    clockEl.textContent = `${formatClock(playback.elapsed())} / ${formatClock(
      playback.duration(),
    )}`;
    if (!playback.state.playing && playBtn.textContent !== "▶") {
      playBtn.textContent = "▶";
    }
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
  const meshes = createActorMeshes(field, meta);

  renderer = new THREE.WebGLRenderer({ canvas, antialias: true });
  renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));

  camera = new THREE.OrthographicCamera(-1, 1, 1, -1, 0.1, 200000);
  // Elevated broadcast-ish angle behind one goal, looking down the length.
  camera.position.set(0, 7200, 14500);

  const controls = new OrbitControls(camera, renderer.domElement);
  controls.enableDamping = true;
  controls.target.set(0, 600, 0);

  resize();
  window.addEventListener("resize", resize);

  const playback = createPlayback(meta, positions, meshes);
  const syncUI = wireControls(playback);
  playback.applyPoses();
  syncUI();
  controlsEl.hidden = false;

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
  }
  requestAnimationFrame(frameLoop);

  const metaEl = document.querySelector('[data-role="meta"]');
  if (metaEl) {
    metaEl.textContent = [meta.game_mode, formatClock(playback.duration())]
      .filter(Boolean)
      .join("  ·  ");
  }

  if (location.search.includes("debug")) {
    window.__replay = { playback, meshes, THREE };
  }
}

function resize() {
  if (!renderer || !camera) return;
  const w = stage.clientWidth;
  const h = stage.clientHeight;
  renderer.setSize(w, h, false);
  const aspect = w / h || 1;
  camera.left = (-VIEW_SIZE * aspect) / 2;
  camera.right = (VIEW_SIZE * aspect) / 2;
  camera.top = VIEW_SIZE / 2;
  camera.bottom = -VIEW_SIZE / 2;
  camera.updateProjectionMatrix();
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
