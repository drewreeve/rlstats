// Browser replay viewer — see docs/replay-viewer.md
//
// Steps 3–5: load a match's metadata + packed position buffer, build a Three.js
// scene (wireframe soccar arena + box cars + sphere ball), and play it back on a
// real-time clock — play/pause, scrub, 0.5×–4× speed. Poses are lerp/slerp'd
// between rrrocket's ~30 Hz samples using the real (non-uniform) frame deltas.
// A slot's mesh is hidden while its actor is between segments (demolitions).

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

function buildArena(world) {
  const box = new THREE.BoxGeometry(FIELD_X, FIELD_Y, FIELD_Z);
  const edges = new THREE.LineSegments(
    new THREE.EdgesGeometry(box),
    new THREE.LineBasicMaterial({ color: 0x4a5d82 }),
  );
  edges.position.set(0, 0, FIELD_Z / 2); // floor at z = 0
  world.add(edges);

  const grid = new THREE.GridHelper(FIELD_Y, 20, 0x2e3d5c, 0x1e2842);
  grid.rotation.x = Math.PI / 2; // GridHelper lies in XZ; rotate onto the RL floor (XY)
  world.add(grid);

  const halfLine = new THREE.LineSegments(
    new THREE.BufferGeometry().setFromPoints([
      new THREE.Vector3(-FIELD_X / 2, 0, 2),
      new THREE.Vector3(FIELD_X / 2, 0, 2),
    ]),
    new THREE.LineBasicMaterial({ color: 0x4a5d82 }),
  );
  world.add(halfLine);
}

function createActorMeshes(world, meta) {
  return meta.slots.map((slot) => {
    const mesh =
      slot.kind === "ball"
        ? new THREE.Mesh(
            new THREE.SphereGeometry(BALL_RADIUS, 24, 16),
            new THREE.MeshLambertMaterial({ color: 0xf0f0f4 }),
          )
        : new THREE.Mesh(
            new THREE.BoxGeometry(...CAR_SIZE),
            new THREE.MeshLambertMaterial({
              color: carColor(slot, meta.tracked_team),
            }),
          );
    world.add(mesh);
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
      const liveI = slotLiveAt(slot, i);
      const liveJ = slotLiveAt(slot, j);
      if (!liveI && !liveJ) {
        meshes[s].visible = false;
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

  buildArena(world);
  const meshes = createActorMeshes(world, meta);

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

    buildScene(meta, positions);
  } catch (err) {
    console.error(err);
    showMessage("This replay could not be loaded.");
  }
}

main();
