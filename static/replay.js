// Browser replay viewer — see docs/replay-viewer.md
//
// Step 3: load the metadata + packed position buffer for a match, build a
// Three.js scene (wireframe soccar arena + box cars + sphere ball) and place
// every actor at its frame-0 pose. No playback yet — an orbitable static shot.

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

const stage = document.querySelector('[data-role="stage"]') ||
  document.querySelector(".replay-stage");
const canvas = document.querySelector('[data-role="scene"]');
const messageEl = document.querySelector('[data-role="message"]');

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

function poseAt(positions, slotCount, frame, slot) {
  const base = (frame * slotCount + slot) * FLOATS_PER_POSE;
  return positions.subarray(base, base + FLOATS_PER_POSE);
}

function segmentsCover(slot, frame) {
  return slot.segments.some(([start, end]) => start <= frame && frame <= end);
}

function carColor(slot, trackedTeam) {
  if (slot.team == null) return TEAM_UNKNOWN;
  return slot.team === trackedTeam ? TEAM_OURS : TEAM_THEIRS;
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

function buildActors(world, meta, positions) {
  const slotCount = meta.slots.length;
  meta.slots.forEach((slot, i) => {
    let mesh;
    if (slot.kind === "ball") {
      mesh = new THREE.Mesh(
        new THREE.SphereGeometry(BALL_RADIUS, 24, 16),
        new THREE.MeshLambertMaterial({ color: 0xf0f0f4 }),
      );
    } else {
      mesh = new THREE.Mesh(
        new THREE.BoxGeometry(...CAR_SIZE),
        new THREE.MeshLambertMaterial({
          color: carColor(slot, meta.tracked_team),
        }),
      );
    }
    const p = poseAt(positions, slotCount, 0, i);
    mesh.position.set(p[0], p[1], p[2]);
    mesh.quaternion.set(p[3], p[4], p[5], p[6]);
    mesh.visible = segmentsCover(slot, 0);
    world.add(mesh);
  });
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
  buildActors(world, meta, positions);

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

  function tick() {
    requestAnimationFrame(tick);
    controls.update();
    renderer.render(scene, camera);
  }
  tick();

  const seconds = meta.frame_times.at(-1) ?? 0;
  const metaEl = document.querySelector('[data-role="meta"]');
  if (metaEl) {
    const mins = Math.floor(seconds / 60);
    const secs = Math.round(seconds % 60).toString().padStart(2, "0");
    metaEl.textContent = [meta.game_mode, `${mins}:${secs}`]
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
