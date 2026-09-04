# ADR-0004: Browser replay viewer — design

Status: **accepted; implemented.** (Formerly `docs/replay-viewer.md`, a working
design doc during the build; converted to an ADR once v1 shipped.)

A ballchasing-style tactical replay viewer at `/match/{id}/replay`: a whole-field
angled/orthographic 3D view of car and ball movement, built from the per-frame
positional data `rrrocket -n` already produces. An **analysis tool** for spotting
the tracked team's positional habits across many matches, not a game-like
re-render. Feasibility was never in question — `frame_analysis.py` already parses
every frame's `RigidBody` state (and discards it); the parse is ~0.1 s.

## Prior art

**ballchasing.com** — proprietary, no public code; server-side network-parse then
a deliberately *un*-game-like WebGL scene (field outline, simple markers, whole
field visible, no game camera). **This is the model we copied.** `calculated.gg` /
`SaltieRL/WebReplayViewer` (full game-like 3D, dormant), `foppage/replay-viewer`
(Svelte) and `Longi94/rocket-viewer` (Three.js) all consume ~the same
`rrrocket -n` JSON — camera / interpolation references only.

## Decisions

| # | Decision | Choice |
|---|----------|--------|
| 1 | Purpose / fidelity | Ballchasing-style tactical overview; low-poly **procedural** shapes only, no ripped models or textures. |
| 2 | Frame-data source | Re-parse the stored `.replay` on demand at view time. **No persistence, no sidecar, no backfill** (the rejected option — see #10). |
| 3 | UI placement | Dedicated route `/match/{id}/replay` + its own static HTML/JS/CSS, linked from the match detail page. |
| 4 | Rendering tech | Three.js, primitive meshes built in code (low-poly cars, sphere ball, wireframe arena). Orthographic/high camera. Keeps the Z axis — height and aerials matter. |
| 5 | Server extraction | `replay_frames.py`: a pure reshape over a `ParsedReplay`, own lean single pass over `replay.frames`. **Not** routed through `queries.py` (not a DB read) or `analyze_frames` (not the aggregate pipeline). |
| 6 | Player identity | Full identity in v1: each car labelled with the player's name, coloured tracked-team vs opponent. Reuses the `car → PRI → (platform, id)` chain from `frame_analysis.py`. |
| 7 | v1 feature scope | See below. |
| 8 | Transport | ~~Small JSON metadata doc + a separate packed **little-endian `Float32`** `.bin` of positions.~~ **Superseded — see addendum.** One-shot, server gzip, no chunking. |
| 9 | Three.js delivery | No importmap. `static/replay.js` imports Three + `OrbitControls` + `RoomEnvironment` by full jsdelivr `/+esm` URL (pre-resolves all bare specifiers). All covered by the existing `script-src cdn.jsdelivr.net` — **no CSP change.** An inline importmap would have needed one. |
| 10 | Caching | ~~None in v1 (measured: rrrocket + reshape well under a second). Add an in-process LRU only if click-to-view latency annoys.~~ **Moot — see addendum.** **Never an on-disk sidecar** — that is the persistence layer rejected in #2. |
| 11 | Arena / mode coverage | Standard soccar arena only (`buildArena` is hardcoded soccar; covers 3v3, 2v2, any soccar-arena mode). Positions in the `.bin` are mode-agnostic, so a second arena is purely additive. **Gap:** the link is *not* mode-gated — `has-replay` is a file-existence probe, so a Hoops/Dropshot match with a replay on disk still opens the viewer, in a soccar arena. **Resolved for hoops in ADR-0005** (`arenaSpec(game_mode)` + a hoops arena; Dropshot/Snowday still fall back to soccar). |
| 12 | Orientation | Normalise: the tracked team always attacks the same on-screen direction, every match, regardless of their colour. Enforced client-side by `field.rotation.z = π` when `tracked_team === 1`. |
| 13 | Link availability & failure | Show "▶ WATCH REPLAY" only when `matches.replay_filename` is set and `replays/<name>` exists; 404 otherwise. On a view-time parse failure return an error payload the page renders — and **never delete the file** (unlike the ingest path). |
| 14 | Car shape | A composed low-poly battle car, not a bare box (`buildCarModel` wrapping the inlined `battle-car.js` model) — a symmetric box gave no read on which way a car faced, the one thing a positional-habits tool most needs. Still procedural, no assets. `CAR_SCALE` is pinned on **width** (wheel track ≈ the 84 uu hitbox width); the ~1.1× length and ~1.2× roof are the overhang a real body has over its collision box. A `RoomEnvironment` PMREM is baked into `scene.environment` so the model's `MeshStandardMaterial` metals don't render near-black. |
| 15 | Testability | Split into a pure core and a render shell — see below. |

### v1 feature scope (decision 7)

**In:** play/pause, draggable scrub bar, speed control (0.5×–4×), one animation
clock · 2 camera presets (broadcast-high, top-down) + drag orbit/zoom · motion
trails · goal markers on the scrub bar + a running scoreboard · actor lifecycle
(a demoed car's mesh is hidden while its actor is out of the frame stream, so it
does not teleport on respawn) · kickoff 3-2-1 countdown overlay · post-goal dead
time collapsed out of the transport.

**Deferred:** shot / save / demolition markers · in-match game clock · boost
amounts · per-player POV · follow-cam · goal-replay cutaways · heatmaps ·
Hoops / Dropshot arenas · any caching. (Boost pad orbs, with collect/respawn
timing, shipped post-v1 — see "Known wrinkles" below; boost *amounts* — a
numeric per-player HUD — are still deferred.)

### Testability (decision 15)

Split into a pure core and a render shell. `static/replay-core.js` (**zero
imports** — no THREE, no DOM) holds all playback/timeline math: `bracket`,
`slotLiveAt`, `writePoses`, `createTransport` (real ↔ compressed seconds),
`countdownLabelAt`, `teamTint`/`carColor`, `outlineHalfWidth`, `slerpQuat` (a
verbatim port of `THREE.Quaternion.slerp@0.170.0`), `formatClock`. `replay.js`
is the shell; its `applyPoses()` (inside `createPlayback`) copies
`writePoses()`'s buffer onto meshes and sizes labels, taking `camera` and a
shared `names` box as constructor arguments rather than reading module scope.
**One code path.**

`tests/js/replay-core.test.js` (`node --test`, no browser/deps) tests the
math; `tests/e2e/replay.spec.js` (Playwright) adds a transport smoke pass, an
`applyPoses() === writePoses()` **parity test** (the guard against
`applyPoses` growing a second, diverging pose computation), a
THREE-vs-`slerpQuat` check, and a **render-smoke check** (a rendered frame's
pixels span several luminance bands; the canvas is on-screen / sized /
unobscured — the "renders fine but the screen is black" failure a pose-math
test can't see). Pixels are read in-`page.evaluate` synchronous with
`renderer.render()`, so there is no production test hook.

**No committed golden pose-trace** — a recording of `writePoses()`'s own
output is a circular oracle and fragile across Chromium float builds; a
one-shot uncommitted trace is the refactor net.
`tests/data/replay-viewer/{meta.json, frames.bin}` (the fixture, *not* a
trace) is regen'd by `tests/e2e/dump_fixture.py`.

## Architecture

### Server

```
GET /match/{id}/replay                   -> replay.html   (404 if no file on disk)
GET /api/matches/{id}/has-replay         -> {"has_replay": bool}   (1 query + stat, no parse)
GET /api/matches/{id}/replay             -> the wire envelope: positions + boost + meta JSON, octet-stream  (404 no file / 422 unparseable)
```

(Superseded — see addendum below. `/replay` originally returned JSON, with a
separate `GET /api/matches/{id}/replay-frames.bin` alongside it; a third route,
`replay-boost.bin`, was added later. All three are now the single envelope
route above.)

- `has-replay` gates the match-page link without paying an rrrocket parse on
  every match-page load. `422` covers "file present but rrrocket failed / no
  network data" — the page renders that, not a dead viewer.
- `GZipMiddleware` (`minimum_size=1024`) compresses the envelope.
- Route glue is in **`replay_view.py`** (`replay_path_for`,
  `build_replay_frames`, `encode_replay_meta`/`encode_replay_meta_bytes`,
  `serialize_replay_envelope`), keeping `server.py`'s closures thin.
  `build_replay_frames` builds the `extract_replay_frames` inputs via
  `ingest.build_replay_context()` (the shared pre-frame preamble — see
  CONTEXT.md "Replay Context") and uses **`rrrocket.run_rrrocket()`**, a
  non-deleting sibling of `process.parse_replay()`. The route offloads only
  the `build_replay_frames` call to a worker thread
  (`asyncio.to_thread`) — the preceding `replay_path_for` DB read stays on the
  event loop, since it's sub-millisecond and offloading it would buy nothing.
- **Match id → file.** Replay files are named by the uploader, *not* by
  `MatchGUID` (`matches.replay_hash` stores the GUID — the two never match). The
  nullable `matches.replay_filename` column (migration 020, source basename) is
  populated by ingest; `queries.match_replay_filename(match_id)` →
  `replays/<name>`.

`extract_replay_frames` signature (mirrors `analyze_frames`):

```python
def extract_replay_frames(
    replay: ParsedReplay,
    *,
    tracked_team: int | None,
    tracked_identities: AbstractSet[PlayerIdentity],
    player_names: dict[PlayerIdentity, str],   # display name preferred, else in-game
    game_mode: str | None,
) -> ReplayFrames
```

It reuses `IdentityResolver` + `from_network_frame` for the `car → PRI → identity`
chain and `NetObj` for name→object-id resolution.

### `ReplayFrames` shape

```python
@dataclass(frozen=True)
class ActorSlot:
    identity: PlayerIdentity | None    # None for unresolved cars (bots, odd platforms)
    name: str                          # display name > in-game name > "Player N"
    team: int | None                   # 0 | 1 for cars (from TeamPaint); None for the ball
    is_tracked: bool
    kind: str                          # "car" | "ball"
    segments: list[tuple[int, int]]    # (start_frame, end_frame) inclusive, frame-index space

@dataclass(frozen=True)
class ReplayFrames:
    frame_times: list[float]           # length F, wall-clock seconds, non-uniform
    slots: list[ActorSlot]             # ball lane + one car lane per player
    positions: bytes                   # F * N * 7 little-endian float32, row-major [frame][slot][x,y,z, qx,qy,qz,qw]
    tracked_team: int | None
    game_mode: str | None
    goals: list[GoalMarker]            # {frame, team} in frame order
    countdowns: list[tuple[int, int]]  # (frame, n) per kickoff tick (3→2→1→0), frame order
    dead_periods: list[tuple[int, int]] # (start, end) inclusive frame indices the viewer collapses
    boost_pads: list[tuple[int, int, int, float, float]]  # (frame, pad, collected, x, y) per pad state flip, frame order
```

`boost_pads` rows carry a **dense 0-based `pad` index** (ascending pad
object id) and `collected` ∈ {0, 1}. `x, y` is the instigating car's
position on a collect row (`collected == 1`), read from the densified
buffer; a respawn row carries `0.0, 0.0`. The client snaps `pad` → a
rendered pad orb once per match off the first collect row's `(x, y)` —
the layout stays a single source of truth in `replay-core.js`. Pickup
actor ids recycle through the match, so the walk rebinds them to their
pad on each `new_actors` announce and emits a row only when the pad's
collected/available state actually flips (the game re-sends it).

*(Pre-addendum snapshot — see below for the current, merged-envelope
transport.)* The JSON endpoint serialises everything except `positions` (via
`ReplayFrames.meta_dict()`, the sole entry point); the `.bin` endpoint returns
`positions`.

Since v1 this shape is pinned in code, not only here: `replay_frames.py` carries a
hand-written `WIRE_META_KEYS` / `WIRE_SLOT_FIELDS` / `WIRE_GOAL_FIELDS` /
`WIRE_TUPLE_WIDTHS` manifest, the positions-buffer geometry is `pose_offset()` /
`packed_buffer_bytes()` (twins of `poseOffset` / `FLOATS_PER_POSE` in
`replay-core.js`), and `tests/test_replay_wire.py` asserts the serialised output
against the manifest — the replay-side analogue of `tests/test_stats_registry.py`.
The listing above is a v1 snapshot; **CONTEXT.md "Replay Wire" and that code are
authoritative.** `meta_dict()` is the one serialisation entry point, not the shape
declaration.

### Extraction rules (server-side invariants, not obvious from the walk)

- **Phase order preserved** from `_process_frame`: new_actors → updated_actors →
  deleted_actors within each frame. A car spawned and PRI-linked in the same
  frame keeps its link.
- **Every frame is recorded** — unlike every `frame_analysis` handler, the walk
  does **not** gate on `ctx.is_playing`. The client keeps the countdowns and
  collapses the post-goal dead spans (`dead_periods`).
- **Actor lifecycle.** At every goal kickoff the ball and *every* car are deleted
  and re-created with a fresh network id at the kickoff spawn — one real
  despawn/respawn per goal, all lanes at once. The initial kickoff is just the
  actors' first appearance (no delete). Demolitions delete per victim.
  Separately, a still-open actor is **re-announced** in `new_actors` with no
  preceding delete every ~10 s (a mid-field keep-alive) — that is a position
  append, not a new life.
- **Segments.** A lane opens a segment when an id first appears, closes it on a
  real `deleted_actors` entry (or at the last frame). A bare re-announcement of
  an open id (the ~10 s keep-alive) does **not** open a new segment — it appends
  a sample and marks `_Segment.resets`. One segment per kickoff (goals + 1) per
  lane, merged into a single car slot by identity, plus an extra split per
  demolition with a gap the client hides the mesh across.
- **Slots keyed by identity.** Segments whose closing `resolve_car` gives the
  same `(platform, id)` merge into one lane. A car whose PRI never resolves
  (bots, odd platforms) gets its own anonymous lane per segment. A resolved
  identity missing from `player_names` falls back to `"Player N"` rather than
  raising.
- **Fill.** `positions` is dense — a full pose per lane per frame. Frames outside
  every segment stay zero (client hides the lane). Within a segment, a frame with
  no `RigidBody` sample is **interpolated** between the two bracketing real
  samples — linear on position, slerp on rotation, weighted by `frame_times`
  (`_densify` / `_slerp` / `_keyframes` / `_lerp_pose`). **Two runs hold the
  previous pose instead** (a cut, not motion — the lane must not drift toward the
  next point): the frames up to a bare re-announcement (`_Segment.resets`), and
  the tail after a lane's final sample. Seed a segment from
  `initial_trajectory.location` (its Euler `yaw/pitch/roll` often has `null`
  components — unusable as a quaternion); rotation before the first real sample
  is **held back** from that sample, not slerped up from identity (this also
  removes the identity-quaternion flick at spawns). A `RigidBody` update may omit
  `rotation` / velocities — carry the previous quaternion forward, identity if
  none seen yet.

## Client (`static/replay.js`, module script)

Coordinates stay in unreal units — no world scaling; the camera frustum frames.
`?debug` exposes `window.__replay` (`playback, meshes, camera, controls,
renderer, scene, THREE, meta, goalFx, boostPads`) + a rolling frame-time
HUD. The object is `buildScene`'s real return value — always built, not
`?debug`-only — and `?debug` only decides whether it's mirrored onto `window`;
`countdownLabelAt` isn't on it (a plain pure import from `replay-core.js`, not
state — a test wanting it imports the module directly, as the `slerpQuat`
cross-check already does). Headless `page.screenshot()` captures the WebGL canvas correctly under
`--headless=new`; a `readPixels` in a *later* task returns zeros
(`preserveDrawingBuffer` unset) — see `reference_replay_viewer_debug`.

- **Axis remap.** RL is Z-up, Three.js Y-up: everything lives in a `world`
  `Group` with `rotation.x = -π/2`, so RL `(x, y, z)` renders at world
  `(x, z, -y)`. Arena + actors + labels sit in an inner `field` group;
  `field.rotation.z = π` when `tracked_team === 1` (decision 12). Net effect for
  both configs: the tracked team defends world **+z** (near camera), attacks
  **−z**. Sprites ignore parent rotation, so label text is never mirrored.
- **Playback.** `createPlayback(meta, positions, meshes)` owns a clock: `state.t`
  in `frame_times` space (starts at `frame_times[0]`), advanced each rAF by
  `realDeltaSeconds * speed`, clamped and auto-paused at the end. `bracket(times,
  t)` binary-searches `[i, j, f]`; per slot the pose is `lerp` on position +
  `slerp` on quaternion between frames `i` and `j`. Uses the real timestamps,
  never a fixed step.
- **Dead-time remap.** `state.t` stays in real seconds (so `bracket` /
  `applyPoses` are untouched) but `createTransport`'s **public axis is
  compressed** — every `meta.dead_periods` span removed. `toCompressed` /
  `toReal` invert each other, resolving a seam to the span's *end* (the resume
  frame) so a seek can't land in a gap. `seek` / `nudge` / `elapsed` /
  `duration` / `progress` / `fractionAt` are all compressed; only `advance` steps
  raw real time. The playhead starts at `toReal(0)` — past the warmup. Always on;
  no toggle.
- **Lifecycle hiding.** `slotLiveAt(slot, frame)` scans the slot's inclusive
  segments. For a bracket `[i, j]`: hidden when neither end is live; otherwise
  `ff` snaps to the live end so the mesh never lerps toward a frame the actor
  isn't in. A long demolition gap stays hidden throughout; 1–2-frame
  kickoff-reset gaps hold at the boundary pose.
- **Labels.** Each car carries a `THREE.Sprite` name tag, rescaled every frame to
  a screen-constant fraction of viewport height and parked clear of the roof;
  hides with the car. `main` awaits `document.fonts.ready`.
- **Trails.** One `THREE.Line` per actor (ball included), a `TRAIL_FRAMES + 1`
  (~1.5 s) buffer, colour baked toward the background, `frustumCulled = false`.
  Head = live interpolated position, then walk backward through the buffer while
  `slotLiveAt` holds (so a trail never jumps a demolition gap).
- **Camera.** One `THREE.OrthographicCamera` + `OrbitControls` (damped).
  `CAM_PRESETS`: `broadcast` (default, 3/4 iso corner, offset toward the tracked
  team's end) and `top` (overhead, `camera.up = (0, 0, -1)` so the far goal is
  screen-up). `applyCamPreset` sets position/target/up/`viewSize` and re-runs
  `resize()`. The active preset button clears on the `OrbitControls` `start`
  event.
- **Goals + scoreboard.** `replay_frames._scan_goals` watches
  `ReplicatedScoredOnTeam` (the `Byte` is the team *scored on*, reset to 255
  between) — a goal is each rising edge into {0, 1}, `GoalMarker{frame,
  team = 1 - byte}`. Frame index maps straight to `frame_times`, no game-clock
  inversion. Client: a colour-coded tick per goal above the range input (cyan =
  tracked, red = opponent); `syncUI` counts goals with `time <= playback.t` into
  an `our – opp` scoreboard. `makeGoalWatcher` fires a team-tinted particle burst
  at the ball's entry point on the forward crossing (the goal instant itself is
  inside the trimmed dead span).
- **Kickoff countdown.** `replay_frames._scan_countdowns` records every
  `ReplicatedRoundCountDownNumber` tick as `(frame, n)` — one `3 → 2 → 1 → 0` run
  per kickoff, `n == 0` the frame live play resumes. `_scan_goals` +
  `_scan_countdowns` also yield `dead_periods`. Client:
  `countdownLabelAt(frame_times, meta.countdowns, t)` returns the numeral (or
  `"GO!"` for ~0.6 s after `n == 0`, else null); `syncUI` writes it to a centred
  overlay on change only. Playback untouched — pure presentation.
- **Arena schematic.** `buildArena` draws the chamfered soccar footprint (four
  corners cut at 45°, `CORNER = 1152` uu, `|x| + |y| = 8064`) as the eight-point
  module-level `ARENA_OUTLINE` + a floor grid clipped to the octagon.
  `buildGoals` draws an open wireframe box at `y = ±5120` with the mouth filled
  by a translucent plane in the defending team's colour. `buildHalfTint` washes
  each half (split at `y = 0`) a faint `0.10` in the defending team's colour —
  the cue that carries the TOP view. `buildBoostPads` draws each pad
  (coords from wiki.rlbot.org) as a small glowing orb; `createBoostPads` then
  hides it while collected and pops it back on respawn from `meta.boost_pads`.
  All arena geometry is parented to `field` so it rides the orientation flip.
  `teamTint(team, trackedTeam)` is the shared colour rule.
- **Core / shell split (decision 15).** All pure math lives in
  `static/replay-core.js` (zero imports). `replay.js` is the shell: scene graph,
  DOM, camera, the rAF loop, and an `applyPoses()` (inside `createPlayback`)
  that copies `writePoses()`'s buffer onto the meshes and sizes labels, reading
  `camera` and the shared `names` show/hide box — both taken once as
  constructor arguments, not module-scope reads. `camera`/`renderer`/`controls`
  themselves live behind one `createCameraRig(canvas, camScale)` factory (the
  same shape as `createPlayback`/`createBoostPads`/`createGoalFx` below it),
  not module-level `let`s — `resize`/`applyPreset` are closures the rig owns.

## Known wrinkles

(Wrinkles 1–2 — inline importmap vs CSP, and the two time bases — were resolved
during implementation: see decision 9 and the goals-from-frame-index approach
above.)

3. **`process.py` deletes `.replay` on parse failure; the view path must not**
   (decision 13).
4. **Frame-stream gaps** during countdowns and goal explosions — the client
   holds / interpolates across; no server-side special-casing.
5. **Non-uniform deltas** — the clock integrates real frame deltas, never a fixed
   step.
6. **Little-endian buffer.** `array.array('f').tobytes()` writes native byte
   order; JS `Float32Array` reads little-endian. True on every target platform —
   recorded so it is an assumption, not an accident.
7. **Anchor arena geometry by RL team, colour by `teamTint`.** All static field
   geometry is inside the `field` group, rotated 180° when `tracked_team === 1`
   (decision 12). Place a half / goal by its raw RL side (team 0 defends `−y`,
   team 1 defends `+y`) and colour it with `teamTint(team, trackedTeam)` —
   **never by "screen side".** Hardcoding "ours = the −y half" renders correctly
   for one `tracked_team` value and mirror-wrong for the other.

## Assumptions

- Desktop-first; no mobile/touch tuning in v1.
- The read endpoints follow the same auth posture as `/api/matches/{id}`.
- The full frame stream is shown (warmup + celebrations); the user scrubs past.

## Addendum (2026-09-04): merge the three routes into one envelope

Decision 8 (transport) and decision 10 (caching) are **superseded**.

**What changed.** A third route, `/api/matches/{id}/replay-boost.bin`, was
added (per-player boost meter) without revisiting decision 10's stated budget
of "two re-parses, well under a second." Opening the viewer was now paying
**three** independent rrrocket subprocess calls for the same file — one per
route, each running the full `build_replay_frames` (parse + reshape) from
scratch. Separately, all three routes were `async def` handlers calling
blocking `subprocess.run` directly: on uvicorn's default single-worker /
single-event-loop setup, each ~250-290ms rrrocket call froze the *entire*
server — every user, every page — not just the requesting viewer's own
request.

**Why not the decision-10 LRU escape hatch instead.** An in-process cache
would cut the rrrocket calls back down without touching the transport split,
but it buys less than merging: it needs invalidation (on `--force`
reprocessing) and a memory bound, for a saving the merge gets for free by
deleting two-thirds of the redundant work outright, with no new state. Nothing
decision 8 protected turns out to be load-bearing today: neither `.bin` route
ever sent cache validators (`Cache-Control: no-store` on both), so no browser
HTTP caching was ever in play; and `replay.js` always awaited all three
responses before building the scene, so there was no progressive-rendering
benefit to losing either. The one real cost is that the meta JSON is no longer
independently curl-able / pretty-printable in devtools — `window.__replay.meta`
under `?debug` covers that in practice.

**New shape.** `GET /api/matches/{id}/replay` (same URL, no longer JSON) now
returns one `application/octet-stream` envelope built by
`replay_view.serialize_replay_envelope()`: a 12-byte header of three
little-endian `uint32` lengths (`positionsLen`, `boostLen`, `metaLen`), then
the positions buffer, then the boost buffer, then the meta JSON bytes.
Positions is placed right after the fixed-size header — not after meta — so
its `Float32Array` view stays 4-byte-aligned regardless of the JSON's byte
length; `metaLen` lets the decoder assert the buffer's total length rather
than trusting an implicit remainder. `decodeReplayEnvelope()` in
`static/replay-core.js` is the client-side counterpart (pure, zero-import,
covered by `tests/js/replay-core.test.js`). `/replay-frames.bin` and
`/replay-boost.bin` are deleted.

The remaining blocking-call problem is fixed independently: the route now
offloads only `replay_view.build_replay_frames()` — not the DB lookup ahead of
it — to a worker thread via `asyncio.to_thread`, so a replay parse no longer
stalls other requests.

See CONTEXT.md "Replay Wire" for the current (post-merge) contract.
