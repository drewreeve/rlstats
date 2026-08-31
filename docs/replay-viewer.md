# Browser Replay Viewer — Design

Status: **in progress** (build sequence started 2026-08-31). Steps 1–4 done —
backend complete; the page renders the arena + actors and plays back on a
real-time clock (play/pause, scrub, 0.5×–4×). Step 5 onward: lifecycle hiding,
labels, trails, camera, event markers.

- Step 1 = `replay_frames.py`; measured on `tests/data/team_size_2.json` (a
  ~5-minute 2v2, 9113 frames), `extract_replay_frames` runs in ~40 ms and yields
  a ~1.2 MiB raw position buffer (5 lanes) — rrrocket + JSON parse on top is
  still well under a second, so decision 10 (no cache) holds.
- Step 1.5 = the `matches.replay_filename` column and backfill (see "Match id →
  `.replay` path"), without which a match cannot be resolved to its file at all.
- Step 2 = `process.run_rrrocket`, `replay_view.py`, and the four routes below.
- Step 3 = `replay.html` / `replay.js` / `replay.css`; Three.js via jsdelivr
  `/+esm` (no importmap needed — decision 9); frame-0 static render.

A ballchasing-style tactical replay viewer at `/match/{id}/replay`: a whole-field
angled/orthographic 3D view of car and ball movement, built from the per-frame
positional data that `rrrocket -n` already produces. It is an **analysis tool**
for spotting the tracked team's positional habits across many matches, not a
game-like re-render.

This document is the durable record of the design decisions and their rationale.
It was produced by a grilling session on 2026-08-31; the numbered decisions below
are the settled branches of that design tree.

## How comparable viewers work

- **ballchasing.com** — solo-built, proprietary, no public code. Server-side
  network-parses each uploaded `.replay` into per-frame positions, then renders a
  deliberately *un*-game-like WebGL scene: field outline, simple car markers,
  ball, boost pads, motion trails, whole field visible at once. It explicitly
  does not reproduce the game camera. **This is the model we are copying.**
- **calculated.gg** — the opposite choice: full 3D game-like replication with
  ripped field/car models (`SaltieRL/WebReplayViewer`, MIT, React + Three.js).
  Org dormant since 2023. Useful only as an interpolation / camera reference.
- **foppage/replay-viewer** (Svelte) and **Longi94/rocket-viewer** (Three.js,
  boxcars-WASM) — both consume essentially the `rrrocket -n` JSON we already
  produce. Closest architectural precedents.

Feasibility is not in question: the data already exists (`frame_analysis.py`
parses every frame's `RigidBody` state and discards it), the parse is ~0.1 s, and
there are working reference implementations on the same input.

## Settled decisions

| # | Decision | Choice |
|---|----------|--------|
| 1 | Purpose / fidelity | Ballchasing-style tactical overview; primitive shapes only. An analysis tool, not a toy. |
| 2 | Frame-data source | Re-parse the stored `.replay` on demand at view time. No persistence, no migration, no backfill. |
| 3 | UI placement | New dedicated route `/match/{id}/replay` + its own static HTML/JS/CSS, linked from the match detail page. |
| 4 | Rendering tech | Three.js with primitive meshes (box cars, sphere ball, wireframe arena). Orthographic/high camera so it reads like ballchasing. Keeps the Z axis — height and aerials matter. |
| 5 | Server extraction architecture | New `replay_frames.py` — a pure reshape function over a `ParsedReplay`, plus (step 2) a **non-deleting** rrrocket wrapper. Not routed through `queries.py` (not a DB read) and not through `analyze_frames` (not the aggregate pipeline). |
| 6 | Player identity | Full identity in v1: each car labelled with the player's name, coloured tracked-team vs opponent. Reuses the `car → PRI → (platform, id)` chain from `frame_analysis.py`. |
| 7 | v1 feature scope | See below. |
| 8 | Transport | Hybrid: small JSON metadata doc (like the other `/api` routes) + a separate packed `Float32` `.bin` of positions. One-shot, server gzip. No chunking. |
| 9 | Three.js delivery | **Simplified in step 3, no importmap.** `static/replay.js` (external module, same-origin `'self'`) imports Three + `OrbitControls` by full URL from jsdelivr's `/+esm` endpoints (`three@0.170.0/+esm`, `.../examples/jsm/controls/OrbitControls.js/+esm`), which pre-resolve all bare specifiers. Both loads are covered by the existing `script-src cdn.jsdelivr.net` — **no inline importmap, no CSP change.** |
| 10 | Caching | None in v1. Measure first; add an in-process LRU only if click-to-view latency annoys. Never an on-disk sidecar (that is the persistence layer rejected in #2). |
| 11 | Arena / mode coverage | Standard soccar arena only (covers 3v3, 2v2, and any other soccar-arena mode). Hoops/Dropshot matches get **no** viewer link in v1. Positions in the `.bin` are mode-agnostic, so a second arena is purely additive later. |
| 12 | Orientation | Normalise: the tracked team always attacks the same on-screen direction, every match, regardless of which colour they were. |
| 13 | Link availability & failure | Show "Watch replay" only when `matches.replay_filename` is set and `replays/<that name>` exists; route 404s otherwise. On a view-time parse failure, return an error payload the page renders — and **never delete the file** (unlike the ingest path). See "Match id → `.replay` path" below: the original `replays/<hash>.replay` premise was wrong. |

### v1 feature scope (decision 7)

**In:**

- Play / pause, draggable scrub bar, speed control (0.5× / 1× / 2× / 4×), one animation clock
- Camera: 2 presets (broadcast-high, top-down) + drag to orbit/zoom. No follow-cam, no player POV.
- Motion trails — short fading tail per car and the ball
- Event markers on the scrub bar — goals / shots / saves / demos, from `match_events.game_seconds`
- Scoreboard readout: score, game clock
- Actor lifecycle (spawn/despawn) — **mandatory**: a demoed car's actor leaves the
  frame stream for ~3 s; the slot's mesh is hidden while it is gone, so the car
  does not teleport on respawn

**Deferred (in rough priority order):** boost amounts + pad respawn timers ·
demolition effects · per-player POV · follow-cam feel · goal-replay cutaways ·
heatmaps · Hoops / Dropshot arenas · any caching.

## Architecture

### Server (steps 2 — done)

```
GET /match/{id}/replay                   -> replay.html   (404 if no file on disk)
GET /api/matches/{id}/has-replay         -> {"has_replay": bool}   (cheap: 1 query + stat, no parse)
GET /api/matches/{id}/replay             -> metadata JSON  (404 no file / 422 unparseable)
GET /api/matches/{id}/replay-frames.bin  -> Float32 position buffer, octet-stream
```

The grill settled on "three routes"; the fourth, `has-replay`, exists so
`match.js` can gate the link without paying an rrrocket parse on every match-page
load. `GZipMiddleware` (added in step 2, `minimum_size=1024`) compresses the
metadata array and the buffer on the wire. `422` covers "file is there but
rrrocket failed or the replay has no network data" — the page renders that as an
error rather than a dead viewer. The meta and `.bin` routes each re-parse the
replay (~250 ms rrrocket + ~40 ms reshape); opening the viewer costs two parses.
That's within decision 10's budget; the in-process LRU is the fallback if it ever
isn't. The route glue lives in **`replay_view.py`** (`replay_path_for`,
`build_replay_frames`), keeping `server.py`'s closures thin.

- **`replay_frames.py`** — the reshape layer. Its core is a pure function whose
  signature mirrors `analyze_frames`:

  ```python
  def extract_replay_frames(
      replay: ParsedReplay,
      *,
      tracked_team: int | None,
      tracked_identities: set[PlayerIdentity],
      player_names: dict[PlayerIdentity, str],   # display name preferred, else in-game
      game_mode: str | None,
  ) -> ReplayFrames
  ```

  It does its own lean single pass over `replay.frames` (it does **not** call
  `analyze_frames`; decision 5). It reuses `IdentityResolver` and
  `from_network_frame` from the existing modules for the `car → PRI → identity`
  chain, and `_resolve_obj_ids`'s pattern for name→object-id resolution.

- The route layer uses **`process.run_rrrocket()`** — a non-deleting sibling of
  `parse_replay()` extracted in step 2 (`parse_replay` now calls it and unlinks
  on failure itself). `replay_view.build_replay_frames` builds the
  `extract_replay_frames` inputs from `replay.properties` via the cheap helpers
  `resolve_perspective`, `build_player_stats`, `detect_game_mode` (un-privatised
  in step 1.5), **without** running `analyze_frames`.

- **Match id → `.replay` path** (corrected in step 1.5). Replay files are named
  by the uploader (`secure_filename(file.filename)`), *not* by `MatchGUID`, and
  `matches.replay_hash` stores the `MatchGUID` — the two never match, and there
  is no other on-disk link. Step 1.5 adds a nullable `matches.replay_filename`
  column (the source basename), populated by ingest and backfilled for the
  existing corpus via `uv run python process.py --force`. The viewer resolves a
  match to its file with `queries.match_replay_filename(match_id)` then
  `replays/<that name>`.

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
```

The JSON metadata endpoint serialises everything except `positions`; the `.bin`
endpoint returns `positions`.

### Extraction rules (the lean frame walk)

- **Phase ordering is preserved** from `_process_frame`: new_actors →
  updated_actors → deleted_actors, within each frame. A car spawned and
  PRI-linked in the same frame must keep its link.
- **Every frame is recorded.** Unlike every existing handler, the walk does
  **not** gate on `ctx.is_playing` — decision 7 shows the full stream (warmup,
  countdowns, celebrations); the user scrubs past dead time.
- **Actor lifecycle — what the frames actually look like** (established while
  building step 1, correcting an earlier guess). A car keeps *one* network actor
  id for the whole match. At every kickoff the replay **re-announces** that live
  actor in `new_actors` with a fresh `initial_trajectory` and *no* preceding
  `deleted_actors` entry — this is a position reset, not a new life. Genuine
  despawns (demolitions) are rare and *do* come through `deleted_actors`. In the
  fixture: 133 car `new_actors` across ~9 distinct ids, but only 5 deletes that
  hit a live car. The ball behaves the same way (re-announced each kickoff).
- **Segments.** A lane opens a segment when an id first appears and closes it on
  a real `deleted_actors` entry (or at the last frame). A re-announcement of an
  already-open id does **not** open a new segment — it just appends a positional
  sample at the kickoff spawn point. So a clean match is one segment per lane;
  each demolition splits a lane into two segments with a gap the client hides the
  mesh across. New lives from a true delete + later recreate of the same id also
  start a fresh segment.
- **Slots keyed by identity.** All segments whose closing `resolve_car` gives the
  same `(platform, id)` merge into one car lane. A car whose PRI never resolves
  (bots, odd platforms) gets its **own anonymous lane**, one per segment —
  without an identity its segments cannot be merged. A resolved identity with no
  entry in `player_names` (a leaver missing from the `PlayerStats` blob) falls
  back to `"Player N"` rather than raising.
- **Carry-forward.** `positions` is dense: each frame holds a full pose for every
  lane. Per lane, maintain the last-known `(x,y,z,qx,qy,qz,qw)` and re-emit it on
  frames where the actor did not update, within each of its segments; frames
  outside every segment stay zero (the client hides the lane there). Seed a
  segment from its `initial_trajectory.location` (note: `initial_trajectory`
  carries Euler `yaw/pitch/roll`, often with `null` components — unusable as a
  quaternion, so rotation is seeded to identity). A `RigidBody` update may omit
  `rotation` / velocities — carry the previous quaternion forward, or identity if
  none seen yet.

### Client (`static/replay.js`, module script)

**Steps 3–4 built the page:** `replay.html` / `replay.js` / `replay.css` (the
last two in `_VERSIONED_ASSETS`), the page fetches `/replay` +
`/replay-frames.bin`, builds the scene, and plays it back. Coordinates stay in
unreal units — no world scaling; the camera frustum (`VIEW_SIZE ≈ 9800 uu`) does
the framing.

**Playback (step 4).** `createPlayback(meta, positions, meshes)` owns a clock:
`state.t` in `frame_times` space (starts at `frame_times[0]`, not 0), advanced
each rAF by `realDeltaSeconds * speed` while playing, clamped and auto-paused at
the end. `bracket(times, t)` binary-searches for `[i, j, f]`; per slot the pose
is `lerp` on position + `slerp` on quaternion between frames `i` and `j` — so
motion is smooth between the ~30 Hz samples, and the non-uniform ~33 ms deltas
are handled by using the real timestamps, not a fixed step. Transport: play/pause
(also space), draggable scrub (`<input type=range>` 0–1000, paused-aware so the
loop doesn't fight the drag), `M:SS / M:SS` clock, 0.5×/1×/2×/4× buttons, arrow
keys seek ±5 s. **No lifecycle hiding yet** — a demolished car's buffer is zeros
during the gap, so it interpolates toward the origin and back until step 5.

Verification: WebGL framebuffer readback confirms the frame-0 render (white ball
centred, cyan/red cars in the kickoff ring, wireframe arena) and that seeking
moves meshes with sub-sample interpolation. **Headless Chromium
`page.screenshot()` returns black for a WebGL canvas** even with
`preserveDrawingBuffer` — verify via `gl.readPixels` or the `?debug` hook
(`window.__replay = { playback, meshes, THREE }`), not screenshots.

- Three.js scene: wireframe soccar arena (8192 × 10240 × 2044 uu), box cars,
  sphere ball. RL is Z-up; Three.js is Y-up — actors and arena live in a parent
  `Group` with `rotation.x = -π/2`, so RL `(x, y, z)` renders at world
  `(x, z, -y)`.
- **Orientation normalisation mechanism (decision 12):** the server emits **raw
  world coordinates** plus `tracked_team`. Team 0 attacks +y (per the
  stolen-boost comment in `frame_analysis.py`). When `tracked_team == 1` the
  client rotates the whole field group 180° about the vertical axis, so the
  tracked team always attacks the same way on screen. The flip is one transform
  on a parent group; the `.bin` stays raw.
- Cars coloured tracked-team vs opponent; each labelled with `slot.name`.
- Playback: one clock; `frame_times` drives a binary-search from elapsed seconds
  to a frame index; position is lerp'd and rotation slerp'd between the two
  bounding samples using the **real** (non-uniform, ~33 ms) delta.
- A slot whose current frame is outside all its `segments` is hidden.
- Trails: a short ring-buffer `BufferGeometry` per car and the ball.

## Known wrinkles (carried into implementation)

1. ~~**Inline importmap vs CSP.**~~ **Resolved in step 3 by not using an
   importmap** — see decision 9. `replay.js` is an external same-origin module
   that imports Three + OrbitControls by full jsdelivr `/+esm` URL; the existing
   `script-src` already allows it, no CSP change.
2. **Two time bases.** Frame `time` is seconds-from-replay-start; `match_events`
   are on the game clock. `MatchEventsHandler.finalize` already reconstructs
   game-clock seconds from `SecondsRemaining` (regulation counts down; OT resets
   to 0 and counts up). Reuse that logic to place scrub-bar markers (build
   step 9).
3. **`process.py` deletes `.replay` on parse failure.** The view path must not
   (decision 13).
4. **Frame-stream gaps** during countdowns and goal explosions — the client
   holds last state / interpolates across; no special handling server-side.
5. **Non-uniform deltas** — the clock integrates real frame deltas, it never
   assumes a fixed step.
6. **Little-endian buffer.** `array.array('f').tobytes()` writes native byte
   order; JS `Float32Array` reads little-endian. True on every target platform;
   recorded here so it is an assumption, not an accident.

## Assumptions (stated, not grilled)

- Desktop-first; no mobile/touch tuning in v1.
- The new read endpoints follow the same auth posture as the existing
  `/api/matches/{id}`.
- The full frame stream is shown (warmup + celebrations); the user scrubs past.

## Build sequence

Incremental commits, tests alongside each step.

| Step | Deliverable | Metadata fields that land here |
|------|-------------|-------------------------------|
| 1 ✅ | `replay_frames.py` — `extract_replay_frames` + `ReplayFrames`; unit tests against `tests/data/team_size_2.json` | `frame_times`, `slots` (identity/name/team/tracked/kind/segments), `positions`, `tracked_team`, `game_mode` |
| 1.5 ✅ | `matches.replay_filename` (migration 020) + ingest wiring + `queries.match_replay_filename` + backfill. Un-privatise `build_player_stats` / `detect_game_mode`. | — |
| 2 ✅ | `process.run_rrrocket` (non-deleting); `replay_view.py`; 4 routes (page + `has-replay` + meta + `.bin`); `GZipMiddleware`; `match.js` "Watch Replay" link; placeholder `replay.html`; 404 / missing-file / 422 tests | — |
| 3 ✅ | `replay.html` / `replay.js` / `replay.css`; Three.js via jsdelivr `/+esm` (no importmap, no CSP change); wireframe arena + box cars + sphere ball at frame-0 pose; orbit camera; no playback | — |
| 4 ✅ | `createPlayback` clock + `bracket` + lerp/slerp; transport bar (play/pause, scrub, `M:SS` clock, 0.5×–4×, space + arrow keys); `?debug` hook | — |
| 5 | Actor lifecycle + segment-based hiding | — |
| 6 | Player labels + team colours + orientation normalisation | — |
| 7 | Trails | — |
| 8 | Camera presets + orbit | — |
| 9 | Scrub-bar event markers + scoreboard | `events` (type + frame index), score timeline, per-frame game-clock seconds |
