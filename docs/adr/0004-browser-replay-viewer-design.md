# ADR-0004: Browser replay viewer — design

Status: **accepted; implemented.** (Formerly `docs/replay-viewer.md`, a working
design doc during the build; converted to an ADR once v1 shipped.)

A ballchasing-style tactical replay viewer at `/match/{id}/replay`: a
whole-field angled/orthographic 3D view of car and ball movement, built from the
per-frame positional data that `rrrocket -n` already produces. It is an
**analysis tool** for spotting the tracked team's positional habits across many
matches, not a game-like re-render.

Feasibility was never in question: the data already exists (`frame_analysis.py`
parses every frame's `RigidBody` state and discards it), the parse is ~0.1 s,
and there are working reference implementations on the same input.

## Prior art

- **ballchasing.com** — solo-built, proprietary, no public code. Server-side
  network-parses each uploaded `.replay` into per-frame positions, then renders a
  deliberately *un*-game-like WebGL scene: field outline, simple car markers,
  ball, boost pads, motion trails, whole field visible at once. It explicitly
  does not reproduce the game camera. **This is the model we copied.**
- **calculated.gg** — the opposite choice: full 3D game-like replication with
  ripped field/car models (`SaltieRL/WebReplayViewer`, MIT, React + Three.js).
  Org dormant since 2023. Useful only as an interpolation / camera reference.
- **foppage/replay-viewer** (Svelte) and **Longi94/rocket-viewer** (Three.js,
  boxcars-WASM) — both consume essentially the `rrrocket -n` JSON we already
  produce. Closest architectural precedents.

## Decisions

| # | Decision | Choice |
|---|----------|--------|
| 1 | Purpose / fidelity | Ballchasing-style tactical overview; low-poly shapes only, no ripped models or textures. An analysis tool, not a toy. (Cars gained a composed low-poly body in a later pass — decision 14 — still procedural, still no assets.) |
| 2 | Frame-data source | Re-parse the stored `.replay` on demand at view time. No persistence, no migration, no backfill. |
| 3 | UI placement | Dedicated route `/match/{id}/replay` + its own static HTML/JS/CSS, linked from the match detail page. |
| 4 | Rendering tech | Three.js with primitive meshes (low-poly cars, sphere ball, wireframe arena), all built in code. Orthographic/high camera so it reads like ballchasing. Keeps the Z axis — height and aerials matter. |
| 5 | Server extraction | New `replay_frames.py` — a pure reshape function over a `ParsedReplay`, plus a **non-deleting** rrrocket wrapper. Not routed through `queries.py` (not a DB read) and not through `analyze_frames` (not the aggregate pipeline). |
| 6 | Player identity | Full identity in v1: each car labelled with the player's name, coloured tracked-team vs opponent. Reuses the `car → PRI → (platform, id)` chain from `frame_analysis.py`. |
| 7 | v1 feature scope | See below. |
| 8 | Transport | Hybrid: small JSON metadata doc (like the other `/api` routes) + a separate packed `Float32` `.bin` of positions. One-shot, server gzip. No chunking. |
| 9 | Three.js delivery | No importmap. `static/replay.js` (external module, same-origin `'self'`) imports Three + `OrbitControls` + `RoomEnvironment` by full URL from jsdelivr's `/+esm` endpoints (`three@0.170.0/+esm`, `.../examples/jsm/controls/OrbitControls.js/+esm`, `.../examples/jsm/environments/RoomEnvironment.js/+esm`), which pre-resolve all bare specifiers. All three loads are covered by the existing `script-src cdn.jsdelivr.net` — **no inline importmap, no CSP change.** |
| 10 | Caching | None in v1. Measured first (`extract_replay_frames` ~40 ms, rrrocket + parse well under a second); add an in-process LRU only if click-to-view latency annoys. Never an on-disk sidecar (that is the persistence layer rejected in #2). |
| 11 | Arena / mode coverage | Standard soccar arena only (covers 3v3, 2v2, and any other soccar-arena mode). Hoops/Dropshot matches get **no** viewer link in v1. Positions in the `.bin` are mode-agnostic, so a second arena is purely additive later. |
| 12 | Orientation | Normalise: the tracked team always attacks the same on-screen direction, every match, regardless of which colour they were. |
| 13 | Link availability & failure | Show "Watch replay" only when `matches.replay_filename` is set and `replays/<that name>` exists; route 404s otherwise. On a view-time parse failure, return an error payload the page renders — and **never delete the file** (unlike the ingest path). |
| 14 | Car shape | A composed low-poly battle car, not a bare box (`createCar` / `buildCarModel`): an extruded curved hull + tinted canopy, graphite aero (splitter, diffuser, skirts, wing, vents), chrome boost nozzles with emissive cores, emissive headlights, and four spoked wheels with body-colour fender flares. Inlined from a Claude-designed three.js model (`battle-car.js`), kept close to source so a re-export stays diffable — only the `<three-d-stage>` harness is dropped and the paint swapped for the team tint (hull + flares); the designed graphite / tinted glass / chrome / rubber are kept. The model's `MeshStandardMaterial` look is kept too: `buildScene` bakes a `RoomEnvironment` PMREM into `scene.environment` (`environmentIntensity` 0.6) so the metals don't render near-black; the Lambert ball and line arena ignore it. The model is authored +X forward / +Y up / +Z lateral, so `buildCarModel` turns it +90° about X (drops +Y onto RL's +Z-up, nose stays on +X — a proper rotation, no mirror), scales it ×`CAR_SCALE` and drops the wheels `CAR_DROP` (17 uu, measured from a grounded car's pose z) below the pose origin, nudged `CAR_NOSE_BIAS` forward. Still procedural, no assets; the one new CDN import (`RoomEnvironment`, same jsdelivr `/+esm` origin) is covered by the existing `script-src`. **Why:** a symmetric box gave no read on which way a car faced — the one thing a positional-habits tool most needs; successive hand-authored lofts never got past "generic car", so a ready-made model was dropped in instead. `CAR_SCALE` is pinned on **width**: the wheel track comes out ~85 uu ≈ the 118×84×36 hitbox's 84, with the ~135 uu length (~1.1×) and ~42 uu roof (~1.2×) the slight overhang a real RL body has over its collision box. |

### v1 feature scope (decision 7)

**In:** play / pause, draggable scrub bar, speed control (0.5×–4×), one animation
clock · 2 camera presets (broadcast-high, top-down) + drag orbit/zoom · motion
trails · goal markers on the scrub bar + a running scoreboard · actor lifecycle
(a demoed car's mesh is hidden while its actor is out of the frame stream, so it
does not teleport on respawn).

**Deferred:** shot / save / demolition markers (need the `PRI_TA:Match*` counter
port from `MatchEventsHandler`) · an in-match game clock (needs the
`SecondsRemaining` regulation/OT reconstruction) · boost amounts + pad respawn
timers · per-player POV · follow-cam · goal-replay cutaways · heatmaps ·
Hoops / Dropshot arenas · any caching. The scrub-bar clock shows elapsed /
total on the dead-time-compressed axis (see "Dead-time remap" below).

## Architecture

### Server

```
GET /match/{id}/replay                   -> replay.html   (404 if no file on disk)
GET /api/matches/{id}/has-replay         -> {"has_replay": bool}   (cheap: 1 query + stat, no parse)
GET /api/matches/{id}/replay             -> metadata JSON: frame_times, slots, tracked_team, game_mode, goals, countdowns, dead_periods  (404 no file / 422 unparseable)
GET /api/matches/{id}/replay-frames.bin  -> Float32 position buffer, octet-stream
```

`has-replay` exists so `match.js` can gate the link without paying an rrrocket
parse on every match-page load. `GZipMiddleware` (`minimum_size=1024`) compresses
the metadata array and the buffer on the wire. `422` covers "file is there but
rrrocket failed or the replay has no network data" — the page renders that as an
error rather than a dead viewer. The meta and `.bin` routes each re-parse the
replay (~250 ms rrrocket + ~40 ms reshape); opening the viewer costs two parses,
within decision 10's budget. The route glue lives in **`replay_view.py`**
(`replay_path_for`, `build_replay_frames`), keeping `server.py`'s closures thin.

- **`replay_frames.py`** — the reshape layer. Its core is a pure function whose
  signature mirrors `analyze_frames`:

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

  It does its own lean single pass over `replay.frames` (it does **not** call
  `analyze_frames`; decision 5). It reuses `IdentityResolver` and
  `from_network_frame` for the `car → PRI → identity` chain and `NetObj` for
  name→object-id resolution.

- The route layer uses **`process.run_rrrocket()`** — a non-deleting sibling of
  `parse_replay()` (`parse_replay` calls it and unlinks on failure itself).
  `replay_view.build_replay_frames` builds the `extract_replay_frames` inputs via
  `ingest.build_replay_context()` — the shared owner of the pre-frame preamble
  (perspective, game mode, name map; see CONTEXT.md "Replay Context") — and
  unpacks its fields, still **without** running `analyze_frames`.

- **Match id → `.replay` path.** Replay files are named by the uploader
  (`secure_filename(file.filename)`), *not* by `MatchGUID`, and
  `matches.replay_hash` stores the `MatchGUID` — the two never match, and there
  is no other on-disk link. The nullable `matches.replay_filename` column
  (migration 020, the source basename) is populated by ingest and was backfilled
  via `uv run python process.py --force`. The viewer resolves a match to its file
  with `queries.match_replay_filename(match_id)` then `replays/<that name>`.

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
```

The JSON metadata endpoint serialises everything except `positions`; the `.bin`
endpoint returns `positions`.

### Extraction rules (the lean frame walk)

- **Phase ordering is preserved** from `_process_frame`: new_actors →
  updated_actors → deleted_actors, within each frame. A car spawned and
  PRI-linked in the same frame must keep its link.
- **Every frame is recorded.** Unlike every `frame_analysis` handler, the walk
  does **not** gate on `ctx.is_playing` — decision 7 shows the full stream
  (warmup, countdowns, celebrations). The client keeps the countdowns but
  collapses the post-goal dead spans out of the transport (`dead_periods`).
- **Actor lifecycle.** At every **goal** kickoff the ball and *every* car are
  deleted (`deleted_actors`) and re-created with a fresh network actor id the
  next frame, at the kickoff spawn — one real despawn/respawn per goal, on every
  lane at once. The *initial* kickoff is just the actors' first appearance (no
  delete). Demolitions also delete, per victim. Separately, a still-open actor is
  **re-announced** in `new_actors` with *no* preceding `deleted_actors` every
  ~10 s (a mid-field keep-alive, not a kickoff) — that one is a position append,
  not a new life.
- **Segments.** A lane opens a segment when an id first appears and closes it on
  a real `deleted_actors` entry (or at the last frame). A bare re-announcement of
  an already-open id (the ~10 s keep-alive) does **not** open a new segment — it
  appends a sample and marks `_Segment.resets`. So a lane carries **one segment
  per kickoff** (goals + 1), merged back into a single car slot by identity, plus
  an extra split per demolition with a gap the client hides the mesh across.
- **Slots keyed by identity.** All segments whose closing `resolve_car` gives the
  same `(platform, id)` merge into one car lane. A car whose PRI never resolves
  (bots, odd platforms) gets its **own anonymous lane**, one per segment. A
  resolved identity with no entry in `player_names` (a leaver missing from the
  `PlayerStats` blob) falls back to `"Player N"` rather than raising.
- **Fill.** `positions` is dense: each frame holds a full pose for every lane.
  Frames outside every segment stay zero (the client hides the lane there).
  Within a segment, a frame with no `RigidBody` sample is **interpolated**
  between the two real samples bracketing it — linear on position, slerp on
  rotation, weighted by wall-clock time (`frame_times`) — so a held actor glides
  rather than freezing then lurching to the next sample (`_densify`,
  `_slerp`, `_keyframes`, `_lerp_pose`). Two runs hold the previous pose
  instead: the frames up to a bare re-announcement (`_Segment.resets` — the
  ~10 s keep-alive; a cut, not motion, so the lane must not drift toward the
  re-announce point) and the tail after a lane's final sample. Seed a segment
  from its `initial_trajectory.location`
  (its Euler `yaw/pitch/roll` often has `null` components — unusable as a
  quaternion); rotation before the first real sample is **held back** from that
  sample rather than slerped up from identity, which also removes the
  identity-quaternion flick at spawns. A `RigidBody` update may omit `rotation` /
  velocities — carry the previous quaternion forward, or identity if none seen
  yet.

## Client (`static/replay.js`, module script)

The page fetches `/replay` + `/replay-frames.bin`, builds the scene, and plays it
back. Coordinates stay in unreal units — no world scaling; the camera frustum
does the framing. The `?debug` query flag exposes `window.__replay = { playback,
meshes, camera, controls, renderer, scene, THREE }` plus a rolling frame-time
HUD (`createDebugHud`). An earlier note here claimed headless Chromium
`page.screenshot()` returns black for a WebGL canvas — that held for the old
SwiftShader `--headless`, but `--headless=new` (Chrome's default since 112)
composites WebGL through ANGLE and `page.screenshot()` via the Playwright MCP
captures the viewer correctly. `gl.readPixels` after a paint is in fact the less
reliable check: with `preserveDrawingBuffer` unset the buffer is cleared post
composite, so a later read returns zeros while the screenshot is fine.

- **Scene / axes.** Wireframe soccar arena (8192 × 10240 × 2044 uu), a low-poly
  battle car per slot (`buildCarModel`, decision 14 — a `THREE.Group` wrapping
  the `battle-car.js` model, turned +90° about X from its authoring basis into
  RL local axes, scaled to the hitbox and dropped so the wheels sit 17 uu below
  the pose origin), sphere ball. RL is Z-up, Three.js is Y-up: everything lives
  in a parent `world` `Group` with `rotation.x = -π/2`, so RL `(x, y, z)`
  renders at world `(x, z, -y)`.
- **Playback.** `createPlayback(meta, positions, meshes)` owns a clock: `state.t`
  in `frame_times` space (starts at `frame_times[0]`, not 0), advanced each rAF
  by `realDeltaSeconds * speed` while playing, clamped and auto-paused at the
  end. `bracket(times, t)` binary-searches for `[i, j, f]`; per slot the pose is
  `lerp` on position + `slerp` on quaternion between frames `i` and `j`, so
  motion is smooth between the ~30 Hz samples and the non-uniform ~33 ms deltas
  are handled by using the real timestamps, not a fixed step. Transport:
  play/pause (also space), draggable scrub (`<input type=range>` 0–1000,
  paused-aware so the loop doesn't fight the drag), `M:SS / M:SS` clock,
  0.5×/1×/2×/4× buttons, arrow keys seek ±5 s.
- **Dead-time remap.** `state.t` stays in real `frame_times` seconds (so
  `bracket`/`applyPoses` are untouched), but `createPlayback`'s **public axis is
  compressed** — every `meta.dead_periods` span removed. Each `[startFrame,
  endFrame]` becomes a real interval `[frame_times[start], frame_times[end + 1])`
  with `.c` = its compressed coordinate; `toCompressed(t) = (t - t0) - Σ cuts
  before t` and `toReal` inverts it, resolving a seam to the span's *end* (the
  resume frame) so a seek can't land in a gap. `seek(c)` / `nudge(Δ)` /
  `elapsed()` / `duration()` / `progress()` / `fractionAt(realT)` are all
  compressed; only `advance` steps raw real time, so only it snaps forward out of
  a span. The playhead starts at `toReal(0)` — past the pre-match warmup. Always
  on; no toggle.
- **Lifecycle hiding.** `slotLiveAt(slot, frame)` scans the slot's inclusive
  `[start, end]` segments. In `applyPoses`, for a slot's bracket `[i, j]`: hidden
  when neither `i` nor `j` is live; otherwise `ff` is snapped to the live end
  (`0` if `j` is the dead one, `1` if `i` is) so the mesh never lerps toward the
  zero-pose of a frame the actor isn't in. A long gap (a real ~3 s demolition ≈
  90 frames) stays hidden throughout; the 1–2-frame kickoff-reset gaps just hold
  at the boundary pose rather than flicker.
- **Labels + orientation (decision 12).** Each car carries a `THREE.Sprite` name
  tag (`makeLabelSprite` draws `slot.name` to a canvas texture in the car's
  colour, `depthTest: false`); `applyPoses` parks it `LABEL_HEIGHT` uu above the
  car and hides it with the car. `main` awaits `document.fonts.ready` so the tag
  renders in DM Mono. The `.bin` stays raw; arena + actors + labels live in an
  inner `field` group inside the axis-remap `world` group, and
  `field.rotation.z = π` when `meta.tracked_team === 1` — a 180° spin in RL's
  horizontal plane. Net effect for both tracked configurations: the tracked team
  defends world **+z** (near the camera) and attacks toward **−z**. Sprites
  ignore parent rotation, so label text is not mirrored.
- **Trails.** `makeTrail(colorHex)` builds one `THREE.Line` per actor (ball
  included) with a `TRAIL_FRAMES + 1` (~1.5 s) `BufferGeometry`: colour baked
  once from the actor's colour to the background, `frustumCulled = false`. Per
  frame in `applyPoses`: vertex 0 = the live interpolated position, then walk
  backward through the position buffer while `slotLiveAt` holds (so a trail never
  jumps across a demolition gap), and `setDrawRange(0, count)`.
- **Camera.** One `THREE.OrthographicCamera` + `OrbitControls` (drag orbit/zoom,
  damping). `CAM_PRESETS` holds `broadcast` (default: a 3/4 isometric corner view
  — offset toward `+x` and the tracked team's end, ~30° elevation, `size` 10200
  uu) and `top` (straight overhead, `camera.up` set to `(0, 0, -1)` so the far
  goal is screen-up, `size` 12800). `applyCamPreset` sets
  position/target/up/`viewSize` and re-runs `resize()` — the frustum height is a
  mutable `viewSize`, not a const. Preset buttons overlay the stage top-right;
  the active one clears on the `OrbitControls` `start` event.
- **Goals + scoreboard.** `replay_frames._scan_goals` does a second light pass
  over the frames watching `ReplicatedScoredOnTeam`: the `Byte` is the team
  *scored on* (0/1), re-sent a few times per goal and reset to 255 between, so a
  goal is each rising edge into {0, 1}, and `GoalMarker{frame, team = 1 - byte}`
  lands in `ReplayFrames.goals` (→ the `/replay` `goals` array). Using the frame
  index maps straight to `frame_times` — no game-clock inversion. Client:
  `renderScrubMarks` places a colour-coded tick per goal on a `.replay-marks`
  overlay above the range input (cyan = tracked team, red = opponent), and
  `syncUI` counts goals with `time <= playback.t` into a `our – opp` scoreboard
  in the top bar.
- **Kickoff countdown.** `replay_frames._scan_countdowns` records every
  `ReplicatedRoundCountDownNumber` tick as `(frame, n)` — one `3 → 2 → 1 → 0`
  run per kickoff (pre-match, each goal, OT), `n == 0` being the frame live play
  resumes (`frame_analysis` flips `is_playing` there). `_scan_goals` +
  `_scan_countdowns` also yield `ReplayFrames.dead_periods` — the frame spans the
  viewer collapses (see the timeline-remap client note). Client:
  `countdownLabelAt(frame_times, meta.countdowns, t)` returns the numeral (or
  `"GO!"` for the ~0.6 s after `n == 0`, else null) and `syncUI` writes it to the
  `.replay-countdown` overlay only on change — a large centred numeral so a still
  kickoff does not read as a frozen viewer. Playback is untouched; the overlay is
  pure presentation.
- **Arena schematic.** `buildArena` draws the chamfered soccar footprint (all
  four corners cut at 45°, `CORNER = 1152` uu per axis, `|x| + |y| = 8064`) as
  the module-level eight-point `ARENA_OUTLINE` — floor loop + ceiling loop + a
  vertical per corner — with a floor grid clipped to that octagon. `buildGoals`
  draws an open wireframe box at `y = ±5120` (mouth `1786 × 643` uu + a matching
  frame `880` uu back + four depth edges, bright-neutral) with the mouth filled
  by a translucent plane in the defending team's colour. `buildHalfTint` washes
  each half of the octagon (split at `y = 0`) a faint `0.10` in the defending
  team's colour — the cue that carries the TOP view, where the goal fills are
  edge-on. `buildBoostPads` draws the 34 standard soccar pads (6 big + 28 small,
  coords from wiki.rlbot.org; the six big ones match
  `frame_analysis.BIG_PAD_POSITIONS["standard"]`) as flat `CircleGeometry` discs
  in a dim grey-gold — static furniture, no collect/respawn state. All of this
  arena geometry is parented to `field` so it rides the orientation flip.
  `teamTint(team, trackedTeam)` is the shared colour rule (`carColor` delegates
  to it).

## Known wrinkles

(Wrinkles 1–2 — inline importmap vs CSP, and the two time bases — were resolved
during implementation: see decision 9, and the goals-from-frame-index approach
above.)

3. **`process.py` deletes `.replay` on parse failure.** The view path must not
   (decision 13).
4. **Frame-stream gaps** during countdowns and goal explosions — the client
   holds last state / interpolates across; no special handling server-side.
5. **Non-uniform deltas** — the clock integrates real frame deltas, it never
   assumes a fixed step.
6. **Little-endian buffer.** `array.array('f').tobytes()` writes native byte
   order; JS `Float32Array` reads little-endian. True on every target platform;
   recorded here so it is an assumption, not an accident.
7. **Anchor arena geometry by RL team, colour by `teamTint`.** All static field
   geometry lives inside the `field` group, which is rotated 180° when
   `tracked_team === 1` (decision 12). So place a half / goal by its raw RL side
   (team 0 defends `−y`, team 1 defends `+y`) and colour it with
   `teamTint(team, trackedTeam)` — never by "screen side". Hardcoding "ours = the
   −y half" renders correctly for one `tracked_team` value and mirror-wrong for
   the other.

## Assumptions

- Desktop-first; no mobile/touch tuning in v1.
- The read endpoints follow the same auth posture as the existing
  `/api/matches/{id}`.
- The full frame stream is shown (warmup + celebrations); the user scrubs past.
