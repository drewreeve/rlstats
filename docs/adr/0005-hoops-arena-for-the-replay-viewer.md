# ADR-0005: Hoops arena for the replay viewer

Status: **accepted; implemented.** Follow-up to ADR-0004, whose decision #11
flagged the exact gap this closes: the viewer draws every replay in the standard
soccar arena, so a Hoops match with a replay on disk opens the viewer in the
wrong field.

The position `.bin` is already mode-agnostic (world coords, no arena assumptions),
and `game_mode` (`"3v3" | "2v2" | "hoops"`) is already detected server-side
(`ingest.detect_game_mode`) and serialised to the client as `meta.game_mode`
(`server.py` → `match_replay_meta`). The render side was expected to be **purely
additive** — but implementation found `extract_replay_frames` never produced any
frames for a hoops replay (the ball actor is named `Ball_BasketBall`, not
`Ball_Default`), so the viewer 404'd before the arena mattered. Decision #10
covers that fix; it's the one non-render change.

The model stays ADR-0004's: a ballchasing-style tactical overview, low-poly
procedural shapes only, no ripped assets. The hoop shape in particular is copied
from how ballchasing renders it — a clean single-stroke rim + shallow basket,
outline only, one accent colour, nothing drawn across the goal opening (cars
drive *under* a hoops rim constantly; a translucent fill would sit between the
camera and the play).

## Decisions

| # | Decision | Choice |
|---|----------|--------|
| 1 | Branch mechanism | A pure `arenaSpec(gameMode)` descriptor in `static/replay-core.js` (zero imports, unit-tested — same rationale as ADR-0004 #15). `replay.js`'s arena builders take the spec. **No `arena.js` extraction** — moving the working soccar builders into a new THREE-importing sibling module is orthogonal cleanup that inflates this diff without buying testability (the builders need THREE to run wherever they live; the testable part is the spec data, which this isolates). Do that extraction separately if `replay.js`'s size warrants it. |
| 2 | Unknown-mode fallback | Only the exact string `"hoops"` selects the hoops spec. `null`, `"3v3"`, `"2v2"`, and anything unrecognised → the standard spec (deep-equal to today's hardcoded values). |
| 3 | Hoops footprint | Chamfered octagon like soccar — the existing octagon math (`ARENA_OUTLINE`, `outlineHalfWidth`, grid clip, half-tint rings) generalises; it just takes hoops extents. Side walls X ±2966.67, back walls Y ±3581, ceiling Z 1820, corner cut ≈ 766 uu (from the wiki's ±5782 diagonal-wall intersection: 2966.67 + 3581 − 5782). |
| 4 | Hoops goal shape | Team-tinted **semicircle rim** + team-tinted **shallow basket** + **chord and short arms** to the wall. No backboard plane (the wall is right there). **No fill disc.** See "Hoop construction" below. |
| 5 | Half-tint & floor grid | Keep both, sized to the hoops footprint. Half-tint: the same chamfered-ring split at y = 0, hoops outline. Floor grid: simplified to a 2×2 division (centre + halfway lines) rather than soccar's 4×3 rotation guide, clipped to the hoops octagon. The separately-drawn brighter centre line stays. |
| 6 | Overview camera | `overviewSize` derived from the spec extents via one formula for both modes — `2 · max(halfX, halfY + goalClearance) · MARGIN`, `MARGIN ≈ 1.06`, `goalClearance` = 880 soccar / 0 hoops (the hoops rim sits inside the field). Verify the soccar result still lands ≈ 12800 (today's `OVERVIEW.size`). |
| 7 | Goal-celebration FX | Reuse as-is; the burst already fires at the ball's real position on the goal frame. Only swap the `GOAL_H`-derived spread scalar for `spec.goal.fxScale` (643 soccar, ~500 hoops). Hoops-specific FX tuning deferred. |
| 8 | Ball radius | Per-mode via `spec.ballRadius`: 98.38 hoops, 91.25 soccar. The ball surface/seams are already a deliberate detail (ADR-0004 era); get the size right too. |
| 9 | Tests | Unit-test `arenaSpec` in `tests/js/replay-core.test.js` (pure, fixture-free — see "Test plan"). Data path: a `test_replay_frames.py` integration pass over `tests/data/hoops.json`. Rendered path: a committed 2v2 hoops `.replay` is ingested as **match 2** by the Playwright fixture server, and `tests/e2e/replay.spec.js` asserts the hoops footprint / ring goals / 20-pad layout are built and the frame renders. |
| 10 | Ball archetype | `replay_frames._ball_archetype_oid()` resolves the ball actor from a list of known archetype names (`Ball_Default` soccar, `Ball_BasketBall` hoops) instead of the single hardcoded `NetObj.BALL_ARCHETYPE`. The frame walk is otherwise unchanged. `frame_analysis.py` keeps the `Ball_Default` assumption for its positional stats — out of scope; hoops match result/MVP come from the `PlayerStats` blob, not the frame walk, so ingest was never blocked. |

## `arenaSpec` shape

`arenaSpec(gameMode)` returns a frozen descriptor. Proposed fields (final names
at implementation):

```
{
  mode:          "standard" | "hoops",
  halfX, halfY,   // side-wall / back-wall half-extents (uu)
  ceiling,        // Z (uu)
  corner,         // 45° chamfer cut (uu): 1152 standard, ~766 hoops
  ballRadius,     // 91.25 standard, 98.38 hoops
  goalClearance,  // uu the goal extends past halfY: 880 standard, 0 hoops
  overviewSize,   // or computed by the caller from the fields above
  grid:  { cols, rows },          // 3×4 standard, 2×2 hoops
  bigPads:   [[x, y], ...],       // 6 standard, 6 hoops
  smallPads: [[x, y], ...],       // 28 standard, 14 hoops
  goal: {
    kind: "box",                  // standard
    halfWidth, height, depth,     //   1786-mouth box behind the wall
    fxScale,
  } | {
    kind: "ring",                 // hoops
    centreY,      // |y| of the rim centre: 2969
    z,            // rim height: 364
    radius,       // 655
    basketDrop,   // ~175  (basket arc below the rim)
    basketInset,  // ~120  (basket arc pulled toward the wall)
    basketRadius, // ~390
    fxScale,      // ~500
  },
}
```

`outlineHalfWidth(x, y)` currently closes over the module-level standard
`HX/HY/CORNER`. It must be parameterised (take the spec or an
`{halfX, halfY, corner}` triple) — a breaking change to an exported, tested
function, so its `replay.js` call site (the grid clip) and its unit tests update
in the same change.

## Hoop construction (decision #4 detail)

All geometry for the `+y` goal; mirror through `y → −y` for the `−y` goal. "Pitch
side" is toward `y = 0`. Everything at rim height `z = 364` unless noted.

- **Rim** — a semicircle arc, radius 655, centre `(0, 2969, 364)`, spanning the
  180° that bulges toward the pitch (chord along x at `y = 2969`, apex at
  `y = 2969 − 655 = 2314`). ~24-segment polyline. Defending team's tint, full
  opacity.
- **Chord** — one line across the flat (wall-facing) side: `(−655, 2969, 364)` →
  `(655, 2969, 364)`. Team tint, full opacity.
- **Arms** — two short lines from the chord ends straight back to the back wall:
  `(±655, 2969, 364)` → `(±655, 3581, 364)` (~612 uu each). Kills the "floating
  ring" look without a backboard. Team tint, reduced opacity.
- **Basket** — a second, smaller semicircle arc: radius ≈ 390, centre
  `(0, 2969 + 120, 364 − 175)` = `(0, 3089, 189)`, same 180° pitch-facing span.
  Joined to the rim by 3–4 short lines connecting matching arc points (e.g. the
  two ends + apex + quarter points). Team tint, reduced opacity.
- **No fill disc.** The end-identification cue is the tinted rim + basket, which
  stay legible from any angle including straight overhead (the rim reads as a
  half-circle stroke).

Rim/basket colour is `teamTint(defendingTeam, trackedTeam)` — the same call the
soccar goal fill uses — so the hoop follows the team-1 field flip
(`field.rotation.z = π`) with the rest of the arena.

## Boost pads

`buildBoostPads` already iterates `[[pads, radius], ...]`; it takes
`spec.bigPads` / `spec.smallPads`. Hoops has **20 pads** (6 big + 14 small) vs
soccar's 34.

- **Big (6)** — `(±2176, ±2944)`, `(±2432, 0)`. These match
  `frame_analysis.BIG_PAD_POSITIONS["hoops"]` exactly; cross-check against it so
  the two lists can't drift (they cite the same wiki source, as the soccar pair
  already do).
- **Small (14)** — from the RLBot wiki hoops page
  (`wiki.rlbot.org/v4/botmaking/hoops/`, the page `frame_analysis.py` already
  cites): `(0, ±2816)`, `(±1280, ±2304)`, `(±1536, ±1024)`, `(±512, ±512)`.

## Test plan (decision #9)

New `describe` block in `tests/js/replay-core.test.js`:

- `arenaSpec("hoops")` — `halfX ≈ 2966.67`, `halfY === 3581`, `ceiling === 1820`,
  `corner` ≈ 766, `ballRadius === 98.38`, `goal.kind === "ring"`,
  `goal.radius === 655`, `goal.z === 364`.
- `arenaSpec("2v2")`, `arenaSpec("3v3")`, `arenaSpec(null)`, `arenaSpec("dropshot")`
  — all deep-equal the standard spec; `goal.kind === "box"`.
- `bigPads.length === 6` and equal (as a set) to
  `frame_analysis.BIG_PAD_POSITIONS["hoops"]`; `smallPads.length === 14`;
  `bigPads.length + smallPads.length === 20`.
- `outlineHalfWidth` with the hoops `{halfX, halfY, corner}` at a few sample
  points — on a flat wall, on the chamfer, at a corner vertex.
- The `overviewSize` formula for both modes — soccar ≈ 12800, hoops smaller,
  ratio ≈ the extent ratio.

Data path: `test_replay_frames.py` gains a `_hoops()` integration block over
`tests/data/hoops.json` — full buffer, one ball + four cars, the tracked pair,
goals/countdowns, poses inside the hoops footprint. Rendered path: the
`hoops arena` describe in `tests/e2e/replay.spec.js` (match 2).

## Follow-ups (out of scope here)

1. **Dropshot / Snowday arenas** — still fall back to soccar. Each is now one
   more `arenaSpec` branch (plus its own ball archetype in decision #10's list);
   ADR-0004 #11's gap is only *partly* closed.
2. **`arena.js` extraction** — deferred cleanup (decision #1).
3. **Hoops-specific goal FX** — vertical bias / ring-sized flash (decision #7).
4. **`frame_analysis.py` ball archetype** — its positional stats still assume
   `Ball_Default`, so hoops matches get no possession / zone numbers. Same
   fix shape as decision #10 if those are ever wanted for hoops.

## Files touched

- `static/replay-core.js` — `arenaSpec` + `STANDARD_SPEC` / `HOOPS_SPEC` +
  `chamferedOutline`; `outlineHalfWidth` takes an optional spec.
- `static/replay.js` — `buildArena` / `buildGoals` / `buildHalfTint` /
  `buildBoostPads` take `spec`; `buildScene` calls `arenaSpec(meta.game_mode)`
  and sets `camScale`; `buildGoals` splits into `buildBoxGoal` / `buildHoopGoal`
  on `spec.goal.kind`; the camera presets, ball radius and `GOAL_FX` flash scale
  read from the spec.
- `replay_frames.py` — `_ball_archetype_oid()` (decision #10).
- `tests/js/replay-core.test.js` — the `arenaSpec` block.
- `tests/test_replay_frames.py` — the hoops integration block.
- `tests/e2e/serve_fixture.py` — ingests both fixture replays, in order.
- `tests/e2e/replay.spec.js` — the `hoops arena` describe.
- `tests/data/D9C3347845961812E9817293F9886DDB.replay` — the 2v2 hoops fixture.
- `docs/adr/0004-browser-replay-viewer-design.md` — decision #11 now points here.
