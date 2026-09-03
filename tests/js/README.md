# Front-end tests

The replay viewer (`static/replay.js`) has no build step and no npm runtime
deps. Its playback + timeline math lives in `static/replay-core.js` (THREE-free,
DOM-free), a real production module `replay.js` imports — so it can be tested
directly, and testing it tests what ships.

## Layers

| Suite | Runner | Command | Needs |
| --- | --- | --- | --- |
| `tests/js/*.test.js` | Node's built-in test runner | `mise run test-js` | node (via mise) |
| `tests/e2e/*.spec.js` | Playwright / headless Chromium | `mise run test-e2e` | `npm ci` + `npx playwright install chromium` |

`tests/e2e/serve_fixture.py` serves the real FastAPI app against a temp DB built
from the one committed replay, ingested as **match 1**.

## What each layer covers

`tests/js/replay-core.test.js`:

- **Example tests** — `bracket`, `slotLiveAt`, `formatClock`, `countdownLabelAt`,
  `outlineHalfWidth`, and `createTransport` (the real ↔ compressed-seconds
  mapping, dead-span edge resolution, `snapForward`), each against small
  hand-built inputs with asserted outcomes.
- **`writePoses` branch tests** — linear position blend, slerp'd (not lerp'd)
  orientation, demolition gap (both ends dead → hidden; one end dead → snap to
  the live end), trail head/walk-back/segment-edge stop, `TRAIL_FRAMES` cap.
- **Property tests over the real buffer** — `tests/data/replay-viewer/{meta.json,
  frames.bin}` fed in as *input* (not a golden oracle): poses finite,
  quaternions unit-norm, no teleports (skipping intervals that straddle a slot's
  own segment edges — those are deliberate cuts), visible iff a bracket end is
  live, trail head == live position.

`tests/e2e/replay.spec.js`:

- transport/UI smoke — boot, clock format, goal ticks, play/pause, ArrowRight
  seek, kickoff countdown overlay, scoreboard tally
- **parity** — `applyPoses()` on the live page writes *exactly* (delta 0) what
  `writePoses()` produces for the same inputs. This guards against `applyPoses`
  growing a second, diverging pose computation (see `docs/adr/0004-*`); an
  identical re-inlining would still pass. It gives no coverage of whether the
  math is *correct* — that is the example tests' job.

Not covered (the agreed "transport second" gaps): speed buttons, scrub-bar
drag, camera presets, name toggle. Add as needed.
- **slerp parity** — the verbatim `slerpQuat` port still matches
  `THREE.Quaternion.slerp` in the THREE build the page loads.

## Fixtures

`tests/data/replay-viewer/{meta.json, frames.bin}` is server output
(`replay_frames.py`). Regenerate with `uv run python tests/e2e/dump_fixture.py` when the
meta shape or packed-buffer format changes, then eyeball `mise run test-js`. The
property tests assert invariants, not specific values, so mild drift is
harmless.

There is deliberately **no committed golden pose-trace**: a recording of
`writePoses()`'s own output would be a circular oracle, and pinning it to ~1e-4
across Chromium versions is fragile. A one-shot uncommitted trace is used only as
a safety net when refactoring the core (capture from a known-good build, refactor,
diff, discard).
