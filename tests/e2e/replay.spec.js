// End-to-end coverage for the replay viewer:
//   - a transport/UI smoke pass (boot, clock, goal ticks, play/pause, seek, score)
//   - the parity check that replay.js's applyPoses() writes exactly what
//     replay-core.js's writePoses() produces — the guard that the extracted core
//     is not bypassed or drifting (docs ADR-0004)
//   - a parity check that the ported slerpQuat still matches the real THREE build
//
// One shared page (serial): the fixture server re-runs rrrocket per request, so
// a fresh navigation per test would be needlessly slow.

import { expect, test } from "@playwright/test";

test.describe.configure({ mode: "serial" });

let page;
let meta;

test.beforeAll(async ({ browser }) => {
  page = await browser.newPage();
  page.on("pageerror", (e) => console.log(`[page error] ${e.message}`));
  await page.goto("/match/1/replay?debug");
  await page.waitForFunction(() => window.__replay?.playback, null, { timeout: 45_000 });
  meta = await page.evaluate(() =>
    fetch("/api/matches/1/replay").then((r) => r.json()),
  );
});

test.afterAll(async () => {
  await page.close();
});

async function resetClock() {
  await page.evaluate(() => {
    const p = window.__replay.playback;
    p.state.playing = false;
    p.seek(0);
    p.applyPoses();
  });
}

test("boots with debug hooks and one mesh per slot", async () => {
  const info = await page.evaluate(() => {
    const r = window.__replay;
    return {
      meshes: r.meshes.length,
      hasClock: typeof r.playback.duration === "function",
    };
  });
  expect(info.meshes).toBe(meta.slots.length);
  expect(info.hasClock).toBe(true);
  await expect(page.locator('[data-role="controls"]')).toBeVisible();
});

test("a rendered frame is not a flat fill", async () => {
  const sample = await page.evaluate(() => {
    const { playback, renderer, scene, camera } = window.__replay;
    playback.state.playing = false;
    playback.seek(playback.duration() * 0.5); // mid-match — arena, cars, trails all on screen
    playback.applyPoses();
    renderer.render(scene, camera);

    // Read the drawing buffer *synchronously*, in the same task as the render:
    // with preserveDrawingBuffer unset it is only intact until the event loop
    // yields, so a readPixels/drawImage in a later task would come back cleared.
    // (page.screenshot() composites and stays correct, but hands back an
    // undecoded PNG with no bundled decoder to analyse — see docs/adr/0004.)
    const buf = document.createElement("canvas");
    buf.width = 64;
    buf.height = 64;
    const ctx = buf.getContext("2d");
    ctx.drawImage(renderer.domElement, 0, 0, 64, 64);
    const { data } = ctx.getImageData(0, 0, 64, 64);
    const buckets = new Uint32Array(16); // luminance, 16 bands over 0..255
    for (let i = 0; i < data.length; i += 4) {
      const lum = data[i] * 0.299 + data[i + 1] * 0.587 + data[i + 2] * 0.114;
      buckets[(lum / 16) | 0]++;
    }
    const { calls, triangles } = renderer.info.render;
    return {
      calls,
      triangles,
      populatedBuckets: buckets.reduce((n, c) => n + (c > 0 ? 1 : 0), 0),
    };
  });

  // the render submitted real geometry ...
  expect(sample.calls).toBeGreaterThan(0);
  expect(sample.triangles).toBeGreaterThan(0);
  // ... and its pixels span several luminance bands rather than one flat fill —
  // the "renders fine but the screen is black" class a pixel read alone catches
  expect(sample.populatedBuckets).toBeGreaterThanOrEqual(4);
});

test("the replay canvas is on-screen, sized and unobscured", async () => {
  const box = await page.evaluate(() => {
    const c = window.__replay.renderer.domElement;
    const r = c.getBoundingClientRect();
    const hit = document.elementFromPoint(
      r.left + r.width / 2,
      r.top + r.height / 2,
    );
    return {
      w: r.width,
      h: r.height,
      drawW: c.width,
      drawH: c.height,
      hitIsCanvas: hit === c,
      opacity: Number(getComputedStyle(c).opacity),
    };
  });
  expect(box.w).toBeGreaterThan(100); // display:none / collapsed container → 0
  expect(box.h).toBeGreaterThan(100);
  expect(box.drawW).toBeGreaterThan(0);
  expect(box.drawH).toBeGreaterThan(0);
  expect(box.hitIsCanvas).toBe(true); // nothing (countdown / message div) covering the centre
  expect(box.opacity).toBeGreaterThan(0);
});

test("clock reads M:SS / M:SS and the compressed duration drops the dead time", async () => {
  await resetClock();
  await page.waitForFunction(() =>
    /^\d+:\d\d \/ \d+:\d\d$/.test(
      document.querySelector('[data-role="clock"]').textContent.trim(),
    ),
  );
  const { duration, realSpan } = await page.evaluate(() => ({
    duration: window.__replay.playback.duration(),
    realSpan:
      window.__replay.meta.frame_times.at(-1) -
      window.__replay.meta.frame_times[0],
  }));
  expect(duration).toBeGreaterThan(0);
  expect(duration).toBeLessThan(realSpan); // dead_periods were removed
});

test("a goal tick sits on the scrub bar for every goal, in compressed-time order", async () => {
  const marks = await page.locator('[data-role="marks"] span').all();
  expect(marks).toHaveLength(meta.goals.length);

  const lefts = await page.evaluate(() => {
    const p = window.__replay.playback;
    const m = window.__replay.meta;
    return m.goals.map((g) => p.fractionAt(m.frame_times[g.frame]) * 100);
  });
  for (let i = 0; i < marks.length; i++) {
    const left = parseFloat(
      (await marks[i].evaluate((el) => el.style.left)).replace("%", ""),
    );
    expect(left).toBeGreaterThanOrEqual(0);
    expect(left).toBeLessThanOrEqual(100);
    expect(Math.abs(left - lefts[i])).toBeLessThan(0.5);
  }
});

test("play advances the clock, pause freezes it", async () => {
  await resetClock();
  await page.locator('[data-role="playpause"]').click();
  await page.waitForTimeout(600);
  const running = await page.evaluate(() => window.__replay.playback.state.t);
  expect(running).toBeGreaterThan(meta.frame_times[0]);

  await page.locator('[data-role="playpause"]').click();
  await page.waitForTimeout(100);
  const paused = await page.evaluate(() => window.__replay.playback.state.t);
  await page.waitForTimeout(400);
  const stillPaused = await page.evaluate(() => window.__replay.playback.state.t);
  expect(stillPaused).toBe(paused);
});

test("ArrowRight seeks forward and the scrub bar follows", async () => {
  await resetClock();
  const before = await page.evaluate(() => window.__replay.playback.state.t);
  await page.locator("body").press("ArrowRight");
  await page.waitForFunction(
    (b) => window.__replay.playback.state.t > b,
    before,
  );
  await page.waitForFunction(
    () => Number(document.querySelector('[data-role="scrub"]').value) > 0,
  );
});

test("the kickoff countdown overlay shows the numeral syncUI is fed", async () => {
  // countdownLabelAt is unit-tested in the core; this checks syncUI actually
  // writes it into the DOM. Seek just past a mid-match '3' tick.
  const frame = await page.evaluate(
    // the 2nd '3' tick — a mid-match kickoff, clear of the t0 boundary
    () => window.__replay.meta.countdowns.filter(([, n]) => n === 3)[1][0],
  );
  await page.evaluate((fr) => {
    const p = window.__replay.playback;
    const m = window.__replay.meta;
    p.state.playing = false;
    p.seek(p.fractionAt(m.frame_times[fr] + 0.2) * p.duration());
    p.applyPoses();
  }, frame);
  const cd = page.locator('[data-role="countdown"]');
  await expect(cd).toBeVisible();
  await expect(cd).toHaveText("3");

  // and it clears once the run is well past
  await page.evaluate(() => {
    const p = window.__replay.playback;
    p.seek(p.duration() * 0.5);
    p.applyPoses();
  });
  await expect(page.locator('[data-role="countdown"]')).toBeHidden();
});

test("the scoreboard counts goals as the clock passes them", async () => {
  const tally = { ours: 0, theirs: 0 };
  for (const g of meta.goals) {
    if (g.team === meta.tracked_team) tally.ours++;
    else tally.theirs++;
  }

  await page.evaluate(() => {
    const p = window.__replay.playback;
    p.state.playing = false;
    p.seek(p.duration());
    p.applyPoses();
  });
  await page.waitForFunction(
    ([o, t]) => {
      const el = document.querySelector('[data-role="score"]');
      return (
        el.querySelector(".ours")?.textContent === String(o) &&
        el.querySelector(".theirs")?.textContent === String(t)
      );
    },
    [tally.ours, tally.theirs],
  );

  // rewind before the first goal -> 0 – 0
  await page.evaluate((gf) => {
    const p = window.__replay.playback;
    const m = window.__replay.meta;
    p.seek(p.fractionAt(m.frame_times[gf] - 1) * p.duration());
    p.applyPoses();
  }, meta.goals[0].frame);
  await page.waitForFunction(() => {
    const el = document.querySelector('[data-role="score"]');
    return (
      el.querySelector(".ours")?.textContent === "0" &&
      el.querySelector(".theirs")?.textContent === "0"
    );
  });
});

test("applyPoses() writes exactly what replay-core.js writePoses() produces", async () => {
  // The copy loop is pure assignment — f64 -> THREE.Vector3/Quaternion (f64),
  // f32 -> f32 trail buffer — and both sides call the same writePoses() on the
  // same inputs, so every delta here must be *exactly* 0. A loose tolerance
  // would let an independent nudge/clamp added inside applyPoses() slip past,
  // which is the drift this test exists to catch.
  const worst = await page.evaluate(async () => {
    const { playback, meshes, meta } = window.__replay;
    const core = await import("/static/replay-core.js");
    const positions = new Float32Array(
      await (await fetch("/api/matches/1/replay-frames.bin")).arrayBuffer(),
    );
    const buf = core.makePoseBuffers(meta.slots.length);
    const TP = core.TRAIL_POINTS;

    let pos = 0;
    let quat = 0;
    let trailVert = 0;
    let visMismatch = 0;
    let trailCountMismatch = 0;

    const fracs = [];
    for (let k = 0; k <= 120; k++) fracs.push(k / 120);
    // tight cluster around every goal seam
    for (const g of meta.goals) {
      const f = playback.fractionAt(meta.frame_times[g.frame]);
      for (const d of [-0.01, -0.0005, 0, 0.0005, 0.01]) fracs.push(f + d);
    }

    for (const fr of fracs) {
      if (fr < 0 || fr > 1) continue;
      playback.seek(fr * playback.duration());
      playback.applyPoses();
      core.writePoses(meta, positions, playback.state.t, buf);

      for (let s = 0; s < meshes.length; s++) {
        const m = meshes[s];
        if (buf.visible[s] !== (m.visible ? 1 : 0)) visMismatch++;
        if (!buf.visible[s]) continue;
        pos = Math.max(
          pos,
          Math.abs(m.position.x - buf.position[s * 3]),
          Math.abs(m.position.y - buf.position[s * 3 + 1]),
          Math.abs(m.position.z - buf.position[s * 3 + 2]),
        );
        quat = Math.max(
          quat,
          Math.abs(m.quaternion.x - buf.quaternion[s * 4]),
          Math.abs(m.quaternion.y - buf.quaternion[s * 4 + 1]),
          Math.abs(m.quaternion.z - buf.quaternion[s * 4 + 2]),
          Math.abs(m.quaternion.w - buf.quaternion[s * 4 + 3]),
        );
        const tr = m.userData.trail;
        const drawn = tr.geometry.drawRange.count;
        if (drawn !== buf.trailCount[s]) trailCountMismatch++;
        const tp = tr.geometry.attributes.position.array;
        for (let k = 0; k < Math.min(drawn, buf.trailCount[s]) * 3; k++) {
          trailVert = Math.max(
            trailVert,
            Math.abs(tp[k] - buf.trail[s * TP * 3 + k]),
          );
        }
      }
    }
    return { pos, quat, trailVert, visMismatch, trailCountMismatch };
  });

  expect(worst).toEqual({
    pos: 0,
    quat: 0,
    trailVert: 0,
    visMismatch: 0,
    trailCountMismatch: 0,
  });
});

test("slerpQuat matches THREE.Quaternion.slerp on the values the viewer feeds it", async () => {
  const worst = await page.evaluate(async () => {
    const { slerpQuat } = await import("/static/replay-core.js");
    const THREE = window.__replay.THREE;

    const rnd = () => Math.random() * 2 - 1;
    const norm = (q) => {
      const l = Math.hypot(...q);
      return q.map((v) => v / l);
    };

    const pairs = [];
    for (let k = 0; k < 500; k++)
      pairs.push([
        norm([rnd(), rnd(), rnd(), rnd()]),
        norm([rnd(), rnd(), rnd(), rnd()]),
      ]);
    const a = norm([rnd(), rnd(), rnd(), rnd()]);
    pairs.push([a, a.map((v) => -v)]); // exactly antipodal — the sign-flip branch
    pairs.push([a, a.slice()]); // identical — the cosHalfTheta >= 1 early return
    // sqrSinHalfTheta === EPSILON exactly — the linear-with-renormalize fallback
    pairs.push([[0, 0, 0, 1], [0, 0, 0, 1 - Number.EPSILON / 2]]);

    let maxErr = 0;
    for (const [qa, qb] of pairs) {
      for (const t of [0, 0.13, 0.5, 0.87, 1]) {
        const got = slerpQuat(
          qa[0], qa[1], qa[2], qa[3],
          qb[0], qb[1], qb[2], qb[3],
          t, [0, 0, 0, 0],
        );
        const ref = new THREE.Quaternion(qa[0], qa[1], qa[2], qa[3]).slerp(
          new THREE.Quaternion(qb[0], qb[1], qb[2], qb[3]),
          t,
        );
        maxErr = Math.max(
          maxErr,
          Math.abs(got[0] - ref.x),
          Math.abs(got[1] - ref.y),
          Math.abs(got[2] - ref.z),
          Math.abs(got[3] - ref.w),
        );
      }
    }
    return maxErr;
  });
  expect(worst).toBeLessThan(1e-6);
});
