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
  // window.__replay.playback only exists once main() has fetched and decoded
  // the real envelope over HTTP to build the scene, so this already exercises
  // the merged envelope's wire (docs/adr/0004's addendum) — no separate fetch
  // needed here.
  await page.waitForFunction(() => window.__replay?.playback, null, { timeout: 45_000 });
  meta = await page.evaluate(() => window.__replay.meta);
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
    window.__replay.boostPads.apply(0);
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

test("boost-pad orbs hide while collected and pop back in on respawn", async () => {
  // Pick a pad from the meta that has a full collect -> respawn cycle, and read
  // its orb's visibility/scale at four moments straight off the scene.
  const probe = await page.evaluate(() => {
    const { boostPads } = window.__replay;

    // first pad index whose timeline is collect@i then available@i+1
    let pad = -1;
    for (let i = 0; i < boostPads.timeline.length; i++) {
      const tl = boostPads.timeline[i];
      if (tl && tl.collected[0] === 1 && tl.collected[1] === 0 && boostPads.bound.has(i)) {
        pad = i;
        break;
      }
    }
    if (pad < 0) return null;
    const tl = boostPads.timeline[pad];
    const mesh = boostPads.bound.get(pad);
    const tCollect = tl.times[0];
    const tRespawn = tl.times[1];

    const at = (t) => {
      boostPads.apply(t);
      return { visible: mesh.visible, scale: mesh.scale.x };
    };
    const byName = (n) => {
      let count = 0;
      window.__replay.scene.traverse((o) => {
        if (o.name === n) count++;
      });
      return count;
    };

    return {
      boundCount: boostPads.bound.size,
      totalOrbs: byName("boost_orb_big") + byName("boost_orb_small"),
      before: at(tCollect - 0.05),
      collected: at((tCollect + tRespawn) / 2),
      midPop: at(tRespawn + 0.05), // < BOOST_ORB_POP (0.15) after respawn
      settled: at(tRespawn + 1),
    };
  });

  expect(probe).not.toBeNull();
  expect(probe.totalOrbs).toBe(34); // fixture replay is standard soccar
  expect(probe.boundCount).toBe(34); // every pad index snapped to its own orb
  expect(probe.before.visible).toBe(true);
  expect(probe.before.scale).toBeCloseTo(1, 5);
  expect(probe.collected.visible).toBe(false);
  expect(probe.midPop.visible).toBe(true);
  expect(probe.midPop.scale).toBeGreaterThan(0.35);
  expect(probe.midPop.scale).toBeLessThan(1);
  expect(probe.settled.visible).toBe(true);
  expect(probe.settled.scale).toBeCloseTo(1, 5);
});

test("every boost-pad orb is visible at a kickoff", async () => {
  // The raw pickup stream self-heals at kickoffs (ADR-0004): no pad reads
  // collected when the countdown fires, so the orbs need no forced reset.
  const allVisible = await page.evaluate(() => {
    const { boostPads, meta } = window.__replay;
    const kickoff = meta.countdowns.find(([, n]) => n === 3);
    boostPads.apply(meta.frame_times[kickoff[0]]);
    return [...boostPads.bound.values()].every((m) => m.visible);
  });
  expect(allVisible).toBe(true);
  await resetClock();
});

test("applyPoses() writes exactly what replay-core.js writePoses() produces", async () => {
  // The copy loop is pure assignment — f64 -> THREE.Vector3/Quaternion (f64),
  // f32 -> f32 trail buffer — and both sides call the same writePoses() on the
  // same inputs, so every delta here must be *exactly* 0. A loose tolerance
  // would let an independent nudge/clamp added inside applyPoses() slip past,
  // which is the drift this test exists to catch.
  const worst = await page.evaluate(async () => {
    const { playback, meshes, meta, positions } = window.__replay;
    const core = await import("/static/replay-core.js");
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

// ---------------------------------------------------------------------------
// The hoops arena (match 2 — a 2v2 hoops replay). Guards that game_mode routes
// the viewer to arenaSpec("hoops"): the smaller chamfered footprint, ring
// goals, the 20-pad layout — and that it still renders. See docs/adr/0005.
// ---------------------------------------------------------------------------

test.describe("hoops arena", () => {
  let hoopsPage;

  test.beforeAll(async ({ browser }) => {
    hoopsPage = await browser.newPage();
    hoopsPage.on("pageerror", (e) => console.log(`[hoops page error] ${e.message}`));
    await hoopsPage.goto("/match/2/replay?debug");
    await hoopsPage.waitForFunction(() => window.__replay?.playback, null, {
      timeout: 45_000,
    });
  });

  test.afterAll(async () => {
    await hoopsPage.close();
  });

  test("meta reports hoops", async () => {
    const gameMode = await hoopsPage.evaluate(() => window.__replay.meta.game_mode);
    expect(gameMode).toBe("hoops");
  });

  test("the scene is built from the hoops spec — footprint, ring goals, 20 pads", async () => {
    const geom = await hoopsPage.evaluate(() => {
      let footprint = null;
      const ringYs = [];
      const ringReach = [];
      let bigPads = 0;
      let smallPads = 0;
      window.__replay.scene.traverse((o) => {
        if (o.name === "boost_orb_big") bigPads++;
        if (o.name === "boost_orb_small") smallPads++;
        if (o.isLineLoop && o.position.z === 0 && footprint === null) {
          // arena floor loop — first LineLoop at z=0
          o.geometry.computeBoundingBox();
          const b = o.geometry.boundingBox;
          footprint = { x: b.max.x, y: b.max.y };
        }
        if (o.isMesh && o.geometry?.type === "TubeGeometry") {
          o.geometry.computeBoundingBox();
          const b = o.geometry.boundingBox;
          // the rim tube: a D-outline of radius 655 centred at z ≈ 364, x ≈ 0 —
          // its span runs from the U apex (|y| ≈ 2314) back to the wall.
          const cz = (b.min.z + b.max.z) / 2;
          const cx = (b.min.x + b.max.x) / 2;
          if (Math.abs(cz - 364) < 20 && Math.abs(cx) < 20 && b.max.x > 600) {
            ringYs.push(Math.round((b.min.y + b.max.y) / 2 / 500) * 500);
            ringReach.push(Math.max(Math.abs(b.min.y), Math.abs(b.max.y)));
          }
        }
      });
      return {
        footprint,
        ringYs: ringYs.sort((a, b) => a - b),
        ringReach,
        big: bigPads,
        small: smallPads,
      };
    });

    // chamfered hoops footprint (±2966.67 × ±3581), not soccar's ±4096 × ±5120
    expect(geom.footprint.x).toBeGreaterThan(2900);
    expect(geom.footprint.x).toBeLessThan(3100);
    expect(geom.footprint.y).toBeGreaterThan(3500);
    expect(geom.footprint.y).toBeLessThan(3700);
    // one rim per end, centred near y = ±2948
    expect(geom.ringYs).toEqual([-3000, 3000]);
    // and each D reaches the back wall (halfY 3581)
    expect(Math.min(...geom.ringReach)).toBeGreaterThan(3400);
    // hoops boost layout: 6 big + 14 small
    expect(geom.big).toBe(6);
    expect(geom.small).toBe(14);
  });

  test("a rendered hoops frame is not a flat fill", async () => {
    const sample = await hoopsPage.evaluate(() => {
      const { playback, renderer, scene, camera } = window.__replay;
      playback.state.playing = false;
      playback.seek(playback.duration() * 0.5);
      playback.applyPoses();
      renderer.render(scene, camera);

      const buf = document.createElement("canvas");
      buf.width = 64;
      buf.height = 64;
      const ctx = buf.getContext("2d");
      ctx.drawImage(renderer.domElement, 0, 0, 64, 64);
      const { data } = ctx.getImageData(0, 0, 64, 64);
      const buckets = new Uint32Array(16);
      for (let i = 0; i < data.length; i += 4) {
        const lum = data[i] * 0.299 + data[i + 1] * 0.587 + data[i + 2] * 0.114;
        buckets[(lum / 16) | 0]++;
      }
      return {
        calls: renderer.info.render.calls,
        populatedBuckets: buckets.reduce((n, c) => n + (c > 0 ? 1 : 0), 0),
      };
    });
    expect(sample.calls).toBeGreaterThan(0);
    expect(sample.populatedBuckets).toBeGreaterThanOrEqual(4);
  });
});
