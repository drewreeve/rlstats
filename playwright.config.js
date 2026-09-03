import { defineConfig, devices } from "@playwright/test";

// End-to-end coverage for the replay viewer: drives the real page in headless
// Chromium against a throwaway server (tests/e2e/serve_fixture.py) that ingests
// the one committed replay as match 1.
//
// The page pulls three@0.170.0 from jsdelivr at runtime, so a run depends on
// that CDN being reachable — hence the retries in CI. Vendoring Three.js is the
// real fix (tracked separately); until then a network blip should not fail CI
// outright.
const PORT = Number(process.env.RL_E2E_PORT || 8899);

export default defineConfig({
  testDir: "tests/e2e",
  testMatch: /.*\.spec\.js/,
  fullyParallel: false,
  workers: 1,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  reporter: process.env.CI ? [["github"], ["list"]] : "list",
  timeout: 60_000,
  expect: { timeout: 10_000 },
  use: {
    baseURL: `http://127.0.0.1:${PORT}`,
    navigationTimeout: 45_000,
    trace: "on-first-retry",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    command: `uv run python tests/e2e/serve_fixture.py --port ${PORT}`,
    url: `http://127.0.0.1:${PORT}/api/matches/1/replay`,
    timeout: 120_000,
    reuseExistingServer: !process.env.CI,
    stdout: "pipe",
    stderr: "pipe",
  },
});
