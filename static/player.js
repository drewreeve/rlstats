/* ── Player Stats Page ──────────────────────────── */

const ALLOWED_MODES = ["3v3", "2v2", "hoops"];
const playerName = decodeURIComponent(window.location.pathname.split("/")[2] || "");
let currentMode =
  new URLSearchParams(window.location.search).get("mode") || "3v3";
if (!ALLOWED_MODES.includes(currentMode)) currentMode = "3v3";

const charts = {};

function destroyCharts() {
  for (const key of Object.keys(charts)) {
    charts[key].destroy();
    delete charts[key];
  }
}

function initPlayerHeader() {
  const color = PLAYER_COLORS[playerName];
  if (!color) return;
  const { r, g, b } = color;
  document.getElementById("player-page-name").textContent =
    playerName.toUpperCase();
  const accent = document.getElementById("player-page-accent");
  accent.style.background = `linear-gradient(90deg, rgb(${r},${g},${b}), transparent)`;
  accent.style.boxShadow = `0 0 30px rgba(${r},${g},${b},0.3)`;
  document.title = `${playerName} — RL Stats`;
}

function renderCareerBar(stats) {
  const color = PLAYER_COLORS[playerName];
  const colorStr = color ? rgba(color, 1) : "var(--text-bright)";
  const wr =
    stats.wins + stats.losses > 0
      ? ((stats.wins / (stats.wins + stats.losses)) * 100).toFixed(1) + "%"
      : "—";
  const items = [
    { value: stats.matches, label: "MATCHES", accent: true },
    { value: wr, label: "WIN RATE", accent: true },
    { value: stats.goals, label: "GOALS" },
    { value: stats.assists, label: "ASSISTS" },
    { value: stats.saves, label: "SAVES" },
    {
      value: stats.shooting_pct != null ? stats.shooting_pct + "%" : "—",
      label: "SHOT %",
    },
    { value: stats.mvp_count, label: "MVPs" },
    { value: stats.avg_score != null ? stats.avg_score : "—", label: "AVG SCORE" },
  ];
  document.getElementById("player-career-bar").innerHTML = items
    .map(
      ({ value, label, accent }) => `
    <div class="career-stat">
      <span class="career-stat-value" ${accent ? `style="color:${colorStr}"` : ""}>${value}</span>
      <span class="career-stat-label">${label}</span>
    </div>`,
    )
    .join("");
}

function renderGAS(data) {
  const canvas = document.getElementById("chart-gas");
  if (!canvas) return;
  const labels = data.map((d) => d.date);
  const defaultWindow = Math.max(0, labels.length - 15);

  const line = (key, color, label) => ({
    label,
    data: data.map((d) => d[key]),
    borderColor: rgba(color, 0.9),
    backgroundColor: rgba(color, 0.08),
    fill: false,
    tension: 0.35,
    pointBackgroundColor: rgba(color, 1),
    pointBorderColor: "#08080C",
    pointBorderWidth: 2,
    pointRadius: 3,
    pointHoverRadius: 5,
    borderWidth: 2,
  });

  charts.gas = new Chart(canvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        line("goals", { r: 255, g: 107, b: 0 }, "Goals"),
        line("assists", { r: 0, g: 229, b: 255 }, "Assists"),
        line("saves", { r: 168, g: 85, b: 247 }, "Saves"),
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      aspectRatio: window.innerWidth <= 768 ? 1.2 : 2.5,
      plugins: {
        legend: { labels: { boxWidth: 10, boxHeight: 10, padding: 16 } },
        zoom: zoomOptions("reset-zoom-gas"),
      },
      scales: {
        y: { beginAtZero: true, ticks: { stepSize: 1 } },
        x: { grid: { display: false }, min: defaultWindow },
      },
    },
  });
}

function renderAreaChart(
  data,
  { canvasId, chartKey, dataKey, label, resetBtnId, aspectRatio = 2, yScale = { beginAtZero: true }, tooltipLabel = null },
) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const color = PLAYER_COLORS[playerName] || { r: 255, g: 107, b: 0 };
  const labels = data.map((d) => d.date);
  const defaultWindow = Math.max(0, labels.length - 15);

  const plugins = {
    legend: { display: false },
    zoom: zoomOptions(resetBtnId),
  };
  if (tooltipLabel) {
    plugins.tooltip = { callbacks: { label: tooltipLabel } };
  }

  charts[chartKey] = new Chart(canvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label,
          data: data.map((d) => d[dataKey]),
          borderColor: rgba(color, 0.9),
          backgroundColor: gradient(canvas, color, 0.2, 0.01),
          fill: true,
          tension: 0.35,
          pointBackgroundColor: rgba(color, 1),
          pointBorderColor: "#08080C",
          pointBorderWidth: 2,
          pointRadius: 3,
          pointHoverRadius: 5,
          borderWidth: 2,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      aspectRatio: window.innerWidth <= 768 ? 1.2 : aspectRatio,
      plugins,
      scales: {
        y: yScale,
        x: { grid: { display: false }, min: defaultWindow },
      },
    },
  });
}

function renderAvgScore(data) {
  renderAreaChart(data, {
    canvasId: "chart-avg-score",
    chartKey: "avgScore",
    dataKey: "avg_score",
    label: "Avg Score",
    resetBtnId: "reset-zoom-avg-score",
  });
}

function renderMVP(data) {
  renderAreaChart(data, {
    canvasId: "chart-mvp",
    chartKey: "mvp",
    dataKey: "mvp_count",
    label: "MVPs",
    resetBtnId: "reset-zoom-mvp",
    yScale: { beginAtZero: true, ticks: { stepSize: 1 } },
  });
}

function renderShooting(data) {
  renderAreaChart(data, {
    canvasId: "chart-shooting",
    chartKey: "shooting",
    dataKey: "shooting_pct",
    label: "Shooting %",
    resetBtnId: "reset-zoom-shooting",
    yScale: { beginAtZero: true, max: 100, ticks: { callback: (v) => v + "%" } },
    tooltipLabel: (ctx) => `${ctx.parsed.y}%`,
  });
}

function renderSpeed(data) {
  renderAreaChart(data, {
    canvasId: "chart-speed",
    chartKey: "speed",
    dataKey: "avg_speed",
    label: "Avg Speed",
    resetBtnId: "reset-zoom-speed",
    yScale: { beginAtZero: false },
  });
}

function renderDemosCard(career) {
  const color = PLAYER_COLORS[playerName];
  if (color) {
    const display = document.getElementById("demos-display");
    display.style.setProperty("--player-r", color.r);
    display.style.setProperty("--player-g", color.g);
    display.style.setProperty("--player-b", color.b);
  }
  document.getElementById("demos-committed-value").textContent =
    career.avg_demos != null ? career.avg_demos : "—";
  document.getElementById("demos-received-value").textContent =
    career.avg_demos_received != null ? career.avg_demos_received : "—";
}

function renderBoostSpeedCard(career) {
  const color = PLAYER_COLORS[playerName];
  if (color) {
    const display = document.getElementById("boost-speed-display");
    display.style.setProperty("--player-r", color.r);
    display.style.setProperty("--player-g", color.g);
    display.style.setProperty("--player-b", color.b);
  }
  document.getElementById("boost-value").textContent =
    career.avg_boost_per_minute != null ? career.avg_boost_per_minute : "—";
  document.getElementById("supersonic-value").textContent =
    career.avg_supersonic_pct != null ? career.avg_supersonic_pct + "%" : "—";
}

async function renderAll() {
  destroyCharts();

  const [career, timeSeries] = await Promise.all([
    fetchJSON(`/api/players/${encodeURIComponent(playerName)}?mode=${currentMode}`),
    fetchJSON(`/api/players/${encodeURIComponent(playerName)}/time-series?mode=${currentMode}`),
  ]);

  renderCareerBar(career);
  renderDemosCard(career);
  renderBoostSpeedCard(career);
  renderGAS(timeSeries);
  renderAvgScore(timeSeries);
  renderMVP(timeSeries);
  renderShooting(timeSeries);
  renderSpeed(timeSeries);
}

document.addEventListener("DOMContentLoaded", () => {
  if (!playerName || !PLAYER_COLORS[playerName]) {
    document.querySelector(".player-detail").innerHTML =
      '<p style="padding:3rem;text-align:center;color:var(--text-dim)">Player not found.</p>';
    return;
  }

  initPlayerHeader();
  renderAll();

  document.querySelectorAll(".player-mode-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      if (btn.dataset.mode === currentMode) return;
      document
        .querySelector(".player-mode-btn.active")
        .classList.remove("active");
      btn.classList.add("active");
      currentMode = btn.dataset.mode;
      const url = new URL(window.location.href);
      url.searchParams.set("mode", currentMode);
      history.pushState({}, "", url);
      renderAll();
    });
  });

  for (const [btnId, chartKey] of [
    ["reset-zoom-gas", "gas"],
    ["reset-zoom-avg-score", "avgScore"],
    ["reset-zoom-shooting", "shooting"],
    ["reset-zoom-speed", "speed"],
  ]) {
    document.getElementById(btnId).addEventListener("click", () => {
      if (charts[chartKey]) {
        charts[chartKey].resetZoom();
        document.getElementById(btnId).hidden = true;
      }
    });
  }

  const navWrap = document.querySelector(".mode-nav-wrap");
  const nav = document.querySelector(".mode-nav");
  if (nav && navWrap) {
    nav.addEventListener("scroll", () => {
      navWrap.classList.toggle(
        "scrolled-end",
        nav.scrollLeft + nav.clientWidth >= nav.scrollWidth - 4,
      );
    });
  }
});
