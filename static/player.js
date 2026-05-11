/* ── Player Stats Page ──────────────────────────── */

const PLAYER_COLORS = {
  Drew: { r: 0, g: 229, b: 255 } /* cyan */,
  Steve: { r: 255, g: 107, b: 0 } /* orange */,
  Jeff: { r: 168, g: 85, b: 247 } /* violet */,
};

function rgba({ r, g, b }, a) {
  return `rgba(${r}, ${g}, ${b}, ${a})`;
}

function gradient(canvas, { r, g, b }, topAlpha, bottomAlpha) {
  const ctx = canvas.getContext("2d");
  const h = canvas.parentElement?.clientHeight || 300;
  const grad = ctx.createLinearGradient(0, 0, 0, h);
  grad.addColorStop(0, `rgba(${r}, ${g}, ${b}, ${topAlpha})`);
  grad.addColorStop(1, `rgba(${r}, ${g}, ${b}, ${bottomAlpha})`);
  return grad;
}

Chart.defaults.color = "#5A5A6E";
Chart.defaults.borderColor = "rgba(255,255,255,0.04)";
Chart.defaults.font.family = "'DM Mono', monospace";
Chart.defaults.font.size = 13;

const ALLOWED_MODES = ["3v3", "2v2", "hoops"];
const playerName = decodeURIComponent(window.location.pathname.split("/")[2] || "");
let currentMode =
  new URLSearchParams(window.location.search).get("mode") || "3v3";
if (!ALLOWED_MODES.includes(currentMode)) currentMode = "3v3";

const charts = {};

function zoomOptions(resetBtnId) {
  const showBtn = () => {
    document.getElementById(resetBtnId).hidden = false;
  };
  return {
    pan: { enabled: true, mode: "x", onPan: showBtn },
    zoom: {
      wheel: { enabled: true },
      pinch: { enabled: true },
      mode: "x",
      onZoom: showBtn,
    },
    limits: { x: { minRange: 5 } },
  };
}

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

function renderAvgScore(data) {
  const canvas = document.getElementById("chart-avg-score");
  if (!canvas) return;
  const color = PLAYER_COLORS[playerName] || { r: 255, g: 107, b: 0 };
  const labels = data.map((d) => d.date);
  const defaultWindow = Math.max(0, labels.length - 15);

  charts.avgScore = new Chart(canvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Avg Score",
          data: data.map((d) => d.avg_score),
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
      aspectRatio: window.innerWidth <= 768 ? 1.2 : 2,
      plugins: {
        legend: { display: false },
        zoom: zoomOptions("reset-zoom-avg-score"),
      },
      scales: {
        y: { beginAtZero: true },
        x: { grid: { display: false }, min: defaultWindow },
      },
    },
  });
}

function renderMVP(data) {
  const canvas = document.getElementById("chart-mvp");
  if (!canvas) return;
  const color = PLAYER_COLORS[playerName] || { r: 255, g: 107, b: 0 };
  const labels = data.map((d) => d.date);

  charts.mvp = new Chart(canvas, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "MVPs",
          data: data.map((d) => d.mvp_count),
          backgroundColor: data.map(() => rgba(color, 0.7)),
          borderColor: data.map(() => rgba(color, 0.9)),
          borderWidth: 1,
          borderRadius: 2,
          borderSkipped: false,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      aspectRatio: window.innerWidth <= 768 ? 1.2 : 2,
      plugins: {
        legend: { display: false },
      },
      scales: {
        y: { beginAtZero: true, ticks: { stepSize: 1 } },
        x: { grid: { display: false } },
      },
    },
  });
}

function renderShooting(data) {
  const canvas = document.getElementById("chart-shooting");
  if (!canvas) return;
  const color = PLAYER_COLORS[playerName] || { r: 255, g: 107, b: 0 };
  const labels = data.map((d) => d.date);
  const defaultWindow = Math.max(0, labels.length - 15);

  charts.shooting = new Chart(canvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Shooting %",
          data: data.map((d) => d.shooting_pct),
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
      aspectRatio: window.innerWidth <= 768 ? 1.2 : 2,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: { label: (ctx) => `${ctx.parsed.y}%` },
        },
        zoom: zoomOptions("reset-zoom-shooting"),
      },
      scales: {
        y: {
          beginAtZero: true,
          max: 100,
          ticks: { callback: (v) => v + "%" },
        },
        x: { grid: { display: false }, min: defaultWindow },
      },
    },
  });
}

function renderSpeed(data) {
  const canvas = document.getElementById("chart-speed");
  if (!canvas) return;
  const color = PLAYER_COLORS[playerName] || { r: 255, g: 107, b: 0 };
  const labels = data.map((d) => d.date);
  const defaultWindow = Math.max(0, labels.length - 15);

  charts.speed = new Chart(canvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Avg Speed",
          data: data.map((d) => d.avg_speed),
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
      aspectRatio: window.innerWidth <= 768 ? 1.2 : 2.5,
      plugins: {
        legend: { display: false },
        zoom: zoomOptions("reset-zoom-speed"),
      },
      scales: {
        y: { beginAtZero: false },
        x: { grid: { display: false }, min: defaultWindow },
      },
    },
  });
}

async function renderAll() {
  destroyCharts();

  const [career, timeSeries] = await Promise.all([
    fetch(`/api/players/${encodeURIComponent(playerName)}?mode=${currentMode}`).then(
      (r) => r.json(),
    ),
    fetch(
      `/api/players/${encodeURIComponent(playerName)}/time-series?mode=${currentMode}`,
    ).then((r) => r.json()),
  ]);

  renderCareerBar(career);
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

  document.getElementById("reset-zoom-gas").addEventListener("click", () => {
    if (charts.gas) {
      charts.gas.resetZoom();
      document.getElementById("reset-zoom-gas").hidden = true;
    }
  });

  document
    .getElementById("reset-zoom-avg-score")
    .addEventListener("click", () => {
      if (charts.avgScore) {
        charts.avgScore.resetZoom();
        document.getElementById("reset-zoom-avg-score").hidden = true;
      }
    });

  document
    .getElementById("reset-zoom-shooting")
    .addEventListener("click", () => {
      if (charts.shooting) {
        charts.shooting.resetZoom();
        document.getElementById("reset-zoom-shooting").hidden = true;
      }
    });

  document
    .getElementById("reset-zoom-speed")
    .addEventListener("click", () => {
      if (charts.speed) {
        charts.speed.resetZoom();
        document.getElementById("reset-zoom-speed").hidden = true;
      }
    });

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
