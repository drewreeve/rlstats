/* ── Player & Stat Colors ───────────────────────── */
const PLAYER_COLORS = {
  Drew: { r: 0, g: 229, b: 255 } /* cyan */,
  Steve: { r: 255, g: 107, b: 0 } /* orange */,
  Jeff: { r: 168, g: 85, b: 247 } /* violet */,
};

const STAT_COLORS = {
  goals: { r: 255, g: 107, b: 0 },
  assists: { r: 0, g: 229, b: 255 },
  saves: { r: 168, g: 85, b: 247 },
  wins: { r: 0, g: 229, b: 255 },
  losses: { r: 255, g: 60, b: 60 },
};

function esc(str) {
  const d = document.createElement("div");
  d.textContent = str;
  return d.innerHTML;
}

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

/* ── Chart.js Defaults ──────────────────────────── */
Chart.defaults.color = "#5A5A6E";
Chart.defaults.borderColor = "rgba(255,255,255,0.04)";
Chart.defaults.font.family = "'DM Mono', monospace";
Chart.defaults.font.size = 13;

let currentMode = "3v3";
const charts = {};

async function fetchJSON(url) {
  const res = await fetch(url);
  return res.json();
}

function destroyCharts() {
  for (const key of Object.keys(charts)) {
    charts[key].destroy();
    delete charts[key];
  }
}

/* ── Chart Renderers ────────────────────────────── */

async function playerBarChart(
  key,
  canvasId,
  endpoint,
  { label, getValue, yPct, tooltipExtra },
) {
  const data = await fetchJSON(`${endpoint}?mode=${currentMode}`);
  const canvas = document.getElementById(canvasId);
  charts[key] = new Chart(canvas, {
    type: "bar",
    data: {
      labels: data.map((d) => d.player),
      datasets: [
        {
          label,
          data: data.map((d) => getValue(d)),
          backgroundColor: data.map((d) =>
            gradient(canvas, PLAYER_COLORS[d.player], 0.85, 0.15),
          ),
          borderColor: data.map((d) => rgba(PLAYER_COLORS[d.player], 0.8)),
          borderWidth: 1,
          borderRadius: 2,
          borderSkipped: false,
        },
      ],
    },
    options: {
      plugins: {
        legend: { display: false },
        tooltip: tooltipExtra
          ? {
              callbacks: {
                afterLabel: (ctx) => tooltipExtra(data[ctx.dataIndex]),
              },
            }
          : {},
      },
      scales: {
        y: {
          beginAtZero: true,
          ...(yPct ? { max: 100, ticks: { callback: (v) => v + "%" } } : {}),
        },
        x: { grid: { display: false } },
      },
    },
  });
}

async function renderWinRateDaily() {
  const data = await fetchJSON(`/api/win-loss-daily?mode=${currentMode}`);
  const canvas = document.getElementById("chart-win-rate");
  const lineColor = { r: 0, g: 229, b: 255 };
  charts.winRate = new Chart(canvas, {
    type: "line",
    data: {
      labels: data.map((d) => d.date ?? ""),
      datasets: [
        {
          label: "Win Rate",
          data: data.map((d) => (d.win_rate ?? 0) * 100),
          borderColor: rgba(lineColor, 0.9),
          backgroundColor: gradient(canvas, lineColor, 0.2, 0.01),
          fill: true,
          tension: 0.35,
          pointBackgroundColor: rgba(lineColor, 1),
          pointBorderColor: "#08080C",
          pointBorderWidth: 2,
          pointRadius: 4,
          pointHoverRadius: 6,
          borderWidth: 2,
        },
      ],
    },
    options: {
      plugins: { legend: { display: false } },
      scales: {
        y: { beginAtZero: true, max: 100, ticks: { callback: (v) => v + "%" } },
        x: { grid: { display: false } },
      },
      onHover: (event, elements) => {
        event.native.target.style.cursor = elements.length ? "pointer" : "";
      },
      onClick: (event, elements) => {
        if (!elements.length) return;
        const idx = elements[0].index;
        const date = data[idx].date;
        if (!date) return;
        document.getElementById("history-date-from").value = date;
        document.getElementById("history-date-to").value = date;
        document.querySelector(".mode-btn.active").classList.remove("active");
        document.querySelector('.mode-btn[data-mode="history"]').classList.add("active");
        currentMode = "history";
        renderAll();
      },
    },
  });
}

async function renderPlayerStats() {
  const data = await fetchJSON(`/api/player-stats?mode=${currentMode}`);
  const canvas = document.getElementById("chart-player-stats");
  charts.playerStats = new Chart(canvas, {
    type: "bar",
    data: {
      labels: data.map((d) => d.player),
      datasets: [
        {
          label: "Goals",
          data: data.map((d) => d.goals),
          backgroundColor: gradient(canvas, STAT_COLORS.goals, 0.85, 0.15),
          borderColor: rgba(STAT_COLORS.goals, 0.8),
          borderWidth: 1,
          borderRadius: 2,
          borderSkipped: false,
        },
        {
          label: "Assists",
          data: data.map((d) => d.assists),
          backgroundColor: gradient(canvas, STAT_COLORS.assists, 0.85, 0.15),
          borderColor: rgba(STAT_COLORS.assists, 0.8),
          borderWidth: 1,
          borderRadius: 2,
          borderSkipped: false,
        },
        {
          label: "Saves",
          data: data.map((d) => d.saves),
          backgroundColor: gradient(canvas, STAT_COLORS.saves, 0.85, 0.15),
          borderColor: rgba(STAT_COLORS.saves, 0.8),
          borderWidth: 1,
          borderRadius: 2,
          borderSkipped: false,
        },
      ],
    },
    options: {
      scales: {
        y: { beginAtZero: true },
        x: { grid: { display: false } },
      },
      plugins: {
        legend: {
          labels: {
            boxWidth: 10,
            boxHeight: 10,
            padding: 16,
            font: { family: "'DM Mono', monospace", size: 13 },
          },
        },
      },
    },
  });
}

function renderMvpWins() {
  return playerBarChart("mvpWins", "chart-mvp-wins", "/api/mvp-wins", {
    label: "MVP Wins",
    getValue: (d) => d.mvp_wins,
    tooltipExtra: (d) =>
      `Win rate as MVP: ${((d.win_rate ?? 0) * 100).toFixed(1)}%`,
  });
}

function renderMvpLosses() {
  return playerBarChart("mvpLosses", "chart-mvp-losses", "/api/mvp-losses", {
    label: "Loss MVPs",
    getValue: (d) => d.loss_mvps,
  });
}

async function renderScoreDifferential() {
  const data = await fetchJSON(`/api/score-differential?mode=${currentMode}`);
  const canvas = document.getElementById("chart-score-diff");
  charts.scoreDiff = new Chart(canvas, {
    type: "bar",
    data: {
      labels: data.map((d) => (d.differential > 0 ? "+" : "") + d.differential),
      datasets: [
        {
          label: "Matches",
          data: data.map((d) => d.match_count),
          backgroundColor: data.map((d) =>
            gradient(
              canvas,
              d.differential > 0 ? STAT_COLORS.wins : STAT_COLORS.losses,
              0.85,
              0.15,
            ),
          ),
          borderColor: data.map((d) =>
            rgba(
              d.differential > 0 ? STAT_COLORS.wins : STAT_COLORS.losses,
              0.8,
            ),
          ),
          borderWidth: 1,
          borderRadius: 2,
          borderSkipped: false,
        },
      ],
    },
    options: {
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => `${ctx.parsed.y} matches`,
          },
        },
      },
      scales: {
        y: { beginAtZero: true },
        x: { grid: { display: false } },
      },
    },
  });
}

async function renderWeekday() {
  const data = await fetchJSON(`/api/weekday?mode=${currentMode}`);
  const canvas = document.getElementById("chart-weekday");
  charts.weekday = new Chart(canvas, {
    type: "bar",
    data: {
      labels: data.map((d) => d.weekday),
      datasets: [
        {
          label: "Wins",
          data: data.map((d) => d.wins),
          backgroundColor: gradient(canvas, STAT_COLORS.wins, 0.85, 0.15),
          borderColor: rgba(STAT_COLORS.wins, 0.8),
          borderWidth: 1,
          borderRadius: 2,
          borderSkipped: false,
        },
        {
          label: "Losses",
          data: data.map((d) => d.losses),
          backgroundColor: gradient(canvas, STAT_COLORS.losses, 0.85, 0.15),
          borderColor: rgba(STAT_COLORS.losses, 0.8),
          borderWidth: 1,
          borderRadius: 2,
          borderSkipped: false,
        },
      ],
    },
    options: {
      plugins: {
        tooltip: {
          callbacks: {
            afterBody: (ctx) => {
              const d = data[ctx[0].dataIndex];
              return `Win rate: ${((d.win_rate ?? 0) * 100).toFixed(1)}%`;
            },
          },
        },
        legend: {
          labels: {
            boxWidth: 10,
            boxHeight: 10,
            padding: 16,
            font: { family: "'DM Mono', monospace", size: 13 },
          },
        },
      },
      scales: {
        x: { stacked: true, grid: { display: false } },
        y: { stacked: true, beginAtZero: true },
      },
    },
  });
}

async function renderStreaks() {
  const data = await fetchJSON(`/api/streaks?mode=${currentMode}`);
  document.getElementById("streak-win-value").textContent =
    data.longest_win_streak ?? "—";
  document.getElementById("streak-loss-value").textContent =
    data.longest_loss_streak ?? "—";
}

function renderGoalContribution() {
  return playerBarChart(
    "goalContribution",
    "chart-goal-contribution",
    "/api/avg-goal-contribution",
    {
      label: "Avg Goal Contribution",
      getValue: (d) => (d.avg_goal_contribution ?? 0) * 100,
      yPct: true,
    },
  );
}

/* ── Visibility & Render ────────────────────────── */

function updateCardVisibility() {
  const is3v3 = currentMode === "3v3";
  document.getElementById("card-win-rate").style.display = is3v3 ? "" : "none";
  document.getElementById("card-weekday").style.display = is3v3 ? "" : "none";
}

function updateViewVisibility() {
  const isHistory = currentMode === "history";
  document.getElementById("chart-view").hidden = isHistory;
  document.getElementById("history-view").hidden = !isHistory;
}

async function renderAll() {
  updateViewVisibility();
  if (currentMode === "history") {
    renderRawTable();
    return;
  }
  destroyCharts();
  updateCardVisibility();
  playerBarChart("shooting", "chart-shooting", "/api/shooting-pct", {
    label: "Shooting %",
    getValue: (d) => (d.shooting_pct ?? 0) * 100,
    yPct: true,
    tooltipExtra: (d) => `${d.goals} goals / ${d.shots} shots`,
  });
  playerBarChart("avgScore", "chart-avg-score", "/api/avg-score", {
    label: "Avg Score",
    getValue: (d) => d.avg_score ?? 0,
    tooltipExtra: (d) => `${d.total_score} total / ${d.matches} matches`,
  });
  if (currentMode === "3v3") {
    renderWinRateDaily();
  }
  renderPlayerStats();
  renderMvpWins();
  renderMvpLosses();
  renderScoreDifferential();
  renderStreaks();
  if (currentMode === "3v3") {
    renderWeekday();
  }
  renderGoalContribution();
}

/* ── Raw Table ─────────────────────────────────── */

let rawPage = 1;
let rawPerPage = 25;
const playerDetailCache = {};
let rawSearchTimer = null;

function updateDateClearButton() {
  const from = document.getElementById("history-date-from").value;
  const to = document.getElementById("history-date-to").value;
  document.getElementById("history-date-clear").hidden = !from && !to;
}

async function renderRawTable() {
  const search = document.getElementById("history-search").value;
  const gameMode = document.getElementById("history-filter-mode").value;
  const result = document.getElementById("history-filter-result").value;

  const dateFrom = document.getElementById("history-date-from").value;
  const dateTo = document.getElementById("history-date-to").value;
  updateDateClearButton();

  const params = new URLSearchParams({
    page: rawPage,
    per_page: rawPerPage,
  });
  if (search) params.set("search", search);
  if (gameMode) params.set("game_mode", gameMode);
  if (result) params.set("result", result);
  if (dateFrom) params.set("date_from", dateFrom);
  if (dateTo) params.set("date_to", dateTo);

  const data = await fetchJSON(`/api/matches?${params}`);
  const tbody = document.getElementById("history-table-body");
  tbody.innerHTML = "";

  for (const m of data.matches) {
    const tr = document.createElement("tr");
    tr.className = "history-row";
    tr.dataset.matchId = m.id;

    const date = m.played_at ? m.played_at.split("T")[0] : "—";
    const isWin = m.result === "win";
    const resultClass = isWin ? "badge-win" : "badge-loss";
    const resultText = isWin ? "W" : "L";
    const forfeitTag = m.forfeit ? '<span class="forfeit-tag">FF</span>' : "";

    tr.innerHTML = `
            <td class="col-date">${date}</td>
            <td><span class="mode-tag">${esc((m.game_mode || "").toUpperCase())}</span></td>
            <td><span class="result-badge ${resultClass}">${resultText}</span>${forfeitTag}</td>
            <td class="col-score">${m.score}</td>
            <td class="col-mvp">${esc(m.mvp || "—")}</td>
        `;

    tr.addEventListener("click", () => toggleDetail(tr, m.id));
    tbody.appendChild(tr);
  }

  renderPagination(data.total, data.page, data.per_page);
}

async function toggleDetail(row, matchId) {
  const existing = row.nextElementSibling;
  if (existing && existing.classList.contains("detail-row")) {
    existing.remove();
    row.classList.remove("expanded");
    return;
  }

  row.classList.add("expanded");

  if (!playerDetailCache[matchId]) {
    playerDetailCache[matchId] = await fetchJSON(
      `/api/matches/${matchId}/players`,
    );
  }
  const players = playerDetailCache[matchId];

  const detailRow = document.createElement("tr");
  detailRow.className = "detail-row";
  const td = document.createElement("td");
  td.colSpan = 5;

  let tableHTML = `<table class="detail-table">
        <thead><tr>
            <th>Player</th><th>Score</th><th>Goals</th><th>Assists</th><th>Saves</th><th>Shots</th><th>Shot%</th>
        </tr></thead><tbody>`;
  for (const p of players) {
    tableHTML += `<tr>
            <td class="player-name">${esc(p.name)}</td>
            <td>${p.score ?? 0}</td>
            <td>${p.goals}</td>
            <td>${p.assists}</td>
            <td>${p.saves}</td>
            <td>${p.shots}</td>
            <td>${p.shooting_pct}%</td>
        </tr>`;
  }
  tableHTML += "</tbody></table>";
  td.innerHTML = tableHTML;
  detailRow.appendChild(td);
  row.after(detailRow);
}

function renderPagination(total, page, perPage) {
  const totalPages = Math.ceil(total / perPage);
  const container = document.getElementById("history-pagination");

  if (totalPages <= 1) {
    container.innerHTML = "";
    return;
  }

  let html = `<button class="page-btn" ${page <= 1 ? "disabled" : ""} data-page="${page - 1}">&laquo; Prev</button>`;

  const start = Math.max(1, page - 2);
  const end = Math.min(totalPages, page + 2);
  for (let i = start; i <= end; i++) {
    html += `<button class="page-btn ${i === page ? "active" : ""}" data-page="${i}">${i}</button>`;
  }

  html += `<button class="page-btn" ${page >= totalPages ? "disabled" : ""} data-page="${page + 1}">Next &raquo;</button>`;
  html += `<span class="page-info">${total} matches</span>`;

  container.innerHTML = html;
  container.querySelectorAll(".page-btn:not([disabled])").forEach((btn) => {
    btn.addEventListener("click", () => {
      rawPage = parseInt(btn.dataset.page);
      renderRawTable();
    });
  });
}

/* ── Init ───────────────────────────────────────── */

document.addEventListener("DOMContentLoaded", () => {
  renderAll();

  document.querySelectorAll(".mode-btn[data-mode]").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelector(".mode-btn.active").classList.remove("active");
      btn.classList.add("active");
      currentMode = btn.dataset.mode;
      renderAll();
    });
  });

  document.getElementById("history-search").addEventListener("input", () => {
    clearTimeout(rawSearchTimer);
    rawSearchTimer = setTimeout(() => {
      rawPage = 1;
      renderRawTable();
    }, 300);
  });

  document
    .getElementById("history-filter-mode")
    .addEventListener("change", () => {
      rawPage = 1;
      renderRawTable();
    });

  document
    .getElementById("history-filter-result")
    .addEventListener("change", () => {
      rawPage = 1;
      renderRawTable();
    });

  document
    .getElementById("history-per-page")
    .addEventListener("change", (e) => {
      rawPerPage = parseInt(e.target.value, 10);
      rawPage = 1;
      renderRawTable();
    });

  document.getElementById("history-date-from").addEventListener("change", () => {
    rawPage = 1;
    renderRawTable();
  });

  document.getElementById("history-date-to").addEventListener("change", () => {
    rawPage = 1;
    renderRawTable();
  });

  document.getElementById("history-date-clear").addEventListener("click", () => {
    document.getElementById("history-date-from").value = "";
    document.getElementById("history-date-to").value = "";
    rawPage = 1;
    renderRawTable();
  });
});
