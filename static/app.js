/* ── Player & Stat Colors ───────────────────────── */
const PLAYER_COLORS = {
    Drew:  { r: 0, g: 229, b: 255 },   /* cyan */
    Steve: { r: 255, g: 107, b: 0 },    /* orange */
    Jeff:  { r: 168, g: 85, b: 247 },   /* violet */
};

const STAT_COLORS = {
    goals:  { r: 255, g: 107, b: 0 },
    assists:{ r: 0, g: 229, b: 255 },
    saves:  { r: 168, g: 85, b: 247 },
    wins:   { r: 0, g: 229, b: 255 },
    losses: { r: 255, g: 60, b: 60 },
};

function rgba({ r, g, b }, a) {
    return `rgba(${r}, ${g}, ${b}, ${a})`;
}

function barGradient(canvas, { r, g, b }) {
    const ctx = canvas.getContext("2d");
    const h = canvas.parentElement?.clientHeight || 300;
    const grad = ctx.createLinearGradient(0, 0, 0, h);
    grad.addColorStop(0, `rgba(${r}, ${g}, ${b}, 0.85)`);
    grad.addColorStop(1, `rgba(${r}, ${g}, ${b}, 0.15)`);
    return grad;
}

function areaGradient(canvas, { r, g, b }) {
    const ctx = canvas.getContext("2d");
    const h = canvas.parentElement?.clientHeight || 300;
    const grad = ctx.createLinearGradient(0, 0, 0, h);
    grad.addColorStop(0, `rgba(${r}, ${g}, ${b}, 0.2)`);
    grad.addColorStop(1, `rgba(${r}, ${g}, ${b}, 0.01)`);
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

async function renderShootingPct() {
    const data = await fetchJSON(`/api/shooting-pct?mode=${currentMode}`);
    const canvas = document.getElementById("chart-shooting");
    charts.shooting = new Chart(canvas, {
        type: "bar",
        data: {
            labels: data.map((d) => d.player),
            datasets: [{
                label: "Shooting %",
                data: data.map((d) => (d.shooting_pct ?? 0) * 100),
                backgroundColor: data.map((d) => barGradient(canvas, PLAYER_COLORS[d.player])),
                borderColor: data.map((d) => rgba(PLAYER_COLORS[d.player], 0.8)),
                borderWidth: 1,
                borderRadius: 2,
                borderSkipped: false,
            }],
        },
        options: {
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        afterLabel: (ctx) => {
                            const d = data[ctx.dataIndex];
                            return `${d.goals} goals / ${d.shots} shots`;
                        },
                    },
                },
            },
            scales: {
                y: { beginAtZero: true, max: 100, ticks: { callback: (v) => v + "%" } },
                x: { grid: { display: false } },
            },
        },
    });
}

async function renderAvgScore() {
    const data = await fetchJSON(`/api/avg-score?mode=${currentMode}`);
    const canvas = document.getElementById("chart-avg-score");
    charts.avgScore = new Chart(canvas, {
        type: "bar",
        data: {
            labels: data.map((d) => d.player),
            datasets: [{
                label: "Avg Score",
                data: data.map((d) => d.avg_score ?? 0),
                backgroundColor: data.map((d) => barGradient(canvas, PLAYER_COLORS[d.player])),
                borderColor: data.map((d) => rgba(PLAYER_COLORS[d.player], 0.8)),
                borderWidth: 1,
                borderRadius: 2,
                borderSkipped: false,
            }],
        },
        options: {
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        afterLabel: (ctx) => {
                            const d = data[ctx.dataIndex];
                            return `${d.total_score} total / ${d.matches} matches`;
                        },
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

async function renderWinRateDaily() {
    const data = await fetchJSON(`/api/win-loss-daily?mode=${currentMode}`);
    const canvas = document.getElementById("chart-win-rate");
    const lineColor = { r: 0, g: 229, b: 255 };
    charts.winRate = new Chart(canvas, {
        type: "line",
        data: {
            labels: data.map((d) => d.date ?? ""),
            datasets: [{
                label: "Win Rate",
                data: data.map((d) => (d.win_rate ?? 0) * 100),
                borderColor: rgba(lineColor, 0.9),
                backgroundColor: areaGradient(canvas, lineColor),
                fill: true,
                tension: 0.35,
                pointBackgroundColor: rgba(lineColor, 1),
                pointBorderColor: "#08080C",
                pointBorderWidth: 2,
                pointRadius: 4,
                pointHoverRadius: 6,
                borderWidth: 2,
            }],
        },
        options: {
            plugins: { legend: { display: false } },
            scales: {
                y: { beginAtZero: true, max: 100, ticks: { callback: (v) => v + "%" } },
                x: { grid: { display: false } },
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
                    backgroundColor: barGradient(canvas, STAT_COLORS.goals),
                    borderColor: rgba(STAT_COLORS.goals, 0.8),
                    borderWidth: 1,
                    borderRadius: 2,
                    borderSkipped: false,
                },
                {
                    label: "Assists",
                    data: data.map((d) => d.assists),
                    backgroundColor: barGradient(canvas, STAT_COLORS.assists),
                    borderColor: rgba(STAT_COLORS.assists, 0.8),
                    borderWidth: 1,
                    borderRadius: 2,
                    borderSkipped: false,
                },
                {
                    label: "Saves",
                    data: data.map((d) => d.saves),
                    backgroundColor: barGradient(canvas, STAT_COLORS.saves),
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

async function renderMvpWins() {
    const data = await fetchJSON(`/api/mvp-wins?mode=${currentMode}`);
    const canvas = document.getElementById("chart-mvp-wins");
    charts.mvpWins = new Chart(canvas, {
        type: "bar",
        data: {
            labels: data.map((d) => d.player),
            datasets: [{
                label: "MVP Wins",
                data: data.map((d) => d.mvp_wins),
                backgroundColor: data.map((d) => barGradient(canvas, PLAYER_COLORS[d.player])),
                borderColor: data.map((d) => rgba(PLAYER_COLORS[d.player], 0.8)),
                borderWidth: 1,
                borderRadius: 2,
                borderSkipped: false,
            }],
        },
        options: {
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        afterLabel: (ctx) => {
                            const d = data[ctx.dataIndex];
                            return `Win rate as MVP: ${((d.win_rate ?? 0) * 100).toFixed(1)}%`;
                        },
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

async function renderMvpLosses() {
    const data = await fetchJSON(`/api/mvp-losses?mode=${currentMode}`);
    const canvas = document.getElementById("chart-mvp-losses");
    charts.mvpLosses = new Chart(canvas, {
        type: "bar",
        data: {
            labels: data.map((d) => d.player),
            datasets: [{
                label: "Loss MVPs",
                data: data.map((d) => d.loss_mvps),
                backgroundColor: data.map((d) => barGradient(canvas, PLAYER_COLORS[d.player])),
                borderColor: data.map((d) => rgba(PLAYER_COLORS[d.player], 0.8)),
                borderWidth: 1,
                borderRadius: 2,
                borderSkipped: false,
            }],
        },
        options: {
            plugins: { legend: { display: false } },
            scales: {
                y: { beginAtZero: true },
                x: { grid: { display: false } },
            },
        },
    });
}

async function renderScoreDifferential() {
    const data = await fetchJSON(`/api/score-differential?mode=${currentMode}`);
    const canvas = document.getElementById("chart-score-diff");
    charts.scoreDiff = new Chart(canvas, {
        type: "bar",
        data: {
            labels: data.map((d) => (d.differential > 0 ? "+" : "") + d.differential),
            datasets: [{
                label: "Matches",
                data: data.map((d) => d.match_count),
                backgroundColor: data.map((d) =>
                    barGradient(canvas, d.differential > 0 ? STAT_COLORS.wins : STAT_COLORS.losses)
                ),
                borderColor: data.map((d) =>
                    rgba(d.differential > 0 ? STAT_COLORS.wins : STAT_COLORS.losses, 0.8)
                ),
                borderWidth: 1,
                borderRadius: 2,
                borderSkipped: false,
            }],
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
                    backgroundColor: barGradient(canvas, STAT_COLORS.wins),
                    borderColor: rgba(STAT_COLORS.wins, 0.8),
                    borderWidth: 1,
                    borderRadius: 2,
                    borderSkipped: false,
                },
                {
                    label: "Losses",
                    data: data.map((d) => d.losses),
                    backgroundColor: barGradient(canvas, STAT_COLORS.losses),
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

/* ── Visibility & Render ────────────────────────── */

function updateCardVisibility() {
    const is3v3 = currentMode === "3v3";
    document.getElementById("card-win-rate").style.display = is3v3 ? "" : "none";
    document.getElementById("card-weekday").style.display = is3v3 ? "" : "none";
}

function updateViewVisibility() {
    const isHistory = currentMode === "history";
    document.getElementById("chart-view").style.display = isHistory ? "none" : "";
    document.getElementById("history-view").style.display = isHistory ? "" : "none";
}

async function renderAll() {
    updateViewVisibility();
    if (currentMode === "history") {
        renderRawTable();
        return;
    }
    destroyCharts();
    updateCardVisibility();
    renderShootingPct();
    renderAvgScore();
    if (currentMode === "3v3") {
        renderWinRateDaily();
    }
    renderPlayerStats();
    renderMvpWins();
    renderMvpLosses();
    renderScoreDifferential();
    if (currentMode === "3v3") {
        renderWeekday();
    }
}

/* ── Raw Table ─────────────────────────────────── */

let rawPage = 1;
const rawPerPage = 20;
const playerDetailCache = {};
let rawSearchTimer = null;

async function renderRawTable() {
    const search = document.getElementById("history-search").value;
    const gameMode = document.getElementById("history-filter-mode").value;
    const result = document.getElementById("history-filter-result").value;

    const params = new URLSearchParams({
        page: rawPage,
        per_page: rawPerPage,
    });
    if (search) params.set("search", search);
    if (gameMode) params.set("game_mode", gameMode);
    if (result) params.set("result", result);

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
            <td><span class="mode-tag">${(m.game_mode || "").toUpperCase()}</span></td>
            <td><span class="result-badge ${resultClass}">${resultText}</span>${forfeitTag}</td>
            <td class="col-score">${m.score}</td>
            <td class="col-mvp">${m.mvp || "—"}</td>
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
        playerDetailCache[matchId] = await fetchJSON(`/api/matches/${matchId}/players`);
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
            <td class="player-name">${p.name}</td>
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

    document.querySelectorAll(".mode-btn").forEach((btn) => {
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

    document.getElementById("history-filter-mode").addEventListener("change", () => {
        rawPage = 1;
        renderRawTable();
    });

    document.getElementById("history-filter-result").addEventListener("change", () => {
        rawPage = 1;
        renderRawTable();
    });
});
