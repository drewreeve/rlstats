const PLAYER_COLORS = {
    Drew: { r: 59, g: 130, b: 246 },
    Steve: { r: 34, g: 197, b: 94 },
    Jeff: { r: 249, g: 115, b: 22 },
};

const STAT_COLORS = {
    goals: { r: 239, g: 68, b: 68 },
    assists: { r: 59, g: 130, b: 246 },
    saves: { r: 34, g: 197, b: 94 },
    wins: { r: 34, g: 197, b: 94 },
    losses: { r: 239, g: 68, b: 68 },
};

function rgba({ r, g, b }, a) {
    return `rgba(${r}, ${g}, ${b}, ${a})`;
}

function barGradient(canvas, { r, g, b }) {
    const ctx = canvas.getContext("2d");
    const grad = ctx.createLinearGradient(0, 0, 0, canvas.parentElement.clientHeight);
    grad.addColorStop(0, `rgba(${r}, ${g}, ${b}, 0.9)`);
    grad.addColorStop(1, `rgba(${r}, ${g}, ${b}, 0.25)`);
    return grad;
}

function areaGradient(canvas, { r, g, b }) {
    const ctx = canvas.getContext("2d");
    const grad = ctx.createLinearGradient(0, 0, 0, canvas.parentElement.clientHeight);
    grad.addColorStop(0, `rgba(${r}, ${g}, ${b}, 0.25)`);
    grad.addColorStop(1, `rgba(${r}, ${g}, ${b}, 0.02)`);
    return grad;
}

Chart.defaults.color = "#a0a0c0";
Chart.defaults.borderColor = "rgba(255,255,255,0.06)";

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

async function renderShootingPct() {
    const data = await fetchJSON(`/api/shooting-pct?mode=${currentMode}`);
    const canvas = document.getElementById("chart-shooting");
    charts.shooting = new Chart(canvas, {
        type: "bar",
        data: {
            labels: data.map((d) => d.player),
            datasets: [
                {
                    label: "Shooting %",
                    data: data.map((d) => (d.shooting_pct ?? 0) * 100),
                    backgroundColor: data.map((d) =>
                        barGradient(canvas, PLAYER_COLORS[d.player]),
                    ),
                    borderColor: data.map((d) =>
                        rgba(PLAYER_COLORS[d.player], 1),
                    ),
                    borderWidth: 1,
                },
            ],
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
                y: {
                    beginAtZero: true,
                    max: 100,
                    ticks: { callback: (v) => v + "%" },
                },
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
            datasets: [
                {
                    label: "Avg Score",
                    data: data.map((d) => d.avg_score ?? 0),
                    backgroundColor: data.map((d) =>
                        barGradient(canvas, PLAYER_COLORS[d.player]),
                    ),
                    borderColor: data.map((d) =>
                        rgba(PLAYER_COLORS[d.player], 1),
                    ),
                    borderWidth: 1,
                },
            ],
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
            },
        },
    });
}

async function renderWinRateDaily() {
    const data = await fetchJSON(`/api/win-loss-daily?mode=${currentMode}`);
    const canvas = document.getElementById("chart-win-rate");
    const lineColor = PLAYER_COLORS.Drew;
    charts.winRate = new Chart(canvas, {
        type: "line",
        data: {
            labels: data.map((d) => d.date ?? ""),
            datasets: [
                {
                    label: "Win Rate",
                    data: data.map((d) => (d.win_rate ?? 0) * 100),
                    borderColor: rgba(lineColor, 1),
                    backgroundColor: areaGradient(canvas, lineColor),
                    fill: true,
                    tension: 0.35,
                    pointBackgroundColor: rgba(lineColor, 1),
                    pointBorderColor: "#16213e",
                    pointBorderWidth: 2,
                    pointRadius: 5,
                    pointHoverRadius: 7,
                },
            ],
        },
        options: {
            plugins: {
                legend: { display: false },
            },
            scales: {
                y: {
                    beginAtZero: true,
                    max: 100,
                    ticks: { callback: (v) => v + "%" },
                },
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
                    borderColor: rgba(STAT_COLORS.goals, 1),
                    borderWidth: 1,
                },
                {
                    label: "Assists",
                    data: data.map((d) => d.assists),
                    backgroundColor: barGradient(canvas, STAT_COLORS.assists),
                    borderColor: rgba(STAT_COLORS.assists, 1),
                    borderWidth: 1,
                },
                {
                    label: "Saves",
                    data: data.map((d) => d.saves),
                    backgroundColor: barGradient(canvas, STAT_COLORS.saves),
                    borderColor: rgba(STAT_COLORS.saves, 1),
                    borderWidth: 1,
                },
            ],
        },
        options: {
            scales: { y: { beginAtZero: true } },
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
            datasets: [
                {
                    label: "MVP Wins",
                    data: data.map((d) => d.mvp_wins),
                    backgroundColor: data.map((d) =>
                        barGradient(canvas, PLAYER_COLORS[d.player]),
                    ),
                    borderColor: data.map((d) =>
                        rgba(PLAYER_COLORS[d.player], 1),
                    ),
                    borderWidth: 1,
                },
            ],
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
            scales: { y: { beginAtZero: true } },
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
            datasets: [
                {
                    label: "Loss MVPs",
                    data: data.map((d) => d.loss_mvps),
                    backgroundColor: data.map((d) =>
                        barGradient(canvas, PLAYER_COLORS[d.player]),
                    ),
                    borderColor: data.map((d) =>
                        rgba(PLAYER_COLORS[d.player], 1),
                    ),
                    borderWidth: 1,
                },
            ],
        },
        options: {
            plugins: { legend: { display: false } },
            scales: { y: { beginAtZero: true } },
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
                    borderColor: rgba(STAT_COLORS.wins, 1),
                    borderWidth: 1,
                },
                {
                    label: "Losses",
                    data: data.map((d) => d.losses),
                    backgroundColor: barGradient(canvas, STAT_COLORS.losses),
                    borderColor: rgba(STAT_COLORS.losses, 1),
                    borderWidth: 1,
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
            },
            scales: {
                x: { stacked: true },
                y: { stacked: true, beginAtZero: true },
            },
        },
    });
}

function updateCardVisibility() {
    const is3v3 = currentMode === "3v3";
    document.getElementById("card-win-rate").style.display = is3v3 ? "" : "none";
    document.getElementById("card-weekday").style.display = is3v3 ? "" : "none";
}

async function renderAll() {
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
    if (currentMode === "3v3") {
        renderWeekday();
    }
}

document.addEventListener("DOMContentLoaded", () => {
    renderAll();

    document.querySelectorAll(".pill").forEach((btn) => {
        btn.addEventListener("click", () => {
            document.querySelector(".pill.active").classList.remove("active");
            btn.classList.add("active");
            currentMode = btn.dataset.mode;
            renderAll();
        });
    });
});
