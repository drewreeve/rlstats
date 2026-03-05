function esc(str) {
  const d = document.createElement("div");
  d.textContent = str;
  return d.innerHTML;
}

function formatDuration(seconds) {
  if (!seconds) return "";
  const total = Math.round(seconds);
  const m = Math.floor(total / 60);
  const s = total % 60;
  return `${m}:${String(s).padStart(2, "0")}`;
}

function formatDate(dateStr) {
  if (!dateStr) return "";
  const d = new Date(dateStr);
  return d.toLocaleDateString("en-US", {
    weekday: "short",
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function ringChart(pct, color, size) {
  const r = size / 2 - 6;
  const circ = 2 * Math.PI * r;
  const filled = circ * (pct / 100);
  return `
    <svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}" class="accuracy-ring">
      <circle cx="${size / 2}" cy="${size / 2}" r="${r}"
        fill="none" stroke="rgba(255,255,255,0.06)" stroke-width="8"/>
      <circle cx="${size / 2}" cy="${size / 2}" r="${r}"
        fill="none" stroke="${color}" stroke-width="8"
        stroke-dasharray="${filled} ${circ - filled}"
        stroke-dashoffset="${circ / 4}"
        stroke-linecap="round"/>
      <text x="${size / 2}" y="${size / 2}" text-anchor="middle" dominant-baseline="central"
        fill="var(--text-bright)" font-family="var(--font-display)" font-size="16" font-weight="700">
        ${pct.toFixed(0)}%
      </text>
    </svg>`;
}

function fmtStat(val, decimals) {
  if (val == null) return "-";
  return decimals ? val.toFixed(decimals) : Math.round(val);
}

function playerRow(p, isMvp) {
  const mvpBadge = isMvp ? '<span class="match-mvp-badge">MVP</span>' : "";
  return `
    <tr>
      <td class="player-name">${esc(p.name)}${mvpBadge}</td>
      <td>${p.score ?? 0}</td>
      <td>${p.goals}</td>
      <td>${p.assists}</td>
      <td>${p.saves}</td>
      <td>${p.shots}</td>
    </tr>`;
}

function playerTable(players, label, isMvpTeam) {
  const topScore = players.length
    ? Math.max(...players.map((p) => p.score ?? 0))
    : -1;
  const rows = players
    .map((p) => playerRow(p, isMvpTeam && (p.score ?? 0) === topScore))
    .join("");
  return `
    <div class="match-player-table">
      <h3 class="match-table-label">${esc(label)}</h3>
      <table>
        <thead>
          <tr>
            <th>Player</th><th>Score</th><th>Goals</th><th>Assists</th>
            <th>Saves</th><th>Shots</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
}

function pitchDiagram(defSec, neuSec, offSec) {
  const total = defSec + neuSec + offSec;
  if (total <= 0) return "";
  const defPct = (defSec / total) * 100;
  const neuPct = (neuSec / total) * 100;
  const offPct = (offSec / total) * 100;

  const w = 560;
  const h = 240;

  // Equal-width zones (defensive left, neutral middle, offensive right)
  const zoneW = w / 3;
  const defW = zoneW;
  const neuW = zoneW;
  const offW = zoneW;

  const defX = 0;
  const neuX = zoneW;
  const offX = zoneW * 2;

  // Center circle
  const cy = h / 2;
  const midX = defW + neuW / 2;
  const cr = Math.min(h, neuW) * 0.25;

  return `
    <div class="pitch-diagram">
      <svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">
        <!-- Defensive zone (left) -->
        <rect x="${defX}" y="0" width="${defW}" height="${h}"
          fill="rgba(255, 60, 60, 0.15)"/>
        <!-- Neutral zone (middle) -->
        <rect x="${neuX}" y="0" width="${neuW}" height="${h}"
          fill="rgba(255, 255, 255, 0.06)"/>
        <!-- Offensive zone (right) -->
        <rect x="${offX}" y="0" width="${offW}" height="${h}"
          fill="rgba(0, 229, 255, 0.15)"/>

        <!-- Border -->
        <rect x="0" y="0" width="${w}" height="${h}"
          fill="none" stroke="rgba(255,255,255,0.12)" stroke-width="2"/>

        <!-- Divider lines -->
        <line x1="${defW}" y1="0" x2="${defW}" y2="${h}"
          stroke="rgba(255,255,255,0.15)" stroke-width="1" stroke-dasharray="6,4"/>
        <line x1="${defW + neuW}" y1="0" x2="${defW + neuW}" y2="${h}"
          stroke="rgba(255,255,255,0.15)" stroke-width="1" stroke-dasharray="6,4"/>

        <!-- Center circle -->
        ${neuW > 20 ? `<circle cx="${midX}" cy="${cy}" r="${cr}" fill="none" stroke="rgba(255,255,255,0.12)" stroke-width="1"/>` : ""}

        <!-- Labels -->
        ${
          defW > 50
            ? `
        <text x="${defX + defW / 2}" y="${cy - 8}" text-anchor="middle" dominant-baseline="central"
          fill="#ff3c3c" font-family="var(--font-display)" font-size="14" font-weight="700">${defPct.toFixed(0)}%</text>
        <text x="${defX + defW / 2}" y="${cy + 10}" text-anchor="middle" dominant-baseline="central"
          fill="rgba(255,60,60,0.6)" font-family="var(--font-display)" font-size="8" font-weight="700" letter-spacing="0.1em">DEFENSIVE</text>
        `
            : ""
        }

        ${
          neuW > 50
            ? `
        <text x="${neuX + neuW / 2}" y="${cy - 8}" text-anchor="middle" dominant-baseline="central"
          fill="var(--text)" font-family="var(--font-display)" font-size="14" font-weight="700">${neuPct.toFixed(0)}%</text>
        <text x="${neuX + neuW / 2}" y="${cy + 10}" text-anchor="middle" dominant-baseline="central"
          fill="var(--text-dim)" font-family="var(--font-display)" font-size="8" font-weight="700" letter-spacing="0.1em">NEUTRAL</text>
        `
            : ""
        }

        ${
          offW > 50
            ? `
        <text x="${offX + offW / 2}" y="${cy - 8}" text-anchor="middle" dominant-baseline="central"
          fill="var(--cyan)" font-family="var(--font-display)" font-size="14" font-weight="700">${offPct.toFixed(0)}%</text>
        <text x="${offX + offW / 2}" y="${cy + 10}" text-anchor="middle" dominant-baseline="central"
          fill="rgba(0,229,255,0.6)" font-family="var(--font-display)" font-size="8" font-weight="700" letter-spacing="0.1em">OFFENSIVE</text>
        `
            : ""
        }
      </svg>
    </div>`;
}

function matchTimeline(events, teamNum, durationSeconds) {
  if (!events || events.length === 0) return "";

  const totalSeconds = durationSeconds || 300;
  const w = 560;
  const h = 175;
  const pad = { left: 40, right: 20, top: 20, bottom: 40 };
  const plotW = w - pad.left - pad.right;
  const midY = pad.top + (h - pad.top - pad.bottom) / 2;

  function xPos(sec) {
    return pad.left + (sec / totalSeconds) * plotW;
  }

  // Time axis markers
  let ticksSvg = "";
  const timeY = h - pad.bottom + 18;
  const interval = totalSeconds <= 300 ? 60 : 60;
  for (let t = interval; t <= totalSeconds; t += interval) {
    const x = xPos(t);
    const min = Math.floor(t / 60);
    const sec = t % 60;
    const label =
      sec === 0 ? `${min}:00` : `${min}:${String(sec).padStart(2, "0")}`;
    ticksSvg += `
      <line x1="${x}" y1="${midY - 12}" x2="${x}" y2="${midY + 12}"
        stroke="rgba(255,255,255,0.08)" stroke-width="1"/>
      <text x="${x}" y="${timeY}" text-anchor="middle"
        fill="var(--text-dim)" font-family="var(--font-mono)" font-size="8">${label}</text>`;
  }

  // Event markers
  const eventConfig = {
    goal: { r: 6, teamColor: "var(--cyan)", oppColor: "#ff3c3c", label: "G" },
    shot: {
      r: 3,
      teamColor: "rgba(0,229,255,0.4)",
      oppColor: "rgba(255,60,60,0.4)",
      label: "S",
    },
    save: {
      r: 4,
      teamColor: "rgba(0,229,255,0.7)",
      oppColor: "rgba(255,60,60,0.7)",
      label: "V",
    },
    demo: { r: 4, teamColor: "#ff6b00", oppColor: "#ff6b00", label: "D" },
  };

  // Group events at same position to avoid overlap
  let markersSvg = "";
  const teamOffsets = {};
  const oppOffsets = {};

  for (const ev of events) {
    const cfg = eventConfig[ev.event_type];
    if (!cfg) continue;
    const isTeam = ev.team === teamNum;
    const x = xPos(ev.game_seconds);
    const color = isTeam ? cfg.teamColor : cfg.oppColor;

    // Stack events vertically per side
    const offsets = isTeam ? teamOffsets : oppOffsets;
    const bucket = Math.round(x);
    offsets[bucket] = (offsets[bucket] || 0) + 1;
    const stackIdx = offsets[bucket] - 1;
    const baseY = isTeam ? midY - 18 : midY + 18;
    const stackDir = isTeam ? -1 : 1;
    const y = baseY + stackIdx * stackDir * 14;

    const label =
      ev.event_type.charAt(0).toUpperCase() + ev.event_type.slice(1);
    const tooltip = `${label} by ${ev.name}`;

    if (ev.event_type === "goal") {
      // Goal: larger filled circle with glow
      markersSvg += `
        <g><title>${esc(tooltip)}</title>
        <circle cx="${x}" cy="${y}" r="${cfg.r}" fill="${color}" opacity="0.9"/>
        <circle cx="${x}" cy="${y}" r="${cfg.r + 3}" fill="none" stroke="${color}" stroke-width="1" opacity="0.3"/>
        </g>`;
    } else if (ev.event_type === "demo") {
      // Demo: diamond shape
      markersSvg += `
        <g><title>${esc(tooltip)}</title>
        <polygon points="${x},${y - cfg.r} ${x + cfg.r},${y} ${x},${y + cfg.r} ${x - cfg.r},${y}"
          fill="${color}" opacity="0.8"/>
        </g>`;
    } else if (ev.event_type === "save") {
      // Save: shield/square
      const s = cfg.r;
      markersSvg += `
        <g><title>${esc(tooltip)}</title>
        <rect x="${x - s}" y="${y - s}" width="${s * 2}" height="${s * 2}" rx="1"
          fill="${color}" opacity="0.8"/>
        </g>`;
    } else {
      // Shot: small triangle
      const s = cfg.r;
      const dir = isTeam ? -1 : 1;
      markersSvg += `
        <g><title>${esc(tooltip)}</title>
        <polygon points="${x},${y - s * dir} ${x + s},${y + s * dir} ${x - s},${y + s * dir}"
          fill="${color}" opacity="0.7"/>
        </g>`;
    }
  }

  // Legend
  const legendY = h - 4;
  const legendItems = [
    { label: "Goal", shape: "circle", color: "var(--text-dim)", r: 5 },
    { label: "Shot", shape: "circle", color: "var(--text-dim)", r: 3 },
    { label: "Save", shape: "rect", color: "var(--text-dim)", r: 3 },
    { label: "Demo", shape: "diamond", color: "var(--text-dim)", r: 3 },
  ];

  return `
    <div class="match-timeline">
      <div class="match-timeline-header">
        <span class="match-timeline-label">MATCH TIMELINE</span>
        <span class="match-timeline-legend">
          <span class="tl-legend-item"><svg width="10" height="10"><circle cx="5" cy="5" r="4" fill="var(--text-dim)"/></svg> Goal</span>
          <span class="tl-legend-item"><svg width="8" height="8"><polygon points="4,1 7,7 1,7" fill="var(--text-dim)"/></svg> Shot</span>
          <span class="tl-legend-item"><svg width="8" height="8"><rect width="7" height="7" rx="1" fill="var(--text-dim)"/></svg> Save</span>
          <span class="tl-legend-item"><svg width="8" height="8"><polygon points="4,0 8,4 4,8 0,4" fill="var(--text-dim)"/></svg> Demo</span>
        </span>
      </div>
      <svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" class="timeline-svg">
        <!-- Center line -->
        <line x1="${pad.left}" y1="${midY}" x2="${w - pad.right}" y2="${midY}"
          stroke="rgba(255,255,255,0.1)" stroke-width="1"/>
        <!-- Team labels -->
        <text x="${pad.left - 6}" y="${midY - 10}" text-anchor="end"
          fill="var(--cyan)" font-family="var(--font-display)" font-size="7" font-weight="700" letter-spacing="0.05em">TEAM</text>
        <text x="${pad.left - 6}" y="${midY + 14}" text-anchor="end"
          fill="#ff3c3c" font-family="var(--font-display)" font-size="7" font-weight="700" letter-spacing="0.05em">OPP</text>
        <!-- Start/end markers -->
        <line x1="${pad.left}" y1="${midY - 14}" x2="${pad.left}" y2="${midY + 14}"
          stroke="rgba(255,255,255,0.15)" stroke-width="1"/>
        <line x1="${w - pad.right}" y1="${midY - 14}" x2="${w - pad.right}" y2="${midY + 14}"
          stroke="rgba(255,255,255,0.15)" stroke-width="1"/>
        <text x="${pad.left}" y="${timeY}" text-anchor="middle"
          fill="var(--text-dim)" font-family="var(--font-mono)" font-size="8">0:00</text>
        ${ticksSvg}
        ${markersSvg}
      </svg>
    </div>`;
}

function barChart(allPlayers, key, label, fmt) {
  const vals = allPlayers.map((p) => p[key]);
  if (vals.every((v) => v == null)) return "";
  const maxVal = Math.max(...vals.filter((v) => v != null));
  const sorted = [
    ...allPlayers.filter((p) => p.isTeam),
    ...allPlayers.filter((p) => !p.isTeam),
  ];
  const firstOppIdx = sorted.findIndex((p) => !p.isTeam);

  const rows = sorted
    .map((p, i) => {
      const pct = p[key] != null ? (p[key] / maxVal) * 100 : 0;
      const valText = p[key] != null ? fmt(p[key]) : "-";
      const side = p.isTeam ? "team" : "opp";
      const sep =
        i === firstOppIdx && firstOppIdx > 0
          ? '<div class="chart-team-separator"></div>'
          : "";
      return `${sep}
      <div class="chart-row">
        <span class="chart-name ${side}-color">${esc(p.name)}</span>
        <span class="chart-bar-track">
          <span class="chart-bar-fill ${side}" style="width:${pct.toFixed(1)}%"></span>
        </span>
        <span class="chart-value">${valText}</span>
      </div>`;
    })
    .join("");

  return `
    <div class="player-chart">
      <div class="chart-label">${label}</div>
      ${rows}
    </div>`;
}

function avgSpeedChart(allPlayers) {
  return barChart(allPlayers, "avg_speed", "AVG SPEED", (v) => Math.round(v));
}

function boostPerMinChart(allPlayers) {
  return barChart(allPlayers, "boost_per_minute", "BOOST / MIN", (v) =>
    v.toFixed(1),
  );
}

function padStatsChart(allPlayers) {
  const hasData = allPlayers.some(
    (p) =>
      p.small_pads != null ||
      p.large_pads != null ||
      p.stolen_small_pads != null ||
      p.stolen_large_pads != null,
  );
  if (!hasData) return "";

  // Segments: stolen is a subset of collected, so split into own + stolen slices
  const segments = [
    { fn: (p) => (p.small_pads ?? 0) - (p.stolen_small_pads ?? 0), cls: "pad-sm-col", label: "Small (own)" },
    { fn: (p) => (p.stolen_small_pads ?? 0),                        cls: "pad-sm-stl", label: "Small (stolen)" },
    { fn: (p) => (p.large_pads ?? 0) - (p.stolen_large_pads ?? 0), cls: "pad-lg-col", label: "Large (own)" },
    { fn: (p) => (p.stolen_large_pads ?? 0),                        cls: "pad-lg-stl", label: "Large (stolen)" },
  ];

  const totals = allPlayers.map((p) => (p.small_pads ?? 0) + (p.large_pads ?? 0));
  const maxTotal = Math.max(...totals, 1);

  const sorted = [
    ...allPlayers.filter((p) => p.isTeam),
    ...allPlayers.filter((p) => !p.isTeam),
  ];
  const firstOppIdx = sorted.findIndex((p) => !p.isTeam);

  const rows = sorted
    .map((p, i) => {
      const total = (p.small_pads ?? 0) + (p.large_pads ?? 0);
      const side = p.isTeam ? "team" : "opp";
      const sep =
        i === firstOppIdx && firstOppIdx > 0
          ? '<div class="chart-team-separator"></div>'
          : "";
      const bars = segments
        .map((seg) => {
          const val = seg.fn(p);
          const pct = (val / maxTotal) * 100;
          return pct > 0
            ? `<span class="chart-bar-fill ${seg.cls}" style="width:${pct.toFixed(1)}%"></span>`
            : "";
        })
        .join("");

      return `${sep}
      <div class="chart-row">
        <span class="chart-name ${side}-color">${esc(p.name)}</span>
        <span class="chart-bar-track">${bars}</span>
        <span class="chart-value">${total}</span>
      </div>`;
    })
    .join("");

  const legend = segments
    .map(
      (seg) =>
        `<span class="chart-legend-item"><span class="chart-legend-swatch ${seg.cls}"></span>${seg.label}</span>`,
    )
    .join("");

  return `
    <div class="player-chart">
      <div class="chart-label">PAD STATS</div>
      ${rows}
      <div class="chart-legend">${legend}</div>
    </div>`;
}

async function loadMatch() {
  const parts = window.location.pathname.split("/");
  const matchId = parts[parts.length - 1];

  const res = await fetch(`/api/matches/${matchId}`);
  if (!res.ok) {
    document.getElementById("match-content").innerHTML =
      '<p style="text-align:center;padding:3rem;color:var(--text-dim)">Match not found.</p>';
    return;
  }

  const { match: m, events, team_players, opponent_players } = await res.json();

  const isWin = m.result === "win";
  const accentClass = isWin ? "match-win" : "match-loss";
  const resultText = isWin ? "WIN" : "LOSS";
  const forfeitTag = m.forfeit ? '<span class="forfeit-tag">FF</span>' : "";

  // Team shooting stats
  const teamShots = team_players.reduce((s, p) => s + p.shots, 0);
  const teamGoals = team_players.reduce((s, p) => s + p.goals, 0);
  const teamPct = teamShots > 0 ? (teamGoals / teamShots) * 100 : 0;

  const oppShots = opponent_players.reduce((s, p) => s + p.shots, 0);
  const oppGoals = opponent_players.reduce((s, p) => s + p.goals, 0);
  const oppPct = oppShots > 0 ? (oppGoals / oppShots) * 100 : 0;

  // Possession
  const hasPossession =
    m.team_possession_seconds != null && m.opponent_possession_seconds != null;
  let possessionHTML = "";
  if (hasPossession) {
    const total = m.team_possession_seconds + m.opponent_possession_seconds;
    const teamPoss = total > 0 ? (m.team_possession_seconds / total) * 100 : 50;
    const oppPoss = 100 - teamPoss;
    possessionHTML = `
      <div class="match-stat-row">
        <span class="match-stat-value team-color">${teamPoss.toFixed(0)}%</span>
        <span class="match-stat-label">POSSESSION</span>
        <span class="match-stat-value opp-color">${oppPoss.toFixed(0)}%</span>
      </div>
      <div class="possession-bar">
        <div class="possession-fill team-fill" style="width:${teamPoss}%"></div>
        <div class="possession-fill opp-fill" style="width:${oppPoss}%"></div>
      </div>`;
  }

  let html = `
    <div class="match-header ${accentClass}">
      <div class="match-teams">
        <div class="match-team-name team-color">Our Team</div>
        <div class="match-score-block">
          <span class="match-score team-color">${m.team_score}</span>
          <span class="match-score-divider">:</span>
          <span class="match-score opp-color">${m.opponent_score}</span>
        </div>
        <div class="match-team-name opp-color">Opponents</div>
      </div>
      <div class="match-result-row">
        <span class="result-badge ${isWin ? "badge-win" : "badge-loss"}">${resultText}</span>
        ${forfeitTag}
      </div>
      <div class="match-meta">
        ${formatDate(m.played_at)}
        ${m.game_mode ? ' &middot; <span class="mode-tag">' + esc(m.game_mode.toUpperCase()) + "</span>" : ""}
        ${m.duration_seconds ? " &middot; " + formatDuration(m.duration_seconds) : ""}
      </div>
    </div>

    <div class="match-stats">
      ${possessionHTML}

      <div class="match-stat-row">
        <span class="match-stat-value team-color">${teamShots}</span>
        <span class="match-stat-label">SHOTS</span>
        <span class="match-stat-value opp-color">${oppShots}</span>
      </div>
      <div class="shots-bar">
        <div class="possession-fill team-fill" style="width:${teamShots + oppShots > 0 ? (teamShots / (teamShots + oppShots)) * 100 : 50}%"></div>
        <div class="possession-fill opp-fill" style="width:${teamShots + oppShots > 0 ? (oppShots / (teamShots + oppShots)) * 100 : 50}%"></div>
      </div>

      ${
        m.team_boost_collected != null
          ? `
      <div class="match-stat-row">
        <span class="match-stat-value team-color">${m.team_boost_collected}</span>
        <span class="match-stat-label">BOOST COLLECTED</span>
        <span class="match-stat-value opp-color">${m.opponent_boost_collected}</span>
      </div>
      <div class="shots-bar">
        <div class="possession-fill team-fill" style="width:${(m.team_boost_collected / (m.team_boost_collected + m.opponent_boost_collected)) * 100}%"></div>
        <div class="possession-fill opp-fill" style="width:${(m.opponent_boost_collected / (m.team_boost_collected + m.opponent_boost_collected)) * 100}%"></div>
      </div>
      <div class="match-stat-row">
        <span class="match-stat-value team-color">${m.team_boost_stolen}</span>
        <span class="match-stat-label">BOOST STOLEN</span>
        <span class="match-stat-value opp-color">${m.opponent_boost_stolen}</span>
      </div>
      <div class="shots-bar">
        <div class="possession-fill team-fill" style="width:${m.team_boost_stolen + m.opponent_boost_stolen > 0 ? (m.team_boost_stolen / (m.team_boost_stolen + m.opponent_boost_stolen)) * 100 : 50}%"></div>
        <div class="possession-fill opp-fill" style="width:${m.team_boost_stolen + m.opponent_boost_stolen > 0 ? (m.opponent_boost_stolen / (m.team_boost_stolen + m.opponent_boost_stolen)) * 100 : 50}%"></div>
      </div>
      `
          : ""
      }

      <div class="match-accuracy">
        <div class="match-accuracy-item">
          <span class="match-accuracy-label">SHOT ACCURACY</span>
          ${ringChart(teamPct, "var(--cyan)", 100)}
        </div>
        ${m.defensive_third_seconds != null ? pitchDiagram(m.defensive_third_seconds, m.neutral_third_seconds, m.offensive_third_seconds) : ""}
        <div class="match-accuracy-item">
          <span class="match-accuracy-label">SHOT ACCURACY</span>
          ${ringChart(oppPct, "#ff3c3c", 100)}
        </div>
      </div>
    </div>

    ${matchTimeline(events, m.team, m.duration_seconds)}

    <div class="player-tables">
      ${playerTable(team_players, "Our Team", isWin)}
      ${playerTable(opponent_players, "Opponents", !isWin)}
    </div>`;

  const allPlayers = [
    ...team_players.map((p) => ({ ...p, isTeam: true })),
    ...opponent_players.map((p) => ({ ...p, isTeam: false })),
  ];

  const speedChart = avgSpeedChart(allPlayers);
  const boostChart = boostPerMinChart(allPlayers);
  const padChart = padStatsChart(allPlayers);

  if (speedChart || boostChart || padChart) {
    const topRow =
      speedChart || boostChart
        ? `
        <div class="player-charts-row">
          ${speedChart}
          ${boostChart}
        </div>`
        : "";
    html += `
      <div class="player-charts">
        ${topRow}
        ${padChart}
      </div>`;
  }

  document.getElementById("match-content").innerHTML = html;
}

document.addEventListener("DOMContentLoaded", () => {
  const navWrap = document.querySelector(".mode-nav-wrap");
  const nav = document.querySelector(".mode-nav");
  if (nav && navWrap) {
    nav.addEventListener("scroll", () => {
      const atEnd = nav.scrollLeft + nav.clientWidth >= nav.scrollWidth - 4;
      navWrap.classList.toggle("scrolled-end", atEnd);
    });
  }

  loadMatch();
});
