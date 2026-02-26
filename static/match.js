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

function playerRow(p, isMvp) {
  const mvpBadge = isMvp
    ? '<span class="match-mvp-badge">MVP</span>'
    : "";
  return `
    <tr>
      <td class="player-name">${esc(p.name)}${mvpBadge}</td>
      <td>${p.score ?? 0}</td>
      <td>${p.goals}</td>
      <td>${p.assists}</td>
      <td>${p.saves}</td>
      <td>${p.shots}</td>
      <td>${p.shooting_pct}%</td>
      <td>${p.demos}</td>
    </tr>`;
}

function playerTable(players, label, isMvpTeam) {
  const topScore = players.length ? Math.max(...players.map((p) => p.score ?? 0)) : -1;
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
            <th>Saves</th><th>Shots</th><th>Shot%</th><th>Demos</th>
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
        ${defW > 50 ? `
        <text x="${defX + defW / 2}" y="${cy - 8}" text-anchor="middle" dominant-baseline="central"
          fill="#ff3c3c" font-family="var(--font-display)" font-size="14" font-weight="700">${defPct.toFixed(0)}%</text>
        <text x="${defX + defW / 2}" y="${cy + 10}" text-anchor="middle" dominant-baseline="central"
          fill="rgba(255,60,60,0.6)" font-family="var(--font-display)" font-size="8" font-weight="700" letter-spacing="0.1em">DEFENSIVE</text>
        ` : ""}

        ${neuW > 50 ? `
        <text x="${neuX + neuW / 2}" y="${cy - 8}" text-anchor="middle" dominant-baseline="central"
          fill="var(--text)" font-family="var(--font-display)" font-size="14" font-weight="700">${neuPct.toFixed(0)}%</text>
        <text x="${neuX + neuW / 2}" y="${cy + 10}" text-anchor="middle" dominant-baseline="central"
          fill="var(--text-dim)" font-family="var(--font-display)" font-size="8" font-weight="700" letter-spacing="0.1em">NEUTRAL</text>
        ` : ""}

        ${offW > 50 ? `
        <text x="${offX + offW / 2}" y="${cy - 8}" text-anchor="middle" dominant-baseline="central"
          fill="var(--cyan)" font-family="var(--font-display)" font-size="14" font-weight="700">${offPct.toFixed(0)}%</text>
        <text x="${offX + offW / 2}" y="${cy + 10}" text-anchor="middle" dominant-baseline="central"
          fill="rgba(0,229,255,0.6)" font-family="var(--font-display)" font-size="8" font-weight="700" letter-spacing="0.1em">OFFENSIVE</text>
        ` : ""}
      </svg>
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

  const { match: m, team_players, opponent_players } = await res.json();

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
  const hasPossession = m.team_possession_seconds != null && m.opponent_possession_seconds != null;
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

  const html = `
    <a href="/" class="match-back">&larr; Back to Dashboard</a>

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

    <div class="player-tables">
      ${playerTable(team_players, "Our Team", true)}
      ${playerTable(opponent_players, "Opponents", false)}
    </div>`;

  document.getElementById("match-content").innerHTML = html;

  document.querySelector(".match-back").addEventListener("click", (e) => {
    e.preventDefault();
    if (document.referrer && new URL(document.referrer).origin === location.origin) {
      history.back();
    } else {
      location.href = "/";
    }
  });
}

document.addEventListener("DOMContentLoaded", loadMatch);
