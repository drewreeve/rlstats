function esc(str) {
  const d = document.createElement("div");
  d.textContent = str;
  return d.innerHTML;
}

/* ── Shared Chart Helpers ────────────────────────── */

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

function formatDuration(seconds) {
  if (!seconds) return "";
  const total = Math.round(seconds);
  const m = Math.floor(total / 60);
  const s = total % 60;
  return `${m}:${String(s).padStart(2, "0")}`;
}

async function fetchJSON(url) {
  const res = await fetch(url);
  return res.json();
}

function formatUTCDateTime(dateStr, { weekday = false } = {}) {
  if (!dateStr) return "—";
  const d = new Date(dateStr.replace(" ", "T") + "Z");
  return d.toLocaleString("en-US", {
    ...(weekday ? { weekday: "short" } : {}),
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
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
