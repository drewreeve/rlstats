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
