function esc(str) {
  const d = document.createElement("div");
  d.textContent = str;
  return d.innerHTML;
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
