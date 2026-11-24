/* ============================================================================
   MetroPulse DC — Visual-First Multi-Page Transit Intelligence Frontend
   Real-Time NATS SSE Push · High-Signal Visual Charts · Daylight & Night Modes
   ============================================================================ */

(() => {
  "use strict";

  const PAGE_META = {
    overview: { path: "/", title: "Executive Pulse" },
    "live-map": { path: "/live-map", title: "Live Fleet Map" },
    "root-cause": { path: "/root-cause", title: "Root Cause Analysis" },
    "ml-forecast": { path: "/ml-forecast", title: "ML Forecast (+15m)" },
    pipeline: { path: "/pipeline", title: "Pipeline Health" }
  };

  const INCIDENT_CATEGORIES = {
    0: "Incident",
    1: "Accident",
    2: "Fog",
    3: "Road Hazard",
    4: "Rain",
    5: "Ice",
    6: "Traffic Jam",
    7: "Lane Closed",
    8: "Road Closed",
    9: "Road Works",
    10: "Wind",
    11: "Flooding",
    14: "Disabled Veh"
  };

  const DIST_META = {
    early: { short: "Early", range: "< -1m", color: "#0284c7" },
    on_time: { short: "On-Time", range: "-1..+2m", color: "#059669" },
    slight: { short: "Slight", range: "+2..+5m", color: "#d97706" },
    late: { short: "Late", range: "+5..+10m", color: "#ea580c" },
    severe: { short: "Severe", range: "> +10m", color: "#dc2626" }
  };

  const CAUSE_COLORS = {
    "Nearby Incident": { cls: "crimson", hex: "#dc2626" },
    "Traffic Congestion": { cls: "amber", hex: "#d97706" },
    "Trip Cascading": { cls: "indigo", hex: "#4f46e5" },
    "Schedule / Dispatch": { cls: "sky", hex: "#0284c7" },
    "Minor Variance": { cls: "neutral", hex: "#64748b" },
    "Healthy": { cls: "emerald", hex: "#059669" }
  };

  // --------------------------------------------------------------------------
  // Custom Visual Chart.js Plugins (Zones, 45° Fit Line, SLA Lines, Labels)
  // --------------------------------------------------------------------------
  const visualCanvasPlugin = {
    id: "visualCanvas",
    beforeDatasetsDraw(chart, _args, opts) {
      if (!opts) return;
      const { ctx, chartArea, scales } = chart;
      if (!chartArea) return;
      ctx.save();

      // 1. Speed vs Delay Tinted Quadrant Zones
      if (opts.quadrantZones && scales.x && scales.y) {
        const ySla = Math.min(chartArea.bottom, Math.max(chartArea.top, scales.y.getPixelForValue(opts.ySplit ?? 5)));
        const xMid = Math.min(chartArea.right, Math.max(chartArea.left, scales.x.getPixelForValue(opts.xSplit ?? 15)));

        // Top-Left: Traffic Bottleneck (Red tint)
        if (ySla > chartArea.top && xMid > chartArea.left) {
          ctx.fillStyle = cssVar("--zone-red");
          ctx.fillRect(chartArea.left, chartArea.top, xMid - chartArea.left, ySla - chartArea.top);
        }
        // Top-Right: Schedule / Dispatch (Amber tint)
        if (ySla > chartArea.top && chartArea.right > xMid) {
          ctx.fillStyle = cssVar("--zone-amber");
          ctx.fillRect(xMid, chartArea.top, chartArea.right - xMid, ySla - chartArea.top);
        }
        // Bottom: Healthy SLA (Green tint)
        if (chartArea.bottom > ySla) {
          ctx.fillStyle = cssVar("--zone-green");
          ctx.fillRect(chartArea.left, ySla, chartArea.right - chartArea.left, chartArea.bottom - ySla);
        }

        // Zone watermark labels
        ctx.font = "800 10px 'Plus Jakarta Sans', sans-serif";
        ctx.globalAlpha = 0.72;
        if (ySla - chartArea.top > 22) {
          ctx.fillStyle = cssVar("--crimson");
          ctx.textAlign = "left";
          ctx.fillText("TRAFFIC BOTTLENECK", chartArea.left + 8, chartArea.top + 15);
          ctx.fillStyle = cssVar("--amber");
          ctx.textAlign = "right";
          ctx.fillText("SCHEDULE / DISPATCH", chartArea.right - 8, chartArea.top + 15);
        }
        ctx.fillStyle = cssVar("--emerald");
        ctx.textAlign = "right";
        ctx.fillText("HEALTHY (≤ 5M SLA)", chartArea.right - 8, chartArea.bottom - 8);
      }

      // 2. ML Calibration Late-Alert Corner + 45° Perfect Fit Line
      if (opts.fitDiagonal && scales.x && scales.y) {
        const x5 = scales.x.getPixelForValue(5);
        const y5 = scales.y.getPixelForValue(5);
        if (x5 < chartArea.right && y5 > chartArea.top) {
          ctx.fillStyle = cssVar("--zone-red");
          const rx = Math.max(chartArea.left, x5);
          const ry = chartArea.top;
          ctx.fillRect(rx, ry, chartArea.right - rx, Math.min(chartArea.bottom, y5) - ry);
          ctx.fillStyle = cssVar("--crimson");
          ctx.font = "800 9.5px 'Plus Jakarta Sans', sans-serif";
          ctx.textAlign = "right";
          ctx.globalAlpha = 0.78;
          ctx.fillText("LATE >5M ZONE", chartArea.right - 6, chartArea.top + 13);
        }

        // Diagonal y = x line
        const lo = Math.max(scales.x.min, scales.y.min);
        const hi = Math.min(scales.x.max, scales.y.max);
        ctx.globalAlpha = 1;
        ctx.beginPath();
        ctx.setLineDash([5, 5]);
        ctx.strokeStyle = cssVar("--brand");
        ctx.lineWidth = 1.6;
        ctx.moveTo(scales.x.getPixelForValue(lo), scales.y.getPixelForValue(lo));
        ctx.lineTo(scales.x.getPixelForValue(hi), scales.y.getPixelForValue(hi));
        ctx.stroke();
        ctx.setLineDash([]);
        ctx.fillStyle = cssVar("--brand");
        ctx.font = "700 10px 'Plus Jakarta Sans', sans-serif";
        ctx.textAlign = "left";
        ctx.fillText("Perfect Fit (y = x)", chartArea.left + 8, chartArea.top + 14);
      }

      ctx.restore();
    },

    afterDatasetsDraw(chart, _args, opts) {
      if (!opts) return;
      const { ctx, chartArea, scales } = chart;
      if (!chartArea) return;
      ctx.save();

      // Reference Y line (horizontal)
      if (opts.yLine !== undefined && scales.y) {
        const yPos = scales.y.getPixelForValue(opts.yLine);
        if (yPos >= chartArea.top && yPos <= chartArea.bottom) {
          ctx.beginPath();
          ctx.setLineDash([5, 5]);
          ctx.strokeStyle = opts.yColor || cssVar("--chart-ref");
          ctx.lineWidth = 1.4;
          ctx.moveTo(chartArea.left, yPos);
          ctx.lineTo(chartArea.right, yPos);
          ctx.stroke();
          if (opts.yLabel) {
            ctx.setLineDash([]);
            ctx.fillStyle = opts.yColor || cssVar("--crimson");
            ctx.font = "700 10px 'Plus Jakarta Sans', sans-serif";
            ctx.textAlign = "right";
            ctx.fillText(opts.yLabel, chartArea.right - 6, yPos - 4);
          }
        }
      }

      // Reference X line (vertical)
      if (opts.xLine !== undefined && scales.x) {
        const xPos = scales.x.getPixelForValue(opts.xLine);
        if (xPos >= chartArea.left && xPos <= chartArea.right) {
          ctx.beginPath();
          ctx.setLineDash([5, 5]);
          ctx.strokeStyle = opts.xColor || cssVar("--chart-ref");
          ctx.lineWidth = 1.4;
          ctx.moveTo(xPos, chartArea.top);
          ctx.lineTo(xPos, chartArea.bottom);
          ctx.stroke();
          if (opts.xLabel) {
            ctx.setLineDash([]);
            ctx.font = "700 9.5px 'Plus Jakarta Sans', sans-serif";
            const tw = ctx.measureText(opts.xLabel).width;
            const lx = xPos + 4;
            const ly = opts.xLabelBottom ? chartArea.bottom - 18 : chartArea.top + 2;
            ctx.fillStyle = cssVar("--bg-surface");
            ctx.globalAlpha = 0.92;
            ctx.fillRect(lx - 2, ly, tw + 6, 14);
            ctx.globalAlpha = 1;
            ctx.fillStyle = opts.xColor || cssVar("--crimson");
            ctx.textAlign = "left";
            ctx.fillText(opts.xLabel, lx + 1, ly + 10.5);
          }
        }
      }

      // Bubble Route Labels with simple collision avoidance
      if (opts.bubbleLabels) {
        const meta = chart.getDatasetMeta(0);
        const ds = chart.data.datasets[0];
        if (meta && ds && ds.data) {
          ctx.font = "700 10px 'JetBrains Mono', monospace";
          ctx.fillStyle = cssVar("--chart-label");
          ctx.textAlign = "left";
          const placed = [];
          meta.data.forEach((pt, idx) => {
            const raw = ds.data[idx];
            if (!raw || !raw.showLabel || !raw.route_id) return;
            const lx = pt.x + (raw.r || 6) + 3;
            const ly = pt.y + 3;
            const collides = placed.some(p => Math.abs(p.x - lx) < 28 && Math.abs(p.y - ly) < 12);
            if (collides) return;
            placed.push({ x: lx, y: ly });
            ctx.fillText(raw.route_id, lx, ly);
          });
        }
      }

      // Direct Data Labels on Line (with clean contrast pill) or Horizontal Bars
      if (opts.directLabels) {
        const cfg = opts.directLabels;
        const dsIndex = cfg.datasetIndex ?? 0;
        const meta = chart.getDatasetMeta(dsIndex);
        const ds = chart.data.datasets[dsIndex];
        if (meta && ds) {
          ctx.font = "700 10.5px 'JetBrains Mono', monospace";
          meta.data.forEach((el, idx) => {
            const val = ds.data[idx];
            if (val === null || val === undefined) return;
            const text = cfg.formatter ? cfg.formatter(val, idx) : String(val);
            const fg = cfg.color ? (typeof cfg.color === "function" ? cfg.color(val, idx) : cfg.color) : cssVar("--chart-label");
            if (cfg.horizontal) {
              ctx.fillStyle = fg;
              ctx.textAlign = "left";
              ctx.fillText(text, Math.min(chartArea.right - 2, el.x + 6), el.y + 3.5);
            } else {
              const tw = ctx.measureText(text).width;
              const px = el.x;
              const py = el.y - 12;
              ctx.fillStyle = cssVar("--bg-surface");
              ctx.globalAlpha = 0.92;
              ctx.fillRect(px - tw / 2 - 4, py - 9, tw + 8, 14);
              ctx.globalAlpha = 1;
              ctx.fillStyle = fg;
              ctx.textAlign = "center";
              ctx.fillText(text, px, py + 1.5);
            }
          });
        }
      }

      ctx.restore();
    }
  };

  if (typeof Chart !== "undefined") {
    Chart.register(visualCanvasPlugin);
    Chart.defaults.animation = false;
  }

  // --------------------------------------------------------------------------
  // State
  // --------------------------------------------------------------------------
  const state = {
    page: "overview",
    theme: localStorage.getItem("metropulse_theme") || "light",
    datePreset: "live7",
    routeId: "",
    sseEnabled: true,
    sseSource: null,
    syncTimer: null,
    lastMaxServiceDate: null,

    ovChartMode: "delay",
    mapShowBuses: true,
    mapShowCorridors: true,
    mapShowIncidents: true,
    mapBusFilter: "all",
    mapInspectorTab: "buses",
    rcSecMode: "hourly",
    rcCauseFilter: "all",
    mlPredFilter: "all",
    pipeLayerFilter: "all",

    streamStatus: null,
    routeRows: [],
    hourlyRows: [],
    modelRows: [],
    predictions: [],
    buses: [],
    corridors: [],
    incidents: [],
    rootCause: null
  };

  let mapInstance = null;
  let busLayer = null;
  let corridorLayer = null;
  let incidentLayer = null;
  const busMarkersById = new Map();
  const charts = {};

  // --------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------
  function cssVar(name) {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  }

  function pctValue(v) {
    if (v === null || v === undefined) return 0;
    const n = Number(v);
    return n <= 1.0 && n > 0 ? n * 100 : n;
  }

  function fmtSignedMin(v, digits = 2) {
    const n = Number(v || 0);
    return `${n >= 0 ? "+" : ""}${n.toFixed(digits)}m`;
  }

  function fmtAge(sec) {
    if (sec === null || sec === undefined || Number.isNaN(Number(sec))) return "—";
    const s = Math.max(0, Math.round(Number(sec)));
    if (s < 60) return `${s}s ago`;
    if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
    return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  }

  function shiftDateIso(isoDateStr, deltaDays) {
    const dt = new Date(isoDateStr + "T00:00:00Z");
    dt.setUTCDate(dt.getUTCDate() + deltaDays);
    return dt.toISOString().slice(0, 10);
  }

  function escapeHtml(str) {
    return String(str ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function parseGeoJsonCoords(geomStr) {
    if (!geomStr) return null;
    try {
      const g = typeof geomStr === "string" ? JSON.parse(geomStr) : geomStr;
      if (!g || !g.coordinates) return null;
      if (g.type === "LineString") return g.coordinates.map(pt => [Number(pt[1]), Number(pt[0])]);
      if (g.type === "MultiLineString" && g.coordinates[0]) return g.coordinates[0].map(pt => [Number(pt[1]), Number(pt[0])]);
    } catch (_e) {
      return null;
    }
    return null;
  }

  async function fetchJsonOrNull(url) {
    try {
      const res = await fetch(url);
      if (!res.ok) return null;
      return await res.json();
    } catch (_e) {
      return null;
    }
  }

  function destroyChart(key, canvasEl = null) {
    if (charts[key]) {
      try { charts[key].destroy(); } catch (_e) {}
      delete charts[key];
    }
    if (canvasEl && typeof Chart !== "undefined") {
      const existing = Chart.getChart(canvasEl);
      if (existing) {
        try { existing.destroy(); } catch (_e) {}
      }
    }
  }

  function safeChart(key, canvasId, config) {
    if (typeof Chart === "undefined") return null;
    const canvas = document.getElementById(canvasId);
    if (!canvas) return null;
    destroyChart(key, canvas);
    try {
      const opts = config.options || {};
      opts.animation = false;
      config.options = opts;
      charts[key] = new Chart(canvas, config);
      return charts[key];
    } catch (err) {
      console.error(`Chart render error (${key}):`, err);
      return null;
    }
  }

  function causeBadgeHtml(cause) {
    const c = String(cause || "Healthy");
    const info = CAUSE_COLORS[c] || { cls: "neutral" };
    return `<span class="badge ${info.cls}">${escapeHtml(c)}</span>`;
  }

  function progressGaugeHtml(prog) {
    if (prog === null || prog === undefined) return `<span class="mono" style="color:var(--text-muted)">—</span>`;
    const p = Math.min(100, Math.max(0, Math.round(Number(prog) * 100)));
    const col = p >= 70 ? "var(--crimson)" : p >= 40 ? "var(--amber)" : "var(--brand)";
    return `
      <span class="prog-track" title="Avg trip progress when late: ${p}%">
        <span class="prog-rail">
          <span class="prog-fill" style="width:${p}%;background:${col}"></span>
          <span class="prog-dot" style="left:${p}%;background:${col}"></span>
        </span>
        <span class="mono" style="font-size:11px">${p}%</span>
      </span>
    `;
  }

  function compactSignalChip(r) {
    const detail = String(r.cause_detail || "");
    const cause = String(r.primary_cause || "");
    if (cause === "Healthy") {
      return `<span class="badge emerald">SLA Met</span>`;
    }
    if (cause === "Nearby Incident") {
      const m = detail.match(/Within\s+(\d+m)\s+of\s+active\s+([A-Za-z ]+?)(?:\s+on|$)/i);
      const label = m ? `${m[2].trim()} · ${m[1]}` : `${r.nearby_incidents || 1} Nearby Inc`;
      return `<span class="badge crimson mono" title="${escapeHtml(detail)}">${escapeHtml(label)}</span>`;
    }
    if (cause === "Traffic Congestion") {
      if (detail.includes("stalled")) {
        return `<span class="badge amber mono" title="${escapeHtml(detail)}">Stalled · ${r.avg_speed_kmh.toFixed(0)} km/h</span>`;
      }
      if (r.corridor_speed_ratio !== null && r.corridor_speed_ratio !== undefined && r.corridor_speed_ratio < 0.78) {
        return `<span class="badge amber mono" title="${escapeHtml(detail)}">${Math.round(r.corridor_speed_ratio * 100)}% Flow</span>`;
      }
      return `<span class="badge amber mono" title="${escapeHtml(detail)}">Slow · ${r.avg_speed_kmh.toFixed(1)} km/h</span>`;
    }
    if (cause === "Trip Cascading") {
      const p = r.avg_progress_late ? `${Math.round(r.avg_progress_late * 100)}%` : "End";
      return `<span class="badge indigo mono" title="${escapeHtml(detail)}">Late @ ${p}</span>`;
    }
    if (cause === "Schedule / Dispatch") {
      const p = r.avg_progress_late ? `${Math.round(r.avg_progress_late * 100)}%` : "Start";
      return `<span class="badge sky mono" title="${escapeHtml(detail)}">Early Lag @ ${p}</span>`;
    }
    return `<span class="badge neutral mono" title="${escapeHtml(detail)}">${fmtSignedMin(r.avg_delay, 1)}</span>`;
  }

  // --------------------------------------------------------------------------
  // Theme & Router
  // --------------------------------------------------------------------------
  function applyTheme(theme, rerender = true) {
    state.theme = theme === "dark" ? "dark" : "light";
    document.documentElement.setAttribute("data-theme", state.theme);
    localStorage.setItem("metropulse_theme", state.theme);
    document.getElementById("theme-light-btn").classList.toggle("active", state.theme === "light");
    document.getElementById("theme-dark-btn").classList.toggle("active", state.theme === "dark");
    if (rerender) renderActivePage();
  }

  function pageFromPath(pathname) {
    const p = (pathname || "/").replace(/\/+$/, "") || "/";
    if (p === "/live-map") return "live-map";
    if (p === "/root-cause") return "root-cause";
    if (p === "/ml-forecast") return "ml-forecast";
    if (p === "/pipeline") return "pipeline";
    return "overview";
  }

  function navigateTo(pageKey, { pushState = true, flyTo = null } = {}) {
    const key = PAGE_META[pageKey] ? pageKey : "overview";
    state.page = key;

    if (pushState && window.location.pathname !== PAGE_META[key].path) {
      window.history.pushState({ page: key }, "", PAGE_META[key].path);
    }

    document.querySelectorAll(".page-view").forEach(el => el.classList.toggle("active", el.id === `page-${key}`));
    document.querySelectorAll(".nav-item").forEach(el => el.classList.toggle("active", el.dataset.page === key));
    document.getElementById("current-page-title").textContent = PAGE_META[key].title;
    document.getElementById("sidebar").classList.remove("open");

    renderActivePage();

    if (key === "live-map") {
      setTimeout(() => {
        initMapIfNeeded();
        if (mapInstance) {
          mapInstance.invalidateSize();
          if (flyTo && flyTo.lat && flyTo.lon) {
            mapInstance.flyTo([Number(flyTo.lat), Number(flyTo.lon)], flyTo.zoom || 15, { duration: 0.55 });
          }
        }
      }, 45);
    }
  }

  function selectRoute(routeId, targetPage = null) {
    const sel = document.getElementById("route-filter");
    if (routeId) {
      let exists = false;
      for (const opt of sel.options) {
        if (opt.value === routeId) { exists = true; break; }
      }
      if (!exists) {
        const opt = document.createElement("option");
        opt.value = routeId;
        opt.textContent = `Route ${routeId}`;
        sel.appendChild(opt);
      }
    }
    state.routeId = routeId || "";
    sel.value = state.routeId;

    const badge = document.getElementById("active-route-badge");
    const clearBtn = document.getElementById("clear-route-btn");
    badge.style.display = state.routeId ? "inline-flex" : "none";
    badge.textContent = state.routeId ? `Route ${state.routeId}` : "";
    clearBtn.style.display = state.routeId ? "inline-flex" : "none";

    if (targetPage) navigateTo(targetPage);
    loadDashboard(Boolean(state.routeId && state.page === "live-map"));
  }

  window.MetroPulse = {
    selectRoute,
    navigateTo,
    focusBusOnMap(vehicleId, lat, lon) {
      navigateTo("live-map", { flyTo: { lat, lon, zoom: 15 } });
      setTimeout(() => {
        const marker = busMarkersById.get(String(vehicleId));
        if (marker) marker.openPopup();
      }, 280);
    },
    focusPointOnMap(lat, lon, zoom = 15) {
      navigateTo("live-map", { flyTo: { lat, lon, zoom } });
    }
  };

  // --------------------------------------------------------------------------
  // Page 1: Executive Pulse
  // --------------------------------------------------------------------------
  function renderOverviewPage() {
    const startDate = document.getElementById("start-date").value;
    const endDate = document.getElementById("end-date").value;
    const rc = state.rootCause || {};
    const exec = rc.executive_summary || {};
    const counts = (state.streamStatus && state.streamStatus.counts) || {};
    const rows = state.routeRows || [];

    let avgOnTime = exec.on_time_pct || 0;
    let avgDelay = 0;
    let avgMed = 0;
    let avgP90 = 0;
    let totalTrips = 0;
    let distinctRoutes = 0;

    if (rows.length > 0) {
      avgOnTime = rows.reduce((s, r) => s + pctValue(r.on_time_pct), 0) / rows.length;
      avgDelay = rows.reduce((s, r) => s + Number(r.avg_deviation || 0), 0) / rows.length;
      avgMed = rows.reduce((s, r) => s + Number(r.median_deviation ?? r.avg_deviation ?? 0), 0) / rows.length;
      avgP90 = rows.reduce((s, r) => s + Number(r.p90_deviation ?? r.avg_deviation ?? 0), 0) / rows.length;
      totalTrips = rows.reduce((s, r) => s + Number(r.trips || 0), 0);
      distinctRoutes = new Set(rows.map(r => r.route_id)).size;
    }

    const liveSpds = state.buses.map(b => b.speed_kmh).filter(s => s !== null && s !== undefined && Number(s) > 0);
    const avgFleetSpd = liveSpds.length
      ? liveSpds.reduce((a, b) => a + Number(b), 0) / liveSpds.length
      : (rows.length ? rows.reduce((s, r) => s + Number(r.avg_speed_kmh || 18.5), 0) / rows.length : 0);

    // Compact Visual Signal Strip (Zero Prose!)
    const slaGap = avgOnTime - 80.0;
    const strip = document.getElementById("exec-signal-strip");
    strip.className = `signal-strip ${avgOnTime >= 80 ? "good" : avgOnTime >= 68 ? "warn" : "crit"}`;

    const sigSla = document.getElementById("sig-sla-pill");
    sigSla.className = `badge ${avgOnTime >= 80 ? "emerald" : avgOnTime >= 68 ? "amber" : "crimson"} mono`;
    sigSla.textContent = avgOnTime >= 80 ? `SLA OK · ${avgOnTime.toFixed(1)}%` : `SLA ${slaGap.toFixed(1)} pts`;

    document.getElementById("sig-cause-val").textContent = `${exec.dominant_cause || "—"} (${exec.dominant_cause_pct || 0}%)`;
    document.getElementById("sig-snowball-val").textContent = `${exec.start_stage_late_pct ?? 0}% → ${exec.end_stage_late_pct ?? 0}% late`;
    const worstRt = exec.worst_route;
    document.getElementById("sig-hotspot-val").textContent = worstRt ? `Route ${worstRt.route_id} (${fmtSignedMin(worstRt.avg_delay, 1)})` : "—";

    // Hero KPIs
    document.getElementById("ov-ontime").textContent = `${avgOnTime.toFixed(1)}%`;
    document.getElementById("ov-sla-gap").textContent = `${slaGap >= 0 ? "+" : ""}${slaGap.toFixed(1)} vs 80%`;
    document.getElementById("ov-routes-count").textContent = `${distinctRoutes} Routes`;
    document.getElementById("ov-trips-count").textContent = `${totalTrips.toLocaleString()} trips`;

    const slaBadge = document.getElementById("ov-sla-badge");
    slaBadge.className = `badge ${avgOnTime >= 80 ? "emerald" : avgOnTime >= 68 ? "amber" : "crimson"}`;
    slaBadge.textContent = avgOnTime >= 80 ? "SLA Met" : "SLA 80%";

    const ring = document.getElementById("ov-ontime-ring");
    const circ = 119.4;
    ring.style.strokeDashoffset = String(circ - (Math.min(100, Math.max(0, avgOnTime)) / 100) * circ);
    ring.style.stroke = avgOnTime >= 80 ? "var(--emerald)" : avgOnTime >= 68 ? "var(--amber)" : "var(--crimson)";

    document.getElementById("ov-avg-delay").textContent = fmtSignedMin(avgDelay, 2);
    document.getElementById("ov-p90-badge").textContent = `P90 ${avgP90.toFixed(1)}m`;
    document.getElementById("ov-median-sub").textContent = `Med ${fmtSignedMin(avgMed, 1)}`;
    document.getElementById("ov-speed-badge").textContent = `${avgFleetSpd.toFixed(1)} km/h`;
    document.getElementById("ov-delay-bar").style.width = `${Math.min(100, Math.max(8, Math.abs(avgDelay) * 12))}%`;
    document.getElementById("ov-delay-bar").style.background =
      avgDelay > 5 ? "var(--crimson)" : avgDelay > 2.5 ? "var(--amber)" : "var(--emerald)";
    document.getElementById("ov-window-label").textContent =
      startDate === endDate ? startDate : `${startDate.slice(5)} → ${endDate.slice(5)}`;

    if (state.predictions.length > 0) {
      const avgPred = state.predictions.reduce((s, p) => s + Number(p.predicted_delay_min || 0), 0) / state.predictions.length;
      const lateCnt = state.predictions.filter(p => Number(p.predicted_late) === 1 || Number(p.predicted_delay_min) > 5).length;
      const latePct = (lateCnt / state.predictions.length) * 100;
      document.getElementById("ov-ml-pred").textContent = fmtSignedMin(avgPred, 2);
      document.getElementById("ov-ml-late-badge").textContent = `Late ${latePct.toFixed(0)}%`;
      document.getElementById("ov-ml-ver-badge").textContent = state.predictions[0].model_version || "v1";
      document.getElementById("ov-ml-bar").style.width = `${Math.min(100, Math.max(10, 100 - latePct))}%`;
      document.getElementById("ov-ml-scored").textContent = `${(counts.gold_predictions || state.predictions.length).toLocaleString()}`;
    }
    if (state.modelRows.length > 0) {
      const mae = state.modelRows.reduce((s, r) => s + Number(r.mae || 0), 0) / state.modelRows.length;
      document.getElementById("ov-ml-err-sub").textContent = `MAE ${mae.toFixed(3)}m`;
    }

    if (state.corridors.length > 0) {
      const avgRatio = state.corridors.reduce((s, c) => s + Number(c.speed_ratio ?? 0.8), 0) / state.corridors.length;
      const avgSpd = state.corridors.reduce((s, c) => s + Number(c.current_speed_kmh ?? 20), 0) / state.corridors.length;
      const avgDel = state.corridors.reduce((s, c) => s + Number(c.delay_sec ?? 0), 0) / state.corridors.length;
      document.getElementById("ov-flow-pct").textContent = `${Math.round(avgRatio * 100)}%`;
      document.getElementById("ov-flow-speed").textContent = `${avgSpd.toFixed(1)} km/h`;
      document.getElementById("ov-flow-delay").textContent = `+${Math.round(avgDel)}s`;
      document.getElementById("ov-flow-bar").style.width = `${Math.min(100, Math.round(avgRatio * 100))}%`;
      document.getElementById("ov-flow-bar").style.background =
        avgRatio >= 0.78 ? "var(--emerald)" : avgRatio >= 0.6 ? "var(--amber)" : "var(--crimson)";
    }
    document.getElementById("ov-inc-badge").textContent = `${state.incidents.length} Inc`;
    document.getElementById("ov-buses-pill").textContent = `${(counts.active_vehicles || state.buses.length || 0).toLocaleString()} buses`;

    // Visual 1: Donut Ring + Proportional Schedule Bars (Zero redundancy!)
    const dist = rc.delay_distribution || [];
    const totalObs = dist.reduce((s, d) => s + Number(d.count || 0), 0);
    const onTimeObsPct = dist
      .filter(d => ["early", "on_time", "slight"].includes(d.key))
      .reduce((s, d) => s + Number(d.pct || 0), 0);
    document.getElementById("ov-dist-total").textContent = `${totalObs.toLocaleString()} obs`;
    document.getElementById("ov-donut-center").textContent = `${onTimeObsPct.toFixed(1)}%`;

    const barsWrap = document.getElementById("ov-dist-bars");
    barsWrap.innerHTML = "";
    dist.forEach(item => {
      const m = DIST_META[item.key] || { short: item.label, range: "", color: "#64748b" };
      const el = document.createElement("div");
      el.className = "vbar-item";
      el.innerHTML = `
        <div class="vbar-head">
          <span class="vbar-name">
            <span class="dot-sm" style="background:${m.color}"></span>
            ${m.short} <span class="vbar-range mono">${m.range}</span>
          </span>
          <span class="vbar-nums">${Number(item.pct || 0).toFixed(1)}% <span style="color:var(--text-muted);font-weight:500">(${Number(item.count || 0).toLocaleString()})</span></span>
        </div>
        <div class="vbar-track"><span style="width:${Math.max(item.count ? 2 : 0, item.pct)}%;background:${m.color}"></span></div>
      `;
      barsWrap.appendChild(el);
    });

    safeChart("ovDonut", "ovDonutChart", {
      type: "doughnut",
      data: {
        labels: dist.map(d => (DIST_META[d.key] ? DIST_META[d.key].short : d.label)),
        datasets: [
          {
            data: dist.map(d => d.count),
            backgroundColor: dist.map(d => (DIST_META[d.key] ? DIST_META[d.key].color : "#64748b")),
            borderColor: cssVar("--bg-surface"),
            borderWidth: 3,
            hoverOffset: 5
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: "71%",
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: ctx => {
                const it = dist[ctx.dataIndex];
                return ` ${it.label}: ${it.pct}% (${Number(it.count).toLocaleString()})`;
              }
            }
          }
        }
      }
    });

    // Visual 2: Top 10 Delayed Routes with vertical 5m SLA Line + Direct Labels
    const diagRoutes = (rc.route_diagnostics || []).slice(0, 10);
    const gridColor = cssVar("--chart-grid");
    const tickColor = cssVar("--chart-tick");
    if (state.ovChartMode === "delay") {
      document.getElementById("ov-bottleneck-title").textContent = "Top 10 Delayed Routes";
      const maxX = Math.max(12, ...diagRoutes.map(r => r.p90_delay || 0)) * 1.12;
      safeChart("ovBottleneck", "ovBottleneckChart", {
        type: "bar",
        data: {
          labels: diagRoutes.map(r => r.route_id),
          datasets: [
            {
              label: "Avg (min)",
              data: diagRoutes.map(r => r.avg_delay),
              backgroundColor: cssVar("--brand") || "#0066b3",
              borderRadius: 4,
              barPercentage: 0.8
            },
            {
              label: "P90 (min)",
              data: diagRoutes.map(r => r.p90_delay),
              backgroundColor: "rgba(217, 119, 6, 0.82)",
              borderRadius: 4,
              barPercentage: 0.8
            }
          ]
        },
        options: {
          indexAxis: "y",
          responsive: true,
          maintainAspectRatio: false,
          onClick: (_e, els) => {
            if (els && els.length > 0) {
              const rt = diagRoutes[els[0].index]?.route_id;
              if (rt) setTimeout(() => selectRoute(rt, "root-cause"), 0);
            }
          },
          plugins: {
            legend: { labels: { color: tickColor, boxWidth: 10, usePointStyle: true } },
            visualCanvas: {
              xLine: 5,
              xLabel: "5m SLA",
              xLabelBottom: true,
              directLabels: {
                datasetIndex: 1,
                horizontal: true,
                formatter: v => `${Number(v).toFixed(0)}m`,
                color: () => tickColor
              }
            },
            tooltip: {
              callbacks: {
                afterBody: items => {
                  const r = diagRoutes[items[0].dataIndex];
                  return r ? [`Cause: ${r.primary_cause}`] : [];
                }
              }
            }
          },
          scales: {
            x: { max: Math.ceil(maxX), ticks: { color: tickColor, callback: v => `${v}m` }, grid: { color: gridColor } },
            y: { ticks: { color: tickColor, font: { family: "'JetBrains Mono', monospace", weight: "700" } }, grid: { display: false } }
          }
        }
      });
    } else {
      document.getElementById("ov-bottleneck-title").textContent = "Top 10 Routes · On-Time %";
      safeChart("ovBottleneck", "ovBottleneckChart", {
        type: "bar",
        data: {
          labels: diagRoutes.map(r => r.route_id),
          datasets: [
            { label: "On-Time", data: diagRoutes.map(r => r.on_time_pct), backgroundColor: "#059669", borderRadius: 4 },
            { label: "Late >5m", data: diagRoutes.map(r => Math.max(0, Number((100 - r.on_time_pct).toFixed(1)))), backgroundColor: "#dc2626", borderRadius: 4 }
          ]
        },
        options: {
          indexAxis: "y",
          responsive: true,
          maintainAspectRatio: false,
          onClick: (_e, els) => {
            if (els && els.length > 0) {
              const rt = diagRoutes[els[0].index]?.route_id;
              if (rt) setTimeout(() => selectRoute(rt, "root-cause"), 0);
            }
          },
          plugins: {
            legend: { labels: { color: tickColor, boxWidth: 10, usePointStyle: true } },
            visualCanvas: { xLine: 80, xLabel: "80% SLA", xColor: cssVar("--emerald") }
          },
          scales: {
            x: { stacked: true, min: 0, max: 100, ticks: { color: tickColor, callback: v => `${v}%` }, grid: { color: gridColor } },
            y: { stacked: true, ticks: { color: tickColor, font: { family: "'JetBrains Mono', monospace", weight: "700" } }, grid: { display: false } }
          }
        }
      });
    }

    // 3 Compact Visual Spotlight Tiles
    if (worstRt) {
      document.getElementById("spot-route-title").textContent = `Route ${worstRt.route_id}`;
      document.getElementById("spot-route-meta").textContent = `${worstRt.primary_cause} · P90 ${worstRt.p90_delay}m`;
      document.getElementById("spot-route-badge").textContent = fmtSignedMin(worstRt.avg_delay, 1);
      document.getElementById("spot-route-card").onclick = () => selectRoute(worstRt.route_id, "root-cause");
    }
    const wc = exec.worst_corridor;
    if (wc) {
      document.getElementById("spot-corridor-title").textContent = wc.corridor_name || "Corridor";
      document.getElementById("spot-corridor-meta").textContent = `${wc.current_speed_kmh} km/h · +${wc.delay_sec}s`;
      document.getElementById("spot-corridor-badge").textContent = `${Math.round(wc.speed_ratio_pct)}%`;
      document.getElementById("spot-corridor-card").onclick = () => window.MetroPulse.focusPointOnMap(wc.query_lat, wc.query_lon, 15);
    }
    const ti = exec.top_incident;
    if (ti) {
      document.getElementById("spot-incident-title").textContent = `${ti.category} · ${ti.road}`;
      document.getElementById("spot-incident-meta").textContent = `${ti.length_m}m affected`;
      document.getElementById("spot-incident-badge").textContent = `+${ti.delay_min}m`;
      document.getElementById("spot-incident-card").onclick = () => window.MetroPulse.focusPointOnMap(ti.lat, ti.lon, 15);
    }
  }

  // --------------------------------------------------------------------------
  // Page 2: Live Fleet Map
  // --------------------------------------------------------------------------
  function initMapIfNeeded() {
    if (mapInstance || typeof L === "undefined") return;
    const el = document.getElementById("dc-map");
    if (!el) return;
    mapInstance = L.map("dc-map", { preferCanvas: true, zoomControl: true }).setView([38.9072, -77.0269], 12);
    L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: "&copy; OpenStreetMap &middot; TomTom &middot; WMATA",
      maxZoom: 19
    }).addTo(mapInstance);
    corridorLayer = L.layerGroup().addTo(mapInstance);
    incidentLayer = L.layerGroup().addTo(mapInstance);
    busLayer = L.layerGroup().addTo(mapInstance);
  }

  function renderMapLayers(zoomToBuses = false) {
    initMapIfNeeded();
    if (!mapInstance) return;

    busLayer.clearLayers();
    corridorLayer.clearLayers();
    incidentLayer.clearLayers();
    busMarkersById.clear();

    const filteredBuses = state.buses.filter(b => {
      const dev = Number(b.deviation_min || 0);
      if (state.mapBusFilter === "late") return dev > 5;
      if (state.mapBusFilter === "moderate") return dev > 2 && dev <= 5;
      if (state.mapBusFilter === "ontime") return dev <= 2;
      return true;
    });

    document.getElementById("map-bus-count").textContent = filteredBuses.length;
    document.getElementById("map-corridor-count").textContent = state.corridors.length;
    document.getElementById("map-incident-count").textContent = state.incidents.length;

    if (state.mapShowCorridors) {
      state.corridors.forEach(c => {
        const ratio = c.speed_ratio !== null && c.speed_ratio !== undefined ? Number(c.speed_ratio) : 0.78;
        const curSpd = Number(c.current_speed_kmh || 20);
        const freeSpd = Number(c.free_flow_speed_kmh || 30);
        const delaySec = Number(c.delay_sec || 0);
        const color = ratio >= 0.78 ? "#059669" : ratio >= 0.55 ? "#d97706" : "#dc2626";
        const popup = `
          <div style="font-family:'Plus Jakarta Sans',sans-serif;min-width:205px">
            <div style="display:flex;justify-content:space-between;align-items:center;gap:8px">
              <strong style="font-size:13px">${escapeHtml(c.corridor_name || c.corridor_id)}</strong>
              <span class="mono" style="font-weight:800;color:${color}">${Math.round(ratio * 100)}%</span>
            </div>
            <div style="height:5px;border-radius:99px;background:var(--bg-elevated);margin:7px 0;overflow:hidden">
              <div style="height:100%;width:${Math.round(ratio * 100)}%;background:${color}"></div>
            </div>
            <div class="mono" style="display:flex;justify-content:space-between;font-size:11.5px">
              <span>${curSpd.toFixed(0)}/${freeSpd.toFixed(0)} km/h</span>
              <span style="color:${color}">+${delaySec}s</span>
            </div>
          </div>
        `;
        const latLngs = parseGeoJsonCoords(c.geometry);
        if (latLngs && latLngs.length >= 2) {
          L.polyline(latLngs, { color, weight: 5, opacity: 0.82 }).bindPopup(popup).addTo(corridorLayer);
        }
        if (c.query_lat && c.query_lon) {
          L.circleMarker([Number(c.query_lat), Number(c.query_lon)], {
            radius: 7, color, weight: 2, fillColor: color, fillOpacity: 0.28
          }).bindPopup(popup).addTo(corridorLayer);
        }
      });
    }

    if (state.mapShowIncidents) {
      state.incidents.forEach(inc => {
        const cat = INCIDENT_CATEGORIES[Number(inc.icon_category)] || "Incident";
        const delayMin = ((Number(inc.delay_sec) || 0) / 60).toFixed(1);
        const severe = Number(inc.magnitude_of_delay) >= 3 || Number(inc.icon_category) === 8;
        const color = severe ? "#dc2626" : "#d97706";
        const popup = `
          <div style="font-family:'Plus Jakarta Sans',sans-serif;min-width:200px">
            <div style="font-size:10px;font-weight:800;color:${color};text-transform:uppercase">${escapeHtml(cat)}</div>
            <div style="font-size:13px;font-weight:800;margin:2px 0">${escapeHtml(inc.road_numbers || inc.from_location || "DC Road")}</div>
            <div class="mono" style="font-size:11.5px;margin-top:6px;display:flex;justify-content:space-between">
              <span style="color:${color};font-weight:700">+${delayMin}m delay</span>
              <span>${Math.round(Number(inc.length_m || 0))}m</span>
            </div>
          </div>
        `;
        const latLngs = parseGeoJsonCoords(inc.geometry);
        if (latLngs && latLngs.length >= 2) {
          L.polyline(latLngs, { color, weight: 3.5, dashArray: "5, 6", opacity: 0.88 }).bindPopup(popup).addTo(incidentLayer);
        } else if (inc.lat && inc.lon) {
          L.circleMarker([Number(inc.lat), Number(inc.lon)], {
            radius: 5.5, color: "#ffffff", weight: 1.5, fillColor: color, fillOpacity: 0.92
          }).bindPopup(popup).addTo(incidentLayer);
        }
      });
    }

    const coords = [];
    if (state.mapShowBuses) {
      filteredBuses.forEach(b => {
        if (!b.lat || !b.lon) return;
        const lat = Number(b.lat);
        const lon = Number(b.lon);
        coords.push([lat, lon]);
        const dev = Number(b.deviation_min || 0);
        const pred = Number(b.predicted_delay_min ?? dev);
        const isLate = dev > 5;
        const isMod = !isLate && dev > 2;
        const color = isLate ? "#dc2626" : isMod ? "#d97706" : "#059669";
        const spdTxt = b.speed_kmh !== null && b.speed_kmh !== undefined ? `${Number(b.speed_kmh).toFixed(0)} km/h` : "—";
        const progPct = b.progress_pct !== null && b.progress_pct !== undefined ? Math.min(100, Math.max(0, Math.round(Number(b.progress_pct) * 100))) : 50;

        const marker = L.circleMarker([lat, lon], {
          radius: isLate ? 6.5 : 5,
          color: state.theme === "dark" ? "#0b1120" : "#ffffff",
          weight: 1.6,
          fillColor: color,
          fillOpacity: 0.95
        }).bindPopup(`
          <div style="font-family:'Plus Jakarta Sans',sans-serif;min-width:235px">
            <div style="display:flex;justify-content:space-between;align-items:center;gap:8px">
              <strong style="font-size:14px">Route ${escapeHtml(b.route_id || "—")} <span style="opacity:0.65;font-size:11px">#${escapeHtml(b.vehicle_id || "")}</span></strong>
              <span class="mono" style="font-weight:800;color:${color}">${fmtSignedMin(dev, 1)}</span>
            </div>
            <div style="margin:6px 0 8px;display:flex;align-items:center;gap:6px">
              <span style="padding:2px 7px;border-radius:99px;font-size:10.5px;font-weight:700;background:${color}1f;color:${color}">${escapeHtml(b.diagnosed_cause || "Status")}</span>
              <span class="mono" style="font-size:11px;opacity:0.8">${spdTxt}</span>
            </div>
            <div style="margin-bottom:8px">
              <div style="display:flex;justify-content:space-between;font-size:10px;opacity:0.7;margin-bottom:3px">
                <span>Trip Progress</span><strong class="mono">${progPct}%</strong>
              </div>
              <div style="height:5px;border-radius:99px;background:var(--bg-elevated);overflow:hidden">
                <div style="height:100%;width:${progPct}%;background:${color}"></div>
              </div>
            </div>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;padding-top:6px;border-top:1px solid var(--border);font-size:11.5px">
              <div>ML +15m: <strong class="mono">${fmtSignedMin(pred, 1)}</strong></div>
              <div>Flow: <strong class="mono">${b.corridor_speed_ratio ? Math.round(b.corridor_speed_ratio * 100) + "%" : "—"}</strong></div>
            </div>
          </div>
        `).addTo(busLayer);

        if (b.vehicle_id) busMarkersById.set(String(b.vehicle_id), marker);
      });
    }

    if (zoomToBuses && coords.length > 0) {
      mapInstance.fitBounds(L.latLngBounds(coords).pad(0.2), { maxZoom: 14 });
    }
  }

  function renderMapInspector() {
    const q = (document.getElementById("map-inspector-search").value || "").toLowerCase().trim();

    const lateBuses = state.buses
      .filter(b => Number(b.deviation_min || 0) > 5)
      .sort((a, b) => Number(b.deviation_min || 0) - Number(a.deviation_min || 0));

    document.getElementById("insp-late-count").textContent = lateBuses.length;
    document.getElementById("insp-inc-count").textContent = state.incidents.length;

    const busTbody = document.getElementById("insp-buses-tbody");
    busTbody.innerHTML = "";
    const shownBuses = lateBuses.filter(b =>
      !q || String(b.route_id || "").toLowerCase().includes(q) || String(b.vehicle_id || "").toLowerCase().includes(q) || String(b.diagnosed_cause || "").toLowerCase().includes(q)
    );
    if (shownBuses.length === 0) {
      busTbody.innerHTML = `<tr><td colspan="4" class="empty-cell">No late buses</td></tr>`;
    } else {
      shownBuses.slice(0, 60).forEach(b => {
        const spd = b.speed_kmh !== null && b.speed_kmh !== undefined ? `${Number(b.speed_kmh).toFixed(0)} km/h` : "—";
        const tr = document.createElement("tr");
        tr.className = "clickable";
        tr.onclick = () => window.MetroPulse.focusBusOnMap(b.vehicle_id, b.lat, b.lon);
        tr.innerHTML = `
          <td><span class="route-chip" onclick="event.stopPropagation();window.MetroPulse.selectRoute('${escapeHtml(b.route_id)}')">${escapeHtml(b.route_id)}</span></td>
          <td><span class="badge crimson mono">${fmtSignedMin(b.deviation_min, 1)}</span></td>
          <td class="mono">${spd}</td>
          <td title="${escapeHtml(b.cause_detail || "")}">${causeBadgeHtml(b.diagnosed_cause)}</td>
        `;
        busTbody.appendChild(tr);
      });
    }

    const flowTbody = document.getElementById("insp-flow-tbody");
    flowTbody.innerHTML = "";
    [...state.corridors]
      .sort((a, b) => Number(a.speed_ratio ?? 1) - Number(b.speed_ratio ?? 1))
      .filter(c => !q || String(c.corridor_name || "").toLowerCase().includes(q))
      .forEach(c => {
        const ratio = Number(c.speed_ratio ?? 0.8);
        const cls = ratio >= 0.78 ? "emerald" : ratio >= 0.55 ? "amber" : "crimson";
        const tr = document.createElement("tr");
        tr.className = "clickable";
        tr.onclick = () => window.MetroPulse.focusPointOnMap(c.query_lat, c.query_lon, 15);
        tr.innerHTML = `
          <td style="max-width:155px;overflow:hidden;text-overflow:ellipsis" title="${escapeHtml(c.corridor_name)}"><strong>${escapeHtml(c.corridor_name || c.corridor_id)}</strong></td>
          <td class="mono">${Number(c.current_speed_kmh || 0).toFixed(0)}/${Number(c.free_flow_speed_kmh || 0).toFixed(0)}</td>
          <td><span class="badge ${cls} mono">${Math.round(ratio * 100)}%</span></td>
          <td class="mono">+${Math.round(Number(c.delay_sec || 0))}s</td>
        `;
        flowTbody.appendChild(tr);
      });

    const incTbody = document.getElementById("insp-inc-tbody");
    incTbody.innerHTML = "";
    const filteredInc = state.incidents.filter(inc =>
      !q || String(inc.road_numbers || "").toLowerCase().includes(q) || String(inc.from_location || "").toLowerCase().includes(q)
    );
    if (filteredInc.length === 0) {
      incTbody.innerHTML = `<tr><td colspan="4" class="empty-cell">No incidents</td></tr>`;
    } else {
      filteredInc.slice(0, 80).forEach(inc => {
        const cat = INCIDENT_CATEGORIES[Number(inc.icon_category)] || "Incident";
        const severe = Number(inc.magnitude_of_delay) >= 3 || Number(inc.icon_category) === 8;
        const tr = document.createElement("tr");
        tr.className = "clickable";
        tr.onclick = () => window.MetroPulse.focusPointOnMap(inc.lat, inc.lon, 15);
        tr.innerHTML = `
          <td><span class="badge ${severe ? "crimson" : "amber"}">${escapeHtml(cat)}</span></td>
          <td style="max-width:125px;overflow:hidden;text-overflow:ellipsis"><strong>${escapeHtml(inc.road_numbers || inc.from_location || "DC")}</strong></td>
          <td class="mono">+${((Number(inc.delay_sec) || 0) / 60).toFixed(1)}m</td>
          <td class="mono">${Math.round(Number(inc.length_m || 0))}m</td>
        `;
        incTbody.appendChild(tr);
      });
    }
  }

  // --------------------------------------------------------------------------
  // Page 3: Root Cause (Visual Donut + Snowball Curve + Tinted Quadrant)
  // --------------------------------------------------------------------------
  function renderRootCausePage() {
    const rc = state.rootCause;
    if (!rc) return;

    const exec = rc.executive_summary || {};
    const drivers = rc.driver_attribution || [];
    const progress = rc.progress_curve || [];
    const tiers = rc.congestion_tiers || [];
    const routes = rc.route_diagnostics || [];
    const gridColor = cssVar("--chart-grid");
    const tickColor = cssVar("--chart-tick");

    // 4 Root-Cause KPIs
    document.getElementById("rc-dom-cause").textContent = exec.dominant_cause || "—";
    document.getElementById("rc-dom-share").textContent = `${exec.dominant_cause_pct || 0}%`;
    document.getElementById("rc-dom-sub").textContent = `${exec.total_late_live_buses || 0} late buses live`;

    const sLate = Number(exec.start_stage_late_pct || 0);
    const eLate = Number(exec.end_stage_late_pct || 0);
    document.getElementById("rc-snowball-val").textContent = `${sLate.toFixed(1)}% → ${eLate.toFixed(1)}%`;
    document.getElementById("rc-snowball-mult").textContent = sLate > 0 ? `${(eLate / sLate).toFixed(1)}×` : "—";

    document.getElementById("rc-inc-active").textContent = `${exec.active_incidents || 0} active`;
    document.getElementById("rc-inc-exposed").textContent = `${exec.late_buses_near_incidents || 0} / ${exec.total_late_live_buses || 0}`;

    const freeTier = tiers.find(t => t.tier.startsWith("Free")) || { avg_delay: 0 };
    const congTier = tiers.find(t => t.tier.startsWith("Congested")) || tiers.find(t => t.tier.startsWith("Moderate")) || { avg_delay: 0, buses: 0 };
    const pen = Number(congTier.avg_delay || 0) - Number(freeTier.avg_delay || 0);
    if (congTier.buses > 0 && pen > 0.1) {
      document.getElementById("rc-cong-penalty").textContent = `+${pen.toFixed(2)}m`;
      document.getElementById("rc-cong-badge").textContent = "Congestion";
      document.getElementById("rc-cong-sub").textContent = `${fmtSignedMin(congTier.avg_delay, 1)} vs ${fmtSignedMin(freeTier.avg_delay, 1)}`;
    } else {
      document.getElementById("rc-cong-penalty").textContent = `${exec.avg_flow_pct || 0}% flow`;
      document.getElementById("rc-cong-badge").textContent = "Flow Clear";
      document.getElementById("rc-cong-sub").textContent = `${exec.active_incidents || 0} road incidents`;
    }

    // Visual 1: Root-Cause Donut + Clean Proportional Bars (No long prose!)
    document.getElementById("rc-late-total-badge").textContent = `${exec.total_late_live_buses || 0} late`;
    document.getElementById("rc-donut-center").textContent = exec.total_late_live_buses || 0;

    const dWrap = document.getElementById("rc-driver-bars");
    dWrap.innerHTML = "";
    drivers.forEach(d => {
      const col = (CAUSE_COLORS[d.cause] && CAUSE_COLORS[d.cause].hex) || "#0066b3";
      const el = document.createElement("div");
      el.className = "vbar-item";
      el.title = d.description || "";
      el.innerHTML = `
        <div class="vbar-head">
          <span class="vbar-name"><span class="dot-sm" style="background:${col}"></span>${escapeHtml(d.cause)}</span>
          <span class="vbar-nums">${d.share_pct}% <span style="color:var(--text-muted);font-weight:500">(${d.count} · ${fmtSignedMin(d.avg_delay, 1)})</span></span>
        </div>
        <div class="vbar-track"><span style="width:${Math.max(d.count ? 3 : 0, d.share_pct)}%;background:${col}"></span></div>
      `;
      dWrap.appendChild(el);
    });

    safeChart("rcDonut", "rcCauseDonut", {
      type: "doughnut",
      data: {
        labels: drivers.map(d => d.cause),
        datasets: [
          {
            data: drivers.map(d => d.count),
            backgroundColor: drivers.map(d => (CAUSE_COLORS[d.cause] && CAUSE_COLORS[d.cause].hex) || "#64748b"),
            borderColor: cssVar("--bg-surface"),
            borderWidth: 3,
            hoverOffset: 5
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: "71%",
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: ctx => {
                const d = drivers[ctx.dataIndex];
                return ` ${d.cause}: ${d.share_pct}% (${d.count} buses, avg ${fmtSignedMin(d.avg_delay, 1)})`;
              }
            }
          }
        }
      }
    });

    // Visual 2: Delay Snowball Dual-Axis Curve with Shaded Fill & Direct % Labels
    const maxPct = Math.max(40, ...progress.map(p => p.late_pct || 0)) * 1.18;
    safeChart("rcProgress", "rcProgressChart", {
      data: {
        labels: progress.map(p => p.stage),
        datasets: [
          {
            type: "bar",
            label: "Avg Delay (min)",
            data: progress.map(p => p.avg_delay),
            backgroundColor: "rgba(0, 102, 179, 0.78)",
            borderRadius: 6,
            maxBarThickness: 36,
            yAxisID: "yDelay"
          },
          {
            type: "line",
            label: "% Late (>5m)",
            data: progress.map(p => p.late_pct),
            borderColor: "#dc2626",
            backgroundColor: "rgba(220, 38, 38, 0.12)",
            fill: true,
            borderWidth: 2.6,
            pointRadius: 4.5,
            pointBackgroundColor: "#dc2626",
            tension: 0.32,
            yAxisID: "yPct"
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { labels: { color: tickColor, boxWidth: 10, usePointStyle: true } },
          visualCanvas: {
            directLabels: {
              datasetIndex: 1,
              formatter: v => `${Number(v).toFixed(0)}%`,
              color: () => cssVar("--crimson")
            }
          }
        },
        scales: {
          x: { ticks: { color: tickColor }, grid: { display: false } },
          yDelay: {
            position: "left",
            ticks: { color: tickColor, callback: v => `${v}m` },
            grid: { color: gridColor }
          },
          yPct: {
            position: "right",
            min: 0,
            max: Math.ceil(maxPct / 10) * 10,
            ticks: { color: "#dc2626", callback: v => `${v}%` },
            grid: { drawOnChartArea: false }
          }
        }
      }
    });

    // Visual 3: Tinted Quadrant Speed vs Delay Scatter with In-Chart Route Labels!
    const scatterPts = routes.filter(r => r.trips > 0).map((r, idx) => {
      const isLate = r.avg_delay > 5;
      const isSlow = r.avg_speed_kmh < 15;
      const color = isLate && isSlow
        ? "rgba(220, 38, 38, 0.82)"
        : isLate
          ? "rgba(217, 119, 6, 0.85)"
          : "rgba(5, 150, 105, 0.68)";
      return {
        x: r.avg_speed_kmh,
        y: r.avg_delay,
        r: Math.min(14, Math.max(5, Math.sqrt(r.trips) * 2.0)),
        route_id: r.route_id,
        trips: r.trips,
        p90: r.p90_delay,
        on_time: r.on_time_pct,
        cause: r.primary_cause,
        showLabel: isLate || idx < 6,
        bg: color
      };
    });

    safeChart("rcQuadrant", "rcQuadrantChart", {
      type: "bubble",
      data: {
        datasets: [
          {
            data: scatterPts,
            backgroundColor: scatterPts.map(p => p.bg),
            borderColor: scatterPts.map(p => p.bg.replace(/0\.\d+\)/, "1)")),
            borderWidth: 1.2
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        onClick: (_e, els) => {
          if (els && els.length > 0) {
            const pt = scatterPts[els[0].index];
            if (pt && pt.route_id) setTimeout(() => selectRoute(pt.route_id), 0);
          }
        },
        plugins: {
          legend: { display: false },
          visualCanvas: {
            quadrantZones: true,
            ySplit: 5,
            xSplit: 15,
            yLine: 5,
            xLine: 15,
            xColor: "rgba(100,116,139,0.45)",
            bubbleLabels: true
          },
          tooltip: {
            callbacks: {
              label: ctx => {
                const p = ctx.raw;
                return [
                  `Route ${p.route_id} (${p.trips} trips) · ${p.cause}`,
                  `Delay ${fmtSignedMin(p.y, 1)} (P90 ${p.p90}m) · Speed ${p.x} km/h`
                ];
              }
            }
          }
        },
        scales: {
          x: {
            suggestedMin: 0,
            suggestedMax: 30,
            ticks: { color: tickColor, callback: v => `${v} km/h` },
            grid: { color: gridColor }
          },
          y: {
            suggestedMin: 0,
            suggestedMax: Math.max(10, Math.ceil(Math.max(8, ...scatterPts.map(p => p.y || 0))) + 2),
            ticks: { color: tickColor, callback: v => `${v}m` },
            grid: { color: gridColor }
          }
        }
      }
    });

    // Visual 4: Dual-Axis Hourly Profile / Flow Tier
    destroyChart("rcHourly");
    if (state.rcSecMode === "hourly") {
      document.getElementById("rc-hourly-title").textContent = "Hourly Delay vs. On-Time %";
      const byHour = {};
      state.hourlyRows.forEach(r => {
        const h = Number(r.hour_local ?? 12);
        if (!byHour[h]) byHour[h] = { dev: 0, ot: 0, count: 0 };
        byHour[h].dev += Number(r.avg_deviation || 0);
        byHour[h].ot += pctValue(r.on_time_pct);
        byHour[h].count += 1;
      });
      const hours = Object.keys(byHour).map(Number).sort((a, b) => a - b);

      safeChart("rcHourly", "rcHourlyChart", {
        data: {
          labels: hours.map(h => `${String(h).padStart(2, "0")}:00`),
          datasets: [
            {
              type: "bar",
              label: "Avg Delay (m)",
              data: hours.map(h => Number((byHour[h].dev / byHour[h].count).toFixed(2))),
              backgroundColor: "rgba(217, 119, 6, 0.82)",
              borderRadius: 6,
              maxBarThickness: 34,
              yAxisID: "yDelay"
            },
            {
              type: "line",
              label: "On-Time (%)",
              data: hours.map(h => Number((byHour[h].ot / byHour[h].count).toFixed(1))),
              borderColor: "#059669",
              borderWidth: 2.5,
              pointRadius: 4.5,
              pointBackgroundColor: "#059669",
              tension: 0.3,
              yAxisID: "yOt"
            }
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { labels: { color: tickColor, boxWidth: 10, usePointStyle: true } } },
          scales: {
            x: { ticks: { color: tickColor }, grid: { display: false } },
            yDelay: { position: "left", ticks: { color: "#d97706", callback: v => `${v}m` }, grid: { color: gridColor } },
            yOt: { position: "right", min: 0, max: 100, ticks: { color: "#059669", callback: v => `${v}%` }, grid: { drawOnChartArea: false } }
          }
        }
      });
    } else {
      document.getElementById("rc-hourly-title").textContent = "Delay by TomTom Flow Tier";
      safeChart("rcHourly", "rcHourlyChart", {
        data: {
          labels: tiers.map(t => `${t.tier.split(" ")[0]} (${t.buses})`),
          datasets: [
            {
              type: "bar",
              label: "Avg Delay (m)",
              data: tiers.map(t => t.avg_delay),
              backgroundColor: ["#059669", "#d97706", "#dc2626"],
              borderRadius: 6,
              maxBarThickness: 46,
              yAxisID: "yDelay"
            },
            {
              type: "line",
              label: "% Late (>5m)",
              data: tiers.map(t => t.late_pct),
              borderColor: "#4f46e5",
              borderWidth: 2.5,
              pointRadius: 5,
              pointBackgroundColor: "#4f46e5",
              yAxisID: "yPct"
            }
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { labels: { color: tickColor, boxWidth: 10, usePointStyle: true } } },
          scales: {
            x: { ticks: { color: tickColor }, grid: { display: false } },
            yDelay: { position: "left", ticks: { color: tickColor, callback: v => `${v}m` }, grid: { color: gridColor } },
            yPct: { position: "right", min: 0, max: 100, ticks: { color: "#4f46e5", callback: v => `${v}%` }, grid: { drawOnChartArea: false } }
          }
        }
      });
    }

    renderRootCauseTable();
  }

  function renderRootCauseTable() {
    const routes = (state.rootCause && state.rootCause.route_diagnostics) || [];
    const q = (document.getElementById("rc-table-search").value || "").toLowerCase().trim();
    const filtered = routes.filter(r => {
      if (state.rcCauseFilter === "issues" && r.primary_cause === "Healthy") return false;
      if (state.rcCauseFilter === "incident" && Number(r.nearby_incidents || 0) === 0) return false;
      if (!q) return true;
      return (
        String(r.route_id).toLowerCase().includes(q) ||
        String(r.primary_cause).toLowerCase().includes(q) ||
        String(r.cause_detail).toLowerCase().includes(q)
      );
    });

    const tbody = document.getElementById("rc-routes-tbody");
    tbody.innerHTML = "";
    if (filtered.length === 0) {
      tbody.innerHTML = `<tr><td colspan="9" class="empty-cell">No routes</td></tr>`;
      return;
    }

    filtered.forEach(r => {
      const delCls = r.avg_delay > 5 ? "crimson" : r.avg_delay > 2.5 ? "amber" : "emerald";
      const barCol = r.on_time_pct >= 80 ? "#059669" : r.on_time_pct >= 68 ? "#d97706" : "#dc2626";
      const spdCol = r.avg_speed_kmh < 12 ? "var(--crimson)" : r.avg_speed_kmh < 18 ? "var(--amber)" : "var(--emerald)";
      const tr = document.createElement("tr");
      tr.className = "clickable";
      tr.onclick = () => selectRoute(r.route_id);
      tr.innerHTML = `
        <td><span class="route-chip">${escapeHtml(r.route_id)}</span></td>
        <td class="mono">${r.trips}</td>
        <td><span class="badge ${delCls} mono">${fmtSignedMin(r.avg_delay, 1)}</span></td>
        <td class="mono">${r.p90_delay.toFixed(1)}m</td>
        <td>
          <div class="mini-bar-wrap">
            <div class="mini-bar"><span style="width:${Math.min(100, Math.round(r.on_time_pct))}%;background:${barCol}"></span></div>
            <span class="mono">${r.on_time_pct.toFixed(0)}%</span>
          </div>
        </td>
        <td class="mono"><span class="dot-sm" style="background:${spdCol};margin-right:5px"></span>${r.avg_speed_kmh.toFixed(1)}</td>
        <td>${progressGaugeHtml(r.avg_progress_late)}</td>
        <td>${causeBadgeHtml(r.primary_cause)}</td>
        <td>${compactSignalChip(r)}</td>
      `;
      tbody.appendChild(tr);
    });
  }

  // --------------------------------------------------------------------------
  // Page 4: ML Forecast (45° Diagonal Fit Scatter + Direct-Label Drivers)
  // --------------------------------------------------------------------------
  function renderMLPage() {
    const preds = state.predictions || [];
    const counts = (state.streamStatus && state.streamStatus.counts) || {};
    const fresh = (state.streamStatus && state.streamStatus.freshness) || {};
    const gridColor = cssVar("--chart-grid");
    const tickColor = cssVar("--chart-tick");

    if (preds.length > 0) {
      const avgPred = preds.reduce((s, p) => s + Number(p.predicted_delay_min || 0), 0) / preds.length;
      const lateCnt = preds.filter(p => Number(p.predicted_late) === 1 || Number(p.predicted_delay_min) > 5).length;
      const latePct = (lateCnt / preds.length) * 100;
      const mv = preds[0].model_version || "v1";

      document.getElementById("ml-model-badge").textContent = `lgbm ${mv}`;
      document.getElementById("ml-kpi-avg").textContent = fmtSignedMin(avgPred, 2);
      document.getElementById("ml-kpi-late-pct").textContent = `${latePct.toFixed(1)}%`;
      document.getElementById("ml-kpi-late-cnt").textContent = `${lateCnt}/${preds.length}`;
      document.getElementById("ml-kpi-ver").textContent = mv;
      document.getElementById("ml-kpi-total").textContent = (counts.gold_predictions || preds.length).toLocaleString();
      document.getElementById("ml-kpi-last-scored").textContent = `${String(fresh.max_predicted_at || "—").slice(11, 19)} UTC`;
    }

    if (state.modelRows.length > 0) {
      const mae = state.modelRows.reduce((s, r) => s + Number(r.mae || 0), 0) / state.modelRows.length;
      const rmse = state.modelRows.reduce((s, r) => s + Number(r.rmse || 0), 0) / state.modelRows.length;
      const acc = state.modelRows.reduce((s, r) => s + pctValue(r.accuracy_late_5 ?? 0.95), 0) / state.modelRows.length;
      document.getElementById("ml-kpi-mae").textContent = `${mae.toFixed(3)}m`;
      document.getElementById("ml-kpi-rmse").textContent = `RMSE ${rmse.toFixed(2)}m`;
      document.getElementById("ml-kpi-acc").textContent = `Acc ${acc.toFixed(1)}%`;

      const evalBadge = document.getElementById("ml-eval-mode-badge");
      const obsCount = Number(counts.gold_observed_ground_truth || 0);
      if (obsCount >= 20) {
        evalBadge.className = "badge emerald mono";
        evalBadge.textContent = `Observed +15m (${obsCount.toLocaleString()})`;
      } else if (acc >= 98.5 && mae < 0.75) {
        evalBadge.className = "badge amber";
        evalBadge.textContent = "Bootstrap Target";
      } else {
        evalBadge.className = "badge emerald";
        evalBadge.textContent = "Observed Actuals";
      }
    }

    // Visual 1: Predicted vs Target 45° Diagonal Fit Scatter (clamped cleanly to [-10, 25])
    const pts = preds
      .filter(p => p.actual_delay_min !== null && p.actual_delay_min !== undefined)
      .slice(0, 120)
      .map(p => {
        const act = Math.max(-10, Math.min(25, Number(p.actual_delay_min)));
        const prd = Math.max(-10, Math.min(25, Number(p.predicted_delay_min || 0)));
        const late = prd > 5 || act > 5;
        return {
          x: Number(act.toFixed(2)),
          y: Number(prd.toFixed(2)),
          route_id: p.route_id,
          err: Number(p.abs_error || Math.abs(prd - act)).toFixed(2),
          color: late ? "rgba(220, 38, 38, 0.8)" : "rgba(79, 70, 229, 0.68)"
        };
      });

    const allVals = pts.flatMap(p => [p.x, p.y]);
    const minAxis = Math.min(-4, Math.floor(Math.min(...(allVals.length ? allVals : [0])) / 2) * 2);
    const maxAxis = Math.max(10, Math.ceil(Math.max(...(allVals.length ? allVals : [10])) / 5) * 5);

    safeChart("mlCal", "mlCalibrationChart", {
      type: "scatter",
      data: {
        datasets: [
          {
            label: "Scored Trips",
            data: pts,
            backgroundColor: pts.map(p => p.color),
            pointRadius: 4.5,
            pointHoverRadius: 6.5
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          visualCanvas: { fitDiagonal: true },
          tooltip: {
            callbacks: {
              label: ctx => {
                const p = ctx.raw;
                return `Route ${p.route_id}: Target ${fmtSignedMin(p.x, 1)} → Pred ${fmtSignedMin(p.y, 1)} (Err ${p.err}m)`;
              }
            }
          }
        },
        scales: {
          x: {
            min: minAxis,
            max: maxAxis,
            ticks: { color: tickColor, callback: v => `${v}m` },
            grid: { color: gridColor },
            title: { display: true, text: "Target / Actual (min)", color: tickColor, font: { size: 11, weight: "600" } }
          },
          y: {
            min: minAxis,
            max: maxAxis,
            ticks: { color: tickColor, callback: v => `${v}m` },
            grid: { color: gridColor },
            title: { display: true, text: "Predicted +15m (min)", color: tickColor, font: { size: 11, weight: "600" } }
          }
        }
      }
    });

    // Visual 2: Feature Drivers with Direct Numeric Labels
    const fDrivers = (state.rootCause && state.rootCause.feature_drivers) || [];
    safeChart("mlDrivers", "mlFeatureDriversChart", {
      type: "bar",
      data: {
        labels: fDrivers.map(f => f.label),
        datasets: [
          {
            data: fDrivers.map(f => f.impact_pct),
            backgroundColor: fDrivers.map(f => (f.direction === "increases_delay" ? "#dc2626" : "#059669")),
            borderRadius: 5,
            maxBarThickness: 22
          }
        ]
      },
      options: {
        indexAxis: "y",
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          visualCanvas: {
            directLabels: {
              datasetIndex: 0,
              horizontal: true,
              formatter: (_v, idx) => {
                const f = fDrivers[idx];
                return f ? `${f.correlation >= 0 ? "+" : ""}${f.correlation.toFixed(2)}` : "";
              },
              color: (_v, idx) => (fDrivers[idx]?.direction === "increases_delay" ? cssVar("--crimson") : cssVar("--emerald"))
            }
          }
        },
        scales: {
          x: { min: 0, max: 118, ticks: { color: tickColor, callback: v => (v <= 100 ? `${v}%` : "") }, grid: { color: gridColor } },
          y: { ticks: { color: tickColor, font: { weight: "600" } }, grid: { display: false } }
        }
      }
    });

    renderPredictionsTable();
  }

  function renderPredictionsTable() {
    const q = (document.getElementById("ml-pred-search").value || "").toLowerCase().trim();
    const filtered = state.predictions.filter(p => {
      const pred = Number(p.predicted_delay_min || 0);
      const nowDev = p.deviation_now !== null && p.deviation_now !== undefined ? Number(p.deviation_now) : null;
      const isLate = Number(p.predicted_late) === 1 || pred > 5;
      const isWorsening = nowDev !== null && pred - nowDev >= 0.4;
      if (state.mlPredFilter === "late" && !isLate) return false;
      if (state.mlPredFilter === "worsening" && !isWorsening) return false;
      if (!q) return true;
      return String(p.route_id || "").toLowerCase().includes(q) || String(p.trip_key || "").toLowerCase().includes(q);
    });

    document.getElementById("ml-pred-count").textContent = filtered.length;
    const tbody = document.getElementById("ml-predictions-tbody");
    tbody.innerHTML = "";
    if (filtered.length === 0) {
      tbody.innerHTML = `<tr><td colspan="9" class="empty-cell">No predictions</td></tr>`;
      return;
    }

    filtered.slice(0, 60).forEach(p => {
      const pred = Number(p.predicted_delay_min || 0);
      const nowDev = p.deviation_now !== null && p.deviation_now !== undefined ? Number(p.deviation_now) : null;
      const delta = nowDev !== null ? pred - nowDev : null;
      const isLate = Number(p.predicted_late) === 1 || pred > 5;
      const fullKey = String(p.trip_key || "");
      const shortKey = fullKey.includes("|") ? `#${fullKey.split("|")[0]}` : fullKey.slice(0, 12);
      const fullSnap = String(p.snapshot_ts || "").replace("T", " ").slice(0, 19);
      const shortSnap = fullSnap.length >= 19 ? fullSnap.slice(11, 19) : fullSnap;

      let trendBadge = `<span class="badge neutral">—</span>`;
      if (delta !== null) {
        if (delta >= 0.4) trendBadge = `<span class="badge crimson mono">+${delta.toFixed(1)}m &uarr;</span>`;
        else if (delta <= -0.4) trendBadge = `<span class="badge emerald mono">${delta.toFixed(1)}m &darr;</span>`;
        else trendBadge = `<span class="badge neutral mono">Steady</span>`;
      }

      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td><span class="route-chip" onclick="window.MetroPulse.selectRoute('${escapeHtml(p.route_id || "")}')">${escapeHtml(p.route_id || "—")}</span></td>
        <td class="mono" style="color:var(--text-secondary)" title="${escapeHtml(fullKey)}">${escapeHtml(shortKey)}</td>
        <td class="mono">${escapeHtml(shortSnap)}</td>
        <td class="mono">${nowDev !== null ? fmtSignedMin(nowDev, 1) : "—"}</td>
        <td class="mono"><strong>${fmtSignedMin(pred, 2)}</strong></td>
        <td>${trendBadge}</td>
        <td class="mono">${p.actual_delay_min !== null && p.actual_delay_min !== undefined ? fmtSignedMin(p.actual_delay_min, 2) : "—"}</td>
        <td class="mono" style="color:var(--text-secondary)">${p.abs_error !== null && p.abs_error !== undefined ? Number(p.abs_error).toFixed(2) + "m" : "—"}</td>
        <td><span class="badge ${isLate ? "crimson" : "emerald"}">${isLate ? "LATE" : "ON-TIME"}</span></td>
      `;
      tbody.appendChild(tr);
    });
  }

  // --------------------------------------------------------------------------
  // Page 5: Pipeline Health (Visual Arrows + Freshness Bars)
  // --------------------------------------------------------------------------
  function setFreshnessBar(barId, ageSec, maxGoodSec = 180) {
    const el = document.getElementById(barId);
    if (!el || ageSec === null || ageSec === undefined) return;
    const s = Math.max(0, Number(ageSec));
    const pct = Math.max(12, Math.min(100, Math.round(100 - (s / (maxGoodSec * 3)) * 100)));
    el.style.width = `${pct}%`;
    el.style.background = s <= maxGoodSec ? "var(--emerald)" : s <= maxGoodSec * 2.5 ? "var(--amber)" : "var(--crimson)";
  }

  function formatRunSummary(rc) {
    if (!rc || typeof rc !== "object") return "—";
    if (rc.scored !== undefined) return `${Number(rc.scored).toLocaleString()} scored · ${Number(rc.predicted_late || 0)} late`;
    if (rc.bus_positions !== undefined) return `${Number(rc.bus_positions).toLocaleString()} buses · ${rc.distinct_routes || 0} rts`;
    if (rc.tomtom_flow_records !== undefined) return `${rc.tomtom_flow_records} flow · ${rc.tomtom_incident_records || 0} inc`;
    const merged = Object.entries(rc)
      .filter(([, val]) => val && typeof val === "object" && Number(val.merged || 0) > 0)
      .map(([tbl, val]) => `${tbl.split(".").pop().replace("bus_trip_", "")} +${Number(val.merged).toLocaleString()}`);
    if (merged.length > 0) return merged.slice(0, 2).join(" · ");
    return "ok";
  }

  function renderPipelinePage() {
    const st = state.streamStatus || {};
    const counts = st.counts || {};
    const fresh = st.freshness || {};
    const tel = st.telemetry || {};
    const runs = st.pipeline_runs || [];
    const gridColor = cssVar("--chart-grid");
    const tickColor = cssVar("--chart-tick");

    document.getElementById("pipe-silver-buses").textContent = (counts.silver_bus_snapshots || 0).toLocaleString();
    document.getElementById("pipe-silver-sub").textContent = `${counts.active_vehicles || 0} veh · ${state.corridors.length}c / ${state.incidents.length}i`;
    document.getElementById("pipe-gold-features").textContent = (counts.gold_features || 0).toLocaleString();
    document.getElementById("pipe-gold-sub").textContent = `${counts.gold_routes || 0} active routes`;
    document.getElementById("pipe-gold-preds").textContent = (counts.gold_predictions || 0).toLocaleString();

    document.getElementById("pipe-snap-age").textContent = fmtAge(tel.snapshot_age_sec);
    document.getElementById("pipe-snap-ts").textContent = String(fresh.max_snapshot_ts || "—").slice(0, 19);
    setFreshnessBar("pipe-snap-bar", tel.snapshot_age_sec, 180);

    document.getElementById("pipe-pred-age").textContent = fmtAge(tel.prediction_age_sec);
    document.getElementById("pipe-pred-ts").textContent = String(fresh.max_predicted_at || "—").slice(0, 19);
    setFreshnessBar("pipe-pred-bar", tel.prediction_age_sec, 120);

    document.getElementById("pipe-tomtom-age").textContent = fmtAge(tel.tomtom_age_sec);
    document.getElementById("pipe-tomtom-ts").textContent = String(tel.tomtom_max_ts || "—").slice(0, 19);
    setFreshnessBar("pipe-tomtom-bar", tel.tomtom_age_sec, 480);

    const natsBadge = document.getElementById("pipe-nats-badge");
    natsBadge.className = `badge ${st.nats_connected ? "emerald" : "amber"}`;
    natsBadge.textContent = st.nats_connected ? "Online" : "Retry";
    const jsTel = tel.jetstream || {};
    const lagVal = jsTel.total_pending !== undefined ? jsTel.total_pending : 0;
    const dlqVal = jsTel.dlq_messages !== undefined ? jsTel.dlq_messages : 0;
    document.getElementById("pipe-sse-clients").textContent = `Lag ${lagVal} · DLQ ${dlqVal}`;

    const finished = runs.filter(r => ["success", "failed"].includes(String(r.status || "").toLowerCase()));
    const ok = finished.filter(r => String(r.status || "").toLowerCase() === "success");
    const sr = finished.length ? Math.round((ok.length / finished.length) * 100) : 100;
    document.getElementById("pipe-success-rate").textContent = `${tel.sse_subscribers || 1} SSE · Jobs ${sr}%`;
    document.getElementById("pipe-sr-bar").style.width = `${sr}%`;

    const filteredRuns = runs.filter(r => state.pipeLayerFilter === "all" || String(r.layer || "").toLowerCase() === state.pipeLayerFilter);
    const rTbody = document.getElementById("pipe-runs-tbody");
    rTbody.innerHTML = "";
    if (filteredRuns.length === 0) {
      rTbody.innerHTML = `<tr><td colspan="6" class="empty-cell">No runs</td></tr>`;
    } else {
      filteredRuns.forEach(r => {
        const stStr = String(r.status || "").toLowerCase();
        const stCls = stStr === "success" ? "emerald" : stStr === "running" ? "brand" : "crimson";
        const lyStr = String(r.layer || "—").toLowerCase();
        const lyCls = lyStr === "gold" ? "indigo" : lyStr === "silver" ? "sky" : "amber";
        const fullStart = String(r.started_at || "").replace("T", " ").slice(0, 19);
        const shortStart = fullStart.length >= 19 ? fullStart.slice(11, 19) : fullStart;
        const dur = r.duration_sec !== null && r.duration_sec !== undefined ? `${Number(r.duration_sec).toFixed(1)}s` : "run";
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td><strong>${escapeHtml(r.job_name || "—")}</strong></td>
          <td><span class="badge ${lyCls}">${escapeHtml(lyStr.toUpperCase())}</span></td>
          <td><span class="badge ${stCls}">${escapeHtml(stStr.toUpperCase())}</span></td>
          <td class="mono" style="color:var(--text-secondary)">${escapeHtml(shortStart)}</td>
          <td class="mono">${escapeHtml(dur)}</td>
          <td class="mono summary-cell" style="font-size:11px;color:var(--text-secondary)">${escapeHtml(formatRunSummary(r.row_counts))}</td>
        `;
        rTbody.appendChild(tr);
      });
    }

    const incBreak = (state.rootCause && state.rootCause.incident_breakdown) || [];
    document.getElementById("pipe-inc-total").textContent = `${state.incidents.length}`;
    const maxInc = Math.max(10, ...incBreak.map(i => i.count || 0)) * 1.18;
    safeChart("pipeInc", "pipeIncChart", {
      type: "bar",
      data: {
        labels: incBreak.slice(0, 6).map(i => i.category),
        datasets: [
          { data: incBreak.slice(0, 6).map(i => i.count), backgroundColor: "#dc2626", borderRadius: 5, maxBarThickness: 22 }
        ]
      },
      options: {
        indexAxis: "y",
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          visualCanvas: {
            directLabels: { datasetIndex: 0, horizontal: true, formatter: v => String(v), color: () => cssVar("--chart-label") }
          }
        },
        scales: {
          x: { max: Math.ceil(maxInc), ticks: { color: tickColor }, grid: { color: gridColor } },
          y: { ticks: { color: tickColor, font: { weight: "600" } }, grid: { display: false } }
        }
      }
    });

    const incTbody = document.getElementById("pipe-inc-tbody");
    incTbody.innerHTML = "";
    incBreak.forEach(i => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td><strong>${escapeHtml(i.category)}</strong></td>
        <td class="mono">${i.count}</td>
        <td class="mono">+${i.avg_delay_min}m</td>
        <td class="mono">${i.total_length_km} km</td>
      `;
      incTbody.appendChild(tr);
    });
  }

  // --------------------------------------------------------------------------
  // Render Coordinator & Data Orchestration
  // --------------------------------------------------------------------------
  function updateSidebarBadges() {
    const lateCount = state.buses.filter(b => Number(b.deviation_min || 0) > 5).length;
    document.getElementById("nav-badge-buses").textContent = state.buses.length;
    document.getElementById("nav-badge-late").textContent = lateCount;
    const tel = (state.streamStatus && state.streamStatus.telemetry) || {};
    document.getElementById("sidebar-snap-age").textContent = fmtAge(tel.snapshot_age_sec);
    if (state.predictions.length > 0) {
      document.getElementById("sidebar-model-ver").textContent = `lgbm ${state.predictions[0].model_version || "v1"}`;
    }
  }

  function renderActivePage(zoomToBuses = false) {
    updateSidebarBadges();
    if (state.page === "overview") renderOverviewPage();
    else if (state.page === "live-map") { renderMapLayers(zoomToBuses); renderMapInspector(); }
    else if (state.page === "root-cause") renderRootCausePage();
    else if (state.page === "ml-forecast") renderMLPage();
    else if (state.page === "pipeline") renderPipelinePage();
  }

  async function populateRoutesDropdown() {
    const sel = document.getElementById("route-filter");
    const resp = await fetchJsonOrNull("/v1/routes/list");
    const routes = (resp && resp.routes) || [];
    const cur = sel.value;
    sel.innerHTML = `<option value="">All Routes (${routes.length})</option>`;
    routes.forEach(r => {
      const opt = document.createElement("option");
      opt.value = r;
      opt.textContent = `Route ${r}`;
      sel.appendChild(opt);
    });
    sel.value = cur;
  }

  function syncDateInputsFromPreset(maxServiceDate) {
    if (!maxServiceDate) return;
    if (state.datePreset === "today") {
      document.getElementById("start-date").value = maxServiceDate;
      document.getElementById("end-date").value = maxServiceDate;
    } else if (state.datePreset === "live7") {
      document.getElementById("end-date").value = maxServiceDate;
      document.getElementById("start-date").value = shiftDateIso(maxServiceDate, -7);
    } else if (state.datePreset === "archive") {
      document.getElementById("start-date").value = "2020-02-01";
      document.getElementById("end-date").value = "2020-02-29";
    }
  }

  function updatePresetButtons() {
    document.getElementById("preset-today-btn").classList.toggle("active", state.datePreset === "today");
    document.getElementById("preset-live7-btn").classList.toggle("active", state.datePreset === "live7");
    document.getElementById("preset-archive-btn").classList.toggle("active", state.datePreset === "archive");
  }

  async function loadDashboard(zoomToBuses = false) {
    const streamResp = await fetchJsonOrNull("/v1/stream/status");
    state.streamStatus = streamResp;
    const fresh = (streamResp && streamResp.freshness) || {};
    const maxDate = fresh.max_service_date || "2026-11-22";

    if (state.datePreset === "today" || state.datePreset === "live7") {
      if (state.lastMaxServiceDate !== maxDate || !document.getElementById("end-date").value) {
        syncDateInputsFromPreset(maxDate);
        state.lastMaxServiceDate = maxDate;
      }
    }

    const startDate = document.getElementById("start-date").value || shiftDateIso(maxDate, -7);
    const endDate = document.getElementById("end-date").value || maxDate;
    const routeParam = state.routeId ? `&route_id=${encodeURIComponent(state.routeId)}` : "";

    const [
      delayResp,
      hourlyResp,
      predResp,
      perfResp,
      corridorsResp,
      incidentsResp,
      busesResp,
      rootCauseResp
    ] = await Promise.all([
      fetchJsonOrNull(`/v1/routes/delay?start_date=${startDate}&end_date=${endDate}${routeParam}`),
      fetchJsonOrNull(`/v1/routes/hourly?start_date=${startDate}&end_date=${endDate}${routeParam}`),
      fetchJsonOrNull(`/v1/predictions/latest?limit=150${routeParam}`),
      fetchJsonOrNull(`/v1/model/performance?start_date=${startDate}&end_date=${endDate}`),
      fetchJsonOrNull("/v1/traffic/corridors"),
      fetchJsonOrNull("/v1/traffic/incidents?limit=200"),
      fetchJsonOrNull(`/v1/buses/live?limit=600${routeParam}`),
      fetchJsonOrNull(`/v1/analytics/root-cause?start_date=${startDate}&end_date=${endDate}${routeParam}`)
    ]);

    state.routeRows = (delayResp && delayResp.data) || [];
    state.hourlyRows = (hourlyResp && hourlyResp.data) || [];
    state.predictions = (predResp && predResp.data) || [];
    state.modelRows = (perfResp && perfResp.data) || [];
    state.corridors = (corridorsResp && corridorsResp.data) || [];
    state.incidents = (incidentsResp && incidentsResp.data) || [];
    state.buses = (busesResp && busesResp.data) || [];
    state.rootCause = rootCauseResp;

    renderActivePage(zoomToBuses);
  }

  // --------------------------------------------------------------------------
  // Persistent Real-Time NATS SSE Connection
  // --------------------------------------------------------------------------
  function flashTicker(msg) {
    const pill = document.getElementById("sse-toggle-btn");
    const txt = document.getElementById("live-ticker-text");
    if (txt && msg) txt.textContent = msg;
    if (pill) {
      pill.classList.add("flash");
      setTimeout(() => pill.classList.remove("flash"), 1300);
    }
  }

  function schedulePushSync(msg) {
    flashTicker(msg);
    if (state.syncTimer) clearTimeout(state.syncTimer);
    state.syncTimer = setTimeout(() => loadDashboard(false), 350);
  }

  function connectSSE() {
    if (state.sseSource) {
      state.sseSource.close();
      state.sseSource = null;
    }
    const dot = document.getElementById("sse-dot");
    const lbl = document.getElementById("sse-status-label");
    const tDot = document.getElementById("ticker-dot");

    if (!state.sseEnabled || typeof EventSource === "undefined") {
      dot.className = "pulse-dot paused";
      lbl.textContent = "Paused";
      tDot.style.background = "var(--amber)";
      document.getElementById("live-ticker-text").textContent = "Push: OFF";
      return;
    }

    state.sseSource = new EventSource("/v1/stream/events");
    state.sseSource.addEventListener("connected", ev => {
      try {
        const info = JSON.parse(ev.data || "{}");
        dot.className = "pulse-dot";
        tDot.style.background = "var(--emerald)";
        lbl.textContent = info.nats_connected ? "NATS SSE Live" : "SSE Ready";
        document.getElementById("live-ticker-text").textContent = "Push: ON";
      } catch (_e) {}
    });
    state.sseSource.addEventListener("inference", ev => {
      try {
        const d = JSON.parse(ev.data || "{}");
        schedulePushSync(`ML · ${Number(d.scored_count || 0).toLocaleString()} scored`);
      } catch (_e) {
        schedulePushSync("ML updated");
      }
    });
    state.sseSource.addEventListener("silver_merged", ev => {
      try {
        const d = JSON.parse(ev.data || "{}");
        schedulePushSync(`Silver · ${d.source === "tomtom" ? "TomTom" : "WMATA"}`);
      } catch (_e) {
        schedulePushSync("Silver merged");
      }
    });
    state.sseSource.addEventListener("bronze_wmata", () => flashTicker("Bronze · WMATA"));
    state.sseSource.addEventListener("bronze_traffic", () => flashTicker("Bronze · TomTom"));
    state.sseSource.onerror = () => {
      dot.className = "pulse-dot paused";
      lbl.textContent = "Reconnecting...";
      tDot.style.background = "var(--amber)";
    };
  }

  // --------------------------------------------------------------------------
  // Event Bindings & Bootstrap
  // --------------------------------------------------------------------------
  function bindEvents() {
    document.querySelectorAll(".nav-item").forEach(btn => {
      btn.addEventListener("click", () => navigateTo(btn.dataset.page));
    });
    window.addEventListener("popstate", () => navigateTo(pageFromPath(window.location.pathname), { pushState: false }));
    document.getElementById("mobile-menu-btn").addEventListener("click", () => document.getElementById("sidebar").classList.toggle("open"));

    document.getElementById("theme-light-btn").addEventListener("click", () => applyTheme("light"));
    document.getElementById("theme-dark-btn").addEventListener("click", () => applyTheme("dark"));

    document.getElementById("route-filter").addEventListener("change", e => selectRoute(e.target.value));
    document.getElementById("clear-route-btn").addEventListener("click", () => selectRoute(""));

    document.getElementById("preset-today-btn").addEventListener("click", () => {
      state.datePreset = "today"; updatePresetButtons(); syncDateInputsFromPreset(state.lastMaxServiceDate || "2026-11-22"); loadDashboard(false);
    });
    document.getElementById("preset-live7-btn").addEventListener("click", () => {
      state.datePreset = "live7"; updatePresetButtons(); syncDateInputsFromPreset(state.lastMaxServiceDate || "2026-11-22"); loadDashboard(false);
    });
    document.getElementById("preset-archive-btn").addEventListener("click", () => {
      state.datePreset = "archive"; updatePresetButtons(); syncDateInputsFromPreset("2020-02-29"); loadDashboard(false);
    });
    document.getElementById("apply-date-btn").addEventListener("click", () => {
      state.datePreset = "custom"; updatePresetButtons(); loadDashboard(false);
    });
    document.getElementById("sse-toggle-btn").addEventListener("click", () => { state.sseEnabled = !state.sseEnabled; connectSSE(); });
    document.getElementById("refresh-now-btn").addEventListener("click", () => loadDashboard(false));

    document.getElementById("btn-go-root-cause").addEventListener("click", () => navigateTo("root-cause"));
    document.getElementById("btn-go-map").addEventListener("click", () => navigateTo("live-map"));
    document.getElementById("ov-chart-delay-btn").addEventListener("click", () => {
      state.ovChartMode = "delay";
      document.getElementById("ov-chart-delay-btn").classList.add("active");
      document.getElementById("ov-chart-ontime-btn").classList.remove("active");
      renderOverviewPage();
    });
    document.getElementById("ov-chart-ontime-btn").addEventListener("click", () => {
      state.ovChartMode = "ontime";
      document.getElementById("ov-chart-ontime-btn").classList.add("active");
      document.getElementById("ov-chart-delay-btn").classList.remove("active");
      renderOverviewPage();
    });

    document.getElementById("toggle-buses-btn").addEventListener("click", e => { state.mapShowBuses = !state.mapShowBuses; e.currentTarget.classList.toggle("active", state.mapShowBuses); renderMapLayers(false); });
    document.getElementById("toggle-corridors-btn").addEventListener("click", e => { state.mapShowCorridors = !state.mapShowCorridors; e.currentTarget.classList.toggle("active", state.mapShowCorridors); renderMapLayers(false); });
    document.getElementById("toggle-incidents-btn").addEventListener("click", e => { state.mapShowIncidents = !state.mapShowIncidents; e.currentTarget.classList.toggle("active", state.mapShowIncidents); renderMapLayers(false); });
    document.querySelectorAll("[data-bus-filter]").forEach(btn => {
      btn.addEventListener("click", () => {
        state.mapBusFilter = btn.dataset.busFilter;
        document.querySelectorAll("[data-bus-filter]").forEach(b => b.classList.toggle("active", b === btn));
        renderMapLayers(false);
      });
    });
    document.getElementById("reset-map-btn").addEventListener("click", () => { if (mapInstance) mapInstance.flyTo([38.9072, -77.0269], 12, { duration: 0.5 }); });

    function setInspTab(tab) {
      state.mapInspectorTab = tab;
      document.getElementById("insp-tab-buses").classList.toggle("active", tab === "buses");
      document.getElementById("insp-tab-flow").classList.toggle("active", tab === "flow");
      document.getElementById("insp-tab-inc").classList.toggle("active", tab === "inc");
      document.getElementById("insp-pane-buses").style.display = tab === "buses" ? "block" : "none";
      document.getElementById("insp-pane-flow").style.display = tab === "flow" ? "block" : "none";
      document.getElementById("insp-pane-inc").style.display = tab === "inc" ? "block" : "none";
    }
    document.getElementById("insp-tab-buses").addEventListener("click", () => setInspTab("buses"));
    document.getElementById("insp-tab-flow").addEventListener("click", () => setInspTab("flow"));
    document.getElementById("insp-tab-inc").addEventListener("click", () => setInspTab("inc"));
    document.getElementById("map-inspector-search").addEventListener("input", renderMapInspector);

    document.getElementById("rc-sec-hourly").addEventListener("click", () => {
      state.rcSecMode = "hourly";
      document.getElementById("rc-sec-hourly").classList.add("active");
      document.getElementById("rc-sec-congestion").classList.remove("active");
      renderRootCausePage();
    });
    document.getElementById("rc-sec-congestion").addEventListener("click", () => {
      state.rcSecMode = "congestion";
      document.getElementById("rc-sec-congestion").classList.add("active");
      document.getElementById("rc-sec-hourly").classList.remove("active");
      renderRootCausePage();
    });
    document.querySelectorAll("[data-cause-filter]").forEach(btn => {
      btn.addEventListener("click", () => {
        state.rcCauseFilter = btn.dataset.causeFilter;
        document.querySelectorAll("[data-cause-filter]").forEach(b => b.classList.toggle("active", b === btn));
        renderRootCauseTable();
      });
    });
    document.getElementById("rc-table-search").addEventListener("input", renderRootCauseTable);

    document.querySelectorAll("[data-pred-filter]").forEach(btn => {
      btn.addEventListener("click", () => {
        state.mlPredFilter = btn.dataset.predFilter;
        document.querySelectorAll("[data-pred-filter]").forEach(b => b.classList.toggle("active", b === btn));
        renderPredictionsTable();
      });
    });
    document.getElementById("ml-pred-search").addEventListener("input", renderPredictionsTable);

    document.querySelectorAll("[data-layer-filter]").forEach(btn => {
      btn.addEventListener("click", () => {
        state.pipeLayerFilter = btn.dataset.layerFilter;
        document.querySelectorAll("[data-layer-filter]").forEach(b => b.classList.toggle("active", b === btn));
        renderPipelinePage();
      });
    });
  }

  document.addEventListener("DOMContentLoaded", async () => {
    applyTheme(state.theme, false);
    bindEvents();
    navigateTo(pageFromPath(window.location.pathname), { pushState: false });
    await populateRoutesDropdown();
    await loadDashboard(false);
    connectSSE();
  });
})();
