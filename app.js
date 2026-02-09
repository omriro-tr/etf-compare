// ── ETF Compare — Frontend (fetches live data from /api/etfs) ────────

let etfData = [];
const RISK_FREE_RATE = 4.0; // annualized %, matches server-side assumption

// ── Utility Helpers ─────────────────────────────────────
function formatCurrency(n) {
  if (n == null) return "N/A";
  return "$" + n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function formatMarketCap(b) {
  if (b == null) return "N/A";
  return "$" + b.toFixed(1) + "B";
}

function formatPercent(n) {
  if (n == null) return "N/A";
  return n.toFixed(2) + "%";
}

function formatLargePercent(n) {
  if (n == null) return "N/A";
  return n.toLocaleString("en-US", { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + "%";
}

function cumulative_return_js(annPct, years) {
  if (annPct == null || years == null || years <= 0) return null;
  return (Math.pow(1 + annPct / 100, years) - 1) * 100;
}

function formatGrowth(cumulPct) {
  // Given cumulative return %, show what $1,000 grows to
  if (cumulPct == null) return "";
  const total = 1000 * (1 + cumulPct / 100);
  if (total >= 1_000_000) return "$" + (total / 1_000_000).toFixed(2) + "M";
  if (total >= 10_000) return "$" + (total / 1_000).toFixed(1) + "K";
  return "$" + total.toLocaleString("en-US", { maximumFractionDigits: 0 });
}

function formatVolume(n) {
  if (n == null) return "N/A";
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + "M";
  if (n >= 1_000) return (n / 1_000).toFixed(0) + "K";
  return n.toString();
}

function formatDate(d) {
  if (!d) return "N/A";
  return new Date(d).toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" });
}

function formatDataSince(d) {
  if (!d) return "";
  const date = new Date(d);
  const month = date.toLocaleDateString("en-US", { month: "short" });
  const year = date.getFullYear();
  return `${month} ${year}`;
}

function formatHoldings(h) {
  if (h == null) return "N/A";
  return h.toLocaleString();
}

// ── Loading & Error UI ──────────────────────────────────
function showLoading() {
  document.getElementById("loadingOverlay").classList.add("visible");
}

function hideLoading() {
  document.getElementById("loadingOverlay").classList.remove("visible");
}

function showError(msg) {
  const el = document.getElementById("errorBanner");
  el.textContent = msg;
  el.classList.add("visible");
}

function hideError() {
  document.getElementById("errorBanner").classList.remove("visible");
}

function updateTimestamp(fetchedAt, source, updating) {
  const el = document.getElementById("dataTimestamp");
  let label = "";
  if (source === "yahoo_finance") {
    label = "Live Yahoo Finance";
  } else if (source === "disk_cache") {
    label = "Cached Yahoo data";
  } else {
    label = "Estimated data";
  }
  if (updating) label += " (updating…)";
  if (fetchedAt) {
    const d = new Date(fetchedAt);
    el.textContent = label + " · " + d.toLocaleString();
  } else {
    el.textContent = label;
  }
}

// ── Fetch Data from Backend ─────────────────────────────
async function fetchData() {
  showLoading();
  hideError();
  try {
    const res = await fetch("/api/etfs");
    if (!res.ok) throw new Error(`Server error: ${res.status}`);
    const json = await res.json();

    if (json.error) throw new Error(json.message || json.error);

    etfData = json.data;
    updateTimestamp(json.fetchedAt, json.source, json.updating);

    renderSummaryCards();
    renderChart(currentChart);
    renderTable();
    initGrowthDefaults();

    // If data is still updating in background, poll for fresh data
    if (json.updating) {
      setTimeout(fetchData, 5000);
    }
  } catch (err) {
    console.error("Failed to load ETF data:", err);
    showError("Failed to load data: " + err.message + ". Retrying in 10s…");
    setTimeout(fetchData, 10000);
  } finally {
    hideLoading();
  }
}

let currentChart = "tenYearReturn";

// ── Summary Cards ───────────────────────────────────────
function renderSummaryCards() {
  const container = document.getElementById("summaryCards");
  if (!etfData.length) return;

  const etfsOnly = etfData.filter(e => !(e.category && e.category.startsWith("Real Estate")));
  const totalMarketCap = etfsOnly.reduce((s, e) => s + (e.marketCap || 0), 0);
  const avg10Y = etfData.reduce((s, e) => s + (e.tenYearReturn || 0), 0) / etfData.length;
  const best10Y = etfData.reduce((best, e) => ((e.tenYearReturn || 0) > (best.tenYearReturn || 0) ? e : best));
  const worstDD = etfData.reduce((worst, e) => ((e.maxDrawdown || 0) < (worst.maxDrawdown || 0) ? e : worst));
  const erData = etfData.filter(e => e.expenseRatio != null);
  const avgER = erData.length ? erData.reduce((s, e) => s + e.expenseRatio, 0) / erData.length : 0;

  const cards = [
    { label: "Best 10Y Performer", value: best10Y.ticker + " — " + (best10Y.tenYearReturn || 0).toFixed(1) + "%", sub: "Annualized return" },
    { label: "Avg 10Y Return", value: avg10Y.toFixed(2) + "%", sub: "Annualized across all ETFs" },
    { label: "Deepest Drawdown", value: worstDD.ticker + " — " + (worstDD.maxDrawdown || 0).toFixed(1) + "%", sub: worstDD.drawdownPeriod || "" },
    { label: "Combined AUM", value: "$" + totalMarketCap.toFixed(1) + "B", sub: "All ETFs" },
    { label: "Avg Expense Ratio", value: avgER.toFixed(3) + "%", sub: "Lower is better" },
  ];

  container.innerHTML = cards
    .map(
      (c) => `
    <div class="summary-card">
      <div class="label">${c.label}</div>
      <div class="value">${c.value}</div>
      <div class="sub">${c.sub}</div>
    </div>`
    )
    .join("");
}

// ── Chart ───────────────────────────────────────────────
let chart = null;

const chartConfigs = {
  fiveYearReturn: {
    label: "5Y Avg Annualized Return (%)",
    data: () => etfData.map((e) => e.fiveYearReturn ?? 0),
    color: "#06b6d4",
    format: (v) => v + "%",
    nullable: true,
  },
  tenYearReturn: {
    label: "10Y Avg Annualized Return (%)",
    data: () => etfData.map((e) => e.tenYearReturn ?? 0),
    color: "#8b5cf6",
    format: (v) => v + "%",
    nullable: true,
  },
  fifteenYearReturn: {
    label: "15Y Avg Annualized Return (%)",
    data: () => etfData.map((e) => e.fifteenYearReturn || 0),
    color: "#a855f7",
    format: (v) => v + "%",
  },
  twentyYearReturn: {
    label: "20Y Avg Annualized Return (%)",
    data: () => etfData.map((e) => e.twentyYearReturn ?? 0),
    color: "#d946ef",
    format: (v) => v + "%",
    nullable: true,
  },
  twentyFiveYearReturn: {
    label: "25Y Avg Annualized Return (%)",
    data: () => etfData.map((e) => e.twentyFiveYearReturn ?? 0),
    color: "#f97316",
    format: (v) => v + "%",
    nullable: true,
  },
  since1990Return: {
    label: "Since 1990 Annualized Return (%)",
    data: () => etfData.map((e) => e.since1990Return ?? 0),
    color: "#e11d48",
    format: (v) => v + "%",
    nullable: true,
  },
  sinceInceptionReturn: {
    label: "Since Inception Annualized Return (%)",
    data: () => etfData.map((e) => e.sinceInceptionReturn ?? 0),
    color: "#14b8a6",
    format: (v) => v + "%",
    nullable: true,
  },
  maxDrawdown: {
    label: "Max Drawdown (%)",
    data: () => etfData.map((e) => Math.abs(e.maxDrawdown || 0)),
    color: "#ef4444",
    format: (v) => "-" + v + "%",
    invertHighlight: true,
  },
  annualizedStdDev: {
    label: "Annualized Std Dev (%)",
    data: () => etfData.map((e) => e.annualizedStdDev || 0),
    color: "#f59e0b",
    format: (v) => v + "%",
    invertHighlight: true,
  },
  sharpeRatio: {
    label: "Sharpe Ratio (10Y)",
    data: () => etfData.map((e) => e.sharpeRatio ?? 0),
    color: "#10b981",
    format: (v) => v.toFixed(2),
    nullable: true,
  },
  marketCap: {
    label: "Market Cap ($B)",
    data: () => etfData.map((e) => e.marketCap || 0),
    color: "#6366f1",
    format: (v) => "$" + v + "B",
  },
  expenseRatio: {
    label: "Expense Ratio (%)",
    data: () => etfData.map((e) => e.expenseRatio || 0),
    color: "#eab308",
    format: (v) => v + "%",
  },
  ytdReturn: {
    label: "YTD Return (%)",
    data: () => etfData.map((e) => e.ytdReturn || 0),
    color: "#22c55e",
    format: (v) => v + "%",
  },
  dividendYield: {
    label: "Dividend Yield (%)",
    data: () => etfData.map((e) => e.dividendYield || 0),
    color: "#38bdf8",
    format: (v) => v + "%",
  },
};

function renderChart(metric = "fifteenYearReturn") {
  if (!etfData.length) return;

  const ctx = document.getElementById("comparisonChart").getContext("2d");
  const config = chartConfigs[metric];
  const data = config.data();
  const labels = etfData.map((e) => e.ticker);

  const highlightVal = config.invertHighlight ? Math.min(...data) : Math.max(...data);
  const colors = data.map((v) => (v === highlightVal ? config.color : config.color + "99"));

  if (chart) chart.destroy();

  chart = new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: config.label,
          data,
          backgroundColor: colors,
          borderColor: config.color,
          borderWidth: 1,
          borderRadius: 6,
          borderSkipped: false,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: "#1e2030",
          titleColor: "#fff",
          bodyColor: "#e4e5eb",
          borderColor: "#2a2e3f",
          borderWidth: 1,
          cornerRadius: 8,
          padding: 12,
          callbacks: {
            label: (ctx) => config.label + ": " + config.format(ctx.raw),
          },
        },
      },
      scales: {
        x: {
          grid: { display: false },
          ticks: { color: "#8b8fa3", font: { weight: 600 } },
        },
        y: {
          grid: { color: "#2a2e3f" },
          ticks: {
            color: "#8b8fa3",
            callback: (v) => config.format(v),
          },
        },
      },
      animation: {
        duration: 600,
        easing: "easeOutQuart",
      },
    },
  });
}

// Chart toggle buttons
document.querySelectorAll(".chart-btn").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".chart-btn").forEach((b) => b.classList.remove("active"));
    btn.classList.add("active");
    currentChart = btn.dataset.chart;
    renderChart(currentChart);
  });
});

// ── Column Visibility ────────────────────────────────────
const COLUMNS = [
  // key must match data-sort on <th> and the column index order (0-based)
  { key: "fiveYearReturn",            label: "5Y Avg Return",    idx: 3,  alwaysOn: false },
  { key: "tenYearReturn",             label: "10Y Avg Return",   idx: 4,  alwaysOn: false },
  { key: "fifteenYearReturn",         label: "15Y Avg Return",   idx: 5,  alwaysOn: false },
  { key: "twentyYearReturn",          label: "20Y Avg Return",   idx: 6,  alwaysOn: false },
  { key: "twentyFiveYearReturn",      label: "25Y Avg Return",   idx: 7,  alwaysOn: false },
  { key: "since1990Return",           label: "Since 1990",       idx: 8,  alwaysOn: false },
  { key: "sinceInceptionReturn",      label: "Since Inception",  idx: 9,  alwaysOn: false },
  { key: "maxDrawdown",               label: "Max Drawdown",     idx: 10, alwaysOn: false },
  { key: "secondDrawdown",            label: "2nd Drawdown",     idx: 11, alwaysOn: false },
  { key: "annualizedStdDev",          label: "Std Dev",          idx: 12, alwaysOn: false },
  { key: "sharpeRatio",               label: "Sharpe",           idx: 13, alwaysOn: false },
  { key: "marketCap",                 label: "Market Cap",       idx: 14, alwaysOn: false },
  { key: "price",                     label: "Price",            idx: 15, alwaysOn: false },
  { key: "expenseRatio",              label: "Expense Ratio",    idx: 16, alwaysOn: false },
  { key: "ytdReturn",                 label: "YTD Return",       idx: 17, alwaysOn: false },
  { key: "dividendYield",             label: "Div. Yield",       idx: 18, alwaysOn: false },
  { key: "avgVolume",                 label: "Avg Volume",       idx: 19, alwaysOn: false },
  { key: "holdings",                  label: "Holdings",         idx: 20, alwaysOn: false },
  { key: "inceptionDate",             label: "Inception",        idx: 21, alwaysOn: false },
];

const COL_STORAGE_KEY = "etf_visible_columns";

function getVisibleColumns() {
  try {
    const saved = localStorage.getItem(COL_STORAGE_KEY);
    if (saved) return JSON.parse(saved);
  } catch (_) {}
  // Default: all visible
  return COLUMNS.map(c => c.key);
}

let visibleColumns = getVisibleColumns();

function saveVisibleColumns() {
  localStorage.setItem(COL_STORAGE_KEY, JSON.stringify(visibleColumns));
}

function applyColumnVisibility() {
  const table = document.getElementById("etfTable");
  if (!table) return;
  COLUMNS.forEach(col => {
    const show = visibleColumns.includes(col.key);
    // Header
    const th = table.querySelector(`thead th[data-sort="${col.key}"]`);
    if (th) th.style.display = show ? "" : "none";
    // Body cells — nth-child is 1-based, idx is 0-based
    table.querySelectorAll(`tbody tr`).forEach(tr => {
      const td = tr.children[col.idx];
      if (td) td.style.display = show ? "" : "none";
    });
  });
}

function buildColumnPicker() {
  const dropdown = document.getElementById("colPickerDropdown");
  if (!dropdown) return;

  let html = COLUMNS.map(col => {
    const checked = visibleColumns.includes(col.key) ? "checked" : "";
    return `<label><input type="checkbox" value="${col.key}" ${checked}> ${col.label}</label>`;
  }).join("");

  html += `<div class="col-picker-actions">
    <button id="colPickerAll">Show All</button>
    <button id="colPickerNone">Hide All</button>
  </div>`;

  dropdown.innerHTML = html;

  // Checkbox change
  dropdown.querySelectorAll('input[type="checkbox"]').forEach(cb => {
    cb.addEventListener("change", () => {
      if (cb.checked) {
        if (!visibleColumns.includes(cb.value)) visibleColumns.push(cb.value);
      } else {
        visibleColumns = visibleColumns.filter(k => k !== cb.value);
      }
      saveVisibleColumns();
      applyColumnVisibility();
    });
  });

  // Show All / Hide All
  dropdown.querySelector("#colPickerAll").addEventListener("click", () => {
    visibleColumns = COLUMNS.map(c => c.key);
    saveVisibleColumns();
    dropdown.querySelectorAll('input[type="checkbox"]').forEach(cb => cb.checked = true);
    applyColumnVisibility();
  });

  dropdown.querySelector("#colPickerNone").addEventListener("click", () => {
    visibleColumns = [];
    saveVisibleColumns();
    dropdown.querySelectorAll('input[type="checkbox"]').forEach(cb => cb.checked = false);
    applyColumnVisibility();
  });
}

// Toggle dropdown open/close
document.getElementById("colPickerBtn").addEventListener("click", (e) => {
  e.stopPropagation();
  const dd = document.getElementById("colPickerDropdown");
  dd.classList.toggle("open");
});

// Close dropdown when clicking outside
document.addEventListener("click", (e) => {
  const dd = document.getElementById("colPickerDropdown");
  const wrap = document.querySelector(".col-picker-wrap");
  if (dd.classList.contains("open") && !wrap.contains(e.target)) {
    dd.classList.remove("open");
  }
});

// Build picker on load
buildColumnPicker();

// ── Table ───────────────────────────────────────────────
let sortKey = "rank";
let sortAsc = true;

function sortData(data) {
  return [...data].sort((a, b) => {
    let va = a[sortKey];
    let vb = b[sortKey];
    if (va == null && vb == null) return 0;
    if (va == null) return 1;
    if (vb == null) return -1;
    if (typeof va === "string") {
      va = va.toLowerCase();
      vb = (vb || "").toLowerCase();
    }
    if (va < vb) return sortAsc ? -1 : 1;
    if (va > vb) return sortAsc ? 1 : -1;
    return 0;
  });
}

function renderTable(data) {
  if (!data) data = etfData;
  const tbody = document.getElementById("etfTableBody");
  if (!data.length) return;

  const sorted = sortData(data);

  tbody.innerHTML = sorted
    .map(
      (e) => `
    <tr data-ticker="${e.ticker}" class="${e.ticker === 'PORT-7' ? 'row-portfolio' : e.category && e.category.startsWith('Real Estate') ? 'row-alt-asset' : ''}">
      <td>
        <div>${e.ticker === 'PORT-7' ? '★' : e.rank}</div>
        ${e.rankReason ? `<div class="rank-reason">${e.rankReason}</div>` : ''}
      </td>
      <td>
        <div class="ticker-cell">
          <span class="ticker-badge${e.ticker === 'PORT-7' ? ' badge-portfolio' : e.category && e.category.startsWith('Real Estate') ? ' badge-alt' : ''}">${e.ticker}</span>
        </div>
      </td>
      <td>
        <div>${e.name}</div>
        ${e.dataStart ? `<div class="data-since-note">data from ${formatDataSince(e.dataStart)}${e.backerNote ? ' *' : ''}</div>` : ''}
      </td>
      <td class="num">
        ${e.fiveYearReturn != null
          ? `<span class="tag tag-green" style="font-size:0.85rem">${formatPercent(e.fiveYearReturn)}</span>
             ${e.fiveYearNote ? `<div class="data-since-note">${e.fiveYearNote}</div>` : ''}
             <div class="cumul-row"><span class="cumul-pct">${formatLargePercent(e.fiveYearCumulativeReturn)}</span> <span class="cumul-money">${formatGrowth(e.fiveYearCumulativeReturn)}</span></div>`
          : '<span class="tag" style="font-size:0.85rem;opacity:0.4">N/A</span>'}
      </td>
      <td class="num">
        ${e.tenYearReturn != null
          ? `<span class="tag tag-green" style="font-size:0.85rem">${formatPercent(e.tenYearReturn)}</span>
             ${e.tenYearNote ? `<div class="data-since-note">${e.tenYearNote}</div>` : ''}
             <div class="cumul-row"><span class="cumul-pct">${formatLargePercent(e.tenYearCumulativeReturn)}</span> <span class="cumul-money">${formatGrowth(e.tenYearCumulativeReturn)}</span></div>`
          : '<span class="tag" style="font-size:0.85rem;opacity:0.4">N/A</span>'}
      </td>
      <td class="num">
        ${e.fifteenYearReturn != null
          ? `<span class="tag tag-green" style="font-size:0.85rem">${formatPercent(e.fifteenYearReturn)}</span>
             ${e.fifteenYearNote ? `<div class="data-since-note">${e.fifteenYearNote}</div>` : ''}
             <div class="cumul-row"><span class="cumul-pct">${formatLargePercent(e.fifteenYearCumulativeReturn)}</span> <span class="cumul-money">${formatGrowth(e.fifteenYearCumulativeReturn)}</span></div>`
          : '<span class="tag" style="font-size:0.85rem;opacity:0.4">N/A</span>'}
      </td>
      <td class="num">
        ${e.twentyYearReturn != null
          ? `<span class="tag tag-green" style="font-size:0.85rem">${formatPercent(e.twentyYearReturn)}</span>
             ${e.twentyYearNote ? `<div class="data-since-note">${e.twentyYearNote}</div>` : ''}
             <div class="cumul-row"><span class="cumul-pct">${formatLargePercent(e.twentyYearCumulativeReturn)}</span> <span class="cumul-money">${formatGrowth(e.twentyYearCumulativeReturn)}</span></div>`
          : '<span class="tag" style="font-size:0.85rem;opacity:0.4">N/A</span>'}
      </td>
      <td class="num">
        ${e.twentyFiveYearReturn != null
          ? `<span class="tag tag-green" style="font-size:0.85rem">${formatPercent(e.twentyFiveYearReturn)}</span>
             ${e.twentyFiveYearNote ? `<div class="data-since-note">${e.twentyFiveYearNote}</div>` : ''}
             <div class="cumul-row"><span class="cumul-pct">${formatLargePercent(e.twentyFiveYearCumulativeReturn)}</span> <span class="cumul-money">${formatGrowth(e.twentyFiveYearCumulativeReturn)}</span></div>`
          : '<span class="tag" style="font-size:0.85rem;opacity:0.4">N/A</span>'}
      </td>
      <td class="num">
        ${e.since1990Return != null
          ? `<span class="tag tag-green" style="font-size:0.85rem">${formatPercent(e.since1990Return)}</span>
             <div class="data-since-note">${e.since1990Years || 0}Y of data</div>
             <div class="cumul-row"><span class="cumul-pct">${formatLargePercent(e.since1990CumulativeReturn || cumulative_return_js(e.since1990Return, e.since1990Years))}</span> <span class="cumul-money">${formatGrowth(e.since1990CumulativeReturn || cumulative_return_js(e.since1990Return, e.since1990Years))}</span></div>`
          : '<span class="tag" style="font-size:0.85rem;opacity:0.4">N/A</span>'}
      </td>
      <td class="num">
        ${e.sinceInceptionReturn != null
          ? `<span class="tag tag-green" style="font-size:0.85rem">${formatPercent(e.sinceInceptionReturn)}</span>
             <div class="data-since-note">${e.sinceInceptionYears || 0}Y of data</div>
             <div class="cumul-row"><span class="cumul-pct">${formatLargePercent(cumulative_return_js(e.sinceInceptionReturn, e.sinceInceptionYears))}</span> <span class="cumul-money">${formatGrowth(cumulative_return_js(e.sinceInceptionReturn, e.sinceInceptionYears))}</span></div>`
          : '<span class="tag" style="font-size:0.85rem;opacity:0.4">N/A</span>'}
      </td>
      <td class="num">
        <span class="tag tag-red" style="font-size:0.85rem">
          ${formatPercent(e.maxDrawdown)}
        </span>
        ${e.drawdownPeriod ? `<div class="data-since-note">${e.drawdownPeriod}</div>` : ''}
        ${e.drawdownLabel ? `<div class="data-since-note" style="opacity:0.5">${e.drawdownLabel}</div>` : ''}
      </td>
      <td class="num">
        ${e.secondDrawdown != null ? `
          <span class="tag tag-red" style="font-size:0.85rem;opacity:0.8">${formatPercent(e.secondDrawdown)}</span>
          ${e.secondDrawdownPeriod ? `<div class="data-since-note">${e.secondDrawdownPeriod}</div>` : ''}
          ${e.secondDrawdownLabel ? `<div class="data-since-note" style="opacity:0.5">${e.secondDrawdownLabel}</div>` : ''}
        ` : '<span style="opacity:0.4">N/A</span>'}
      </td>
      <td class="num">${e.annualizedStdDev != null ? formatPercent(e.annualizedStdDev) : "N/A"}</td>
      <td class="num">
        ${e.sharpeRatio != null ? `<span class="tag ${e.sharpeRatio >= 0.5 ? 'tag-green' : e.sharpeRatio >= 0 ? '' : 'tag-red'}" style="font-size:0.85rem">${e.sharpeRatio.toFixed(2)}</span>` : 'N/A'}
      </td>
      <td class="num">${formatMarketCap(e.marketCap)}</td>
      <td class="num">${formatCurrency(e.price)}</td>
      <td class="num">${e.expenseRatio != null ? formatPercent(e.expenseRatio) : "N/A"}</td>
      <td class="num">
        <span class="tag ${(e.ytdReturn || 0) >= 0 ? "tag-green" : "tag-red"}">
          ${(e.ytdReturn || 0) >= 0 ? "+" : ""}${formatPercent(e.ytdReturn)}
        </span>
      </td>
      <td class="num">${formatPercent(e.dividendYield)}</td>
      <td class="num">${formatVolume(e.avgVolume)}</td>
      <td class="num">${formatHoldings(e.holdings)}</td>
      <td>${formatDate(e.inceptionDate)}</td>
    </tr>`
    )
    .join("");

  // Row click → open modal
  tbody.querySelectorAll("tr").forEach((tr) => {
    tr.addEventListener("click", () => {
      const ticker = tr.dataset.ticker;
      const etf = etfData.find((e) => e.ticker === ticker);
      if (etf) openModal(etf);
    });
  });

  // Apply column visibility to the freshly rendered rows
  applyColumnVisibility();
}

// Sort headers
document.querySelectorAll("thead th[data-sort]").forEach((th) => {
  th.addEventListener("click", () => {
    const key = th.dataset.sort;
    if (sortKey === key) {
      sortAsc = !sortAsc;
    } else {
      sortKey = key;
      sortAsc = true;
    }

    document.querySelectorAll("thead th").forEach((t) => t.classList.remove("sorted-asc", "sorted-desc"));
    th.classList.add(sortAsc ? "sorted-asc" : "sorted-desc");

    renderTable(getFilteredData());
  });
});

// Search / Filter
function getFilteredData() {
  const query = document.getElementById("searchInput").value.toLowerCase().trim();
  if (!query) return etfData;
  return etfData.filter(
    (e) =>
      e.ticker.toLowerCase().includes(query) ||
      (e.name || "").toLowerCase().includes(query) ||
      (e.issuer || "").toLowerCase().includes(query) ||
      (e.category || "").toLowerCase().includes(query)
  );
}

document.getElementById("searchInput").addEventListener("input", () => {
  renderTable(getFilteredData());
});

// ── Modal ───────────────────────────────────────────────
function openModal(etf) {
  const overlay = document.getElementById("modalOverlay");
  const content = document.getElementById("modalContent");

  const isAltAsset = etf.category && etf.category.startsWith("Real Estate");

  // ── Return stats (always shown) ────────────────────────
  let statsHtml = `
      <div class="modal-stat" style="background:rgba(6,182,212,0.1);border-color:rgba(6,182,212,0.3)">
        <div class="stat-label">5Y Return</div>
        <div class="stat-value ${etf.fiveYearReturn != null && etf.fiveYearReturn >= 0 ? 'positive' : etf.fiveYearReturn < 0 ? 'negative' : ''}" style="font-size:1.3rem">${etf.fiveYearReturn != null ? formatPercent(etf.fiveYearReturn) : "N/A"} <span style="font-size:0.75rem;opacity:0.65">ann.</span></div>
        ${etf.fiveYearCumulativeReturn != null ? `<div class="modal-cumul-row"><span class="modal-cumul-pct">${formatLargePercent(etf.fiveYearCumulativeReturn)} cumul.</span><span class="modal-cumul-money">$1K &rarr; ${formatGrowth(etf.fiveYearCumulativeReturn)}</span></div>` : ''}
      </div>
      <div class="modal-stat" style="background:rgba(139,92,246,0.1);border-color:rgba(139,92,246,0.3)">
        <div class="stat-label">10Y Return</div>
        <div class="stat-value ${etf.tenYearReturn != null && etf.tenYearReturn >= 0 ? 'positive' : etf.tenYearReturn < 0 ? 'negative' : ''}" style="font-size:1.3rem">${etf.tenYearReturn != null ? formatPercent(etf.tenYearReturn) : "N/A"} <span style="font-size:0.75rem;opacity:0.65">ann.</span></div>
        ${etf.tenYearCumulativeReturn != null ? `<div class="modal-cumul-row"><span class="modal-cumul-pct">${formatLargePercent(etf.tenYearCumulativeReturn)} cumul.</span><span class="modal-cumul-money">$1K &rarr; ${formatGrowth(etf.tenYearCumulativeReturn)}</span></div>` : ''}
      </div>
      <div class="modal-stat" style="background:rgba(168,85,247,0.1);border-color:rgba(168,85,247,0.3)">
        <div class="stat-label">15Y Return${etf.fifteenYearNote ? " (" + etf.fifteenYearNote + ")" : ""}</div>
        <div class="stat-value positive" style="font-size:1.3rem">${formatPercent(etf.fifteenYearReturn)} <span style="font-size:0.75rem;opacity:0.65">ann.</span></div>
        ${etf.fifteenYearCumulativeReturn != null ? `<div class="modal-cumul-row"><span class="modal-cumul-pct">${formatLargePercent(etf.fifteenYearCumulativeReturn)} cumul.</span><span class="modal-cumul-money">$1K &rarr; ${formatGrowth(etf.fifteenYearCumulativeReturn)}</span></div>` : ''}
      </div>
      <div class="modal-stat" style="background:rgba(217,70,239,0.1);border-color:rgba(217,70,239,0.3)">
        <div class="stat-label">20Y Return${etf.twentyYearNote ? " (" + etf.twentyYearNote + ")" : ""}</div>
        <div class="stat-value ${etf.twentyYearReturn != null ? 'positive' : ''}" style="font-size:1.3rem">${etf.twentyYearReturn != null ? formatPercent(etf.twentyYearReturn) : "N/A"} <span style="font-size:0.75rem;opacity:0.65">ann.</span></div>
        ${etf.twentyYearCumulativeReturn != null ? `<div class="modal-cumul-row"><span class="modal-cumul-pct">${formatLargePercent(etf.twentyYearCumulativeReturn)} cumul.</span><span class="modal-cumul-money">$1K &rarr; ${formatGrowth(etf.twentyYearCumulativeReturn)}</span></div>` : ''}
      </div>
      <div class="modal-stat" style="background:rgba(249,115,22,0.1);border-color:rgba(249,115,22,0.3)">
        <div class="stat-label">25Y Return${etf.twentyFiveYearNote ? " (" + etf.twentyFiveYearNote + ")" : ""}</div>
        <div class="stat-value ${etf.twentyFiveYearReturn != null ? 'positive' : ''}" style="font-size:1.3rem">${etf.twentyFiveYearReturn != null ? formatPercent(etf.twentyFiveYearReturn) : "N/A"} <span style="font-size:0.75rem;opacity:0.65">ann.</span></div>
        ${etf.twentyFiveYearCumulativeReturn != null ? `<div class="modal-cumul-row"><span class="modal-cumul-pct">${formatLargePercent(etf.twentyFiveYearCumulativeReturn)} cumul.</span><span class="modal-cumul-money">$1K &rarr; ${formatGrowth(etf.twentyFiveYearCumulativeReturn)}</span></div>` : ''}
      </div>
      ${etf.since1990Return != null ? `
      <div class="modal-stat" style="background:rgba(225,29,72,0.1);border-color:rgba(225,29,72,0.3)">
        <div class="stat-label">Since 1990 (${etf.since1990Years || 0}Y of data)</div>
        <div class="stat-value positive" style="font-size:1.3rem">${formatPercent(etf.since1990Return)} <span style="font-size:0.75rem;opacity:0.65">ann.</span></div>
        <div class="modal-cumul-row"><span class="modal-cumul-pct">${formatLargePercent(etf.since1990CumulativeReturn || cumulative_return_js(etf.since1990Return, etf.since1990Years))} cumul.</span><span class="modal-cumul-money">$1K &rarr; ${formatGrowth(etf.since1990CumulativeReturn || cumulative_return_js(etf.since1990Return, etf.since1990Years))}</span></div>
      </div>` : ''}
      ${etf.sinceInceptionReturn != null ? `
      <div class="modal-stat" style="background:rgba(20,184,166,0.1);border-color:rgba(20,184,166,0.3)">
        <div class="stat-label">Since Inception (${etf.sinceInceptionYears || 0}Y of data)</div>
        <div class="stat-value positive" style="font-size:1.3rem">${formatPercent(etf.sinceInceptionReturn)} <span style="font-size:0.75rem;opacity:0.65">ann.</span></div>
        <div class="modal-cumul-row"><span class="modal-cumul-pct">${formatLargePercent(cumulative_return_js(etf.sinceInceptionReturn, etf.sinceInceptionYears))} cumul.</span><span class="modal-cumul-money">$1K &rarr; ${formatGrowth(cumulative_return_js(etf.sinceInceptionReturn, etf.sinceInceptionYears))}</span></div>
      </div>` : ''}
      <div class="modal-stat" style="background:rgba(239,68,68,0.08);border-color:rgba(239,68,68,0.25)">
        <div class="stat-label">Worst Drawdown (${etf.drawdownPeriod || "N/A"})</div>
        <div class="stat-value negative" style="font-size:1.3rem">${formatPercent(etf.maxDrawdown)}</div>
        ${etf.drawdownLabel ? `<div class="stat-sub">${etf.drawdownLabel}</div>` : ''}
      </div>
      ${etf.secondDrawdown != null ? `
      <div class="modal-stat" style="background:rgba(251,146,60,0.08);border-color:rgba(251,146,60,0.25)">
        <div class="stat-label">2nd Worst Drawdown (${etf.secondDrawdownPeriod || "N/A"})</div>
        <div class="stat-value negative" style="font-size:1.3rem">${formatPercent(etf.secondDrawdown)}</div>
        ${etf.secondDrawdownLabel ? `<div class="stat-sub">${etf.secondDrawdownLabel}</div>` : ''}
      </div>` : ''}
      <div class="modal-stat" style="background:rgba(245,158,11,0.08);border-color:rgba(245,158,11,0.25)">
        <div class="stat-label">Annualized Std Dev</div>
        <div class="stat-value" style="font-size:1.3rem">${etf.annualizedStdDev != null ? formatPercent(etf.annualizedStdDev) : "N/A"}</div>
        <div class="stat-sub">Volatility (annualized)</div>
      </div>
      <div class="modal-stat" style="background:rgba(16,185,129,0.08);border-color:rgba(16,185,129,0.25)">
        <div class="stat-label">Sharpe Ratio (10Y)</div>
        <div class="stat-value ${etf.sharpeRatio != null && etf.sharpeRatio >= 0.5 ? 'positive' : etf.sharpeRatio != null && etf.sharpeRatio < 0 ? 'negative' : ''}" style="font-size:1.3rem">${etf.sharpeRatio != null ? etf.sharpeRatio.toFixed(2) : "N/A"}</div>
        <div class="stat-sub">(Return − ${RISK_FREE_RATE}% risk-free) / Std Dev</div>
      </div>
      <div class="modal-stat" style="background:rgba(99,102,241,0.08);border-color:rgba(99,102,241,0.25)">
        <div class="stat-label">Data Available Since</div>
        <div class="stat-value" style="font-size:1.1rem">${etf.dataStart ? formatDataSince(etf.dataStart) : "N/A"}</div>
        ${etf.backerNote ? `<div class="stat-sub">${etf.backerNote}</div>` : ''}
      </div>`;

  // ── Shared short-term returns ──────────────────────────
  statsHtml += `
      <div class="modal-stat">
        <div class="stat-label">YTD Return</div>
        <div class="stat-value ${(etf.ytdReturn || 0) >= 0 ? "positive" : "negative"}">
          ${(etf.ytdReturn || 0) >= 0 ? "+" : ""}${formatPercent(etf.ytdReturn)}
        </div>
      </div>
      <div class="modal-stat">
        <div class="stat-label">1-Year Return</div>
        <div class="stat-value ${(etf.oneYearReturn || 0) >= 0 ? "positive" : "negative"}">
          ${(etf.oneYearReturn || 0) >= 0 ? "+" : ""}${formatPercent(etf.oneYearReturn)}
        </div>
      </div>
      <div class="modal-stat">
        <div class="stat-label">3-Year Return (Ann.)</div>
        <div class="stat-value ${(etf.threeYearReturn || 0) >= 0 ? "positive" : "negative"}">
          ${(etf.threeYearReturn || 0) >= 0 ? "+" : ""}${formatPercent(etf.threeYearReturn)}
        </div>
      </div>`;

  if (isAltAsset) {
    // Real estate-specific fields
    statsHtml += `
      <div class="modal-stat">
        <div class="stat-label">Data Source</div>
        <div class="stat-value" style="font-size:0.95rem">${etf.issuer || "N/A"}</div>
      </div>
      <div class="modal-stat">
        <div class="stat-label">Data Since</div>
        <div class="stat-value">${formatDate(etf.inceptionDate)}</div>
      </div>
      <div class="modal-stat">
        <div class="stat-label">Category</div>
        <div class="stat-value" style="font-size:0.95rem">${etf.category || "N/A"}</div>
      </div>`;
  } else {
    // ETF-specific fields
    statsHtml += `
      <div class="modal-stat">
        <div class="stat-label">Market Cap</div>
        <div class="stat-value">${formatMarketCap(etf.marketCap)}</div>
      </div>
      <div class="modal-stat">
        <div class="stat-label">Price</div>
        <div class="stat-value">${formatCurrency(etf.price)}</div>
      </div>
      <div class="modal-stat">
        <div class="stat-label">Expense Ratio</div>
        <div class="stat-value">${etf.expenseRatio != null ? formatPercent(etf.expenseRatio) : "N/A"}</div>
      </div>
      <div class="modal-stat">
        <div class="stat-label">Dividend Yield</div>
        <div class="stat-value">${formatPercent(etf.dividendYield)}</div>
      </div>
      <div class="modal-stat">
        <div class="stat-label">Holdings</div>
        <div class="stat-value">${formatHoldings(etf.holdings)}</div>
      </div>
      <div class="modal-stat">
        <div class="stat-label">Avg Daily Volume</div>
        <div class="stat-value">${formatVolume(etf.avgVolume)}</div>
      </div>
      <div class="modal-stat">
        <div class="stat-label">Inception Date</div>
        <div class="stat-value">${formatDate(etf.inceptionDate)}</div>
      </div>
      <div class="modal-stat">
        <div class="stat-label">Category</div>
        <div class="stat-value" style="font-size:0.95rem">${etf.category || "N/A"}</div>
      </div>
      <div class="modal-stat">
        <div class="stat-label">Tracks Index</div>
        <div class="stat-value" style="font-size:0.95rem">${etf.index || "N/A"}</div>
      </div>`;
  }

  content.innerHTML = `
    <div class="modal-ticker">${etf.ticker} <span style="font-size:0.9rem;color:var(--text-muted);font-weight:400">#${etf.rank} — ${etf.rankReason || ''}</span></div>
    <div class="modal-name">${etf.name}${etf.issuer ? " · " + etf.issuer : ""}</div>
    <div class="modal-grid">${statsHtml}</div>
    <div class="modal-description">${etf.description || ""}</div>
  `;

  overlay.classList.add("open");
}

function closeModal() {
  document.getElementById("modalOverlay").classList.remove("open");
}

document.getElementById("modalClose").addEventListener("click", closeModal);
document.getElementById("modalOverlay").addEventListener("click", (e) => {
  if (e.target === e.currentTarget) closeModal();
});
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") closeModal();
});

// Refresh button — triggers a background re-fetch then polls
document.getElementById("refreshBtn").addEventListener("click", async () => {
  try {
    await fetch("/api/refresh", { method: "POST" });
    // Give it a moment to start, then poll
    setTimeout(fetchData, 2000);
  } catch (e) {
    fetchData(); // fallback: just re-read current data
  }
});

// ══════════════════════════════════════════════════════════
//  Growth of $10K Chart
// ══════════════════════════════════════════════════════════

const GROWTH_COLORS = [
  "#6366f1", // indigo (portfolio)
  "#22d3ee", // cyan
  "#f97316", // orange
  "#10b981", // emerald
  "#f43f5e", // rose
];

let growthChart = null;
let growthYears = "10"; // string: "5", "10", "15", "20", "25", or "max"
let growthSelected = []; // tickers currently selected (max 5)

// Build the asset picker dropdown from etfData
function buildGrowthPicker() {
  const dd = document.getElementById("growthPickerDropdown");
  if (!etfData.length) return;

  dd.innerHTML = etfData.map(e => {
    const checked = growthSelected.includes(e.ticker) ? "checked" : "";
    return `<label class="growth-picker-item" data-ticker="${e.ticker}">
      <input type="checkbox" value="${e.ticker}" ${checked}>
      <span class="gp-ticker">${e.ticker}</span>
      <span class="gp-name">${e.name}</span>
    </label>`;
  }).join("");

  // Wire up checkboxes
  dd.querySelectorAll("input[type=checkbox]").forEach(cb => {
    cb.addEventListener("change", () => {
      const ticker = cb.value;
      if (cb.checked) {
        if (growthSelected.length >= 5) {
          cb.checked = false;
          return;
        }
        growthSelected.push(ticker);
      } else {
        growthSelected = growthSelected.filter(t => t !== ticker);
      }
      updatePickerState();
      fetchGrowthData();
    });
  });
  updatePickerState();
}

function updatePickerState() {
  const dd = document.getElementById("growthPickerDropdown");
  const atLimit = growthSelected.length >= 5;
  dd.querySelectorAll(".growth-picker-item").forEach(item => {
    const cb = item.querySelector("input");
    if (!cb.checked && atLimit) {
      item.classList.add("disabled");
      cb.disabled = true;
    } else {
      item.classList.remove("disabled");
      cb.disabled = false;
    }
  });
  // Update button label
  const btn = document.getElementById("growthPickerBtn");
  const count = growthSelected.length;
  btn.querySelector("svg").nextSibling.textContent =
    count ? ` ${growthSelected.join(", ")} (${count}/5)` : " Select Assets (max 5)";
}

// Dropdown toggle
document.getElementById("growthPickerBtn").addEventListener("click", (e) => {
  e.stopPropagation();
  document.getElementById("growthPickerDropdown").classList.toggle("open");
});
document.addEventListener("click", (e) => {
  const dd = document.getElementById("growthPickerDropdown");
  if (!dd.contains(e.target) && e.target !== document.getElementById("growthPickerBtn")) {
    dd.classList.remove("open");
  }
});

// Period buttons
document.querySelectorAll(".growth-period-btns .chart-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".growth-period-btns .chart-btn").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    growthYears = btn.dataset.years; // "5", "10", ..., "max"
    fetchGrowthData();
  });
});

async function fetchGrowthData() {
  if (!growthSelected.length) {
    if (growthChart) { growthChart.destroy(); growthChart = null; }
    return;
  }
  try {
    const url = `/api/growth?tickers=${growthSelected.join(",")}&years=${growthYears}`;
    const res = await fetch(url);
    const data = await res.json();
    renderGrowthChart(data);
  } catch (err) {
    console.error("Growth chart fetch error:", err);
  }
}

function renderGrowthChart(seriesData) {
  const ctx = document.getElementById("growthChart").getContext("2d");
  if (growthChart) growthChart.destroy();

  const meta = seriesData._meta || {};
  const datasets = [];
  let colorIdx = 0;

  // Compute actual span in years for axis formatting
  let spanYears = parseInt(growthYears, 10) || 30;
  if (meta.commonStart && meta.commonEnd) {
    spanYears = (new Date(meta.commonEnd) - new Date(meta.commonStart)) / (365.25 * 86400000);
  }

  for (const ticker of growthSelected) {
    const pts = seriesData[ticker];
    if (!pts || !pts.length) continue;
    const color = GROWTH_COLORS[colorIdx % GROWTH_COLORS.length];
    colorIdx++;

    datasets.push({
      label: ticker,
      data: pts.map(p => ({ x: p[0], y: p[1] })),
      borderColor: color,
      backgroundColor: color + "18",
      borderWidth: ticker.startsWith("PORT-") ? 3 : 2,
      pointRadius: 0,
      pointHoverRadius: 4,
      fill: false,
      tension: 0.25,
    });
  }

  // Update chart title with common period info
  const titleEl = document.querySelector(".growth-section .chart-header h2");
  if (titleEl && meta.commonStart) {
    const startStr = new Date(meta.commonStart).toLocaleDateString("en-US", { year: "numeric", month: "short" });
    const endStr = meta.commonEnd ? new Date(meta.commonEnd).toLocaleDateString("en-US", { year: "numeric", month: "short" }) : "now";
    titleEl.textContent = `Growth of $10,000 — ${startStr} to ${endStr} (${spanYears.toFixed(1)}Y)`;
  } else {
    titleEl.textContent = "Growth of $10,000 Over Time";
  }

  growthChart = new Chart(ctx, {
    type: "line",
    data: { datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: {
          labels: { color: "#c4c7e0", padding: 16, usePointStyle: true, pointStyle: "line",
                    font: { size: 12, weight: 600 } },
        },
        tooltip: {
          backgroundColor: "#1e2030",
          titleColor: "#fff",
          bodyColor: "#e4e5eb",
          borderColor: "#2a2e3f",
          borderWidth: 1,
          cornerRadius: 8,
          padding: 12,
          callbacks: {
            title: (items) => {
              if (!items.length) return "";
              const d = items[0].raw.x;
              return new Date(d).toLocaleDateString("en-US", { year: "numeric", month: "short" });
            },
            label: (ctx) => {
              const val = ctx.raw.y;
              const formatted = val >= 1_000_000
                ? "$" + (val / 1_000_000).toFixed(2) + "M"
                : "$" + val.toLocaleString("en-US", { maximumFractionDigits: 0 });
              const gain = ((val / 10000 - 1) * 100).toFixed(1);
              return ` ${ctx.dataset.label}: ${formatted}  (${gain >= 0 ? "+" : ""}${gain}%)`;
            },
          },
        },
      },
      scales: {
        x: {
          type: "time",
          time: { unit: spanYears <= 5 ? "month" : "year", tooltipFormat: "MMM yyyy",
                  displayFormats: { month: "MMM yy", year: "yyyy" } },
          grid: { display: false },
          ticks: { color: "#8b8fa3", maxTicksLimit: 14 },
        },
        y: {
          grid: { color: "#2a2e3f" },
          ticks: {
            color: "#8b8fa3",
            callback: (v) => {
              if (v >= 1_000_000) return "$" + (v / 1_000_000).toFixed(1) + "M";
              if (v >= 1000) return "$" + (v / 1000).toFixed(0) + "K";
              return "$" + v;
            },
          },
          beginAtZero: false,
        },
      },
    },
  });
}

// Set default selections once data loads — PORT-7 + top 4
function initGrowthDefaults() {
  if (!etfData.length) return;
  // PORT-7 first, then the first 4 non-portfolio assets (which are top 4 by 10Y)
  growthSelected = [];
  const port = etfData.find(e => e.ticker.startsWith("PORT-"));
  if (port) growthSelected.push(port.ticker);
  for (const e of etfData) {
    if (growthSelected.length >= 5) break;
    if (!e.ticker.startsWith("PORT-") && !growthSelected.includes(e.ticker)) {
      growthSelected.push(e.ticker);
    }
  }
  buildGrowthPicker();
  fetchGrowthData();
}

// ── Initialize ──────────────────────────────────────────
fetchData();
