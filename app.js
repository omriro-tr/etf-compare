// ── ETF Compare — Frontend (fetches live data from /api/etfs) ────────

let etfData = [];

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

let currentChart = "fifteenYearReturn";

// ── Summary Cards ───────────────────────────────────────
function renderSummaryCards() {
  const container = document.getElementById("summaryCards");
  if (!etfData.length) return;

  const totalMarketCap = etfData.reduce((s, e) => s + (e.marketCap || 0), 0);
  const avg15Y = etfData.reduce((s, e) => s + (e.fifteenYearReturn || 0), 0) / etfData.length;
  const best15Y = etfData.reduce((best, e) => ((e.fifteenYearReturn || 0) > (best.fifteenYearReturn || 0) ? e : best));
  const worstDD = etfData.reduce((worst, e) => ((e.maxDrawdown || 0) < (worst.maxDrawdown || 0) ? e : worst));
  const avgER = etfData.filter(e => e.expenseRatio != null).reduce((s, e) => s + e.expenseRatio, 0) / etfData.filter(e => e.expenseRatio != null).length;

  const cards = [
    { label: "Best 15Y Performer", value: best15Y.ticker + " — " + (best15Y.fifteenYearReturn || 0).toFixed(1) + "%", sub: "Annualized return" },
    { label: "Avg 15Y Return", value: avg15Y.toFixed(2) + "%", sub: "Annualized across all 10" },
    { label: "Deepest Drawdown", value: worstDD.ticker + " — " + (worstDD.maxDrawdown || 0).toFixed(1) + "%", sub: worstDD.drawdownPeriod || "" },
    { label: "Combined AUM", value: "$" + totalMarketCap.toFixed(1) + "B", sub: "All 10 ETFs" },
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
  fifteenYearReturn: {
    label: "15Y Annualized Return (%)",
    data: () => etfData.map((e) => e.fifteenYearReturn || 0),
    color: "#a855f7",
    format: (v) => v + "%",
  },
  maxDrawdown: {
    label: "Max Drawdown (%)",
    data: () => etfData.map((e) => Math.abs(e.maxDrawdown || 0)),
    color: "#ef4444",
    format: (v) => "-" + v + "%",
    invertHighlight: true,
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
    <tr data-ticker="${e.ticker}">
      <td>${e.rank}</td>
      <td>
        <div class="ticker-cell">
          <span class="ticker-badge">${e.ticker}</span>
        </div>
      </td>
      <td>${e.name}</td>
      <td class="num">
        <span class="tag tag-green" style="font-size:0.85rem">
          ${formatPercent(e.fifteenYearReturn)}${e.fifteenYearNote ? " *" : ""}
        </span>
      </td>
      <td class="num">
        <span class="tag tag-red" style="font-size:0.85rem">
          ${formatPercent(e.maxDrawdown)}
        </span>
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

  content.innerHTML = `
    <div class="modal-ticker">${etf.ticker} <span style="font-size:0.9rem;color:var(--text-muted);font-weight:400">#${etf.rank} by 15Y return</span></div>
    <div class="modal-name">${etf.name} · ${etf.issuer}</div>
    <div class="modal-grid">
      <div class="modal-stat" style="background:rgba(168,85,247,0.1);border-color:rgba(168,85,247,0.3)">
        <div class="stat-label">15Y Ann. Return${etf.fifteenYearNote ? " (" + etf.fifteenYearNote + ")" : ""}</div>
        <div class="stat-value positive" style="font-size:1.4rem">${formatPercent(etf.fifteenYearReturn)}</div>
      </div>
      <div class="modal-stat" style="background:rgba(239,68,68,0.08);border-color:rgba(239,68,68,0.25)">
        <div class="stat-label">Max Drawdown (${etf.drawdownPeriod || "N/A"})</div>
        <div class="stat-value negative" style="font-size:1.4rem">${formatPercent(etf.maxDrawdown)}</div>
      </div>
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
      </div>
    </div>
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

// ── Initialize ──────────────────────────────────────────
fetchData();
