const sampleStocks = [
  {
    ticker: "BKDB",
    company: "Bank Dhofar",
    sector: "Banking",
    price: 0.168,
    marketCapM: 430,
    peRatio: 8.2,
    pbRatio: 0.78,
    dividendYield: 6.4,
    earningsGrowth: 12.8,
    priceChange1Y: 14.2,
    debtToEquity: 1.8,
    roe: 10.4,
    currentRatio: 1.3,
    bidVolume: 0,
    askVolume: 0,
    buyPressure: 0,
    demandScore: 0,
    rsi14: 0,
    bollingerUpper: 0,
    bollingerMiddle: 0,
    bollingerLower: 0,
    volumeVsAvg20: 0,
    dailyChange: 0,
  },
  {
    ticker: "BKMB",
    company: "Bank Muscat",
    sector: "Banking",
    price: 0.292,
    marketCapM: 2330,
    peRatio: 9.5,
    pbRatio: 1.1,
    dividendYield: 5.8,
    earningsGrowth: 10.1,
    priceChange1Y: 11.9,
    debtToEquity: 1.5,
    roe: 11.8,
    currentRatio: 1.2,
    bidVolume: 0,
    askVolume: 0,
    buyPressure: 0,
    demandScore: 0,
    rsi14: 0,
    bollingerUpper: 0,
    bollingerMiddle: 0,
    bollingerLower: 0,
    volumeVsAvg20: 0,
    dailyChange: 0,
  },
];

const state = {
  stocks: [],
  filteredStocks: [],
  selectedTicker: null,
  dataSource: "loading",
  warning: "",
  fetchedAt: "",
  chartCache: {},
  technicalReady: 0,
  technicalTotal: 0,
  refreshTimer: null,
  aiOutputHtml: null,
  stockAnalysisCache: {},
  stockChatHistory: {},
};

const strategyWeights = {
  balanced: { value: 0.2, income: 0.17, growth: 0.2, momentum: 0.2, quality: 0.23 },
  value: { value: 0.45, income: 0.08, growth: 0.07, momentum: 0.1, quality: 0.3 },
  income: { value: 0.12, income: 0.4, growth: 0.06, momentum: 0.08, quality: 0.34 },
  growth: { value: 0.06, income: 0.04, growth: 0.4, momentum: 0.24, quality: 0.26 },
  quality: { value: 0.08, income: 0.08, growth: 0.15, momentum: 0.12, quality: 0.57 },
};

const els = {
  loadSampleBtn: document.getElementById("loadSampleBtn"),
  fileInput: document.getElementById("fileInput"),
  searchInput: document.getElementById("searchInput"),
  sectorFilter: document.getElementById("sectorFilter"),
  strategySelect: document.getElementById("strategySelect"),
  sortSelect: document.getElementById("sortSelect"),
  statsGrid: document.getElementById("statsGrid"),
  tableContainer: document.getElementById("tableContainer"),
  resultCount: document.getElementById("resultCount"),
  detailTitle: document.getElementById("detailTitle"),
  detailContent: document.getElementById("detailContent"),
  refreshLiveBtn: document.getElementById("refreshLiveBtn"),
  sourceStatus: document.getElementById("sourceStatus"),
  downloadXlsxBtn: document.getElementById("downloadXlsxBtn"),
};

function clamp(value, min = 0, max = 100) {
  return Math.max(min, Math.min(max, value));
}

function scoreValue(stock) {
  const peComponent = stock.peRatio > 0 ? 55 - stock.peRatio * 2.4 : 25;
  const pbComponent = stock.pbRatio > 0 ? 24 - stock.pbRatio * 12 : 12;
  return clamp(peComponent + pbComponent);
}

function scoreIncome(stock) {
  return clamp(stock.dividendYield * 10 + stock.currentRatio * 6 - stock.debtToEquity * 4);
}

function scoreGrowth(stock) {
  return clamp(stock.earningsGrowth * 4.2 + stock.roe * 2);
}

function scoreMomentum(stock) {
  const annualMomentum = stock.priceChange1Y * 1.5;
  const dailyMomentum = stock.dailyChange * 2.4;
  const rsiBias = stock.rsi14 > 0 ? (stock.rsi14 - 50) * 0.8 : 0;
  const demandBoost = stock.demandScore * 0.4;
  return clamp(annualMomentum + dailyMomentum + rsiBias + demandBoost);
}

function scoreQuality(stock) {
  const bollingerBalance = stock.bollingerPercentB > 0 && stock.bollingerPercentB < 85 ? 10 : 0;
  return clamp(stock.roe * 4 + stock.currentRatio * 18 - stock.debtToEquity * 10 + bollingerBalance);
}

function enrichStocks(stocks) {
  const weights = strategyWeights[els.strategySelect.value];
  return stocks.map((stock) => {
    const scores = {
      value: Math.round(scoreValue(stock)),
      income: Math.round(scoreIncome(stock)),
      growth: Math.round(scoreGrowth(stock)),
      momentum: Math.round(scoreMomentum(stock)),
      quality: Math.round(scoreQuality(stock)),
    };
    const overallScore = Math.round(
      scores.value * weights.value +
      scores.income * weights.income +
      scores.growth * weights.growth +
      scores.momentum * weights.momentum +
      scores.quality * weights.quality
    );
    return { ...stock, scores, overallScore };
  });
}

function parseNumber(value) {
  const num = Number.parseFloat(String(value).trim());
  return Number.isFinite(num) ? num : 0;
}

function parseCsv(text) {
  const rows = text.trim().split(/\r?\n/);
  if (rows.length < 2) return [];
  const headers = rows[0].split(",").map((header) => header.trim());

  return rows.slice(1).map((row) => {
    const cells = row.split(",").map((cell) => cell.trim());
    const item = {};
    headers.forEach((header, index) => {
      item[header] = cells[index] ?? "";
    });
    return {
      ticker: item.ticker || "",
      company: item.company || "",
      sector: item.sector || "Unknown",
      price: parseNumber(item.price),
      marketCapM: parseNumber(item.marketCapM),
      peRatio: parseNumber(item.peRatio),
      pbRatio: parseNumber(item.pbRatio),
      dividendYield: parseNumber(item.dividendYield),
      earningsGrowth: parseNumber(item.earningsGrowth),
      priceChange1Y: parseNumber(item.priceChange1Y),
      debtToEquity: parseNumber(item.debtToEquity),
      roe: parseNumber(item.roe),
      currentRatio: parseNumber(item.currentRatio),
      bidVolume: parseNumber(item.bidVolume),
      askVolume: parseNumber(item.askVolume),
      buyPressure: parseNumber(item.buyPressure),
      demandScore: parseNumber(item.demandScore),
      rsi14: parseNumber(item.rsi14),
      bollingerUpper: parseNumber(item.bollingerUpper),
      bollingerMiddle: parseNumber(item.bollingerMiddle),
      bollingerLower: parseNumber(item.bollingerLower),
      volumeVsAvg20: parseNumber(item.volumeVsAvg20),
      dailyChange: parseNumber(item.dailyChange),
      bollingerPercentB: parseNumber(item.bollingerPercentB),
    };
  }).filter((item) => item.ticker && item.company);
}

function formatNumber(value, digits = 1) {
  return Number(value || 0).toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function formatPercent(value) {
  return `${formatNumber(value)}%`;
}

function formatCurrency(value) {
  return `${formatNumber(value, 3)} OMR`;
}

function formatDateTime(value) {
  if (!value) return "";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

function renderSourceStatus() {
  const modeLabel = state.dataSource === "live" ? "Live MSX fetch" : state.dataSource === "loading" ? "Loading" : "Sample fallback";
  const fetchedLine = state.fetchedAt ? `Last refresh: ${formatDateTime(state.fetchedAt)}` : "";
  const technicalLine = state.technicalTotal ? ` Indicators ready: ${state.technicalReady}/${state.technicalTotal}.` : "";
  const warningLine = state.warning ? ` ${state.warning}` : "";
  els.sourceStatus.textContent = `${modeLabel}. ${fetchedLine}${technicalLine}${warningLine}`.trim();
}

function renderStats(stocks) {
  if (!stocks.length) {
    els.statsGrid.innerHTML = "";
    return;
  }

  const avgYield = stocks.reduce((sum, item) => sum + item.dividendYield, 0) / stocks.length;
  const avgPE = stocks.reduce((sum, item) => sum + item.peRatio, 0) / stocks.length;
  const topPerformer = [...stocks].sort((a, b) => b.overallScore - a.overallScore)[0];
  const strongestDemand = [...stocks].sort((a, b) => b.demandScore - a.demandScore)[0];
  const strongestRsi = [...stocks].filter((item) => item.rsi14 > 0).sort((a, b) => b.rsi14 - a.rsi14)[0] || stocks[0];

  const stats = [
    {
      label: "Coverage",
      value: `${stocks.length} stocks`,
      meta: `${new Set(stocks.map((item) => item.sector)).size} sectors included`,
    },
    {
      label: "Average Dividend Yield",
      value: formatPercent(avgYield),
      meta: "Useful for income-focused screening",
    },
    {
      label: "Average P/E Ratio",
      value: formatNumber(avgPE),
      meta: "Quick valuation temperature check",
    },
    {
      label: "Top Ranked",
      value: topPerformer.ticker,
      meta: `${topPerformer.company} scored ${topPerformer.overallScore}/100`,
    },
    {
      label: "Strongest Demand",
      value: strongestDemand.ticker,
      meta: `Demand ${formatNumber(strongestDemand.demandScore)} | Buy pressure ${formatPercent(strongestDemand.buyPressure)}`,
    },
    {
      label: "Highest RSI",
      value: strongestRsi.ticker,
      meta: `RSI ${formatNumber(strongestRsi.rsi14)} on current daily chart`,
    },
  ];

  els.statsGrid.innerHTML = stats.map((stat) => `
    <article class="stat-card">
      <p class="stat-label">${stat.label}</p>
      <p class="stat-value">${stat.value}</p>
      <p class="stat-meta">${stat.meta}</p>
    </article>
  `).join("");
}

function scorePill(score) {
  if (score >= 75) return '<span class="pill good">Strong</span>';
  if (score >= 55) return '<span class="pill warn">Watch</span>';
  return '<span class="pill bad">Weak</span>';
}

function demandPill(stock) {
  if (stock.demandScore >= 70) return '<span class="pill good">Buy Flow</span>';
  if (stock.demandScore >= 50) return '<span class="pill warn">Mixed</span>';
  return '<span class="pill bad">Light</span>';
}

function renderTable(stocks) {
  els.resultCount.textContent = `${stocks.length} result${stocks.length === 1 ? "" : "s"}`;

  if (!stocks.length) {
    els.tableContainer.innerHTML = '<p class="empty-state">No stocks match the current filters.</p>';
    return;
  }

  els.tableContainer.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Company</th>
          <th>Sector</th>
          <th>Price</th>
          <th><span class="th-wrap">1D <span class="tip-icon" tabindex="0">?</span><span class="tooltip">Today's price change as a percentage vs yesterday's close. Comes from the live MSX feed — shows 0% on demo data.</span></span></th>
          <th><span class="th-wrap">1Y <span class="tip-icon" tabindex="0">?</span><span class="tooltip">Price change over the past 12 months. Positive means the stock has grown in price. A quick momentum sanity check.</span></span></th>
          <th><span class="th-wrap">Demand <span class="tip-icon" tabindex="0">?</span><span class="tooltip">Score (0–100) from live bid vs ask volume. High = more buyers than sellers right now. Shows as Buy Flow (≥70), Mixed (≥50), or Light.</span></span></th>
          <th><span class="th-wrap">RSI <span class="tip-icon" tabindex="0">?</span><span class="tooltip">Relative Strength Index over 14 days. Above 70 = overbought (stretched). Below 30 = oversold (potentially cheap). 40–65 is a healthy range.</span></span></th>
          <th><span class="th-wrap">Yield <span class="tip-icon" tabindex="0">?</span><span class="tooltip">Annual dividend as a % of the current price. A 6% yield means 6 OMR back per 100 OMR invested each year. Higher is better for income seekers.</span></span></th>
          <th><span class="th-wrap">P/E <span class="tip-icon" tabindex="0">?</span><span class="tooltip">Price-to-Earnings ratio. How much you pay for each OMR of profit. Lower generally means cheaper. MSX stocks below 10 are considered good value.</span></span></th>
          <th><span class="th-wrap">Score <span class="tip-icon" tabindex="0">?</span><span class="tooltip">Composite score out of 100 blending Value, Income, Growth, Momentum, and Quality. The blend ratio shifts with your chosen Strategy.</span></span></th>
          <th><span class="th-wrap">Signal <span class="tip-icon" tabindex="0">?</span><span class="tooltip">Two pills: demand flow (Buy Flow / Mixed / Light) and overall score (Strong ≥75 / Watch ≥55 / Weak below 55). Best combo: Buy Flow + Strong.</span></span></th>
        </tr>
      </thead>
      <tbody>
        ${stocks.map((stock) => `
          <tr data-ticker="${stock.ticker}" class="${stock.ticker === state.selectedTicker ? "active" : ""}">
            <td class="ticker-cell">
              <strong>${stock.ticker}</strong>
              <span>${stock.company}</span>
            </td>
            <td>${stock.sector}</td>
            <td>${formatCurrency(stock.price)}</td>
            <td>${formatPercent(stock.dailyChange)}</td>
            <td>${formatPercent(stock.priceChange1Y)}</td>
            <td>${formatNumber(stock.demandScore)}</td>
            <td>${formatNumber(stock.rsi14)}</td>
            <td>${formatPercent(stock.dividendYield)}</td>
            <td>${formatNumber(stock.peRatio)}</td>
            <td>${stock.overallScore}</td>
            <td>${demandPill(stock)} ${scorePill(stock.overallScore)}</td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;

  els.tableContainer.querySelectorAll("tbody tr").forEach((row) => {
    row.addEventListener("click", () => {
      state.selectedTicker = row.dataset.ticker;
      renderAll();
    });
  });
}

function buildHighlights(stock) {
  const strengths = [];
  const risks = [];

  if (stock.dividendYield >= 6) strengths.push("Above-average dividend yield for income screening.");
  if (stock.peRatio > 0 && stock.peRatio <= 8.5) strengths.push("Low earnings multiple suggests attractive valuation.");
  if (stock.earningsGrowth >= 12) strengths.push("Healthy earnings growth supports upside potential.");
  if (stock.roe >= 12) strengths.push("Return on equity indicates efficient capital use.");
  if (stock.demandScore >= 65) strengths.push("Buy-side interest is strong based on live bid volume and recent volume behavior.");
  if (stock.rsi14 >= 55 && stock.rsi14 <= 70) strengths.push("RSI is constructive without being deeply overbought.");

  if (stock.debtToEquity > 1.5) risks.push("Leverage is elevated and could pressure resilience.");
  if (stock.currentRatio > 0 && stock.currentRatio < 1) risks.push("Liquidity is thin relative to near-term obligations.");
  if (stock.priceChange1Y < 5) risks.push("Longer-term momentum is soft compared with stronger peers.");
  if (stock.pbRatio > 1.2) risks.push("Price-to-book is on the richer side for this screen.");
  if (stock.rsi14 >= 75) risks.push("RSI is stretched and could indicate an overbought setup.");
  if (stock.buyPressure < 40 && stock.askVolume > 0) risks.push("Ask-side queue currently outweighs bid-side interest.");

  return {
    strengths: strengths.length ? strengths : ["Metrics look balanced, but there is no standout edge in the current model."],
    risks: risks.length ? risks : ["No major red flags from the selected screening metrics."],
  };
}

function buildPolyline(points, width, height, minY, maxY, accessor) {
  if (!points.length || maxY === minY) return "";
  return points.map((point, index) => {
    const x = (index / Math.max(points.length - 1, 1)) * width;
    const yValue = accessor(point);
    const y = height - ((yValue - minY) / (maxY - minY)) * height;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
}

function renderPriceChart(chartData) {
  const points = chartData.series || [];
  if (!points.length) return '<p class="chart-empty">No chart history returned by MSX for this stock.</p>';
  // PAD_L small (no left labels), PAD_R large (right labels), PAD_T top padding, PAD_B for x-axis
  const width = 720, height = 280, PAD_L = 6, PAD_R = 58, PAD_B = 28, PAD_T = 14;
  const chartW = width - PAD_L - PAD_R, chartH = height - PAD_B - PAD_T;
  const values = points.flatMap(p => [p.close, p.bollingerUpper ?? p.close, p.bollingerLower ?? p.close]).filter(Number.isFinite);
  const minY = Math.min(...values), maxY = Math.max(...values), range = maxY - minY || 1;
  function toX(i) { return PAD_L + (i / Math.max(points.length - 1, 1)) * chartW; }
  function toY(v) { return PAD_T + chartH - ((v - minY) / range) * chartH; }
  function buildLine(pts, acc) { return pts.map((p, i) => `${toX(i).toFixed(1)},${toY(acc(p)).toFixed(1)}`).join(" "); }
  const bandPts = points.filter(p => p.bollingerUpper !== null && p.bollingerLower !== null);
  const closeLine = buildLine(points, p => p.close);
  const upperLine = buildLine(bandPts, p => p.bollingerUpper);
  const midLine   = buildLine(bandPts, p => p.bollingerMiddle);
  const lowerLine = buildLine(bandPts, p => p.bollingerLower);
  // Y-axis: 6 ticks on right side, clamped so top label never clips
  const yLabels = Array.from({length: 6}, (_, i) => {
    const val = minY + (i / 5) * range;
    const y = toY(val);
    const labelY = Math.max(PAD_T + 6, Math.min(y, height - PAD_B - 6));
    const isBold = i === 5;
    return `<line x1="${PAD_L}" y1="${y.toFixed(1)}" x2="${(width - PAD_R).toFixed(1)}" y2="${y.toFixed(1)}" stroke="rgba(68,56,36,0.08)" stroke-width="1"/>
            <text x="${(width - PAD_R + 5).toFixed(1)}" y="${labelY.toFixed(1)}" text-anchor="start" dominant-baseline="middle" font-size="10" fill="${isBold ? "#241d12" : "#6f6251"}" font-weight="${isBold ? "700" : "400"}">${val.toFixed(3)}</text>`;
  }).join("");
  const step = Math.max(1, Math.floor(points.length / 5));
  const xLabels = points.map((p, i) => {
    if (i % step !== 0 && i !== points.length - 1) return "";
    return `<text x="${toX(i).toFixed(1)}" y="${(chartH + 18).toFixed(1)}" text-anchor="middle" font-size="10" fill="#6f6251">${(p.date||"").slice(5)}</text>`;
  }).join("");
  // Last price dot only — no green box label
  const last = points[points.length - 1];
  const dotX = toX(points.length - 1), dotY = toY(last.close);
  const lastDot = `<circle cx="${dotX.toFixed(1)}" cy="${dotY.toFixed(1)}" r="4" fill="#0b6e4f"/>`;
  return `<div class="chart-wrap">
    <div class="chart-legend">
      <span><i class="chart-dot" style="background:#0b6e4f"></i>Close</span>
      <span><i class="chart-dot" style="background:#d7a84a"></i>Bollinger upper</span>
      <span><i class="chart-dot" style="background:#8f7b55"></i>Bollinger mid</span>
      <span><i class="chart-dot" style="background:#b85c38"></i>Bollinger lower</span>
    </div>
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="xMidYMid meet" aria-label="Price chart with Bollinger bands">
      ${yLabels}${xLabels}
      <polyline points="${upperLine}" fill="none" stroke="#d7a84a" stroke-width="2"/>
      <polyline points="${midLine}" fill="none" stroke="#8f7b55" stroke-width="1.5" stroke-dasharray="5 4"/>
      <polyline points="${lowerLine}" fill="none" stroke="#b85c38" stroke-width="2"/>
      <polyline points="${closeLine}" fill="none" stroke="#0b6e4f" stroke-width="2.5"/>
      ${lastDot}
    </svg>
    <p class="chart-caption">Recent daily closes with 20-day Bollinger bands. <span style="color:var(--accent);cursor:pointer;font-weight:600;" data-expand="price">Click to expand ↗</span></p>
  </div>`;
}

function renderRsiChart(chartData) {
  const points = (chartData.series || []).filter((point) => point.rsi14 !== null);
  if (!points.length) {
    return '<p class="chart-empty">RSI could not be calculated from the available history yet.</p>';
  }

  const width = 720;
  const height = 160;
  const line = buildPolyline(points, width, height, 0, 100, (point) => point.rsi14);
  const y70 = height - ((70 - 0) / (100 - 0)) * height;
  const y30 = height - ((30 - 0) / (100 - 0)) * height;

  return `
    <div class="chart-wrap">
      <div class="chart-legend">
        <span><i class="chart-dot" style="background:#241d12"></i>RSI 14</span>
        <span><i class="chart-dot" style="background:#b13c31"></i>70 overbought</span>
        <span><i class="chart-dot" style="background:#0b6e4f"></i>30 oversold</span>
      </div>
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="RSI chart">
        <line x1="0" y1="${y70}" x2="${width}" y2="${y70}" stroke="#b13c31" stroke-dasharray="5 4" />
        <line x1="0" y1="${y30}" x2="${width}" y2="${y30}" stroke="#0b6e4f" stroke-dasharray="5 4" />
        <polyline points="${line}" fill="none" stroke="#241d12" stroke-width="2.5" />
      </svg>
      <p class="chart-caption">RSI uses a 14-period calculation on the daily MSX closing series. <span style="color:var(--accent);cursor:pointer;font-weight:600;" data-expand="rsi">Click to expand ↗</span></p>
    </div>
  `;
}

async function ensureChartData(ticker) {
  if (!ticker) return null;
  if (state.chartCache[ticker]) return state.chartCache[ticker];
  const response = await fetch(`/api/msx/stocks/${ticker}/chart`);
  if (!response.ok) {
    throw new Error(`Chart request failed with ${response.status}`);
  }
  const payload = await response.json();
  state.chartCache[ticker] = payload;
  return payload;
}

async function renderDetail(stock) {
  if (!stock) {
    els.detailTitle.textContent = "Pick a stock";
    els.detailContent.innerHTML = `
      <p class="empty-state">
        Choose a company from the table to see score breakdown, strengths, risks, and quick interpretation.
      </p>
    `;
    return;
  }

  // If this ticker is already rendered in the panel, just restore AI output
  // without rebuilding the whole panel (prevents the 15s timer wiping analysis)
  const currentTicker = els.detailContent.dataset.ticker;
  if (currentTicker === stock.ticker) {
    const detailAiOutput = document.getElementById("detailAiOutput");
    if (detailAiOutput && state.stockAnalysisCache[stock.ticker]) {
      if (!detailAiOutput.querySelector(".ai-pick-card")) {
        detailAiOutput.innerHTML = state.stockAnalysisCache[stock.ticker];
      }
    }
    return;
  }

  const highlights = buildHighlights(stock);
  els.detailTitle.textContent = `${stock.company} (${stock.ticker})`;
  els.detailContent.dataset.ticker = stock.ticker;

  els.detailContent.innerHTML = `
    <div class="score-hero">
      <p class="section-kicker">Overall Score</p>
      <h3>${stock.overallScore}</h3>
      <p class="muted-text">${stock.sector} | ${formatCurrency(stock.price)} | Daily ${formatPercent(stock.dailyChange)} | One year ${formatPercent(stock.priceChange1Y)}</p>
    </div>

    <div class="score-grid">
      ${Object.entries(stock.scores).map(([label, value]) => `
        <div class="score-row">
          <span>${label[0].toUpperCase()}${label.slice(1)}</span>
          <div class="bar-track">
            <div class="bar-fill" style="width:${value}%"></div>
          </div>
          <strong>${value}</strong>
        </div>
      `).join("")}
    </div>

    <div class="metric-grid">
      <div class="metric-card"><p class="metric-label">Dividend Yield</p><p class="metric-value">${formatPercent(stock.dividendYield)}</p></div>
      <div class="metric-card"><p class="metric-label">P/E Ratio</p><p class="metric-value">${formatNumber(stock.peRatio)}</p></div>
      <div class="metric-card"><p class="metric-label">Earnings Growth</p><p class="metric-value">${formatPercent(stock.earningsGrowth)}</p></div>
      <div class="metric-card"><p class="metric-label">Return on Equity</p><p class="metric-value">${formatPercent(stock.roe)}</p></div>
      <div class="metric-card"><p class="metric-label">Bid Volume</p><p class="metric-value">${formatNumber(stock.bidVolume, 0)}</p></div>
      <div class="metric-card"><p class="metric-label">Ask Volume</p><p class="metric-value">${formatNumber(stock.askVolume, 0)}</p></div>
      <div class="metric-card"><p class="metric-label">Buy Pressure</p><p class="metric-value">${formatPercent(stock.buyPressure)}</p></div>
      <div class="metric-card"><p class="metric-label">Demand Score</p><p class="metric-value">${formatNumber(stock.demandScore)}</p></div>
      <div class="metric-card"><p class="metric-label">RSI 14</p><p class="metric-value">${formatNumber(stock.rsi14)}</p></div>
      <div class="metric-card"><p class="metric-label">Volume vs 20D Avg</p><p class="metric-value">${formatNumber(stock.volumeVsAvg20)}x</p></div>
      <div class="metric-card"><p class="metric-label">Bollinger Upper</p><p class="metric-value">${formatNumber(stock.bollingerUpper, 3)}</p></div>
      <div class="metric-card"><p class="metric-label">Bollinger Lower</p><p class="metric-value">${formatNumber(stock.bollingerLower, 3)}</p></div>
    </div>

    <div id="chartPanel" class="chart-panel">
      <p class="chart-empty">Loading chart analytics from MSX...</p>
    </div>

    <div class="highlights">
      ${highlights.strengths.map((item) => `<div class="highlight-item">${item}</div>`).join("")}
      ${highlights.risks.map((item) => `<div class="highlight-item risk">${item}</div>`).join("")}
    </div>

    <div class="detail-ai-section">
      <div class="detail-ai-header">
        <div>
          <p class="section-kicker" style="margin:0 0 4px;">AI Trader</p>
          <h3 style="margin:0;font-size:1rem;">Trade analysis</h3>
        </div>
        <button class="detail-ai-btn" id="detailAiBtn">
          <span class="ai-btn-icon">◈</span> Analyse this stock
        </button>
      </div>
      <div id="detailAiOutput" class="detail-ai-output">
        <p class="ai-placeholder">Click <strong>Analyse this stock</strong> to get entry, target, stop levels and a full trade breakdown.</p>
      </div>
    </div>

    <div class="stock-chat-section">
      <div class="stock-chat-header">
        <p class="section-kicker" style="margin:0 0 2px;">AI Chat</p>
        <h3 style="margin:0;font-size:1rem;">Ask about ${stock.ticker}</h3>
      </div>
      <div id="stockChatMessages" class="stock-chat-messages"></div>
      <div class="stock-chat-input-row">
        <input id="stockChatInput" type="text" class="stock-chat-input" placeholder="e.g. Is now a good time to buy? What's the downside risk?" autocomplete="off" />
        <button id="stockChatSend" class="stock-chat-send-btn">
          <span id="stockChatSendIcon">↑</span>
        </button>
      </div>
      <div class="stock-chat-suggestions">
        <button class="chat-suggestion" data-q="Is now a good time to buy this stock?">Good time to buy?</button>
        <button class="chat-suggestion" data-q="What is the downside risk if I buy now?">Downside risk?</button>
        <button class="chat-suggestion" data-q="Compare this stock's valuation to typical MSX stocks.">Valuation check</button>
        <button class="chat-suggestion" data-q="What technical signals suggest this stock could move up soon?">Bullish signals?</button>
        <button class="chat-suggestion" data-q="What would make you change your view and sell this stock?">Exit triggers?</button>
      </div>
    </div>
  `;

  const chartPanel = document.getElementById("chartPanel");
  try {
    const chartData = await ensureChartData(stock.ticker);
    const priceHtml = renderPriceChart(chartData);
    const rsiHtml = renderRsiChart(chartData);
    chartPanel.innerHTML = priceHtml + rsiHtml;
    chartPanel.querySelectorAll("[data-expand]").forEach(btn => {
      btn.addEventListener("click", () => {
        openChartModal(btn.dataset.expand === "price" ? priceHtml : rsiHtml);
      });
    });
  } catch (error) {
    chartPanel.innerHTML = `<p class="chart-empty">Unable to load chart analytics right now. ${error.message}</p>`;
  }

  // Wire up per-stock AI button
  const detailAiBtn = document.getElementById("detailAiBtn");
  const detailAiOutput = document.getElementById("detailAiOutput");
  if (detailAiBtn && detailAiOutput) {
    // Restore cached result if already analysed
    if (state.stockAnalysisCache[stock.ticker]) {
      detailAiOutput.innerHTML = state.stockAnalysisCache[stock.ticker];
    }
    detailAiBtn.addEventListener("click", async () => {
      detailAiBtn.disabled = true;
      detailAiBtn.classList.add("loading");
      detailAiBtn.querySelector(".ai-btn-icon").textContent = "◌";
      detailAiOutput.innerHTML = `<div class="ai-thinking"><div class="ai-thinking-dots"><span></span><span></span><span></span></div>Analysing ${stock.ticker}…</div>`;
      setTimeout(async () => {
        try {
          const analysis = scoreStock(stock);
          const pick = {
            ticker: stock.ticker,
            company: stock.company,
            conviction: analysis.score > 60 ? "high" : "medium",
            entry_thesis: analysis.entry_thesis,
            rationale: analysis.rationale,
            signals: analysis.signals,
            risk: analysis.risk,
            _entry: analysis._entry,
            _target: analysis._target,
            _stop: analysis._stop,
            _support: analysis._support,
          };
          const html = renderAiPick(pick, 0);
          detailAiOutput.innerHTML = html;

          // ── Groq: add a concise AI trade note ────────────────────────
          if (window.AI.hasKey()) {
            const a = scoreStock(stock);
            const ctx = `MSX analyst. 2-3 sentence trade note for ${stock.ticker} (${stock.company}). Price:${stock.price.toFixed(3)} OMR, 1Y:${stock.priceChange1Y.toFixed(1)}%, Daily:${stock.dailyChange.toFixed(2)}%, Vol:${stock.volumeVsAvg20.toFixed(1)}x, Demand:${stock.demandScore}, RSI:${stock.rsi14.toFixed(0)}, PE:${stock.peRatio.toFixed(1)}, Yield:${stock.dividendYield.toFixed(1)}%, Entry:${a._entry.toFixed(3)}, Target:${a._target.toFixed(3)}, Stop:${a._stop.toFixed(3)}. Be direct, cite numbers, give a clear buy/hold/avoid verdict.`;
            try {
              const note = await window.AI.call(ctx, 200);
              const noteDiv = document.createElement("div");
              noteDiv.className = "ai-market-read";
              noteDiv.style.marginTop = "12px";
              noteDiv.innerHTML = `<span class="ai-market-read-label">◈ AI Trade Note</span><p>${note.split("\n").join("<br>")}</p>`;
              detailAiOutput.appendChild(noteDiv);
            } catch {}
          }

          state.stockAnalysisCache[stock.ticker] = detailAiOutput.innerHTML;
        } catch (err) {
          detailAiOutput.innerHTML = `<p class="ai-error">Analysis error: ${err.message}</p>`;
        } finally {
          detailAiBtn.disabled = false;
          detailAiBtn.classList.remove("loading");
          detailAiBtn.querySelector(".ai-btn-icon").textContent = "◈";
        }
      }, 60);
    });
  }

  // ── Stock chat wiring ────────────────────────────────────────────────────
  wireStockChat(stock);
}


// ── Stock chat ─────────────────────────────────────────────────────────────

function buildStockContext(stock) {
  const a = scoreStock(stock);
  return `You are a senior professional stock trader and technical analyst specialising in the Muscat Stock Exchange (MSX). You are having a direct conversation with an investor about a specific stock. Be direct, specific, and professional — like a trusted trading desk analyst, not a generic chatbot. Use plain language, cite actual numbers from the data, and give actionable opinions. Do not hedge everything with disclaimers — give a real view while being honest about uncertainty.

STOCK UNDER DISCUSSION:
Ticker: ${stock.ticker}
Company: ${stock.company}
Sector: ${stock.sector}
Current price: ${stock.price.toFixed(3)} OMR
1Y price change: ${stock.priceChange1Y > 0 ? "+" : ""}${stock.priceChange1Y.toFixed(1)}%
Daily change: ${stock.dailyChange > 0 ? "+" : ""}${stock.dailyChange.toFixed(2)}%
P/E ratio: ${stock.peRatio > 0 ? stock.peRatio.toFixed(1) : "unavailable"}
P/B ratio: ${stock.pbRatio > 0 ? stock.pbRatio.toFixed(2) : "unavailable"}
Dividend yield: ${stock.dividendYield > 0 ? stock.dividendYield.toFixed(1) + "%" : "unavailable"}
Earnings growth: ${stock.earningsGrowth > 0 ? "+" : ""}${stock.earningsGrowth.toFixed(1)}%
Return on equity: ${stock.roe.toFixed(1)}%
Debt/equity: ${stock.debtToEquity.toFixed(2)}x
Current ratio: ${stock.currentRatio.toFixed(2)}
RSI 14: ${stock.rsi14 > 0 ? stock.rsi14.toFixed(1) : "unavailable (live data not loaded)"}
Bollinger %B: ${stock.bollingerPercentB > 0 ? stock.bollingerPercentB.toFixed(1) : "unavailable"}
Demand score: ${stock.demandScore > 0 ? stock.demandScore.toFixed(0) + "/100" : "unavailable"}
Buy pressure: ${stock.buyPressure > 0 ? stock.buyPressure.toFixed(0) + "%" : "unavailable"}
Volume vs 20D avg: ${stock.volumeVsAvg20 > 0 ? stock.volumeVsAvg20.toFixed(2) + "x" : "unavailable"}
Overall score: ${stock.overallScore}/100
Rule-based conviction: ${a.score > 60 ? "HIGH" : a.score > 40 ? "MEDIUM" : "LOW"} (score: ${a.score})
Suggested entry: ${a._entry.toFixed(3)} OMR
Suggested target: ${a._target.toFixed(3)} OMR
Suggested stop: ${a._stop.toFixed(3)} OMR
Risk/reward ratio: ${a._entry > a._stop ? ((a._target - a._entry) / (a._entry - a._stop)).toFixed(1) + ":1" : "n/a"}

MSX CONTEXT:
- P/E below 10 is considered good value on MSX
- Dividend yield above 5% is income-supportive
- RSI below 35 = oversold opportunity, above 70 = overbought
- Bollinger %B below 20 = price near lower band (buy zone)
- Demand score above 65 = strong institutional buy flow

Answer the investor's question directly and concisely. If they ask for a recommendation, give one with reasoning. Keep responses under 200 words unless a longer answer is clearly needed. Format with short paragraphs — no bullet lists unless listing multiple distinct items.`;
}

function renderChatMessage(role, text) {
  const isUser = role === "user";
  const html = text.split("\n").join("<br>");
  return `<div class="chat-msg ${isUser ? "chat-msg-user" : "chat-msg-ai"}"><div class="chat-msg-bubble">${html}</div></div>`;
}

function wireStockChat(stock) {
  const messagesEl = document.getElementById("stockChatMessages");
  const inputEl    = document.getElementById("stockChatInput");
  const sendBtn    = document.getElementById("stockChatSend");
  const iconEl     = document.getElementById("stockChatSendIcon");
  const suggestions = document.querySelectorAll(".chat-suggestion");
  if (!messagesEl || !inputEl || !sendBtn) return;

  // Restore previous conversation for this ticker
  if (!state.stockChatHistory[stock.ticker]) {
    state.stockChatHistory[stock.ticker] = [];
  }
  const history = state.stockChatHistory[stock.ticker];

  function renderHistory() {
    messagesEl.innerHTML = history.map(m => renderChatMessage(m.role, m.content)).join("");
    messagesEl.scrollTop = messagesEl.scrollHeight;
  }
  renderHistory();

  async function sendMessage(userText) {
    if (!userText.trim()) return;
    inputEl.value = "";
    sendBtn.disabled = true;
    iconEl.textContent = "…";

    history.push({ role: "user", content: userText });
    renderHistory();

    // Add thinking indicator
    const thinkingId = "chat-thinking-" + Date.now();
    messagesEl.innerHTML += `<div id="${thinkingId}" class="chat-msg chat-msg-ai"><div class="chat-msg-bubble ai-thinking-dots-inline"><span></span><span></span><span></span></div></div>`;
    messagesEl.scrollTop = messagesEl.scrollHeight;

    try {
      const systemContext = buildStockContext(stock);
      const priorTurns = history.slice(0, -1).map(m => (m.role === "user" ? "Investor" : "Analyst") + ": " + m.content).join("\n");
      const fullPrompt = systemContext + "\n\n" + (priorTurns ? "Conversation so far:\n" + priorTurns + "\n\n" : "") + "Investor: " + userText;
      const raw = await window.AI.call(fullPrompt, 400);
      document.getElementById(thinkingId)?.remove();
      history.push({ role: "assistant", content: raw });
      renderHistory();
    } catch (err) {
      document.getElementById(thinkingId)?.remove();
      const errMsg = err.message.includes("No API key")
        ? "Enter your Groq key in the chat panel below — it will be shared across all AI features."
        : "Error: " + err.message;
      history.push({ role: "assistant", content: errMsg });
      renderHistory();
    } finally {
      sendBtn.disabled = false;
      iconEl.textContent = "↑";
      inputEl.focus();
    }
  }

  sendBtn.addEventListener("click", () => sendMessage(inputEl.value));
  inputEl.addEventListener("keydown", e => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); sendMessage(inputEl.value); } });
  suggestions.forEach(btn => {
    btn.addEventListener("click", () => sendMessage(btn.dataset.q));
  });
}

function renderSectorOptions(stocks) {
  const sectors = [...new Set(stocks.map((stock) => stock.sector))].sort();
  const current = els.sectorFilter.value;
  els.sectorFilter.innerHTML = `
    <option value="all">All sectors</option>
    ${sectors.map((sector) => `<option value="${sector}">${sector}</option>`).join("")}
  `;
  els.sectorFilter.value = sectors.includes(current) ? current : "all";
}

function getSortedFilteredStocks() {
  const query = els.searchInput.value.trim().toLowerCase();
  const sector = els.sectorFilter.value;
  const sortBy = els.sortSelect.value;

  const BOND_KEYWORDS = /bond|sukuk|t-bill|tbill|treasury|fixed.?income|govt.?sec|gdc/i;
  const stocks = enrichStocks(state.stocks).filter((stock) => {
    if (BOND_KEYWORDS.test(stock.company) || BOND_KEYWORDS.test(stock.sector) || BOND_KEYWORDS.test(stock.ticker)) return false;
    const matchesQuery = !query ||
      stock.ticker.toLowerCase().includes(query) ||
      stock.company.toLowerCase().includes(query);
    const matchesSector = sector === "all" || stock.sector === sector;
    return matchesQuery && matchesSector;
  });

  stocks.sort((a, b) => {
    if (sortBy === "peRatio") {
      if (a.peRatio === 0) return 1;
      if (b.peRatio === 0) return -1;
      return a.peRatio - b.peRatio;
    }
    return (b[sortBy] || 0) - (a[sortBy] || 0);
  });

  return stocks;
}

function renderAll() {
  renderSourceStatus();
  state.filteredStocks = getSortedFilteredStocks();
  renderStats(state.filteredStocks);
  renderTable(state.filteredStocks);

  const selected = state.filteredStocks.find((stock) => stock.ticker === state.selectedTicker)
    || state.filteredStocks[0];

  state.selectedTicker = selected?.ticker ?? null;
  renderDetail(selected);

  // Restore AI output if it was already run — do not wipe it on re-render
  if (state.aiOutputHtml) {
    const aiOut = document.getElementById("aiAnalysisOutput");
    if (aiOut && !aiOut.querySelector(".ai-pick-card")) {
      aiOut.innerHTML = state.aiOutputHtml;
    }
  }

  // Run breakout scan on every render
  runBreakoutScan();
}

function loadStocks(stocks, options = {}) {
  state.stocks = stocks;
  state.dataSource = options.dataSource || "sample";
  state.warning = options.warning || "";
  state.fetchedAt = options.fetchedAt || "";
  state.technicalReady = options.technicalReady || 0;
  state.technicalTotal = options.technicalTotal || 0;
  state.chartCache = {};
  state.aiOutputHtml = null;
  // stockAnalysisCache intentionally preserved across reloads
  if (state.refreshTimer) {
    clearTimeout(state.refreshTimer);
    state.refreshTimer = null;
  }
  renderSectorOptions(stocks);
  const tickerStillPresent = stocks.some((s) => s.ticker === state.selectedTicker);
  if (!tickerStillPresent) {
    state.selectedTicker = stocks[0]?.ticker ?? null;
  }
  renderAll();

  if (state.dataSource === "live" && state.technicalTotal > 0 && state.technicalReady < state.technicalTotal) {
    state.refreshTimer = setTimeout(() => {
      loadLiveStocks();
    }, 15000);
  }
}

async function loadLiveStocks() {
  state.dataSource = "loading";
  state.warning = "Refreshing from official MSX pages...";
  renderSourceStatus();

  try {
    const response = await fetch("/api/msx/stocks");
    if (!response.ok) {
      throw new Error(`Server returned ${response.status}`);
    }
    const payload = await response.json();
    loadStocks(payload.stocks || sampleStocks, {
      dataSource: payload.source || "sample",
      warning: payload.warning || "",
      fetchedAt: payload.fetchedAt || "",
      technicalReady: payload.technicalReady || 0,
      technicalTotal: payload.technicalTotal || 0,
    });
  } catch (error) {
    loadStocks(sampleStocks, {
      dataSource: "sample",
      warning: `Live fetch failed in the browser. ${error.message}`,
      fetchedAt: "",
      technicalReady: 0,
      technicalTotal: 0,
    });
  }
}

els.loadSampleBtn.addEventListener("click", () => {
  loadStocks(sampleStocks, {
    dataSource: "sample",
    warning: "Showing the local sample dataset.",
    fetchedAt: "",
    technicalReady: 0,
    technicalTotal: sampleStocks.length,
  });
});

els.refreshLiveBtn.addEventListener("click", () => {
  loadLiveStocks();
});

els.fileInput.addEventListener("change", async (event) => {
  const [file] = event.target.files;
  if (!file) return;
  const text = await file.text();
  const parsed = parseCsv(text);
  if (parsed.length) {
    loadStocks(parsed, {
      dataSource: "sample",
      warning: "Loaded a user CSV file.",
      fetchedAt: "",
      technicalReady: 0,
      technicalTotal: parsed.length,
    });
  } else {
    window.alert("No valid stock rows found. Check the CSV header format and try again.");
  }
});

[els.searchInput, els.sectorFilter, els.strategySelect, els.sortSelect].forEach((element) => {
  element.addEventListener("input", renderAll);
  element.addEventListener("change", renderAll);
});


// ══════════════════════════════════════════════════════════════════════════════
// GLOBAL AI CHAT AGENT
// ══════════════════════════════════════════════════════════════════════════════

(function() {
  const PROVIDER_CONFIG = {
    groq:    { label: "⚡ Groq",    keyRequired: true, keyLink: "https://console.groq.com/keys",       keyPlaceholder: "Paste Groq API key — free at console.groq.com (no credit card)" },
    openai:  { label: "⬡ ChatGPT", keyRequired: true, keyLink: "https://platform.openai.com/api-keys", keyPlaceholder: "Paste OpenAI API key" },
  };

  let currentProvider = "groq";
  let providerKeys = { groq: "", openai: "" };
  let globalChatHistory = [];

  const messagesEl   = document.getElementById("globalChatMessages");
  const inputEl      = document.getElementById("globalChatInput");
  const sendBtn      = document.getElementById("globalChatSend");
  const keyRow       = document.getElementById("providerKeyRow");
  const keyInput     = document.getElementById("providerApiKey");
  const keyLink      = document.getElementById("providerKeyLink");
  const providerBtns = document.querySelectorAll(".provider-btn");
  const suggestions  = document.querySelectorAll(".global-chat-suggestions .chat-suggestion");

  if (!messagesEl || !inputEl || !sendBtn) return;

  // ── Provider toggle ─────────────────────────────────────────────────────────
  function setProvider(p) {
    if (!PROVIDER_CONFIG[p]) p = "groq"; // fallback
    currentProvider = p;
    window.AI.provider = p;
    providerBtns.forEach(b => b.classList.toggle("active", b.dataset.provider === p));
    const cfg = PROVIDER_CONFIG[p];
    keyRow.style.display = "flex";
    keyInput.placeholder = cfg.keyPlaceholder;
    keyLink.href = cfg.keyLink;
    keyLink.textContent = "Get free key ↗";
    keyInput.value = providerKeys[p] || window.AI.getKey() || "";
  }

  providerBtns.forEach(btn => {
    btn.addEventListener("click", () => setProvider(btn.dataset.provider));
  });

  keyInput.addEventListener("input", () => {
    const key = keyInput.value.trim();
    providerKeys[currentProvider] = key;
    // Share key with global AI manager so all features use it
    window.AI.provider = currentProvider;
    if (currentProvider === "groq") window.AI.groqKey = key;
    else if (currentProvider === "openai") window.AI.openaiKey = key;
    window.AI._notifyKeyChange();
  });

  // ── Build full stock context for the global agent ───────────────────────────
  function buildGlobalContext(userText) {
    const stocks = state.stocks || [];  // Use ALL stocks, not just filtered
    if (!stocks.length) return "No stock data loaded. " + userText;

    // Check if user is asking about a specific ticker
    const tickerMention = userText.match(/([A-Z]{3,5})/g) || [];
    const mentionedTickers = new Set(tickerMention);

    // Detect accumulation / short-term query
    const isAccumQuery = /accum|تجميع|breakout|انطلاق|volume|حجم|demand|week|month|يوم|أسبوع|شهر|short.term|recent|اليوم|الأسبوع/i.test(userText);

    // Sort: if accumulation query, rank by short-term signals; else by overall score
    let sortedStocks;
    if (isAccumQuery) {
      sortedStocks = [...stocks].sort((a, b) => {
        const aS = (a.demandScore||0)*0.4 + (a.volumeVsAvg20||0)*15 + (a.buyPressure||0)*0.3 + Math.max(0, a.dailyChange||0)*3;
        const bS = (b.demandScore||0)*0.4 + (b.volumeVsAvg20||0)*15 + (b.buyPressure||0)*0.3 + Math.max(0, b.dailyChange||0)*3;
        return bS - aS;
      });
    } else {
      sortedStocks = [...stocks].sort((a, b) => (b.overallScore||0) - (a.overallScore||0));
    }

    const topIds = new Set(sortedStocks.slice(0, 12).map(s => s.ticker));
    const selected = stocks.filter(s => mentionedTickers.has(s.ticker) || topIds.has(s.ticker));
    const mentioned = stocks.filter(s => mentionedTickers.has(s.ticker) && !selected.find(x => x.ticker === s.ticker));
    const final = [...mentioned, ...selected].slice(0, 20);

    const table = final.map(s => {
      const a = scoreStock(s);
      const conv = a.score > 60 ? "H" : a.score > 40 ? "M" : "L";
      const price = s.price > 0 ? s.price.toFixed(3) : "n/a";
      return `${s.ticker}|${s.company}|${price}|Daily:${s.dailyChange > 0 ? "+" : ""}${s.dailyChange.toFixed(2)}%|Vol:${s.volumeVsAvg20 > 0 ? s.volumeVsAvg20.toFixed(1) + "x" : "-"}|Demand:${s.demandScore > 0 ? s.demandScore.toFixed(0) : "-"}|BuyPres:${s.buyPressure > 0 ? s.buyPressure.toFixed(0) + "%" : "-"}|RSI:${s.rsi14 > 0 ? s.rsi14.toFixed(0) : "-"}|1Y:${s.priceChange1Y.toFixed(1)}%|PE:${s.peRatio > 0 ? s.peRatio.toFixed(1) : "-"}|Yld:${s.dividendYield > 0 ? s.dividendYield.toFixed(1) + "%" : "-"}|Score:${s.overallScore}|${conv}|Entry:${a._entry.toFixed(3)}|Target:${a._target.toFixed(3)}|Stop:${a._stop.toFixed(3)}`;
    }).join("\n");

    const allTickers = stocks.map(s => s.ticker + "=" + s.company).join(", ");

    const accumGuide = isAccumQuery ? `
SHORT-TERM ACCUMULATION SIGNALS (days to weeks — NOT 1Y data):
- Volume > 1.3x avg WITH stable/rising price = smart money quietly accumulating
- Demand score > 60 = institutional buy orders dominating
- Buy pressure > 52% = more bids than asks in order book  
- Positive daily change + volume spike = momentum building NOW
- RSI 40-58 = healthy zone, not overbought, room to run
- DO NOT rank by 1Y returns for accumulation — use Daily, Vol, Demand, BuyPressure
` : "";

    return `You are a senior MSX (Muscat Stock Exchange) trading analyst. Direct, specific, cite exact numbers. Under 180 words. Respond in same language as user (Arabic or English).
${accumGuide}
ALL ${stocks.length} MSX STOCKS: ${allTickers}

TOP STOCKS FOR THIS QUERY (ticker|company|price|daily|volume|demand|buyPressure|RSI|1Y|PE|yield|score|conv|entry|target|stop):
${table}

PE<10=cheap, yield>5%=good income, RSI<35=oversold opportunity, RSI>70=overbought avoid, H/M/L=conviction.
If stock not in table: find it in the full list above and say you only have basic data for it.

Question: ${userText}`;
  }

  // ── Render message ──────────────────────────────────────────────────────────
  function appendMessage(role, text) {
    const div = document.createElement("div");
    div.className = "chat-msg " + (role === "user" ? "chat-msg-user" : "chat-msg-ai");
    div.innerHTML = `<div class="chat-msg-bubble">${text.split("\n").join("<br>")}</div>`;
    messagesEl.appendChild(div);
    messagesEl.scrollTop = messagesEl.scrollHeight;
  }

  function appendThinking() {
    const id = "thinking-" + Date.now();
    const div = document.createElement("div");
    div.id = id;
    div.className = "chat-msg chat-msg-ai";
    div.innerHTML = `<div class="chat-msg-bubble ai-thinking-dots-inline"><span></span><span></span><span></span></div>`;
    messagesEl.appendChild(div);
    messagesEl.scrollTop = messagesEl.scrollHeight;
    return id;
  }

  // ── Send message ────────────────────────────────────────────────────────────
  async function sendGlobalMessage(userText) {
    if (!userText.trim()) return;
    inputEl.value = "";
    inputEl.style.height = "auto";
    sendBtn.disabled = true;

    globalChatHistory.push({ role: "user", content: userText });
    appendMessage("user", userText);
    const thinkingId = appendThinking();

    // Detect ticker mentions and fetch live data for them
    const allStocks = state.stocks || [];
    const tickerMatches = userText.match(/([A-Z]{3,5})/g) || [];
    let extraStockData = "";

    for (const t of tickerMatches) {
      // Check if it looks like a ticker (all caps, 3-5 chars)
      const existsInData = allStocks.find(s => s.ticker === t);
      if (!existsInData || !existsInData.price) {
        // Try to fetch it directly from MSX
        try {
          const resp = await fetch("/api/msx/stock/" + t);
          const data = await resp.json();
          if (data.chart && data.chart.series && data.chart.series.length > 0) {
            const series = data.chart.series;
            const last = series[series.length - 1];
            const prev = series[series.length - 2] || last;
            const oldest = series[0];
            const dailyChg = prev.close > 0 ? ((last.close - prev.close) / prev.close * 100) : 0;
            const yearChg = oldest.close > 0 ? ((last.close - oldest.close) / oldest.close * 100) : 0;
            const vols = series.map(p => p.volume || 0);
            const avgVol = vols.slice(-20).reduce((a, b) => a + b, 0) / Math.min(20, vols.length) || 1;
            const volRatio = last.volume > 0 ? (last.volume / avgVol) : 0;
            const ind = data.chart.indicators || {};
            if (data.stock) {
              extraStockData += `\n[LIVE DATA FOR ${t}] `;
              extraStockData += `Price:${last.close.toFixed(3)} OMR, Daily:${dailyChg > 0 ? "+" : ""}${dailyChg.toFixed(2)}%, `;
              extraStockData += `1Y:${yearChg > 0 ? "+" : ""}${yearChg.toFixed(1)}%, Vol:${volRatio > 0 ? volRatio.toFixed(1) + "x avg" : "n/a"}, `;
              extraStockData += `RSI:${ind.rsi14 > 0 ? ind.rsi14.toFixed(1) : "n/a"}, BollingerB%:${ind.bollingerPercentB > 0 ? ind.bollingerPercentB.toFixed(1) : "n/a"}, `;
              extraStockData += `Company:${data.stock.company || t}, Sector:${data.stock.sector || "n/a"}`;
            } else {
              extraStockData += `\n[CHART DATA FOR ${t} — not in main listing] `;
              extraStockData += `Price:${last.close.toFixed(3)} OMR, Daily:${dailyChg > 0 ? "+" : ""}${dailyChg.toFixed(2)}%, 1Y:${yearChg > 0 ? "+" : ""}${yearChg.toFixed(1)}%`;
              extraStockData += `, Volume:${volRatio > 0 ? volRatio.toFixed(1) + "x avg" : "n/a"}`;
              extraStockData += `, RSI:${ind.rsi14 > 0 ? ind.rsi14.toFixed(1) : "n/a"}`;
            }
          } else if (!existsInData) {
            extraStockData += `\n[${t}] Not found in MSX database or chart data unavailable.`;
          }
        } catch (e) {
          // silently skip
        }
      }
    }

    // Build compact prompt — only last 2 exchanges of history to save tokens
    const recentHistory = globalChatHistory.slice(-5, -1);
    const history = recentHistory
      .map(m => (m.role === "user" ? "Q" : "A") + ": " + m.content.slice(0, 200))
      .join("\n");
    const context = buildGlobalContext(userText);
    const fullPrompt = context
      + (extraStockData ? "\n\nADDITIONAL LIVE DATA FETCHED:" + extraStockData : "")
      + (history ? "\nPrior: " + history : "");

    try {
      let endpoint, body;

      // Sync key from input to AI manager before calling
      const inputKey = keyInput.value.trim();
      if (inputKey) {
        providerKeys[currentProvider] = inputKey;
        if (currentProvider === "groq") window.AI.groqKey = inputKey;
        else window.AI.openaiKey = inputKey;
      }
      if (!window.AI.hasKey()) throw new Error("Please paste your API key above to start chatting.");
      const raw = await window.AI.call(fullPrompt, 512);

      document.getElementById(thinkingId)?.remove();
      globalChatHistory.push({ role: "assistant", content: raw });
      appendMessage("assistant", raw);

    } catch (err) {
      document.getElementById(thinkingId)?.remove();
      globalChatHistory.push({ role: "assistant", content: "⚠ " + err.message });
      appendMessage("assistant", "⚠ " + err.message);
    } finally {
      sendBtn.disabled = false;
      inputEl.focus();
    }
  }

  // ── Auto-resize textarea ────────────────────────────────────────────────────
  inputEl.addEventListener("input", () => {
    inputEl.style.height = "auto";
    inputEl.style.height = Math.min(inputEl.scrollHeight, 120) + "px";
  });

  inputEl.addEventListener("keydown", e => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendGlobalMessage(inputEl.value);
    }
  });

  sendBtn.addEventListener("click", () => sendGlobalMessage(inputEl.value));

  suggestions.forEach(btn => {
    btn.addEventListener("click", () => sendGlobalMessage(btn.dataset.q));
  });

  // ── Ollama status check on load ───────────────────────────────────────────
  let ollamaModel = "llama3.2";

  async function checkOllamaStatus() {
    const ollamaBtn = document.querySelector('.provider-btn[data-provider="ollama"]');
    try {
      const resp = await fetch("/api/ai/ollama/status");
      const data = await resp.json();
      if (data.running && data.models.length > 0) {
        ollamaModel = data.models[0];
        // Build model selector if multiple models
        if (data.models.length > 1) {
          const sel = document.createElement("select");
          sel.className = "ollama-model-select";
          sel.title = "Select Ollama model";
          data.models.forEach(m => {
            const opt = document.createElement("option");
            opt.value = m; opt.textContent = m;
            sel.appendChild(opt);
          });
          sel.addEventListener("change", () => { ollamaModel = sel.value; });
          const toggle = document.querySelector(".ai-provider-toggle");
          if (toggle && !toggle.querySelector(".ollama-model-select")) {
            toggle.appendChild(sel);
          }
        }
        if (ollamaBtn) {
          ollamaBtn.textContent = "🖥 Ollama ✓";
          ollamaBtn.title = "Model: " + ollamaModel;
        }
      } else if (!data.running) {
        if (ollamaBtn) {
          ollamaBtn.textContent = "🖥 Ollama (offline)";
          ollamaBtn.title = "Ollama is not running. Run: ollama serve";
          ollamaBtn.style.opacity = "0.6";
        }
      }
    } catch {
      if (ollamaBtn) {
        ollamaBtn.textContent = "🖥 Ollama (offline)";
        ollamaBtn.style.opacity = "0.6";
      }
    }
  }

  // Override sendGlobalMessage to pass the selected model
  const _origSend = sendGlobalMessage;

  // Patch the Ollama body to include the selected model
  const _origFetch = window.fetch;

  // Instead patch the body construction for ollama inside sendGlobalMessage
  // by storing model on window so the send function picks it up
  window._ollamaModel = ollamaModel;

  // Update window._ollamaModel when sel changes
  document.addEventListener("change", e => {
    if (e.target.classList.contains("ollama-model-select")) {
      window._ollamaModel = e.target.value;
      ollamaModel = e.target.value;
    }
  });

  // Init
  setProvider("ollama");
  checkOllamaStatus();
})();


// ══════════════════════════════════════════════════════════════════════════════
// GLOBAL AI MANAGER — single Groq key shared across all features
// ══════════════════════════════════════════════════════════════════════════════

window.AI = {
  groqKey: "",
  openaiKey: "",
  provider: "groq",

  setKey(key) {
    key = key.trim();
    if (key.startsWith("gsk_")) {
      this.groqKey = key;
      this.provider = "groq";
    } else if (key.startsWith("sk-")) {
      this.openaiKey = key;
      this.provider = "openai";
    }
    // Persist in sessionStorage so key survives page navigation
    try { sessionStorage.setItem("ai_groq_key", this.groqKey); sessionStorage.setItem("ai_openai_key", this.openaiKey); } catch {}
    this._notifyKeyChange();
  },

  getKey() {
    return this.provider === "groq" ? this.groqKey : this.openaiKey;
  },

  loadSaved() {
    try {
      this.groqKey = sessionStorage.getItem("ai_groq_key") || "";
      this.openaiKey = sessionStorage.getItem("ai_openai_key") || "";
      if (this.groqKey) this.provider = "groq";
      else if (this.openaiKey) this.provider = "openai";
    } catch {}
  },

  hasKey() {
    return !!(this.groqKey || this.openaiKey);
  },

  _listeners: [],
  onKeyChange(fn) { this._listeners.push(fn); },
  _notifyKeyChange() { this._listeners.forEach(fn => fn()); },

  async call(prompt, maxTokens = 500) {
    // Try server-side key first (no user key needed)
    try {
      const resp = await fetch("/api/ai/default", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ prompt })
      });
      const data = await resp.json();
      if (resp.ok && data.response) return data.response;
      // If server has no key configured, fall through to user key
      if (resp.status !== 500 || !data.error?.includes("not configured")) {
        throw new Error(data.error || "AI error");
      }
    } catch (e) {
      if (!e.message?.includes("not configured") && !e.message?.includes("Failed to fetch")) {
        throw e;
      }
    }

    // Fallback: use user-provided key
    if (!this.hasKey()) {
      throw new Error("No API key set. Enter your Groq key in the chat panel below.");
    }
    if (this.provider === "groq") {
      const resp = await fetch("/api/ai/chat/groq", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ apiKey: this.groqKey, prompt })
      });
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.error || "Groq error");
      return data.response || "";
    } else {
      const resp = await fetch("/api/ai/chat/openai", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ apiKey: this.openaiKey, prompt })
      });
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.error || "OpenAI error");
      return data.response || "";
    }
  }
};

window.AI.loadSaved();

loadLiveStocks();

// ══════════════════════════════════════════════════════════════════════════════
// BREAKOUT WATCH — detects accumulation + pre-breakout setups
// ══════════════════════════════════════════════════════════════════════════════

function detectBreakout(stock) {
  const signals = [];
  let score = 0;

  const price       = stock.price || 0;
  const rsi         = stock.rsi14 || 0;
  const bpct        = stock.bollingerPercentB || 0;
  const demand      = stock.demandScore || 0;
  const bp          = stock.buyPressure || 0;
  const vol         = stock.volumeVsAvg20 || 0;
  const mom         = stock.priceChange1Y || 0;
  const daily       = stock.dailyChange || 0;
  const pe          = stock.peRatio || 0;
  const yld         = stock.dividendYield || 0;
  const eg          = stock.earningsGrowth || 0;
  const roe         = stock.roe || 0;
  const dte         = stock.debtToEquity || 0;

  // ── 1. BOLLINGER SQUEEZE (price compressing = energy building) ─────────────
  // bollingerPercentB between 25-55 = mid-band consolidation after a dip
  // bollingerPercentB < 25 = near lower band, coiling for bounce
  if (bpct > 0) {
    if (bpct >= 20 && bpct <= 55) {
      score += 22;
      signals.push({ icon: "⚡", text: "Bollinger squeeze — price coiling mid-band (%B " + bpct.toFixed(0) + ")" });
    } else if (bpct < 20) {
      score += 16;
      signals.push({ icon: "📉→📈", text: "Price near lower Bollinger band (%B " + bpct.toFixed(0) + ") — mean reversion setup" });
    }
  }

  // ── 2. RSI MOMENTUM BUILDING (not overbought, gaining strength) ────────────
  if (rsi > 0) {
    if (rsi >= 45 && rsi <= 62) {
      score += 20;
      signals.push({ icon: "📶", text: "RSI " + rsi.toFixed(0) + " — building momentum without being overbought" });
    } else if (rsi >= 35 && rsi < 45) {
      score += 14;
      signals.push({ icon: "🔄", text: "RSI " + rsi.toFixed(0) + " — recovering from oversold, early accumulation" });
    } else if (rsi > 62 && rsi <= 70) {
      score += 8;
      signals.push({ icon: "📈", text: "RSI " + rsi.toFixed(0) + " — strong but not yet overbought" });
    }
  }

  // ── 3. VOLUME ACCUMULATION (rising volume with stable/flat price = smart money) ─
  if (vol > 0) {
    if (vol >= 1.4 && Math.abs(daily) < 1.5) {
      score += 24;
      signals.push({ icon: "🏦", text: "Volume " + vol.toFixed(1) + "x avg with contained price — classic accumulation signal" });
    } else if (vol >= 1.2) {
      score += 14;
      signals.push({ icon: "📊", text: "Above-average volume " + vol.toFixed(1) + "x — increased interest" });
    }
  }

  // ── 4. DEMAND / ORDER FLOW (smart money fingerprint) ───────────────────────
  if (demand > 62) {
    score += 18;
    signals.push({ icon: "💰", text: "Demand score " + demand.toFixed(0) + "/100 — institutional buy flow detected" });
  } else if (demand > 50) {
    score += 10;
    signals.push({ icon: "👆", text: "Demand score " + demand.toFixed(0) + " — buyers slightly dominant" });
  }

  if (bp > 54) {
    score += 12;
    signals.push({ icon: "🟢", text: "Buy pressure " + bp.toFixed(0) + "% — bid side leading the order book" });
  }

  // ── 5. PRICE ACTION (flat/shallow dip after uptrend = base building) ────────
  if (mom > 5 && mom < 30 && Math.abs(daily) < 0.8) {
    score += 16;
    signals.push({ icon: "🏗️", text: "Trending stock +" + mom.toFixed(1) + "% 1Y, currently consolidating — base forming" });
  } else if (mom > 0 && daily >= 0.3 && daily < 2) {
    score += 12;
    signals.push({ icon: "🚀", text: "Positive daily move +" + daily.toFixed(2) + "% on uptrend — momentum resuming" });
  }

  // ── 6. FUNDAMENTAL SUPPORT (breakout needs something to run on) ─────────────
  if (pe > 0 && pe < 11 && eg > 8) {
    score += 14;
    signals.push({ icon: "💎", text: "P/E " + pe.toFixed(1) + " with " + eg.toFixed(1) + "% earnings growth — undervalued growth" });
  } else if (yld > 5 && dte < 1.8) {
    score += 10;
    signals.push({ icon: "💵", text: "Yield " + yld.toFixed(1) + "% with low leverage — income floor supporting price" });
  } else if (roe > 12 && pe > 0 && pe < 14) {
    score += 8;
    signals.push({ icon: "✅", text: "ROE " + roe.toFixed(1) + "% at fair value P/E " + pe.toFixed(1) });
  }

  // ── 7. MACD PRE-CROSSOVER (simulate: MACD approaching zero from below) ──────
  // We approximate this from RSI trend + momentum
  if (rsi > 0 && rsi >= 42 && rsi <= 58 && mom > 3 && vol > 1.0) {
    score += 14;
    signals.push({ icon: "⚙️", text: "MACD likely approaching bullish crossover — RSI + momentum aligning" });
  }

  // ── DISQUALIFIERS ──────────────────────────────────────────────────────────
  if (rsi > 72)                    { score -= 30; } // overbought
  if (bpct > 80)                   { score -= 20; } // price extended
  if (demand > 0 && demand < 35)   { score -= 15; } // sellers dominating
  if (daily < -2)                  { score -= 18; } // breaking down today
  if (dte > 2.5)                   { score -= 10; } // over-leveraged
  if (mom < -15)                   { score -= 15; } // strong downtrend

  // When no live technical data available, boost fundamentally strong stocks
  const hasLiveTech = rsi > 0 || vol > 0 || demand > 0 || bpct > 0;
  if (!hasLiveTech) {
    // Use fundamentals as proxy for accumulation potential
    if (pe > 0 && pe < 10 && eg > 5)  { score += 18; signals.push({ icon: "📊", text: "No live data — fundamentally attractive: P/E " + pe.toFixed(1) + ", growth " + eg.toFixed(1) + "%" }); }
    if (yld > 6)                        { score += 14; signals.push({ icon: "💵", text: "No live data — high yield " + yld.toFixed(1) + "% attracts accumulation" }); }
    if (roe > 12)                       { score += 10; signals.push({ icon: "✅", text: "No live data — strong ROE " + roe.toFixed(1) + "% suggests quality accumulation target" }); }
    if (signals.length === 0) return null; // no basis at all — skip
  }

  if (signals.length < 2) return null;
  if (score < 38) return null; // slightly lower threshold when no live data

  // Timeframe estimate — add note when no live data
  const noDataNote = !hasLiveTech ? " (no live data)" : "";
  const urgency = score >= 80 ? "1-2 days" : score >= 65 ? "2-4 days" : "3-5 days" + noDataNote;
  const conviction = score >= 80 ? "high" : score >= 62 ? "medium" : "watch";

  return { score, signals: signals.slice(0, 5), urgency, conviction };
}

function renderBreakoutCard(stock, analysis) {
  const convColor = analysis.conviction === "high"
    ? "#0b6e4f" : analysis.conviction === "medium"
    ? "#b85c38" : "#8f7b55";
  const convLabel = analysis.conviction === "high"
    ? "🔥 High Alert" : analysis.conviction === "medium"
    ? "⚡ Watch" : "👀 On Radar";

  const signalHtml = analysis.signals.map(s =>
    `<div class="breakout-signal"><span class="breakout-signal-icon">${s.icon}</span><span>${s.text}</span></div>`
  ).join("");

  // Simple spark — price path approximation
  const price = stock.price || 1;
  const pct = (stock.priceChange1Y || 0) / 100;
  const startP = price / (1 + pct);
  const seed = stock.ticker.split("").reduce((a, c) => a + c.charCodeAt(0), 0);
  const W = 120, H = 36;
  const pts = Array.from({ length: 20 }, (_, i) => {
    const t = i / 19;
    return startP * (1 + t * pct + Math.sin(i * 1.4 + seed % 4) * 0.01);
  });
  const minV = Math.min(...pts), maxV = Math.max(...pts) || minV + 0.001;
  const sparkLine = pts.map((v, i) => {
    const x = (i / 19) * W;
    const y = H - ((v - minV) / (maxV - minV)) * H;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  const sparkColor = pct >= 0 ? "#0b6e4f" : "#b13c31";

  return `
    <div class="breakout-card" data-ticker="${stock.ticker}">
      <div class="breakout-card-top">
        <div class="breakout-card-info">
          <div class="breakout-card-ticker">${stock.ticker}</div>
          <div class="breakout-card-company">${stock.company}</div>
          <div class="breakout-card-sector">${stock.sector || ""}</div>
        </div>
        <div class="breakout-card-spark">
          <svg viewBox="0 0 ${W} ${H}" style="width:${W}px;height:${H}px;">
            <polyline points="${sparkLine}" fill="none" stroke="${sparkColor}" stroke-width="1.8"/>
            <circle cx="${W}" cy="${(H - ((pts[pts.length-1]-minV)/(maxV-minV))*H).toFixed(1)}" r="3" fill="${sparkColor}"/>
          </svg>
          <div class="breakout-spark-price">${price.toFixed(3)} OMR</div>
        </div>
        <div class="breakout-card-meta">
          <span class="breakout-conviction-badge" style="background:${convColor}15;color:${convColor};border-color:${convColor}40;">${convLabel}</span>
          <div class="breakout-timeframe">⏱ Est. breakout: <strong>${analysis.urgency}</strong></div>
          <div class="breakout-score">Score: <strong>${analysis.score}</strong>/100</div>
        </div>
      </div>
      <div class="breakout-signals">${signalHtml}</div>
    </div>`;
}

function runBreakoutScan() {
  const panel  = document.getElementById("breakoutPanel");
  const cards  = document.getElementById("breakoutCards");
  const timestamp = document.getElementById("breakoutTimestamp");
  if (!panel || !cards) return;

  const stocks = state.filteredStocks || [];
  if (!stocks.length) { panel.style.display = "none"; return; }

  const hits = stocks
    .map(s => ({ stock: s, analysis: detectBreakout(s) }))
    .filter(h => h.analysis !== null)
    .sort((a, b) => b.analysis.score - a.analysis.score)
    .slice(0, 6);

  if (!hits.length) {
    panel.style.display = "none";
    return;
  }

  panel.style.display = "";
  cards.innerHTML = hits.map(h => renderBreakoutCard(h.stock, h.analysis)).join("");
  if (timestamp) {
    const now = new Date();
    timestamp.textContent = "Last scan: " + now.toLocaleTimeString();
  }

  // Click card → select that stock in the table
  cards.querySelectorAll(".breakout-card").forEach(card => {
    card.addEventListener("click", () => {
      const ticker = card.dataset.ticker;
      state.selectedTicker = ticker;
      renderAll();
      document.querySelector(".rankings-panel")?.scrollIntoView({ behavior: "smooth" });
    });
  });
}

// ── SheetJS ───────────────────────────────────────────────────────────────────
(function() {
  const s = document.createElement("script");
  s.src = "https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js";
  document.head.appendChild(s);
})();

// ── XLSX Export ───────────────────────────────────────────────────────────────
function downloadXlsx() {
  if (typeof XLSX === "undefined") { alert("XLSX library not loaded yet, please try again."); return; }
  const stocks = state.filteredStocks;
  if (!stocks.length) return;
  const headers = ["Ticker","Company","Sector","Price (OMR)","Market Cap (M)","P/E","P/B","Div Yield %","Earnings Growth %","1Y Change %","Debt/Equity","ROE %","Current Ratio","RSI 14","Demand Score","Buy Pressure %","Overall Score"];
  const rows = stocks.map(s => [s.ticker,s.company,s.sector,s.price,s.marketCapM,s.peRatio,s.pbRatio,s.dividendYield,s.earningsGrowth,s.priceChange1Y,s.debtToEquity,s.roe,s.currentRatio,s.rsi14,s.demandScore,s.buyPressure,s.overallScore]);
  const wb = XLSX.utils.book_new();
  const ws = XLSX.utils.aoa_to_sheet([headers, ...rows]);
  ws["!cols"] = headers.map(() => ({ wch: 16 }));
  XLSX.utils.book_append_sheet(wb, ws, "MSX Stocks");
  XLSX.writeFile(wb, "msx-stocks.xlsx");
}
if (els.downloadXlsxBtn) els.downloadXlsxBtn.addEventListener("click", downloadXlsx);

// ── Chart Modal ───────────────────────────────────────────────────────────────
function openChartModal(chartHtml) {
  const existing = document.getElementById("chartModal");
  if (existing) existing.remove();
  const overlay = document.createElement("div");
  overlay.id = "chartModal";
  overlay.style.cssText = "position:fixed;inset:0;background:rgba(36,29,18,0.72);z-index:9999;display:flex;align-items:center;justify-content:center;padding:24px;backdrop-filter:blur(4px);";
  overlay.innerHTML = `<div style="background:#fffaf2;border-radius:24px;padding:28px 24px 24px;width:min(960px,100%);max-height:90vh;overflow-y:auto;box-shadow:0 32px 80px rgba(36,29,18,0.28);position:relative;">
    <button id="closeChartModal" style="position:absolute;top:14px;right:14px;background:rgba(36,29,18,0.08);border:none;border-radius:999px;width:36px;height:36px;font-size:1rem;cursor:pointer;">✕</button>
    ${chartHtml}</div>`;
  document.body.appendChild(overlay);
  overlay.addEventListener("click", e => { if (e.target === overlay) overlay.remove(); });
  overlay.querySelector("#closeChartModal").addEventListener("click", () => overlay.remove());
}

// ── AI ANALYSIS (rule-based, no API key required) ─────────────────────────────

const aiBtn    = document.getElementById("runAiAnalysisBtn");
const aiOutput = document.getElementById("aiAnalysisOutput");

// ── Technical indicator helpers ───────────────────────────────────────────────
function calcEMA(prices, period) {
  const k = 2 / (period + 1);
  const result = new Array(prices.length).fill(null);
  let ema = prices.slice(0, period).reduce((a, b) => a + b, 0) / period;
  result[period - 1] = ema;
  for (let i = period; i < prices.length; i++) {
    ema = prices[i] * k + ema * (1 - k);
    result[i] = ema;
  }
  return result;
}

function calcMACD(prices) {
  const ema12 = calcEMA(prices, 12);
  const ema26 = calcEMA(prices, 26);
  const macdLine = prices.map((_, i) => ema12[i] !== null && ema26[i] !== null ? ema12[i] - ema26[i] : null);
  // Signal: 9-period EMA of macdLine
  const macdValues = macdLine.filter(v => v !== null);
  const signalRaw = calcEMA(macdValues, 9);
  let sigIdx = 0;
  const signal = macdLine.map(v => v !== null ? (signalRaw[sigIdx++] ?? null) : null);
  const histogram = macdLine.map((v, i) => v !== null && signal[i] !== null ? v - signal[i] : null);
  return { macdLine, signal, histogram };
}

function calcStochastic(prices, period = 14, smoothK = 3, smoothD = 3) {
  const rawK = prices.map((_, i) => {
    if (i < period - 1) return null;
    const slice = prices.slice(i - period + 1, i + 1);
    const low = Math.min(...slice), high = Math.max(...slice);
    return high === low ? 50 : ((prices[i] - low) / (high - low)) * 100;
  });
  // Smooth %K
  const smoothed = rawK.map((_, i) => {
    const vals = rawK.slice(Math.max(0, i - smoothK + 1), i + 1).filter(v => v !== null);
    return vals.length === smoothK ? vals.reduce((a, b) => a + b, 0) / smoothK : null;
  });
  // %D = SMA of smoothed %K
  const dLine = smoothed.map((_, i) => {
    const vals = smoothed.slice(Math.max(0, i - smoothD + 1), i + 1).filter(v => v !== null);
    return vals.length === smoothD ? vals.reduce((a, b) => a + b, 0) / smoothD : null;
  });
  return { K: smoothed, D: dLine };
}

// ── Professional 5-panel trading chart ───────────────────────────────────────
function renderTradingChart(stock, levels) {
  const W = 520, PL = 8, PR = 54, PT = 10, PB = 22, GAP = 6;
  const PRICE_H = 130, VOL_H = 28, RSI_H = 44, MACD_H = 48, STOCH_H = 44;
  const TOTAL_H = PT + PRICE_H + GAP + VOL_H + GAP + RSI_H + GAP + MACD_H + GAP + STOCH_H + PB;
  const CW = W - PL - PR;

  const price = stock.price || 1;
  const pct = (stock.priceChange1Y || 0) / 100;
  const startPrice = price / (1 + pct);
  const N = 60;
  const seed = stock.ticker.split("").reduce((a, c) => a + c.charCodeAt(0), 0);
  const rsiReal = stock.rsi14 || 50;
  const demandScore = stock.demandScore || 50;
  const buyPressure = stock.buyPressure || 50;

  // ── Generate synthetic price series ──────────────────────────────────────
  const prices = Array.from({ length: N }, (_, i) => {
    const t = i / (N - 1);
    const trend = startPrice * (1 + t * pct);
    const wave1 = Math.sin(i * 1.7 + seed % 5) * price * 0.016;
    const wave2 = Math.sin(i * 0.4 + seed % 3) * price * 0.024;
    const wave3 = Math.sin(i * 3.1 + seed % 7) * price * 0.008;
    const rsiBias = i > N * 0.72 ? ((rsiReal - 50) / 50) * price * 0.035 * ((i - N * 0.72) / (N * 0.28)) : 0;
    const demBias = i > N * 0.85 && demandScore > 60 ? price * 0.008 * ((i - N * 0.85) / (N * 0.15)) : 0;
    return Math.max(price * 0.45, trend + wave1 + wave2 + wave3 + rsiBias + demBias);
  });

  // ── EMA lines (9 and 21 period) ───────────────────────────────────────────
  const ema9  = calcEMA(prices, 9);
  const ema21 = calcEMA(prices, 21);

  // ── Bollinger bands (20-period) ───────────────────────────────────────────
  const bollinger = prices.map((_, i) => {
    if (i < 19) return null;
    const w = prices.slice(i - 19, i + 1);
    const mean = w.reduce((a, b) => a + b, 0) / 20;
    const sd = Math.sqrt(w.reduce((a, b) => a + (b - mean) ** 2, 0) / 20);
    return { upper: mean + 2 * sd, mid: mean, lower: mean - 2 * sd };
  });

  // ── Volume ────────────────────────────────────────────────────────────────
  const volSeries = prices.map((_, i) => {
    const base = 0.6 + Math.abs(Math.sin(i * 1.3 + seed % 7)) * 0.9;
    const spike = i > N * 0.8 && demandScore > 60 ? 0.7 : 0;
    return base + spike;
  });

  // ── RSI series ────────────────────────────────────────────────────────────
  const rsiSeries = prices.map((_, i) => {
    if (i < 14) return 50;
    const t = i / (N - 1);
    const base = 50 + (pct > 0 ? 9 : -9) * t;
    const wave = Math.sin(i * 0.52 + seed % 4) * 14;
    const blend = Math.max(0, (i - N * 0.58) / (N * 0.42));
    return Math.min(95, Math.max(5, base + wave + blend * (rsiReal - (base + wave))));
  });

  // ── MACD ──────────────────────────────────────────────────────────────────
  const { macdLine, signal: macdSignal, histogram } = calcMACD(prices);

  // ── Stochastic ────────────────────────────────────────────────────────────
  const { K: stochK, D: stochD } = calcStochastic(prices, 14, 3, 3);

  // ── Layout helpers ────────────────────────────────────────────────────────
  function rx(i) { return PL + (i / (N - 1)) * CW; }

  // Price panel
  const { entry: entryPrice, target: targetPrice, stop: stopPrice, support } = levels;
  const allP = [...prices, entryPrice, targetPrice, stopPrice, support].filter(Boolean);
  const padP = (Math.max(...allP) - Math.min(...allP)) * 0.1;
  const minP = Math.min(...allP) - padP, maxP = Math.max(...allP) + padP;
  function py(v) { return PT + PRICE_H - ((v - minP) / (maxP - minP)) * PRICE_H; }

  // Volume panel
  const VOL_Y0 = PT + PRICE_H + GAP;
  const maxVol = Math.max(...volSeries);
  function volY(v) { return VOL_Y0 + VOL_H - (v / maxVol) * (VOL_H - 3); }
  const barW = Math.max(1.5, CW / N - 1);

  // RSI panel
  const RSI_Y0 = VOL_Y0 + VOL_H + GAP;
  function rsiPY(v) { return RSI_Y0 + RSI_H - (v / 100) * RSI_H; }

  // MACD panel
  const MACD_Y0 = RSI_Y0 + RSI_H + GAP;
  const macdVals = [...macdLine, ...macdSignal, ...histogram].filter(v => v !== null);
  const macdMin = Math.min(...macdVals, 0), macdMax = Math.max(...macdVals, 0);
  const macdRange = macdMax - macdMin || 0.001;
  function macdPY(v) { return MACD_Y0 + MACD_H - ((v - macdMin) / macdRange) * MACD_H; }
  const macdZeroY = macdPY(0);

  // Stochastic panel
  const STOCH_Y0 = MACD_Y0 + MACD_H + GAP;
  function stochPY(v) { return STOCH_Y0 + STOCH_H - (v / 100) * STOCH_H; }

  // ── SVG line builders ─────────────────────────────────────────────────────
  function polyline(series, yFn) {
    return series.map((v, i) => v !== null ? `${rx(i).toFixed(1)},${yFn(v).toFixed(1)}` : null)
      .filter(Boolean).join(" ");
  }

  // Bollinger fill path
  const bFill = (() => {
    const u = bollinger.map((b, i) => b ? `${rx(i).toFixed(1)},${py(b.upper).toFixed(1)}` : null).filter(Boolean);
    const l = bollinger.map((b, i) => b ? `${rx(i).toFixed(1)},${py(b.lower).toFixed(1)}` : null).filter(Boolean).reverse();
    return u.length ? `M ${u.join(" L ")} L ${l.join(" L ")} Z` : "";
  })();

  // MACD histogram bars
  const macdBars = histogram.map((v, i) => {
    if (v === null) return "";
    const bx = rx(i) - barW / 2;
    const barTop = Math.min(macdPY(v), macdZeroY);
    const barH = Math.abs(macdPY(v) - macdZeroY);
    const col = v >= 0 ? "rgba(11,110,79,0.6)" : "rgba(177,60,49,0.6)";
    return `<rect x="${bx.toFixed(1)}" y="${barTop.toFixed(1)}" width="${barW.toFixed(1)}" height="${Math.max(1, barH).toFixed(1)}" fill="${col}"/>`;
  }).join("");

  // Volume bars
  const volBars = volSeries.map((v, i) => {
    const bx = rx(i) - barW / 2;
    const bh = Math.max(2, (v / maxVol) * (VOL_H - 4));
    const by = VOL_Y0 + VOL_H - bh;
    return `<rect x="${bx.toFixed(1)}" y="${by.toFixed(1)}" width="${barW.toFixed(1)}" height="${bh.toFixed(1)}" fill="${v > 1.3 ? "rgba(11,110,79,0.55)" : "rgba(136,135,128,0.3)"}"/>`;
  }).join("");

  // ── Level line helper ─────────────────────────────────────────────────────
  function lvl(p, col, lbl, dashed = false) {
    if (!p) return "";
    const y = py(p).toFixed(1);
    const d = dashed ? 'stroke-dasharray="5 3"' : "";
    return `<line x1="${PL}" y1="${y}" x2="${W - PR}" y2="${y}" stroke="${col}" stroke-width="1.1" ${d} opacity="0.9"/>` +
      `<rect x="${(W - PR - 54).toFixed(1)}" y="${(parseFloat(y) - 9).toFixed(1)}" width="54" height="13" rx="3" fill="${col}" opacity="0.9"/>` +
      `<text x="${(W - PR - 27).toFixed(1)}" y="${(parseFloat(y) + 1).toFixed(1)}" text-anchor="middle" font-size="8.5" fill="#fff" font-weight="600">${lbl} ${p.toFixed(3)}</text>`;
  }

  // ── Y-axis price ticks ────────────────────────────────────────────────────
  const yTicks = [minP, minP + (maxP - minP) * 0.33, minP + (maxP - minP) * 0.66, maxP].map(v =>
    `<text x="${(W - PR + 4).toFixed(0)}" y="${py(v).toFixed(1)}" text-anchor="start" font-size="8.5" fill="#6f6251" dominant-baseline="middle">${v.toFixed(3)}</text>` +
    `<line x1="${PL}" y1="${py(v).toFixed(1)}" x2="${W - PR}" y2="${py(v).toFixed(1)}" stroke="rgba(68,56,36,0.07)" stroke-width="0.5"/>`
  ).join("");

  // ── X-axis labels ─────────────────────────────────────────────────────────
  const xLabels = ["12M", "9M", "6M", "3M", "Now"].map((l, i) =>
    `<text x="${rx(Math.round(i / 4 * (N - 1))).toFixed(1)}" y="${TOTAL_H - 3}" text-anchor="middle" font-size="8.5" fill="#6f6251">${l}</text>`
  ).join("");

  // Entry zone shading removed — shown as dashed line only
  const eZone = "";

  // ── Current price callout ─────────────────────────────────────────────────
  const lastP = prices[N - 1], lx = rx(N - 1), ly = py(lastP);
  const callout = `<circle cx="${lx.toFixed(1)}" cy="${ly.toFixed(1)}" r="4" fill="#241d12"/>` +
    `<rect x="${(lx - 29).toFixed(1)}" y="${(ly - 14).toFixed(1)}" width="58" height="14" rx="3" fill="#241d12"/>` +
    `<text x="${lx.toFixed(1)}" y="${(ly - 4).toFixed(1)}" text-anchor="middle" font-size="9" fill="#fff" font-weight="600">${price.toFixed(3)}</text>`;

  // ── RSI/Stoch current value labels ────────────────────────────────────────
  const lastRsi = rsiSeries[N - 1];
  const rsiCol = lastRsi < 35 ? "#0b6e4f" : lastRsi > 68 ? "#b13c31" : "#d7a84a";
  const lastK = stochK.filter(v => v !== null).slice(-1)[0] || 50;
  const stochCol = lastK < 20 ? "#0b6e4f" : lastK > 80 ? "#b13c31" : "#d7a84a";

  // ── MACD crossover signal markers ─────────────────────────────────────────
  let crossovers = "";
  for (let i = 1; i < N; i++) {
    if (macdLine[i] === null || macdSignal[i] === null) continue;
    const prev = macdLine[i - 1], prevSig = macdSignal[i - 1];
    if (prev === null || prevSig === null) continue;
    if (prev < prevSig && macdLine[i] >= macdSignal[i]) {
      // Bullish crossover
      crossovers += `<circle cx="${rx(i).toFixed(1)}" cy="${macdPY(macdLine[i]).toFixed(1)}" r="3" fill="#0b6e4f" opacity="0.85"/>`;
    } else if (prev > prevSig && macdLine[i] <= macdSignal[i]) {
      // Bearish crossover
      crossovers += `<circle cx="${rx(i).toFixed(1)}" cy="${macdPY(macdLine[i]).toFixed(1)}" r="3" fill="#b13c31" opacity="0.85"/>`;
    }
  }

  return `<div class="ai-trading-chart">
    <svg viewBox="0 0 ${W} ${TOTAL_H}" aria-label="Trading analysis chart for ${stock.ticker}">

      <!-- Panel backgrounds -->
      <rect x="${PL}" y="${PT}" width="${CW}" height="${PRICE_H}" fill="rgba(255,252,246,0.5)" rx="2"/>
      <rect x="${PL}" y="${VOL_Y0}" width="${CW}" height="${VOL_H}" fill="rgba(255,252,246,0.35)" rx="2"/>
      <rect x="${PL}" y="${RSI_Y0}" width="${CW}" height="${RSI_H}" fill="rgba(255,252,246,0.35)" rx="2"/>
      <rect x="${PL}" y="${MACD_Y0}" width="${CW}" height="${MACD_H}" fill="rgba(255,252,246,0.35)" rx="2"/>
      <rect x="${PL}" y="${STOCH_Y0}" width="${CW}" height="${STOCH_H}" fill="rgba(255,252,246,0.35)" rx="2"/>

      <!-- Price axis -->
      ${yTicks}

      <!-- Bollinger bands -->
      ${bFill ? `<path d="${bFill}" fill="rgba(215,168,74,0.06)"/>` : ""}
      ${polyline(bollinger.map(b => b ? b.upper : null), py) ? `<polyline points="${polyline(bollinger.map(b => b ? b.upper : null), py)}" fill="none" stroke="#d7a84a" stroke-width="0.9" stroke-dasharray="4 3" opacity="0.65"/>` : ""}
      ${polyline(bollinger.map(b => b ? b.mid : null), py) ? `<polyline points="${polyline(bollinger.map(b => b ? b.mid : null), py)}" fill="none" stroke="#8f7b55" stroke-width="0.7" stroke-dasharray="3 3" opacity="0.45"/>` : ""}
      ${polyline(bollinger.map(b => b ? b.lower : null), py) ? `<polyline points="${polyline(bollinger.map(b => b ? b.lower : null), py)}" fill="none" stroke="#d7a84a" stroke-width="0.9" stroke-dasharray="4 3" opacity="0.65"/>` : ""}

      <!-- EMA lines -->
      <polyline points="${polyline(ema9, py)}" fill="none" stroke="#378ADD" stroke-width="1.2" opacity="0.8"/>
      <polyline points="${polyline(ema21, py)}" fill="none" stroke="#b85c38" stroke-width="1.2" opacity="0.8"/>

      <!-- Entry zone + level lines -->
      ${eZone}
      ${lvl(targetPrice, "#0b6e4f", "TARGET")}
      ${lvl(entryPrice, "#378ADD", "ENTRY", true)}
      ${lvl(support, "#d7a84a", "SUPPORT", true)}
      ${lvl(stopPrice, "#b13c31", "STOP")}

      <!-- Price line -->
      <polyline points="${polyline(prices, py)}" fill="none" stroke="#241d12" stroke-width="2"/>
      ${callout}

      <!-- EMA labels -->
      <text x="${PL + 3}" y="${PT + 10}" font-size="8.5" fill="#6f6251" font-weight="600">PRICE · BB · EMA9 · EMA21</text>
      <text x="${(PL + 70).toFixed(0)}" y="${PT + 10}" font-size="8" fill="#378ADD">── EMA9</text>
      <text x="${(PL + 110).toFixed(0)}" y="${PT + 10}" font-size="8" fill="#b85c38">── EMA21</text>

      <!-- Volume panel -->
      ${volBars}
      <text x="${PL + 3}" y="${VOL_Y0 + 10}" font-size="8.5" fill="#6f6251" font-weight="600">VOLUME</text>

      <!-- RSI panel -->
      <line x1="${PL}" y1="${rsiPY(70).toFixed(1)}" x2="${W - PR}" y2="${rsiPY(70).toFixed(1)}" stroke="#b13c31" stroke-width="0.5" stroke-dasharray="3 3" opacity="0.5"/>
      <line x1="${PL}" y1="${rsiPY(50).toFixed(1)}" x2="${W - PR}" y2="${rsiPY(50).toFixed(1)}" stroke="rgba(68,56,36,0.1)" stroke-width="0.5"/>
      <line x1="${PL}" y1="${rsiPY(30).toFixed(1)}" x2="${W - PR}" y2="${rsiPY(30).toFixed(1)}" stroke="#0b6e4f" stroke-width="0.5" stroke-dasharray="3 3" opacity="0.5"/>
      <polyline points="${polyline(rsiSeries, rsiPY)}" fill="none" stroke="${rsiCol}" stroke-width="1.4"/>
      <text x="${PL + 3}" y="${RSI_Y0 + 10}" font-size="8.5" fill="#6f6251" font-weight="600">RSI 14</text>
      <text x="${W - PR - 2}" y="${RSI_Y0 + 10}" text-anchor="end" font-size="8.5" fill="${rsiCol}" font-weight="700">${lastRsi.toFixed(0)}</text>
      <text x="${(W - PR + 3).toFixed(0)}" y="${rsiPY(70).toFixed(1)}" text-anchor="start" font-size="7.5" fill="#b13c31" dominant-baseline="middle">70</text>
      <text x="${(W - PR + 3).toFixed(0)}" y="${rsiPY(30).toFixed(1)}" text-anchor="start" font-size="7.5" fill="#0b6e4f" dominant-baseline="middle">30</text>

      <!-- MACD panel -->
      ${macdBars}
      <line x1="${PL}" y1="${macdZeroY.toFixed(1)}" x2="${W - PR}" y2="${macdZeroY.toFixed(1)}" stroke="rgba(68,56,36,0.15)" stroke-width="0.8"/>
      <polyline points="${polyline(macdLine, macdPY)}" fill="none" stroke="#241d12" stroke-width="1.3"/>
      <polyline points="${polyline(macdSignal, macdPY)}" fill="none" stroke="#b85c38" stroke-width="1.1" stroke-dasharray="4 2"/>
      ${crossovers}
      <text x="${PL + 3}" y="${MACD_Y0 + 10}" font-size="8.5" fill="#6f6251" font-weight="600">MACD (12,26,9)</text>
      <text x="${(PL + 80).toFixed(0)}" y="${MACD_Y0 + 10}" font-size="8" fill="#241d12">── MACD</text>
      <text x="${(PL + 115).toFixed(0)}" y="${MACD_Y0 + 10}" font-size="8" fill="#b85c38">-- Signal</text>
      <text x="${(PL + 155).toFixed(0)}" y="${MACD_Y0 + 10}" font-size="8" fill="#0b6e4f">▮ Histogram</text>

      <!-- Stochastic panel -->
      <line x1="${PL}" y1="${stochPY(80).toFixed(1)}" x2="${W - PR}" y2="${stochPY(80).toFixed(1)}" stroke="#b13c31" stroke-width="0.5" stroke-dasharray="3 3" opacity="0.5"/>
      <line x1="${PL}" y1="${stochPY(20).toFixed(1)}" x2="${W - PR}" y2="${stochPY(20).toFixed(1)}" stroke="#0b6e4f" stroke-width="0.5" stroke-dasharray="3 3" opacity="0.5"/>
      <polyline points="${polyline(stochK, stochPY)}" fill="none" stroke="${stochCol}" stroke-width="1.3"/>
      <polyline points="${polyline(stochD, stochPY)}" fill="none" stroke="#8f7b55" stroke-width="1" stroke-dasharray="4 2"/>
      <text x="${PL + 3}" y="${STOCH_Y0 + 10}" font-size="8.5" fill="#6f6251" font-weight="600">STOCH (14,3,3)</text>
      <text x="${(PL + 85).toFixed(0)}" y="${STOCH_Y0 + 10}" font-size="8" fill="${stochCol}">── %K</text>
      <text x="${(PL + 110).toFixed(0)}" y="${STOCH_Y0 + 10}" font-size="8" fill="#8f7b55">-- %D</text>
      <text x="${W - PR - 2}" y="${STOCH_Y0 + 10}" text-anchor="end" font-size="8.5" fill="${stochCol}" font-weight="700">${lastK.toFixed(0)}</text>
      <text x="${(W - PR + 3).toFixed(0)}" y="${stochPY(80).toFixed(1)}" text-anchor="start" font-size="7.5" fill="#b13c31" dominant-baseline="middle">80</text>
      <text x="${(W - PR + 3).toFixed(0)}" y="${stochPY(20).toFixed(1)}" text-anchor="start" font-size="7.5" fill="#0b6e4f" dominant-baseline="middle">20</text>

      ${xLabels}
    </svg>

    <div class="ai-chart-legend">
      <span><span class="ai-legend-dot" style="background:#241d12"></span>Price</span>
      <span><span class="ai-legend-dot" style="background:#d7a84a;opacity:0.7"></span>Bollinger</span>
      <span><span class="ai-legend-dot" style="background:#378ADD"></span>EMA 9 / Entry</span>
      <span><span class="ai-legend-dot" style="background:#b85c38"></span>EMA 21</span>
      <span><span class="ai-legend-dot" style="background:#0b6e4f"></span>Target</span>
      <span><span class="ai-legend-dot" style="background:#b13c31"></span>Stop loss</span>
    </div>
  </div>`;
}

function renderAiPick(pick, index) {
  const stock = state.filteredStocks.find(s => s.ticker === pick.ticker) || {};
  const conviction = pick.conviction === "high" ? "high" : "medium";
  const price = stock.price || 1;
  const entry  = pick._entry  || price;
  const target = pick._target || price * 1.10;
  const stop   = pick._stop   || price * 0.930;
  const support = pick._support || price * 0.955;
  const rrRatio = entry > stop ? ((target-entry)/(entry-stop)).toFixed(1) : "—";
  const upside  = entry > 0 ? (((target-entry)/entry)*100).toFixed(1) : "—";
  const downside = entry > 0 ? (((entry-stop)/entry)*100).toFixed(1) : "—";
  const chips = (pick.signals||[]).map(sig =>
    `<span class="ai-indicator-chip ${sig.type==="bullish"?"bullish":"caution"}">${sig.type==="bullish"?"▲":"▼"} ${sig.label}</span>`
  ).join("");
  return `<div class="ai-pick-card">
    <div class="ai-pick-header">
      <div class="ai-pick-rank">${index+1}</div>
      <div class="ai-pick-title">
        <strong>${pick.ticker} — ${pick.company||stock.company||pick.ticker}</strong>
        <span>${stock.sector||""} · ${price.toFixed(3)} OMR</span>
      </div>
      <span class="ai-conviction ${conviction}">${conviction==="high"?"◈ High conviction":"◇ Medium conviction"}</span>
    </div>
    <div class="ai-pick-body">
      ${renderTradingChart(stock,{entry,target,stop,support})}
      <div class="ai-trade-levels">
        <div class="ai-level-box entry"><span class="ai-level-label">Entry</span><span class="ai-level-price">${entry.toFixed(3)} OMR</span><span class="ai-level-note">Buy zone</span></div>
        <div class="ai-level-box target"><span class="ai-level-label">Target</span><span class="ai-level-price">${target.toFixed(3)} OMR</span><span class="ai-level-note">+${upside}% upside</span></div>
        <div class="ai-level-box stop"><span class="ai-level-label">Stop loss</span><span class="ai-level-price">${stop.toFixed(3)} OMR</span><span class="ai-level-note">−${downside}% risk</span></div>
        <div class="ai-level-box rr"><span class="ai-level-label">Risk / Reward</span><span class="ai-level-price">${rrRatio}:1</span><span class="ai-level-note">R:R ratio</span></div>
      </div>
      ${pick.entry_thesis ? `<p class="ai-entry-thesis">${pick.entry_thesis}</p>` : ""}
      <div class="ai-indicators">${chips}</div>
      <p class="ai-pick-rationale">${pick.rationale}</p>
      ${pick.risk ? `<div class="ai-risk-note"><span class="ai-risk-label">⚠ Risk</span> ${pick.risk}</div>` : ""}
    </div>
  </div>`;
}

// ── Scoring engine ────────────────────────────────────────────────────────────
function scoreStock(s) {
  let score=0; const signals=[], cautions=[];
  const mom=s.priceChange1Y||0, daily=s.dailyChange||0;
  // 1Y momentum — reduced weight, context only
  if(mom>15){score+=8;signals.push({type:"bullish",label:`1Y trend +${mom.toFixed(1)}% — established uptrend`});}
  else if(mom>5){score+=4;}
  else if(mom<-15){score-=8;cautions.push({type:"caution",label:`1Y downtrend -${Math.abs(mom).toFixed(1)}% — headwind`});}
  // Daily price action — higher weight (short-term signal)
  if(daily>1.0){score+=14;signals.push({type:"bullish",label:`Strong daily move +${daily.toFixed(2)}% — momentum today`});}
  else if(daily>0.3){score+=8;signals.push({type:"bullish",label:`Positive daily +${daily.toFixed(2)}% — buyers active`});}
  else if(daily<-2){score-=12;cautions.push({type:"caution",label:`Selling today -${Math.abs(daily).toFixed(2)}% — avoid entry`});}
  else if(daily<-0.5){score-=4;cautions.push({type:"caution",label:`Slight daily weakness -${Math.abs(daily).toFixed(2)}%`});}
  const rsi=s.rsi14||0;
  if(rsi>0){
    if(rsi<30){score+=22;signals.push({type:"bullish",label:`RSI ${rsi.toFixed(0)} — deeply oversold`});}
    else if(rsi<42){score+=14;signals.push({type:"bullish",label:`RSI ${rsi.toFixed(0)} — oversold, buy opportunity`});}
    else if(rsi<=58){score+=8;signals.push({type:"bullish",label:`RSI ${rsi.toFixed(0)} — healthy trend zone`});}
    else if(rsi<70){score+=2;}
    else{score-=14;cautions.push({type:"caution",label:`RSI ${rsi.toFixed(0)} — overbought, avoid chasing`});}
  } else { score+=3; }
  const bpct=s.bollingerPercentB||0;
  if(bpct>0){
    if(bpct<15){score+=20;signals.push({type:"bullish",label:`Bollinger %B ${bpct.toFixed(0)} — at lower band`});}
    else if(bpct<40){score+=12;signals.push({type:"bullish",label:`Bollinger %B ${bpct.toFixed(0)} — lower half, room to run`});}
    else if(bpct<65){score+=5;}
    else if(bpct>85){score-=10;cautions.push({type:"caution",label:`Bollinger %B ${bpct.toFixed(0)} — price extended`});}
  }
  const demand=s.demandScore||0, bp=s.buyPressure||0, vol=s.volumeVsAvg20||0;
  // Demand & order flow — primary short-term accumulation indicator
  if(demand>70){score+=22;signals.push({type:"bullish",label:`Demand ${demand.toFixed(0)}/100 — institutional buy flow, strong accumulation`});}
  else if(demand>58){score+=14;signals.push({type:"bullish",label:`Demand ${demand.toFixed(0)}/100 — buyers dominant`});}
  else if(demand>45){score+=6;}
  else if(demand>0&&demand<35){score-=10;cautions.push({type:"caution",label:`Demand ${demand.toFixed(0)}/100 — sellers in control`});}
  if(bp>60){score+=14;signals.push({type:"bullish",label:`Buy pressure ${bp.toFixed(0)}% — aggressive bid accumulation`});}
  else if(bp>52){score+=8;signals.push({type:"bullish",label:`Buy pressure ${bp.toFixed(0)}% — bid side leading`});}
  else if(bp>0&&bp<42){score-=8;cautions.push({type:"caution",label:`Buy pressure ${bp.toFixed(0)}% — ask side heavy`});}
  // Volume — key accumulation signal, higher weight
  if(vol>2.0){score+=22;signals.push({type:"bullish",label:`Volume ${vol.toFixed(1)}x avg — strong institutional accumulation`});}
  else if(vol>1.4&&Math.abs(daily)<1.5){score+=18;signals.push({type:"bullish",label:`Volume ${vol.toFixed(1)}x avg + stable price — classic accumulation`});}
  else if(vol>1.2){score+=10;signals.push({type:"bullish",label:`Volume ${vol.toFixed(1)}x avg — above-average activity`});}
  else if(vol>0&&vol<0.6){score-=6;cautions.push({type:"caution",label:`Volume ${vol.toFixed(1)}x avg — very thin, low conviction`});}
  const pe=s.peRatio||0, yld=s.dividendYield||0, eg=s.earningsGrowth||0, roe=s.roe||0;
  if(pe>0&&pe<8){score+=18;signals.push({type:"bullish",label:`P/E ${pe.toFixed(1)} — deeply undervalued`});}
  else if(pe>0&&pe<11){score+=10;signals.push({type:"bullish",label:`P/E ${pe.toFixed(1)} — attractive value`});}
  else if(pe>0&&pe<14){score+=5;}
  else if(pe>16){score-=5;cautions.push({type:"caution",label:`P/E ${pe.toFixed(1)} — growth premium required`});}
  if(yld>7){score+=16;signals.push({type:"bullish",label:`Yield ${yld.toFixed(1)}% — exceptional income`});}
  else if(yld>5){score+=10;signals.push({type:"bullish",label:`Yield ${yld.toFixed(1)}% — strong income support`});}
  else if(yld>3){score+=4;}
  if(eg>15&&pe>0&&pe<12){score+=14;signals.push({type:"bullish",label:`Growth ${eg.toFixed(1)}% at P/E ${pe.toFixed(1)} — sweet spot`});}
  else if(eg>10){score+=8;signals.push({type:"bullish",label:`Earnings growth ${eg.toFixed(1)}%`});}
  else if(eg>5){score+=4;}
  else if(eg<0){score-=8;cautions.push({type:"caution",label:`Earnings declining ${eg.toFixed(1)}%`});}
  if(roe>14){score+=12;signals.push({type:"bullish",label:`ROE ${roe.toFixed(1)}% — excellent capital efficiency`});}
  else if(roe>10){score+=6;signals.push({type:"bullish",label:`ROE ${roe.toFixed(1)}% — solid returns`});}
  else if(roe>0&&roe<6){score-=4;cautions.push({type:"caution",label:`ROE ${roe.toFixed(1)}% — below-average`});}
  const dte=s.debtToEquity||0, cr=s.currentRatio||0;
  if(dte>2.5){score-=12;cautions.push({type:"caution",label:`Debt/equity ${dte.toFixed(1)}x — high leverage`});}
  else if(dte>1.8){score-=6;cautions.push({type:"caution",label:`Debt/equity ${dte.toFixed(1)}x — elevated leverage`});}
  else if(dte>0&&dte<0.8){score+=6;signals.push({type:"bullish",label:`Low leverage ${dte.toFixed(1)}x`});}
  if(cr>0&&cr<1){score-=10;cautions.push({type:"caution",label:`Current ratio ${cr.toFixed(2)} — liquidity risk`});}
  else if(cr>1.5){score+=5;signals.push({type:"bullish",label:`Current ratio ${cr.toFixed(2)} — healthy liquidity`});}
  const sector=(s.sector||"").toLowerCase();
  if((sector.includes("bank")||sector.includes("financial"))&&roe>10&&yld>4) score+=8;
  else if((sector.includes("industrial")||sector.includes("service"))&&eg>8&&pe>0&&pe<13) score+=6;
  else if((sector.includes("util")||sector.includes("power")||sector.includes("energy"))&&yld>6&&dte<2) score+=8;
  const allSignals=[...signals,...cautions].slice(0,5);
  const sp=s.price||1, spct=(s.priceChange1Y||0)/100;
  const _entry=sp, _target=sp*(1+Math.min(Math.abs(spct)*0.5+0.07,0.22));
  const _stop=sp*0.930, _support=sp*0.955;
  const techAvail=rsi>0||bpct>0||demand>0;
  const topBullish=signals.slice(0,3).map(x=>x.label).join("; ");
  const topRisk=cautions[0]?cautions[0].label:"no major red flags";
  let rationale=`${s.company} presents a ${score>55?"high-conviction":"watchlist-worthy"} setup with ${signals.length} bullish factors. Key drivers: ${topBullish||"fundamental strength"}. `;
  rationale+=`The stock trades at ${pe>0?"P/E "+pe.toFixed(1)+" with ":""}${yld>0?yld.toFixed(1)+"% dividend yield":"solid fundamentals"}, ${mom>0?"confirmed by +"+mom.toFixed(1)+"% 1Y momentum":"though momentum is currently subdued"}. `;
  rationale+=`${!techAvail?"Note: live technical data not loaded — RSI and order flow signals unavailable. ":""}Primary risk: ${topRisk}.`;
  const entry_thesis=`${score>60?"Strong":"Moderate"} buy — ${rsi>0&&rsi<40?"oversold RSI + ":""}${pe>0&&pe<10?"deep value P/E + ":""}${yld>5?"high yield + ":""}${mom>10?"strong momentum":eg>10?"earnings growth":"MSX opportunity"}`.replace(/\s*\+\s*$/,"");
  const risk=cautions[0]?cautions[0].label:`Leverage at ${dte.toFixed(1)}x could pressure in risk-off conditions.`;
  return{score,signals:allSignals,rationale,entry_thesis,risk,_entry,_target,_stop,_support};
}

async function runAiAnalysis() {
  const stocks = state.filteredStocks;
  if (!stocks.length) { aiOutput.innerHTML=`<p class="ai-error">No stock data loaded. Please load data first.</p>`; return; }
  aiBtn.disabled=true; aiBtn.classList.add("loading"); aiBtn.querySelector(".ai-btn-icon").textContent="◌";
  aiOutput.innerHTML=`<div class="ai-thinking"><div class="ai-thinking-dots"><span></span><span></span><span></span></div>Running professional trading analysis across ${stocks.length} stocks…</div>`;
  setTimeout(async ()=>{
    try{
      const scored=stocks.map(s=>({stock:s,...scoreStock(s)}));
      scored.sort((a,b)=>b.score-a.score);
      const picks=scored.slice(0,5).filter(p=>p.score>10);
      if(!picks.length){aiOutput.innerHTML=`<p class="ai-error">No suitable opportunities found in current dataset.</p>`;return;}
      const techLoaded=stocks.some(s=>(s.rsi14||0)>0||(s.demandScore||0)>0);
      const qualityNote=!techLoaded?`<p class="ai-data-warning">⚠ Live technical indicators not loaded — RSI, Bollinger and demand unavailable. Click "Refresh Live MSX" then re-run for full signals.</p>`:"";
      const avgScore=picks.reduce((s,p)=>s+p.score,0)/picks.length;
      const highConv=picks.filter(p=>p.score>60).length;
      const topSectors=[...new Set(picks.map(p=>p.stock.sector))].join(", ");
      const marketRead=`Screening ${stocks.length} MSX stocks identifies ${picks.length} buying opportunities, with ${highConv} high-conviction setup${highConv!==1?"s":""}. Opportunity is concentrated in ${topSectors||"mixed sectors"}. Average conviction score: ${avgScore.toFixed(0)}/100. ${techLoaded?"Technical indicators confirm the fundamental picture.":"Load live data for full technical confirmation."}`;
      const summaryHtml=`<div class="ai-market-read"><span class="ai-market-read-label">Market read</span><p>${marketRead}</p></div>`;
      const picksHtml=picks.map((p,i)=>{
        const pick={ticker:p.stock.ticker,company:p.stock.company,conviction:p.score>60?"high":"medium",entry_thesis:p.entry_thesis,rationale:p.rationale,signals:p.signals,risk:p.risk,_entry:p._entry,_target:p._target,_stop:p._stop,_support:p._support};
        return renderAiPick(pick,i);
      }).join("");
      // Save to state so re-renders don't wipe it
      state.aiOutputHtml = qualityNote+summaryHtml+picksHtml;
      aiOutput.innerHTML = state.aiOutputHtml;

      // ── Groq enrichment: add AI narrative below the cards ──────────────
      if (window.AI.hasKey()) {
        const topTickers = picks.slice(0,5).map(p => {
          const s = p.stock;
          const a = scoreStock(s);
          return `${s.ticker}(${s.company}):price=${s.price.toFixed(3)},1Y=${s.priceChange1Y.toFixed(1)}%,daily=${s.dailyChange.toFixed(2)}%,vol=${s.volumeVsAvg20.toFixed(1)}x,demand=${s.demandScore},RSI=${s.rsi14.toFixed(0)},PE=${s.peRatio.toFixed(1)},yield=${s.dividendYield.toFixed(1)}%,score=${a.score}`;
        }).join("\n");
        const groqPrompt = "You are a senior MSX (Muscat Stock Exchange) trading analyst. In 3-4 sentences, give a morning-briefing style summary of these top stock picks and the overall market opportunity. Be direct and cite specific numbers. Stocks:\n" + topTickers;
        try {
          const narrative = await window.AI.call(groqPrompt, 300);
          const narDiv = document.createElement("div");
          narDiv.className = "ai-market-read";
          narDiv.innerHTML = `<span class="ai-market-read-label">◈ AI Morning Brief</span><p>${narrative.split("\n").join("<br>")}</p>`;
          aiOutput.insertBefore(narDiv, aiOutput.firstChild);
          state.aiOutputHtml = aiOutput.innerHTML;
        } catch {}
      }
    }catch(err){
      aiOutput.innerHTML=`<p class="ai-error">Analysis error: ${err.message}</p>`;
    }finally{
      aiBtn.disabled=false; aiBtn.classList.remove("loading"); aiBtn.querySelector(".ai-btn-icon").textContent="◈";
    }
  },80);
}

if(aiBtn) aiBtn.addEventListener("click", runAiAnalysis);

