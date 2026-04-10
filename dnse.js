/**
 * Market Data Module — VNDirect REST API for price data
 *
 * Polls VNDirect's public chart API for real-time prices during trading hours.
 * No authentication required. Updates market_prices in SQLite + broadcasts via SSE.
 */

const VNDIRECT_API = 'https://dchart-api.vndirect.com.vn/dchart/history';

let db = null;
let subscribedTickers = new Set();

// Full quote cache: ticker -> { matchPrice, refPrice, change, changePct, ... }
const quotesCache = new Map();

// Candle cache: ticker -> [ { time, open, high, low, close } ]
const candleCache = new Map();

// SSE clients
const sseClients = new Set();

// Derivative watchlist
const derivativeWatchlist = new Set();

function init(database) {
  db = database;
  generateDerivativeWatchlist();
}

function generateDerivativeWatchlist() {
  derivativeWatchlist.add('VN30F1M');
  derivativeWatchlist.add('VN30F2M');
  ['41I1G4000', '41I1G5000', '41I1G6000', '41I1G9000',
   '41I2G4000', '41I2G5000', '41I2G6000', '41I2G9000']
    .forEach(c => derivativeWatchlist.add(c));
}

function isTradingHours() {
  const now = new Date();
  const day = now.getDay();
  if (day === 0 || day === 6) return false;
  const mins = now.getHours() * 60 + now.getMinutes();
  return mins >= 8 * 60 + 45 && mins <= 15 * 60 + 15;
}

// ─── VNDirect REST price fetching ────────────────────────────────────────────

async function fetchLatestPrice(ticker) {
  const now = Math.floor(Date.now() / 1000);
  const from = now - 600; // last 10 minutes
  try {
    const url = `${VNDIRECT_API}?symbol=${ticker}&resolution=1&from=${from}&to=${now}`;
    const resp = await fetch(url);
    if (!resp.ok) return null;
    const data = await resp.json();
    if (data.s !== 'ok' || !data.t || data.t.length === 0) return null;

    const i = data.t.length - 1;
    return {
      matchPrice: data.c[i],
      openPrice: data.o[0],
      highPrice: Math.max(...data.h),
      lowPrice: Math.min(...data.l),
      totalVolume: data.v ? data.v.reduce((a, b) => a + b, 0) : 0,
      refPrice: data.o[0], // use session open as ref
      time: data.t[i],
    };
  } catch (e) { return null; }
}

async function pollAllPrices() {
  const allTickers = getAllTickers();
  if (allTickers.length === 0) return;

  // Fetch 1-min candles for last 10 mins for each ticker
  for (const ticker of allTickers) {
    try {
      const quote = await fetchLatestPrice(ticker);
      if (!quote || quote.matchPrice <= 0) continue;

      const prev = quotesCache.get(ticker);
      const ref = prev?.refPrice || quote.refPrice || quote.openPrice;
      const change = quote.matchPrice - ref;
      const changePct = ref > 0 ? (change / ref * 100) : 0;

      const fullQuote = {
        symbol: ticker,
        matchPrice: quote.matchPrice,
        refPrice: ref,
        openPrice: quote.openPrice,
        highPrice: quote.highPrice,
        lowPrice: quote.lowPrice,
        change, changePct,
        totalVolume: quote.totalVolume,
        matchVolume: 0,
        bid1Price: 0, bid1Volume: 0, bid2Price: 0, bid2Volume: 0, bid3Price: 0, bid3Volume: 0,
        ask1Price: 0, ask1Volume: 0, ask2Price: 0, ask2Volume: 0, ask3Price: 0, ask3Volume: 0,
        updatedAt: Date.now(),
      };

      quotesCache.set(ticker, fullQuote);
      upsertPrice(ticker, quote.matchPrice);
      broadcastSSE({ type: 'quote', ticker, quote: fullQuote });

      // Update candle
      updateCandleFromTick(ticker, quote.matchPrice, Math.floor(Date.now() / 1000));
    } catch (e) { /* skip */ }
  }
}

function getAllTickers() {
  const tickers = new Set(derivativeWatchlist);
  if (db) {
    try {
      const rows = db.prepare("SELECT DISTINCT ticker FROM holdings_lots WHERE remaining_quantity > 0").all();
      rows.forEach(r => tickers.add(r.ticker.toUpperCase()));
    } catch (e) {}
  }
  return Array.from(tickers);
}

// ─── Price DB ────────────────────────────────────────────────────────────────

function upsertPrice(ticker, price) {
  if (!db || !price) return;
  const priceVND = price <= 500 ? Math.round(price * 1000) : Math.round(price);
  try {
    db.prepare(`
      INSERT INTO market_prices (ticker, price, updated_at) VALUES (?, ?, datetime('now','localtime'))
      ON CONFLICT(ticker) DO UPDATE SET price = excluded.price, updated_at = excluded.updated_at
    `).run(ticker, priceVND);
  } catch (e) {}
}

// ─── Candle tracking ─────────────────────────────────────────────────────────

function updateCandleFromTick(ticker, price, timeSec) {
  if (!candleCache.has(ticker)) candleCache.set(ticker, []);
  const candles = candleCache.get(ticker);
  const minuteTime = Math.floor(timeSec / 60) * 60;
  let candle;

  if (candles.length > 0 && candles[candles.length - 1].time === minuteTime) {
    candle = candles[candles.length - 1];
    candle.high = Math.max(candle.high, price);
    candle.low = Math.min(candle.low, price);
    candle.close = price;
  } else {
    candle = { time: minuteTime, open: price, high: price, low: price, close: price };
    candles.push(candle);
    if (candles.length > 500) candles.splice(0, candles.length - 500);
  }

  persistCandle(ticker, candle);
  broadcastSSE({ type: 'candle', ticker, candle });
}

function persistCandle(ticker, candle) {
  if (!db) return;
  try {
    db.prepare(`
      INSERT INTO candles (ticker, time, open, high, low, close, volume)
      VALUES (?, ?, ?, ?, ?, ?, 0)
      ON CONFLICT(ticker, time) DO UPDATE SET
        high = MAX(candles.high, excluded.high),
        low = MIN(candles.low, excluded.low),
        close = excluded.close
    `).run(ticker, candle.time, candle.open, candle.high, candle.low, candle.close);
  } catch (e) {}
}

function getCandles(ticker) {
  const t = ticker.toUpperCase();
  if (candleCache.has(t) && candleCache.get(t).length > 0) return candleCache.get(t);
  if (!db) return [];
  try {
    const rows = db.prepare(
      'SELECT time, open, high, low, close FROM candles WHERE ticker = ? ORDER BY time DESC LIMIT 500'
    ).all(t).reverse();
    if (rows.length > 0) candleCache.set(t, rows);
    return rows;
  } catch (e) { return []; }
}

async function fetchHistoricalCandles(ticker, resolution = '1', days = 3) {
  const t = ticker.toUpperCase();
  const now = Math.floor(Date.now() / 1000);
  const from = now - days * 86400;

  const symbols = [t];
  if (t.includes('VN30F')) symbols.push('VN30F1M', 'VN30F2M');

  for (const sym of symbols) {
    try {
      const url = `${VNDIRECT_API}?symbol=${sym}&resolution=${resolution}&from=${from}&to=${now}`;
      const resp = await fetch(url);
      if (!resp.ok) continue;
      const data = await resp.json();
      if (data.s !== 'ok' || !data.t || data.t.length === 0) continue;

      const candles = data.t.map((time, i) => ({
        time, open: data.o[i], high: data.h[i], low: data.l[i], close: data.c[i],
      }));

      candleCache.set(t, candles);

      // Persist to DB
      if (db) {
        const stmt = db.prepare(`
          INSERT INTO candles (ticker, time, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, 0)
          ON CONFLICT(ticker, time) DO UPDATE SET open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close
        `);
        const tx = db.transaction(() => { for (const c of candles) stmt.run(t, c.time, c.open, c.high, c.low, c.close); });
        tx();
      }

      // Also update quote from last candle
      const last = candles[candles.length - 1];
      if (last && !quotesCache.has(t)) {
        quotesCache.set(t, {
          symbol: t, matchPrice: last.close, refPrice: candles[0].open,
          change: last.close - candles[0].open,
          changePct: candles[0].open > 0 ? ((last.close - candles[0].open) / candles[0].open * 100) : 0,
          updatedAt: Date.now(),
        });
      }

      console.log(`[Market] Loaded ${candles.length} candles for ${t} (via ${sym}, res=${resolution})`);
      return candles;
    } catch (e) { continue; }
  }
  return [];
}

// ─── SSE ─────────────────────────────────────────────────────────────────────

function addSSEClient(res) {
  sseClients.add(res);
  res.on('close', () => sseClients.delete(res));
  const all = {};
  quotesCache.forEach((q, t) => { all[t] = q; });
  res.write(`data: ${JSON.stringify({ type: 'init', quotes: all })}\n\n`);
}

function broadcastSSE(msg) {
  const data = `data: ${JSON.stringify(msg)}\n\n`;
  sseClients.forEach(res => {
    try { res.write(data); } catch (e) { sseClients.delete(res); }
  });
}

// ─── Auto-refresh (polling) ──────────────────────────────────────────────────

let pollInterval = null;
let _connected = false;

function startAutoRefresh() {
  if (pollInterval) return;
  console.log('[Market] Auto-refresh started (VNDirect REST polling)');

  checkAndPoll(); // initial

  pollInterval = setInterval(() => {
    checkAndPoll();
  }, 15 * 1000); // poll every 15 seconds
}

async function checkAndPoll() {
  if (isTradingHours()) {
    _connected = true;
    await pollAllPrices();
  } else {
    _connected = false;
  }
}

function stopAutoRefresh() {
  if (pollInterval) { clearInterval(pollInterval); pollInterval = null; }
}

function isConnected() { return _connected; }

function getStatus() {
  return {
    connected: _connected,
    tradingHours: isTradingHours(),
    subscribedTickers: getAllTickers(),
    sseClients: sseClients.size,
    cachedQuotes: quotesCache.size,
  };
}

function getQuotesCache() {
  const out = {};
  quotesCache.forEach((q, t) => { out[t] = q; });
  return out;
}

function getDerivativeWatchlist() { return Array.from(derivativeWatchlist); }

function addToWatchlist(ticker) {
  derivativeWatchlist.add(ticker.toUpperCase());
}

// ─── One-shot fetch for admin manual price update ────────────────────────────

async function fetchOnce(timeoutMs = 12000) {
  const tickers = getAllTickers();
  if (tickers.length === 0) return { ok: true, updated: [], errors: [], message: 'No tickers' };

  const updated = [], errors = [];
  const now = Math.floor(Date.now() / 1000);
  const from = now - 86400; // last day

  for (const ticker of tickers) {
    try {
      const url = `${VNDIRECT_API}?symbol=${ticker}&resolution=1&from=${from}&to=${now}`;
      const resp = await fetch(url);
      const data = await resp.json();
      if (data.s === 'ok' && data.c && data.c.length > 0) {
        const price = data.c[data.c.length - 1];
        upsertPrice(ticker, price);
        updated.push({ ticker, price_vnd: price <= 500 ? price * 1000 : price, source: 'vndirect' });
      } else {
        errors.push({ ticker, error: 'No data' });
      }
    } catch (e) {
      errors.push({ ticker, error: e.message });
    }
  }

  return { ok: true, updated, errors, message: `${updated.length} updated, ${errors.length} errors` };
}

// Compat exports
function connect() { /* no-op, polling handles it */ }
function disconnect() { /* no-op */ }
function subscribeTickers(t) { if (Array.isArray(t)) t.forEach(x => derivativeWatchlist.add(x.toUpperCase())); }
function refreshSubscriptions() { /* handled by polling */ }
function getTicks() { return []; }

module.exports = {
  init, connect, disconnect,
  subscribeTickers, refreshSubscriptions,
  startAutoRefresh, stopAutoRefresh, fetchOnce,
  isConnected, isTradingHours, getStatus,
  getQuotesCache, getDerivativeWatchlist, addToWatchlist,
  addSSEClient, getCandles, getTicks, fetchHistoricalCandles,
};
