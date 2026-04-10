/**
 * DNSE LightSpeed API - Market Data via MQTT over WebSocket
 *
 * Connects to DNSE's real-time market data feed and updates market_prices in SQLite.
 * Supports full quote caching and SSE broadcasting for live board updates.
 */

const mqtt = require('mqtt');

const DNSE_API_KEY = 'eyJvcmciOiJkbnNlIiwiaWQiOiJmMjI1ZDMwMjQwYzQ0MjlhYmNjNzdkNGJmMTI2NzRhNSIsImgiOiJtdXJtdXIxMjgifQ==';
const DNSE_API_SECRET = '69HiF25BRksGfYtWXA63C_B1PuUDtqlRwCHfVpU53fQYGxBVTxmoj7-vWp0daVJctyYHWM2uTRBOTMGAVIFNsw';

const BROKER_URL = 'wss://datafeed-lts.dnse.com.vn/wss';
const STOCK_TOPIC_PREFIX = 'plaintext/quotes/stock/SI/';
const OHLC_TOPIC_PREFIX = 'plaintext/quotes/stock/OHLC/1M/'; // 1-minute OHLC

// VNDirect public chart API for historical OHLC data
const VNDIRECT_CHART_API = 'https://dchart-api.vndirect.com.vn/dchart/history';

// Mapping: our watchlist tickers -> VNDirect symbols for chart data
// VN30F1M = front-month, VN30F2M = next-month, etc.
const DERIVATIVE_CHART_SYMBOLS = {
  // Will be dynamically populated
};

let client = null;
let db = null;
let subscribedTickers = new Set();

// Full quote cache: ticker -> { symbol, matchPrice, refPrice, ceiling, floor, bid/ask, volume, etc. }
const quotesCache = new Map();

// Tick history for chart: ticker -> [ { time, price, volume } ] (last ~500 ticks)
const tickHistory = new Map();

// OHLC candle cache: ticker -> [ { time, open, high, low, close } ]
const candleCache = new Map();

// SSE clients for real-time push
const sseClients = new Set();

// Derivative watchlist (always subscribed during trading hours)
const derivativeWatchlist = new Set();

function init(database) {
  db = database;
  // Generate current VN30 futures watchlist
  generateDerivativeWatchlist();
}

function generateDerivativeWatchlist() {
  // Use VN30F1M (front month), VN30F2M (next month) as aliases
  // Plus the KRX-format codes from DNSE
  derivativeWatchlist.add('VN30F1M');
  derivativeWatchlist.add('VN30F2M');

  // Also add KRX codes from DNSE API
  const krxCodes = ['41I1G4000', '41I1G5000', '41I1G6000', '41I1G9000',
                    '41I2G4000', '41I2G5000', '41I2G6000', '41I2G9000'];
  krxCodes.forEach(c => derivativeWatchlist.add(c));
}

function isTradingHours() {
  const now = new Date();
  const h = now.getHours();
  const m = now.getMinutes();
  const day = now.getDay();
  if (day === 0 || day === 6) return false;
  const mins = h * 60 + m;
  return mins >= 8 * 60 + 45 && mins <= 15 * 60 + 15;
}

function connect() {
  if (client && client.connected) return;

  const clientId = `trade-mom-${Date.now()}-${Math.floor(Math.random() * 1000)}`;

  client = mqtt.connect(BROKER_URL, {
    clientId,
    username: DNSE_API_KEY,
    password: DNSE_API_SECRET,
    protocolVersion: 5,
    clean: true,
    reconnectPeriod: 5000,
    connectTimeout: 10000,
  });

  client.on('connect', () => {
    console.log('[DNSE] Connected to market data feed');
    // Subscribe to wildcard for all stock info + OHLC
    client.subscribe(STOCK_TOPIC_PREFIX + '+', { qos: 0 });
    client.subscribe(OHLC_TOPIC_PREFIX + '+', { qos: 0 });
    // Also re-subscribe specific tickers
    subscribedTickers.forEach(t => {
      client.subscribe(STOCK_TOPIC_PREFIX + t, { qos: 0 });
      client.subscribe(OHLC_TOPIC_PREFIX + t, { qos: 0 });
    });
  });

  client.on('message', (topic, payload) => {
    try {
      const data = JSON.parse(payload.toString());

      // Check if OHLC message
      if (topic.includes('/OHLC/')) {
        handleOHLC(topic, data);
        return;
      }

      const ticker = extractTicker(topic, data);
      if (!ticker) return;

      const quote = parseQuote(ticker, data);
      quotesCache.set(ticker, quote);

      // Record tick for chart
      if (quote.matchPrice > 0) {
        recordTick(ticker, quote.matchPrice, quote.matchVolume);
        upsertPrice(ticker, quote.matchPrice);
      }

      // Broadcast to SSE clients
      broadcastSSE({ type: 'quote', ticker, quote });
    } catch (e) {
      // Ignore parse errors
    }
  });

  client.on('error', (err) => {
    console.log('[DNSE] Connection error:', err.message);
  });

  client.on('close', () => {
    console.log('[DNSE] Connection closed');
  });
}

function disconnect() {
  if (client) {
    client.end(true);
    client = null;
    console.log('[DNSE] Disconnected');
  }
}

function extractTicker(topic, data) {
  const parts = topic.split('/');
  if (parts.length >= 5) return parts[4].toUpperCase();
  return (data.symbol || data.ticker || data.code || '').toUpperCase();
}

function parseNum(val) {
  if (val == null) return 0;
  const n = parseFloat(val);
  return isNaN(n) ? 0 : n;
}

function parseQuote(ticker, data) {
  // Parse full quote from DNSE MQTT message
  // Field names vary; try multiple candidates
  const mp = parseNum(data.matchPrice || data.match_price || data.lastPrice || data.last_price || data.closePrice || data.close || data.price || data.last);
  const ref = parseNum(data.refPrice || data.ref_price || data.referencePrice || data.basicPrice || data.basic_price);
  const ceil = parseNum(data.ceilingPrice || data.ceiling_price || data.ceiling || data.highLimit);
  const floor = parseNum(data.floorPrice || data.floor_price || data.floor || data.lowLimit);
  const open = parseNum(data.openPrice || data.open_price || data.open);
  const high = parseNum(data.highPrice || data.high_price || data.high);
  const low = parseNum(data.lowPrice || data.low_price || data.low);
  const vol = parseNum(data.matchVolume || data.match_volume || data.matchedVolume || data.volume || data.lastVolume);
  const totalVol = parseNum(data.totalVolume || data.total_volume || data.accumulatedVolume || data.totalMatchVolume);

  const change = parseNum(data.change || data.priceChange || data.price_change);
  const changePct = parseNum(data.changePct || data.changePercent || data.change_percent || data.pctChange);

  return {
    symbol: ticker,
    matchPrice: mp,
    refPrice: ref,
    ceilingPrice: ceil,
    floorPrice: floor,
    openPrice: open,
    highPrice: high,
    lowPrice: low,
    matchVolume: vol,
    totalVolume: totalVol,
    change: change || (ref > 0 ? mp - ref : 0),
    changePct: changePct || (ref > 0 ? ((mp - ref) / ref * 100) : 0),
    bid1Price: parseNum(data.bidPrice1 || data.bid_price_1 || data.bestBidPrice || data.best1BidPri),
    bid1Volume: parseNum(data.bidVolume1 || data.bid_volume_1 || data.bestBidVolume || data.best1BidVol),
    bid2Price: parseNum(data.bidPrice2 || data.bid_price_2 || data.best2BidPri),
    bid2Volume: parseNum(data.bidVolume2 || data.bid_volume_2 || data.best2BidVol),
    bid3Price: parseNum(data.bidPrice3 || data.bid_price_3 || data.best3BidPri),
    bid3Volume: parseNum(data.bidVolume3 || data.bid_volume_3 || data.best3BidVol),
    ask1Price: parseNum(data.askPrice1 || data.ask_price_1 || data.bestAskPrice || data.best1OfferPri),
    ask1Volume: parseNum(data.askVolume1 || data.ask_volume_1 || data.bestAskVolume || data.best1OfferVol),
    ask2Price: parseNum(data.askPrice2 || data.ask_price_2 || data.best2OfferPri),
    ask2Volume: parseNum(data.askVolume2 || data.ask_volume_2 || data.best2OfferVol),
    ask3Price: parseNum(data.askPrice3 || data.ask_price_3 || data.best3OfferPri),
    ask3Volume: parseNum(data.askVolume3 || data.ask_volume_3 || data.best3OfferVol),
    updatedAt: Date.now(),
    _raw: data, // keep raw for debugging
  };
}

// ─── Tick & Candle tracking ──────────────────────────────────────────────────

function recordTick(ticker, price, volume) {
  if (!tickHistory.has(ticker)) tickHistory.set(ticker, []);
  const ticks = tickHistory.get(ticker);
  const now = Math.floor(Date.now() / 1000);
  ticks.push({ time: now, price, volume: volume || 0 });
  // Keep last 2000 ticks
  if (ticks.length > 2000) ticks.splice(0, ticks.length - 2000);

  // Build/update 1-minute candle from ticks
  updateCandleFromTick(ticker, price, now);
}

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

  // Persist to DB
  persistCandle(ticker, candle);

  broadcastSSE({ type: 'candle', ticker, candle });
}

function handleOHLC(topic, data) {
  // Topic: plaintext/quotes/stock/OHLC/1M/{TICKER}
  const parts = topic.split('/');
  const ticker = (parts[parts.length - 1] || '').toUpperCase();
  if (!ticker) return;

  const o = parseNum(data.open || data.openPrice || data.o);
  const h = parseNum(data.high || data.highPrice || data.h);
  const l = parseNum(data.low || data.lowPrice || data.l);
  const c = parseNum(data.close || data.closePrice || data.c);
  const t = parseNum(data.time || data.timestamp || data.t) || Math.floor(Date.now() / 1000);
  const time = t > 1e12 ? Math.floor(t / 1000) : t; // handle ms vs sec

  if (o > 0 && h > 0 && l > 0 && c > 0) {
    if (!candleCache.has(ticker)) candleCache.set(ticker, []);
    const candles = candleCache.get(ticker);
    const minuteTime = Math.floor(time / 60) * 60;
    let candle;

    if (candles.length > 0 && candles[candles.length - 1].time === minuteTime) {
      candle = candles[candles.length - 1];
      candle.open = o; candle.high = h; candle.low = l; candle.close = c;
    } else {
      candle = { time: minuteTime, open: o, high: h, low: l, close: c };
      candles.push(candle);
      if (candles.length > 500) candles.splice(0, candles.length - 500);
    }
    persistCandle(ticker, candle);
    broadcastSSE({ type: 'candle', ticker, candle });
  }
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
  if (candleCache.has(t) && candleCache.get(t).length > 0) {
    return candleCache.get(t);
  }
  if (!db) return [];
  try {
    const rows = db.prepare(
      'SELECT time, open, high, low, close FROM candles WHERE ticker = ? ORDER BY time DESC LIMIT 500'
    ).all(t).reverse();
    if (rows.length > 0) candleCache.set(t, rows);
    return rows;
  } catch (e) { return []; }
}

/**
 * Fetch historical OHLC from VNDirect public API and cache + persist.
 * Supports both stocks and derivatives.
 */
async function fetchHistoricalCandles(ticker, resolution = '1', days = 3) {
  const t = ticker.toUpperCase();
  const now = Math.floor(Date.now() / 1000);
  const from = now - days * 86400;

  // For derivatives, try both the ticker directly and VN30F1M/VN30F2M aliases
  const symbols = [t];
  if (t.includes('VN30F')) {
    symbols.push('VN30F1M', 'VN30F2M');
  }
  // Also try 41I* KRX format
  symbols.push(t);

  for (const sym of symbols) {
    try {
      const url = `${VNDIRECT_CHART_API}?symbol=${sym}&resolution=${resolution}&from=${from}&to=${now}`;
      const resp = await fetch(url);
      if (!resp.ok) continue;
      const data = await resp.json();
      if (data.s !== 'ok' || !data.t || data.t.length === 0) continue;

      const candles = [];
      for (let i = 0; i < data.t.length; i++) {
        candles.push({
          time: data.t[i],
          open: data.o[i],
          high: data.h[i],
          low: data.l[i],
          close: data.c[i],
        });
      }

      // Cache in memory
      candleCache.set(t, candles);

      // Persist to DB
      if (db) {
        const stmt = db.prepare(`
          INSERT INTO candles (ticker, time, open, high, low, close, volume)
          VALUES (?, ?, ?, ?, ?, ?, 0)
          ON CONFLICT(ticker, time) DO UPDATE SET
            open = excluded.open, high = excluded.high,
            low = excluded.low, close = excluded.close
        `);
        const tx = db.transaction(() => {
          for (const c of candles) {
            stmt.run(t, c.time, c.open, c.high, c.low, c.close);
          }
        });
        tx();
      }

      console.log(`[DNSE] Loaded ${candles.length} candles for ${t} (via ${sym}, res=${resolution})`);
      return candles;
    } catch (e) {
      continue;
    }
  }
  return [];
}

function getTicks(ticker) {
  return tickHistory.get(ticker.toUpperCase()) || [];
}

function normalizePrice(price) {
  if (price <= 500) return Math.round(price * 1000);
  return Math.round(price);
}

function upsertPrice(ticker, price) {
  if (!db) return;
  const priceVND = normalizePrice(price);
  try {
    db.prepare(`
      INSERT INTO market_prices (ticker, price, updated_at) VALUES (?, ?, datetime('now','localtime'))
      ON CONFLICT(ticker) DO UPDATE SET price = excluded.price, updated_at = excluded.updated_at
    `).run(ticker, priceVND);
  } catch (e) {}
}

// ─── SSE ─────────────────────────────────────────────────────────────────────

function addSSEClient(res) {
  sseClients.add(res);
  res.on('close', () => sseClients.delete(res));
  // Send current cache as initial state
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

// ─── Subscriptions ───────────────────────────────────────────────────────────

function subscribeTickers(tickers) {
  if (!Array.isArray(tickers)) return;
  tickers.forEach(t => {
    const ticker = t.toUpperCase();
    if (!subscribedTickers.has(ticker)) {
      subscribedTickers.add(ticker);
      if (client && client.connected) {
        client.subscribe(STOCK_TOPIC_PREFIX + ticker, { qos: 0 });
      }
    }
  });
}

function refreshSubscriptions() {
  if (!db) return;
  try {
    const rows = db.prepare(
      "SELECT DISTINCT ticker FROM holdings_lots WHERE remaining_quantity > 0"
    ).all();
    const dbTickers = new Set(rows.map(r => r.ticker.toUpperCase()));

    // Merge with derivative watchlist
    derivativeWatchlist.forEach(t => dbTickers.add(t));

    dbTickers.forEach(t => {
      if (!subscribedTickers.has(t)) {
        subscribedTickers.add(t);
        if (client && client.connected) {
          client.subscribe(STOCK_TOPIC_PREFIX + t, { qos: 0 });
        }
      }
    });
  } catch (e) {}
}

// ─── Auto-refresh ────────────────────────────────────────────────────────────

let autoRefreshInterval = null;

function startAutoRefresh() {
  if (autoRefreshInterval) return;
  checkAndManageConnection();
  autoRefreshInterval = setInterval(checkAndManageConnection, 60 * 1000);
  console.log('[DNSE] Auto-refresh started');
}

function checkAndManageConnection() {
  if (isTradingHours()) {
    if (!client || !client.connected) connect();
    refreshSubscriptions();
  } else {
    if (client && client.connected) disconnect();
  }
}

function stopAutoRefresh() {
  if (autoRefreshInterval) { clearInterval(autoRefreshInterval); autoRefreshInterval = null; }
  disconnect();
}

function isConnected() {
  return !!(client && client.connected);
}

function getStatus() {
  return {
    connected: isConnected(),
    tradingHours: isTradingHours(),
    subscribedTickers: Array.from(subscribedTickers),
    sseClients: sseClients.size,
    cachedQuotes: quotesCache.size,
  };
}

function getQuotesCache() {
  const out = {};
  quotesCache.forEach((q, t) => { out[t] = q; });
  return out;
}

function getDerivativeWatchlist() {
  return Array.from(derivativeWatchlist);
}

function addToWatchlist(ticker) {
  const t = ticker.toUpperCase();
  derivativeWatchlist.add(t);
  subscribeTickers([t]);
}

// ─── One-shot fetch ──────────────────────────────────────────────────────────

function fetchOnce(timeoutMs = 10000) {
  return new Promise((resolve) => {
    const updated = [];
    if (!db) return resolve({ updated, errors: [], message: 'No database' });

    const rows = db.prepare(
      "SELECT DISTINCT ticker FROM holdings_lots WHERE remaining_quantity > 0"
    ).all();
    const tickers = rows.map(r => r.ticker.toUpperCase());
    if (tickers.length === 0) return resolve({ updated: [], errors: [], message: 'No tickers to fetch' });

    const remaining = new Set(tickers);
    const tempClient = mqtt.connect(BROKER_URL, {
      clientId: `trade-mom-fetch-${Date.now()}`,
      username: DNSE_API_KEY,
      password: DNSE_API_SECRET,
      protocolVersion: 5,
      clean: true,
      connectTimeout: 8000,
    });

    const finish = () => {
      tempClient.end(true);
      const errors = Array.from(remaining).map(t => ({ ticker: t, error: 'No data received' }));
      resolve({ ok: true, updated, errors, message: `DNSE: ${updated.length} updated, ${errors.length} no data` });
    };

    const timeout = setTimeout(finish, timeoutMs);

    tempClient.on('connect', () => {
      tickers.forEach(t => tempClient.subscribe(STOCK_TOPIC_PREFIX + t, { qos: 0 }));
    });

    tempClient.on('message', (topic, payload) => {
      try {
        const data = JSON.parse(payload.toString());
        const ticker = extractTicker(topic, data);
        const quote = parseQuote(ticker, data);
        if (ticker && quote.matchPrice > 0 && remaining.has(ticker)) {
          upsertPrice(ticker, quote.matchPrice);
          quotesCache.set(ticker, quote);
          updated.push({ ticker, price_vnd: normalizePrice(quote.matchPrice), source: 'dnse_mqtt' });
          remaining.delete(ticker);
          if (remaining.size === 0) { clearTimeout(timeout); finish(); }
        }
      } catch (e) {}
    });

    tempClient.on('error', () => { clearTimeout(timeout); finish(); });
  });
}

module.exports = {
  init, connect, disconnect,
  subscribeTickers, refreshSubscriptions,
  startAutoRefresh, stopAutoRefresh, fetchOnce,
  isConnected, isTradingHours, getStatus,
  getQuotesCache, getDerivativeWatchlist, addToWatchlist,
  addSSEClient, getCandles, getTicks, fetchHistoricalCandles,
};
