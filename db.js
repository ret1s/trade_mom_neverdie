const Database = require('better-sqlite3');
const bcrypt = require('bcryptjs');
const path = require('path');

const DB_PATH = path.join(__dirname, 'trade_mom.db');
let db;

function initDB() {
  db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');

  db.exec(`
    CREATE TABLE IF NOT EXISTS tournaments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT UNIQUE NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
    );

    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT UNIQUE NOT NULL,
      password TEXT NOT NULL,
      display_name TEXT,
      balance REAL NOT NULL DEFAULT 1000000000,
      derivative_balance REAL NOT NULL DEFAULT 1000000000,
      role TEXT NOT NULL DEFAULT 'user',
      created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
    );

    CREATE TABLE IF NOT EXISTS orders (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      ticker TEXT NOT NULL,
      order_type TEXT NOT NULL,
      quantity INTEGER NOT NULL,
      price REAL NOT NULL,
      fee REAL NOT NULL,
      total_value REAL NOT NULL,
      market TEXT NOT NULL DEFAULT 'stock',
      status TEXT NOT NULL DEFAULT 'pending',
      admin_note TEXT DEFAULT '',
      settlement_date TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
      processed_at TEXT,
      FOREIGN KEY (user_id) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS holdings_lots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      ticker TEXT NOT NULL,
      quantity INTEGER NOT NULL,
      remaining_quantity INTEGER NOT NULL,
      cost_price REAL NOT NULL,
      settlement_date TEXT NOT NULL,
      order_id INTEGER NOT NULL,
      market TEXT NOT NULL DEFAULT 'stock',
      FOREIGN KEY (user_id) REFERENCES users(id),
      FOREIGN KEY (order_id) REFERENCES orders(id)
    );

    CREATE TABLE IF NOT EXISTS market_prices (
      ticker TEXT PRIMARY KEY,
      price REAL NOT NULL,
      updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
    );

    CREATE TABLE IF NOT EXISTS candles (
      ticker TEXT NOT NULL,
      time INTEGER NOT NULL,
      open REAL NOT NULL,
      high REAL NOT NULL,
      low REAL NOT NULL,
      close REAL NOT NULL,
      volume REAL NOT NULL DEFAULT 0,
      PRIMARY KEY (ticker, time)
    );

    CREATE TABLE IF NOT EXISTS nav_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      total_assets REAL NOT NULL,
      recorded_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
      note TEXT DEFAULT '',
      FOREIGN KEY (user_id) REFERENCES users(id)
    );
  `);

  // Migrate: add tournament_id to users if not exists
  try { db.exec('ALTER TABLE users ADD COLUMN tournament_id INTEGER REFERENCES tournaments(id)'); } catch(e) {}
  // Migrate: add derivative_balance to users
  try { db.exec('ALTER TABLE users ADD COLUMN derivative_balance REAL NOT NULL DEFAULT 1000000000'); } catch(e) {}
  // Migrate: add market column to orders
  try { db.exec("ALTER TABLE orders ADD COLUMN market TEXT NOT NULL DEFAULT 'stock'"); } catch(e) {}
  // Migrate: add market column to holdings_lots
  try { db.exec("ALTER TABLE holdings_lots ADD COLUMN market TEXT NOT NULL DEFAULT 'stock'"); } catch(e) {}

  // Seed admin if not exists
  const admin = db.prepare('SELECT id FROM users WHERE username = ?').get('admin');
  if (!admin) {
    const hash = bcrypt.hashSync('admin123', 10);
    db.prepare('INSERT INTO users (username, password, balance, role) VALUES (?, ?, 0, ?)').run('admin', hash, 'admin');
    console.log('Admin created: admin / admin123');
  }
}

// ─── Users ───────────────────────────────────────────────────────────────────

function getUserByUsername(username) {
  return db.prepare('SELECT * FROM users WHERE username = ?').get(username);
}

function getUserById(id) {
  return db.prepare('SELECT * FROM users WHERE id = ?').get(id);
}

function createUser(username, password) {
  const r = db.prepare('INSERT INTO users (username, password) VALUES (?, ?)').run(username, password);
  return getUserById(r.lastInsertRowid);
}

function createUserWithDisplayName(username, password, displayName) {
  const r = db.prepare('INSERT INTO users (username, password, display_name) VALUES (?, ?, ?)').run(username, password, displayName);
  return getUserById(r.lastInsertRowid);
}

const USER_SELECT = `
  SELECT u.id, u.username, u.display_name, u.balance, u.derivative_balance, u.role, u.created_at, u.tournament_id,
    t.name as tournament_name,
    COALESCE((
      SELECT SUM(hl.remaining_quantity * COALESCE(mp.price, hl.cost_price))
      FROM holdings_lots hl LEFT JOIN market_prices mp ON mp.ticker = hl.ticker
      WHERE hl.user_id = u.id AND hl.remaining_quantity > 0 AND hl.market = 'stock'
    ), 0) as holdings_value,
    COALESCE((
      SELECT SUM(hl.remaining_quantity * hl.cost_price)
      FROM holdings_lots hl WHERE hl.user_id = u.id AND hl.remaining_quantity > 0 AND hl.market = 'stock'
    ), 0) as holdings_cost,
    COALESCE((SELECT SUM(total_value + fee) FROM orders o WHERE o.user_id = u.id AND o.order_type = 'buy' AND o.status = 'pending' AND o.market = 'stock'), 0) as pending_buy_cost,
    COALESCE((
      SELECT SUM(hl.remaining_quantity * COALESCE(mp.price, hl.cost_price))
      FROM holdings_lots hl LEFT JOIN market_prices mp ON mp.ticker = hl.ticker
      WHERE hl.user_id = u.id AND hl.remaining_quantity > 0 AND hl.market = 'derivative'
    ), 0) as deriv_holdings_value,
    COALESCE((
      SELECT SUM(hl.remaining_quantity * hl.cost_price)
      FROM holdings_lots hl WHERE hl.user_id = u.id AND hl.remaining_quantity > 0 AND hl.market = 'derivative'
    ), 0) as deriv_holdings_cost,
    COALESCE((SELECT SUM(total_value + fee) FROM orders o WHERE o.user_id = u.id AND o.order_type = 'buy' AND o.status = 'pending' AND o.market = 'derivative'), 0) as deriv_pending_buy_cost
  FROM users u
  LEFT JOIN tournaments t ON t.id = u.tournament_id
`;

const ORDER_BY_NAV = `
  ORDER BY (u.balance + u.derivative_balance + COALESCE((
    SELECT SUM(hl.remaining_quantity * COALESCE(mp.price, hl.cost_price))
    FROM holdings_lots hl LEFT JOIN market_prices mp ON mp.ticker = hl.ticker
    WHERE hl.user_id = u.id AND hl.remaining_quantity > 0
  ), 0)) DESC
`;

function getAllUsers() {
  return db.prepare(`${USER_SELECT} WHERE u.role != 'admin' ${ORDER_BY_NAV}`).all();
}

function getUsersByTournament(tournamentId) {
  return db.prepare(`${USER_SELECT} WHERE u.role != 'admin' AND u.tournament_id = ? ${ORDER_BY_NAV}`).all(tournamentId);
}

// ─── Tournaments ──────────────────────────────────────────────────────────────

function getAllTournaments() {
  return db.prepare('SELECT * FROM tournaments ORDER BY id').all();
}

function getTournamentById(id) {
  return db.prepare('SELECT * FROM tournaments WHERE id = ?').get(id);
}

function createTournament(name) {
  const r = db.prepare('INSERT INTO tournaments (name) VALUES (?)').run(name);
  return getTournamentById(r.lastInsertRowid);
}

function setUserTournament(userId, tournamentId) {
  db.prepare('UPDATE users SET tournament_id = ? WHERE id = ?').run(tournamentId, userId);
}

// ─── NAV History ──────────────────────────────────────────────────────────────

function getUserNAV(userId, market = 'stock') {
  const user = getUserById(userId);
  const bal = market === 'derivative' ? user.derivative_balance : user.balance;
  const r = db.prepare(`
    SELECT COALESCE((
      SELECT SUM(hl.remaining_quantity * COALESCE(mp.price, hl.cost_price))
      FROM holdings_lots hl LEFT JOIN market_prices mp ON mp.ticker = hl.ticker
      WHERE hl.user_id = ? AND hl.remaining_quantity > 0 AND hl.market = ?
    ), 0) as holdings_value
  `).get(userId, market);
  return bal + r.holdings_value;
}

function recordNAV(userId, note = '', market = 'stock') {
  const totalAssets = getUserNAV(userId, market);
  db.prepare('INSERT INTO nav_history (user_id, total_assets, note) VALUES (?, ?, ?)').run(userId, totalAssets, note);
  return totalAssets;
}

function getNAVHistory(userId, limit = 50) {
  return db.prepare('SELECT * FROM nav_history WHERE user_id = ? ORDER BY recorded_at DESC LIMIT ?').all(userId, limit);
}

// ─── Public Profile ───────────────────────────────────────────────────────────

function getUserPublicProfile(username) {
  return db.prepare(`
    ${USER_SELECT} WHERE u.username = ? AND u.role != 'admin'
  `).get(username);
}

// ─── Balance ──────────────────────────────────────────────────────────────────

function getAvailableBalance(userId, market = 'stock') {
  const user = getUserById(userId);
  const bal = market === 'derivative' ? user.derivative_balance : user.balance;
  const r = db.prepare(`
    SELECT COALESCE(SUM(total_value + fee), 0) as pending
    FROM orders WHERE user_id = ? AND order_type = 'buy' AND status = 'pending' AND market = ?
  `).get(userId, market);
  return bal - r.pending;
}

// ─── Holdings ─────────────────────────────────────────────────────────────────

function getHoldings(userId, market = 'stock') {
  const today = new Date().toISOString().split('T')[0];
  return db.prepare(`
    SELECT
      hl.ticker,
      SUM(hl.remaining_quantity) as total_quantity,
      SUM(CASE WHEN hl.settlement_date <= ? THEN hl.remaining_quantity ELSE 0 END) as available_quantity,
      SUM(CASE WHEN hl.settlement_date > ? THEN hl.remaining_quantity ELSE 0 END) as locked_quantity,
      CAST(SUM(hl.remaining_quantity * hl.cost_price) AS REAL) / SUM(hl.remaining_quantity) as avg_cost,
      MIN(CASE WHEN hl.settlement_date > ? THEN hl.settlement_date END) as next_settlement,
      mp.price as market_price,
      mp.updated_at as price_updated_at
    FROM holdings_lots hl
    LEFT JOIN market_prices mp ON mp.ticker = hl.ticker
    WHERE hl.user_id = ? AND hl.remaining_quantity > 0 AND hl.market = ?
    GROUP BY hl.ticker
    ORDER BY hl.ticker
  `).all(today, today, today, userId, market);
}

function getAvailableQuantity(userId, ticker, market = 'stock') {
  const today = new Date().toISOString().split('T')[0];
  const settled = db.prepare(`
    SELECT COALESCE(SUM(remaining_quantity), 0) as total
    FROM holdings_lots WHERE user_id = ? AND ticker = ? AND remaining_quantity > 0 AND settlement_date <= ? AND market = ?
  `).get(userId, ticker, today, market);
  const pendingSells = db.prepare(`
    SELECT COALESCE(SUM(quantity), 0) as total
    FROM orders WHERE user_id = ? AND ticker = ? AND order_type = 'sell' AND status = 'pending' AND market = ?
  `).get(userId, ticker, market);
  return Math.max(0, settled.total - pendingSells.total);
}

// ─── Orders ───────────────────────────────────────────────────────────────────

function getUserOrders(userId, limit = 0, market = 'stock') {
  if (limit > 0) return db.prepare('SELECT * FROM orders WHERE user_id = ? AND market = ? ORDER BY created_at DESC LIMIT ?').all(userId, market, limit);
  return db.prepare('SELECT * FROM orders WHERE user_id = ? AND market = ? ORDER BY created_at DESC').all(userId, market);
}

function createOrder({ user_id, ticker, order_type, quantity, price, fee, total_value, market = 'stock' }) {
  const r = db.prepare(`
    INSERT INTO orders (user_id, ticker, order_type, quantity, price, fee, total_value, market)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run(user_id, ticker, order_type, quantity, price, fee, total_value, market);
  return r.lastInsertRowid;
}

function getPendingOrders() {
  return db.prepare(`
    SELECT o.*, u.username
    FROM orders o JOIN users u ON o.user_id = u.id
    WHERE o.status = 'pending'
    ORDER BY o.created_at ASC
  `).all();
}

function getAllOrdersAdmin() {
  return db.prepare(`
    SELECT o.*, u.username
    FROM orders o JOIN users u ON o.user_id = u.id
    ORDER BY o.created_at DESC LIMIT 200
  `).all();
}

function getOrderById(id) {
  return db.prepare('SELECT * FROM orders WHERE id = ?').get(id);
}

// ─── Admin Actions ────────────────────────────────────────────────────────────

function approveOrder(orderId, note, getT2DateFn) {
  const order = getOrderById(orderId);
  if (!order || order.status !== 'pending') throw new Error('Lệnh không hợp lệ hoặc đã được xử lý');

  const now = new Date().toISOString();
  const isDerivative = order.market === 'derivative';
  const balanceCol = isDerivative ? 'derivative_balance' : 'balance';

  if (order.order_type === 'buy') {
    const user = getUserById(order.user_id);
    const cost = order.total_value + order.fee;
    const currentBalance = isDerivative ? user.derivative_balance : user.balance;
    if (currentBalance < cost) throw new Error(`Không đủ số dư. Cần: ${Math.round(cost).toLocaleString()}, Có: ${Math.round(currentBalance).toLocaleString()}`);

    // Derivatives: T+0 (settle immediately), Stocks: T+2
    const settlementDate = isDerivative
      ? new Date().toISOString().split('T')[0]
      : getT2DateFn(order.created_at);

    db.prepare(`UPDATE users SET ${balanceCol} = ${balanceCol} - ? WHERE id = ?`).run(cost, order.user_id);
    db.prepare(`
      INSERT INTO holdings_lots (user_id, ticker, quantity, remaining_quantity, cost_price, settlement_date, order_id, market)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(order.user_id, order.ticker, order.quantity, order.quantity, order.price, settlementDate, orderId, order.market);
    db.prepare(`
      UPDATE orders SET status = 'approved', admin_note = ?, settlement_date = ?, processed_at = ? WHERE id = ?
    `).run(note, settlementDate, now, orderId);

  } else {
    const today = new Date().toISOString().split('T')[0];
    const lots = db.prepare(`
      SELECT * FROM holdings_lots
      WHERE user_id = ? AND ticker = ? AND remaining_quantity > 0 AND settlement_date <= ? AND market = ?
      ORDER BY settlement_date ASC, id ASC
    `).all(order.user_id, order.ticker, today, order.market);

    let remaining = order.quantity;
    for (const lot of lots) {
      if (remaining <= 0) break;
      const consume = Math.min(remaining, lot.remaining_quantity);
      db.prepare('UPDATE holdings_lots SET remaining_quantity = remaining_quantity - ? WHERE id = ?').run(consume, lot.id);
      remaining -= consume;
    }
    if (remaining > 0) throw new Error(`Không đủ ${isDerivative ? 'hợp đồng' : 'cổ phiếu'} khả dụng. Thiếu ${remaining}`);

    const proceeds = order.total_value - order.fee;
    db.prepare(`UPDATE users SET ${balanceCol} = ${balanceCol} + ? WHERE id = ?`).run(proceeds, order.user_id);
    db.prepare('UPDATE orders SET status = ?, admin_note = ?, processed_at = ? WHERE id = ?').run('approved', note, now, orderId);
  }
}

function rejectOrder(orderId, note) {
  const order = getOrderById(orderId);
  if (!order || order.status !== 'pending') throw new Error('Lệnh không hợp lệ');
  const now = new Date().toISOString();
  db.prepare('UPDATE orders SET status = ?, admin_note = ?, processed_at = ? WHERE id = ?').run('rejected', note, now, orderId);
}

// ─── Market Prices ────────────────────────────────────────────────────────────

function getMarketPrices() {
  return db.prepare('SELECT * FROM market_prices ORDER BY ticker').all();
}

function upsertMarketPrice(ticker, price) {
  db.prepare(`
    INSERT INTO market_prices (ticker, price, updated_at) VALUES (?, ?, datetime('now','localtime'))
    ON CONFLICT(ticker) DO UPDATE SET price = excluded.price, updated_at = excluded.updated_at
  `).run(ticker, price);
}

// All unique tickers currently held (with holder count + avg cost across all users)
function getHeldTickers(market = null) {
  const whereMarket = market ? 'AND hl.market = ?' : '';
  const params = market ? [market] : [];
  return db.prepare(`
    SELECT
      hl.ticker,
      hl.market,
      COUNT(DISTINCT hl.user_id) as holder_count,
      CAST(SUM(hl.remaining_quantity * hl.cost_price) AS REAL) / SUM(hl.remaining_quantity) as avg_cost_all,
      SUM(hl.remaining_quantity) as total_qty,
      mp.price as market_price,
      mp.updated_at as price_updated_at
    FROM holdings_lots hl
    LEFT JOIN market_prices mp ON mp.ticker = hl.ticker
    WHERE hl.remaining_quantity > 0 ${whereMarket}
    GROUP BY hl.ticker
    ORDER BY hl.ticker
  `).all(...params);
}

// Portfolio for all users (grouped by user then ticker), with market price
function getAllPortfolios(market = null) {
  const today = new Date().toISOString().split('T')[0];
  const whereMarket = market ? 'AND hl.market = ?' : '';
  const params = market ? [today, today, market] : [today, today];
  return db.prepare(`
    SELECT
      u.id as user_id,
      u.username,
      u.display_name,
      u.balance,
      u.derivative_balance,
      hl.ticker,
      hl.market,
      SUM(hl.remaining_quantity) as total_quantity,
      SUM(CASE WHEN hl.settlement_date <= ? THEN hl.remaining_quantity ELSE 0 END) as available_quantity,
      SUM(CASE WHEN hl.settlement_date > ? THEN hl.remaining_quantity ELSE 0 END) as locked_quantity,
      CAST(SUM(hl.remaining_quantity * hl.cost_price) AS REAL) / SUM(hl.remaining_quantity) as avg_cost,
      SUM(hl.remaining_quantity * hl.cost_price) as cost_basis,
      mp.price as market_price,
      mp.updated_at as price_updated_at
    FROM holdings_lots hl
    JOIN users u ON hl.user_id = u.id
    LEFT JOIN market_prices mp ON mp.ticker = hl.ticker
    WHERE hl.remaining_quantity > 0 ${whereMarket}
    GROUP BY hl.user_id, hl.ticker
    ORDER BY u.display_name, hl.ticker
  `).all(...params);
}

// ─── Candles ─────────────────────────────────────────────────────────────────

function upsertCandle(ticker, time, open, high, low, close, volume) {
  db.prepare(`
    INSERT INTO candles (ticker, time, open, high, low, close, volume)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(ticker, time) DO UPDATE SET
      high = MAX(candles.high, excluded.high),
      low = MIN(candles.low, excluded.low),
      close = excluded.close,
      volume = excluded.volume
  `).run(ticker, time, open, high, low, close, volume || 0);
}

function getCandles(ticker, limit = 500) {
  return db.prepare(
    'SELECT time, open, high, low, close, volume FROM candles WHERE ticker = ? ORDER BY time DESC LIMIT ?'
  ).all(ticker, limit).reverse();
}

function getDatabase() { return db; }

module.exports = {
  initDB, getDatabase,
  getUserByUsername, getUserById, createUser, createUserWithDisplayName, getAllUsers,
  getUsersByTournament, getUserPublicProfile,
  getAvailableBalance,
  getHoldings, getAvailableQuantity,
  getUserOrders, createOrder, getPendingOrders, getAllOrdersAdmin, getOrderById,
  approveOrder, rejectOrder,
  getMarketPrices, upsertMarketPrice, getHeldTickers, getAllPortfolios,
  upsertCandle, getCandles,
  getAllTournaments, getTournamentById, createTournament, setUserTournament,
  recordNAV, getNAVHistory,
};
