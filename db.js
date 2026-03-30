const Database = require('better-sqlite3');
const bcrypt = require('bcryptjs');
const path = require('path');

const DB_PATH = path.join(__dirname, 'trade_mom.db');
let db;

function initDB() {
  db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');

  db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT UNIQUE NOT NULL,
      password TEXT NOT NULL,
      display_name TEXT,
      balance REAL NOT NULL DEFAULT 1000000000,
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
      FOREIGN KEY (user_id) REFERENCES users(id),
      FOREIGN KEY (order_id) REFERENCES orders(id)
    );

    CREATE TABLE IF NOT EXISTS market_prices (
      ticker TEXT PRIMARY KEY,
      price REAL NOT NULL,
      updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
    );
  `);

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

function getAllUsers() {
  return db.prepare(`
    SELECT u.id, u.username, u.display_name, u.balance, u.role, u.created_at,
      COALESCE((
        SELECT SUM(hl.remaining_quantity * COALESCE(mp.price, hl.cost_price))
        FROM holdings_lots hl LEFT JOIN market_prices mp ON mp.ticker = hl.ticker
        WHERE hl.user_id = u.id AND hl.remaining_quantity > 0
      ), 0) as holdings_value,
      COALESCE((
        SELECT SUM(hl.remaining_quantity * hl.cost_price)
        FROM holdings_lots hl WHERE hl.user_id = u.id AND hl.remaining_quantity > 0
      ), 0) as holdings_cost,
      COALESCE((SELECT SUM(total_value + fee) FROM orders o WHERE o.user_id = u.id AND o.order_type = 'buy' AND o.status = 'pending'), 0) as pending_buy_cost
    FROM users u
    WHERE u.role != 'admin'
    ORDER BY (u.balance + COALESCE((
      SELECT SUM(hl.remaining_quantity * COALESCE(mp.price, hl.cost_price))
      FROM holdings_lots hl LEFT JOIN market_prices mp ON mp.ticker = hl.ticker
      WHERE hl.user_id = u.id AND hl.remaining_quantity > 0
    ), 0)) DESC
  `).all();
}

// ─── Balance ──────────────────────────────────────────────────────────────────

function getAvailableBalance(userId) {
  const user = getUserById(userId);
  const r = db.prepare(`
    SELECT COALESCE(SUM(total_value + fee), 0) as pending
    FROM orders WHERE user_id = ? AND order_type = 'buy' AND status = 'pending'
  `).get(userId);
  return user.balance - r.pending;
}

// ─── Holdings ─────────────────────────────────────────────────────────────────

function getHoldings(userId) {
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
    WHERE hl.user_id = ? AND hl.remaining_quantity > 0
    GROUP BY hl.ticker
    ORDER BY hl.ticker
  `).all(today, today, today, userId);
}

function getAvailableQuantity(userId, ticker) {
  const today = new Date().toISOString().split('T')[0];
  const settled = db.prepare(`
    SELECT COALESCE(SUM(remaining_quantity), 0) as total
    FROM holdings_lots WHERE user_id = ? AND ticker = ? AND remaining_quantity > 0 AND settlement_date <= ?
  `).get(userId, ticker, today);
  const pendingSells = db.prepare(`
    SELECT COALESCE(SUM(quantity), 0) as total
    FROM orders WHERE user_id = ? AND ticker = ? AND order_type = 'sell' AND status = 'pending'
  `).get(userId, ticker);
  return Math.max(0, settled.total - pendingSells.total);
}

// ─── Orders ───────────────────────────────────────────────────────────────────

function getUserOrders(userId, limit = 0) {
  if (limit > 0) return db.prepare('SELECT * FROM orders WHERE user_id = ? ORDER BY created_at DESC LIMIT ?').all(userId, limit);
  return db.prepare('SELECT * FROM orders WHERE user_id = ? ORDER BY created_at DESC').all(userId);
}

function createOrder({ user_id, ticker, order_type, quantity, price, fee, total_value }) {
  const r = db.prepare(`
    INSERT INTO orders (user_id, ticker, order_type, quantity, price, fee, total_value)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(user_id, ticker, order_type, quantity, price, fee, total_value);
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

  if (order.order_type === 'buy') {
    const user = getUserById(order.user_id);
    const cost = order.total_value + order.fee;
    if (user.balance < cost) throw new Error(`Không đủ số dư. Cần: ${Math.round(cost).toLocaleString()}, Có: ${Math.round(user.balance).toLocaleString()}`);

    const settlementDate = getT2DateFn(order.created_at);

    db.prepare('UPDATE users SET balance = balance - ? WHERE id = ?').run(cost, order.user_id);
    db.prepare(`
      INSERT INTO holdings_lots (user_id, ticker, quantity, remaining_quantity, cost_price, settlement_date, order_id)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(order.user_id, order.ticker, order.quantity, order.quantity, order.price, settlementDate, orderId);
    db.prepare(`
      UPDATE orders SET status = 'approved', admin_note = ?, settlement_date = ?, processed_at = ? WHERE id = ?
    `).run(note, settlementDate, now, orderId);

  } else {
    const today = new Date().toISOString().split('T')[0];
    const lots = db.prepare(`
      SELECT * FROM holdings_lots
      WHERE user_id = ? AND ticker = ? AND remaining_quantity > 0 AND settlement_date <= ?
      ORDER BY settlement_date ASC, id ASC
    `).all(order.user_id, order.ticker, today);

    let remaining = order.quantity;
    for (const lot of lots) {
      if (remaining <= 0) break;
      const consume = Math.min(remaining, lot.remaining_quantity);
      db.prepare('UPDATE holdings_lots SET remaining_quantity = remaining_quantity - ? WHERE id = ?').run(consume, lot.id);
      remaining -= consume;
    }
    if (remaining > 0) throw new Error(`Không đủ cổ phiếu khả dụng. Thiếu ${remaining} cổ`);

    const proceeds = order.total_value - order.fee;
    db.prepare('UPDATE users SET balance = balance + ? WHERE id = ?').run(proceeds, order.user_id);
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
function getHeldTickers() {
  return db.prepare(`
    SELECT
      hl.ticker,
      COUNT(DISTINCT hl.user_id) as holder_count,
      CAST(SUM(hl.remaining_quantity * hl.cost_price) AS REAL) / SUM(hl.remaining_quantity) as avg_cost_all,
      SUM(hl.remaining_quantity) as total_qty,
      mp.price as market_price,
      mp.updated_at as price_updated_at
    FROM holdings_lots hl
    LEFT JOIN market_prices mp ON mp.ticker = hl.ticker
    WHERE hl.remaining_quantity > 0
    GROUP BY hl.ticker
    ORDER BY hl.ticker
  `).all();
}

// Portfolio for all users (grouped by user then ticker), with market price
function getAllPortfolios() {
  const today = new Date().toISOString().split('T')[0];
  return db.prepare(`
    SELECT
      u.id as user_id,
      u.username,
      u.display_name,
      u.balance,
      hl.ticker,
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
    WHERE hl.remaining_quantity > 0
    GROUP BY hl.user_id, hl.ticker
    ORDER BY u.display_name, hl.ticker
  `).all(today, today);
}

module.exports = {
  initDB,
  getUserByUsername, getUserById, createUser, createUserWithDisplayName, getAllUsers,
  getAvailableBalance,
  getHoldings, getAvailableQuantity,
  getUserOrders, createOrder, getPendingOrders, getAllOrdersAdmin, getOrderById,
  approveOrder, rejectOrder,
  getMarketPrices, upsertMarketPrice, getHeldTickers, getAllPortfolios,
};
