/**
 * Import all buy orders from trade_mom_2.xlsx → Sổ Lệnh
 * Run once: node seed_excel.js
 * Safe to re-run — skips users/orders already imported.
 */

const bcrypt = require('bcryptjs');
const { initDB, getUserByUsername, createUserWithDisplayName, getOrderById } = require('./db');
const Database = require('better-sqlite3');
const path = require('path');

initDB();
const db = new Database(path.join(__dirname, 'trade_mom.db'));
db.pragma('journal_mode = WAL');

// ─── Player mapping ───────────────────────────────────────────────────────────
// displayName (from Excel) → { username, password }
const PLAYERS = {
  'Tai T Nguyen':  { username: 'tai',   password: 'tai123',   displayName: 'Tai T Nguyen' },
  'Chị Ngọc':     { username: 'ngoc',  password: 'ngoc123',  displayName: 'Chị Ngọc' },
  'Hanh':          { username: 'hanh',  password: 'hanh123',  displayName: 'Hanh' },
  'Ngô Quang Hiệp':{ username: 'hiep', password: 'hiep123',  displayName: 'Ngô Quang Hiệp' },
  'Phương':        { username: 'phuong',password: 'phuong123',displayName: 'Phương' },
};

// ─── Raw orders from Excel (prices in thousands VND → multiply × 1000) ───────
// Fields: displayName, ticker, qty, price_k (×1000 = actual VND), date (DD/MM/YYYY)
const RAW_ORDERS = [
  { displayName: 'Tai T Nguyen',   ticker: 'TPB', qty: 5000,  price_k: 16.15,  date: '25/03/2026' },
  { displayName: 'Tai T Nguyen',   ticker: 'VIC', qty: 1000,  price_k: 128.7,  date: '25/03/2026' },
  { displayName: 'Chị Ngọc',      ticker: 'FPT', qty: 3000,  price_k: 77.0,   date: '25/03/2026' },
  { displayName: 'Hanh',           ticker: 'VSC', qty: 5000,  price_k: 23.95,  date: '25/03/2026' },
  { displayName: 'Hanh',           ticker: 'MBB', qty: 5000,  price_k: 26.0,   date: '25/03/2026' },
  { displayName: 'Chị Ngọc',      ticker: 'DGC', qty: 3000,  price_k: 54.7,   date: '25/03/2026' },
  { displayName: 'Ngô Quang Hiệp', ticker: 'GVR', qty: 5000,  price_k: 30.5,   date: '25/03/2026' },
  { displayName: 'Phương',         ticker: 'VGI', qty: 5000,  price_k: 87.4,   date: '25/03/2026' },
  { displayName: 'Phương',         ticker: 'FOX', qty: 5000,  price_k: 79.1,   date: '25/03/2026' },
  { displayName: 'Hanh',           ticker: 'VGC', qty: 1000,  price_k: 45.0,   date: '26/03/2026' },
  { displayName: 'Hanh',           ticker: 'HHV', qty: 10000, price_k: 12.05,  date: '26/03/2026' },
  { displayName: 'Hanh',           ticker: 'VPX', qty: 2000,  price_k: 29.5,   date: '26/03/2026' },
  { displayName: 'Phương',         ticker: 'NTL', qty: 5000,  price_k: 19.5,   date: '26/03/2026' },
  { displayName: 'Tai T Nguyen',   ticker: 'BSR', qty: 5000,  price_k: 27.4,   date: '26/03/2026' },
  { displayName: 'Chị Ngọc',      ticker: 'DXG', qty: 5000,  price_k: 13.9,   date: '26/03/2026' },
  { displayName: 'Phương',         ticker: 'CII', qty: 5000,  price_k: 18.5,   date: '27/03/2026' },
  { displayName: 'Hanh',           ticker: 'MWG', qty: 2000,  price_k: 81.2,   date: '27/03/2026' },
  { displayName: 'Ngô Quang Hiệp', ticker: 'VPB', qty: 5000,  price_k: 26.35,  date: '27/03/2026' },
];

// ─── Helpers ──────────────────────────────────────────────────────────────────

function parseDate(ddmmyyyy) {
  const [d, m, y] = ddmmyyyy.split('/');
  return new Date(`${y}-${m}-${d}T09:00:00`);
}

function getT2Date(dateObj) {
  const d = new Date(dateObj);
  let bdays = 0;
  while (bdays < 2) {
    d.setDate(d.getDate() + 1);
    const dow = d.getDay();
    if (dow !== 0 && dow !== 6) bdays++;
  }
  return d.toISOString().split('T')[0];
}

// ─── Ensure users exist ───────────────────────────────────────────────────────

const userIdMap = {}; // displayName → user id

for (const [displayName, info] of Object.entries(PLAYERS)) {
  let user = getUserByUsername(info.username);
  if (!user) {
    user = createUserWithDisplayName(info.username, bcrypt.hashSync(info.password, 10), info.displayName);
    console.log(`  Created user: ${info.username} (${displayName}) — password: ${info.password}`);
  } else {
    // Update display_name if missing
    if (!user.display_name) {
      db.prepare('UPDATE users SET display_name = ? WHERE id = ?').run(info.displayName, user.id);
    }
    console.log(`  User exists:  ${info.username} (${displayName})`);
  }
  userIdMap[displayName] = user.id;
}

// ─── Check if already seeded ──────────────────────────────────────────────────

const existingCount = db.prepare(
  `SELECT COUNT(*) as c FROM orders WHERE created_at LIKE '2026-03-%' AND order_type = 'buy' AND status = 'approved'`
).get().c;

if (existingCount >= RAW_ORDERS.length) {
  console.log(`\n  Already imported ${existingCount} orders. Nothing to do.`);
  process.exit(0);
}

// ─── Import orders in a transaction ──────────────────────────────────────────

console.log('\n  Importing orders...\n');

const importAll = db.transaction(() => {
  for (const o of RAW_ORDERS) {
    const userId = userIdMap[o.displayName];
    const price  = Math.round(o.price_k * 1000);           // actual VND
    const totalValue = o.qty * price;
    const fee    = Math.round(totalValue * 0.001);          // 0.1%
    const cost   = totalValue + fee;

    const dateObj   = parseDate(o.date);
    const createdAt = dateObj.toISOString().replace('T', ' ').slice(0, 19);
    const settlement = getT2Date(dateObj);
    const now = new Date().toISOString().replace('T', ' ').slice(0, 19);

    // Insert approved order
    const r = db.prepare(`
      INSERT INTO orders (user_id, ticker, order_type, quantity, price, fee, total_value, status, admin_note, settlement_date, created_at, processed_at)
      VALUES (?, ?, 'buy', ?, ?, ?, ?, 'approved', 'Import từ Excel', ?, ?, ?)
    `).run(userId, o.ticker, o.qty, price, fee, totalValue, settlement, createdAt, now);

    const orderId = r.lastInsertRowid;

    // Add holdings lot
    db.prepare(`
      INSERT INTO holdings_lots (user_id, ticker, quantity, remaining_quantity, cost_price, settlement_date, order_id)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(userId, o.ticker, o.qty, o.qty, price, settlement, orderId);

    // Deduct balance
    db.prepare('UPDATE users SET balance = balance - ? WHERE id = ?').run(cost, userId);

    console.log(`  ✓ #${orderId} ${o.displayName.padEnd(16)} ${o.ticker.padEnd(5)} ${o.qty.toLocaleString().padStart(6)} × ${price.toLocaleString().padStart(8)}₫  → settle: ${settlement}`);
  }
});

importAll();

// ─── Summary ──────────────────────────────────────────────────────────────────

console.log('\n  ─── Final Balances ─────────────────────────────────────────────');
for (const [displayName, info] of Object.entries(PLAYERS)) {
  const u = db.prepare('SELECT * FROM users WHERE username = ?').get(info.username);
  console.log(`  ${(u.display_name || u.username).padEnd(18)} balance: ₫${u.balance.toLocaleString()}`);
}

console.log('\n  Done. Start the app with: npm start\n');
