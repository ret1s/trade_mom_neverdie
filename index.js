const express = require('express');
const session = require('express-session');
const bcrypt = require('bcryptjs');
const path = require('path');
const db = require('./db');

const app = express();
const PORT = process.env.PORT || 3000;

db.initDB();

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(session({
  secret: 'trade-mom-neverdie-2025',
  resave: false,
  saveUninitialized: false,
  cookie: { maxAge: 24 * 60 * 60 * 1000 },
}));

// ─── Helpers ──────────────────────────────────────────────────────────────────

function formatVND(amount) {
  if (amount == null) return '0';
  return new Intl.NumberFormat('vi-VN').format(Math.round(amount));
}

function formatDate(str) {
  if (!str) return '—';
  try { return new Date(str).toLocaleString('vi-VN', { hour12: false }); } catch { return str; }
}

function getT2Date(fromDateStr) {
  // Parse as local date (SQLite stores datetime('now','localtime'))
  const date = new Date(fromDateStr.replace(' ', 'T'));
  let businessDays = 0;
  while (businessDays < 2) {
    date.setDate(date.getDate() + 1);
    const d = date.getDay();
    if (d !== 0 && d !== 6) businessDays++;
  }
  return date.toISOString().split('T')[0];
}

// ─── Middleware ───────────────────────────────────────────────────────────────

app.use((req, res, next) => {
  res.locals.formatVND = formatVND;
  res.locals.formatDate = formatDate;
  res.locals.currentPath = req.path;
  res.locals.user = null;
  res.locals.flash = { error: req.query.error || null, success: req.query.success || null };
  if (req.session.userId) {
    const user = db.getUserById(req.session.userId);
    if (user) res.locals.user = user;
  }
  next();
});

const requireAuth = (req, res, next) => {
  if (!req.session.userId) return res.redirect('/login');
  next();
};

const requireAdmin = (req, res, next) => {
  if (!req.session.userId || req.session.role !== 'admin') return res.redirect('/dashboard');
  next();
};

// ─── Auth ─────────────────────────────────────────────────────────────────────

app.get('/', (req, res) => res.redirect(req.session.userId ? '/dashboard' : '/login'));

app.get('/login', (req, res) => {
  if (req.session.userId) return res.redirect(req.session.role === 'admin' ? '/admin' : '/dashboard');
  res.render('login', { error: null });
});

app.post('/login', (req, res) => {
  const { username, password } = req.body;
  const user = db.getUserByUsername(username?.trim());
  if (!user || !bcrypt.compareSync(password, user.password)) {
    return res.render('login', { error: 'Sai tên đăng nhập hoặc mật khẩu' });
  }
  req.session.userId = user.id;
  req.session.role = user.role;
  res.redirect(user.role === 'admin' ? '/admin' : '/dashboard');
});

app.get('/register', (req, res) => {
  if (req.session.userId) return res.redirect('/dashboard');
  res.render('register', { error: null });
});

app.post('/register', (req, res) => {
  const { username, password, confirmPassword } = req.body;
  const u = username?.trim();
  if (!u || u.length < 3) return res.render('register', { error: 'Tên đăng nhập phải có ít nhất 3 ký tự' });
  if (!password || password.length < 6) return res.render('register', { error: 'Mật khẩu phải có ít nhất 6 ký tự' });
  if (password !== confirmPassword) return res.render('register', { error: 'Mật khẩu không khớp' });
  if (db.getUserByUsername(u)) return res.render('register', { error: 'Tên đăng nhập đã tồn tại' });

  const user = db.createUser(u, bcrypt.hashSync(password, 10));
  req.session.userId = user.id;
  req.session.role = user.role;
  res.redirect('/dashboard');
});

app.get('/logout', (req, res) => { req.session.destroy(); res.redirect('/login'); });

// ─── Dashboard ────────────────────────────────────────────────────────────────

app.get('/dashboard', requireAuth, (req, res) => {
  const user = db.getUserById(req.session.userId);
  const holdings = db.getHoldings(req.session.userId);
  const recentOrders = db.getUserOrders(req.session.userId, 8);
  const availableBalance = db.getAvailableBalance(req.session.userId);
  const holdingsValue = holdings.reduce((s, h) => s + h.avg_cost * h.total_quantity, 0);
  res.render('dashboard', { user, holdings, recentOrders, availableBalance, holdingsValue });
});

// ─── Orders ───────────────────────────────────────────────────────────────────

app.get('/orders', requireAuth, (req, res) => {
  const orders = db.getUserOrders(req.session.userId);
  res.render('orders', { orders });
});

app.get('/orders/new', requireAuth, (req, res) => {
  const user = db.getUserById(req.session.userId);
  const holdings = db.getHoldings(req.session.userId);
  const availableBalance = db.getAvailableBalance(req.session.userId);
  res.render('new-order', {
    user, holdings, availableBalance,
    error: null, success: null,
    prefill: { ticker: req.query.ticker || '', type: req.query.type || 'buy' },
  });
});

app.post('/orders', requireAuth, (req, res) => {
  const user = db.getUserById(req.session.userId);
  const holdings = db.getHoldings(req.session.userId);
  const availableBalance = db.getAvailableBalance(req.session.userId);
  const { ticker, order_type, quantity, price } = req.body;

  const renderErr = (error) => res.render('new-order', {
    user, holdings, availableBalance, error, success: null,
    prefill: { ticker: ticker || '', type: order_type || 'buy' },
  });

  const tickerClean = ticker?.trim().toUpperCase();
  if (!tickerClean || tickerClean.length < 1 || tickerClean.length > 10) return renderErr('Mã cổ phiếu không hợp lệ (1–10 ký tự)');
  if (!['buy', 'sell'].includes(order_type)) return renderErr('Loại lệnh không hợp lệ');

  const qty  = parseInt(quantity);
  const prcK = parseFloat(price);          // user input: thousands VND (e.g. 28.4)
  const prc  = Math.round(prcK * 1000);   // store as actual VND (28,400)
  if (!qty || qty <= 0 || !Number.isInteger(qty)) return renderErr('Số lượng phải là số nguyên dương');
  if (qty % 100 !== 0) return renderErr('Số lượng phải là bội số của 100 (lô chuẩn)');
  if (!prcK || prcK <= 0) return renderErr('Giá không hợp lệ');

  const totalValue = qty * prc;
  const fee = Math.round(totalValue * 0.001 * 100) / 100; // 0.1%

  if (order_type === 'buy') {
    const totalCost = totalValue + fee;
    if (availableBalance < totalCost) {
      return renderErr(`Số dư khả dụng không đủ. Cần: ₫${formatVND(totalCost)}, Có: ₫${formatVND(availableBalance)}`);
    }
  } else {
    const avail = db.getAvailableQuantity(req.session.userId, tickerClean);
    if (avail < qty) {
      return renderErr(`Không đủ cổ phiếu khả dụng. ${tickerClean} khả dụng: ${avail.toLocaleString('vi-VN')} cổ`);
    }
  }

  db.createOrder({ user_id: req.session.userId, ticker: tickerClean, order_type, quantity: qty, price: prc, fee, total_value: totalValue });

  res.render('new-order', {
    user: db.getUserById(req.session.userId),
    holdings: db.getHoldings(req.session.userId),
    availableBalance: db.getAvailableBalance(req.session.userId),
    error: null,
    success: `Lệnh ${order_type === 'buy' ? 'MUA' : 'BÁN'} ${tickerClean} đã được gửi thành công! Chờ admin xét duyệt.`,
    prefill: { ticker: '', type: 'buy' },
  });
});

// ─── Leaderboard ─────────────────────────────────────────────────────────────

app.get('/leaderboard', requireAuth, (req, res) => {
  const users = db.getAllUsers();
  res.render('leaderboard', { users });
});

// ─── Admin ────────────────────────────────────────────────────────────────────

app.get('/admin', requireAdmin, (req, res) => {
  const pendingOrders = db.getPendingOrders();
  const allOrders = db.getAllOrdersAdmin();
  const users = db.getAllUsers();
  res.render('admin', { pendingOrders, allOrders, users });
});

app.post('/admin/orders/:id/approve', requireAdmin, (req, res) => {
  const orderId = parseInt(req.params.id);
  const note = req.body.note?.trim() || '';
  try {
    db.approveOrder(orderId, note, getT2Date);
    res.redirect('/admin?success=Đã duyệt lệnh thành công');
  } catch (err) {
    res.redirect('/admin?error=' + encodeURIComponent(err.message));
  }
});

app.post('/admin/orders/:id/reject', requireAdmin, (req, res) => {
  const orderId = parseInt(req.params.id);
  const note = req.body.note?.trim() || '';
  try {
    db.rejectOrder(orderId, note);
    res.redirect('/admin?success=Đã từ chối lệnh');
  } catch (err) {
    res.redirect('/admin?error=' + encodeURIComponent(err.message));
  }
});

// ─── Admin Portfolio ──────────────────────────────────────────────────────────

app.get('/admin/portfolio', requireAdmin, (req, res) => {
  const tickers = db.getHeldTickers();
  const portfolios = db.getAllPortfolios();
  res.render('admin-portfolio', { tickers, portfolios });
});

app.post('/admin/prices', requireAdmin, (req, res) => {
  const { prices } = req.body; // prices = { VCB: '28.4', MBB: '25.75', ... }
  if (prices && typeof prices === 'object') {
    for (const [ticker, val] of Object.entries(prices)) {
      const p = parseFloat(val);
      if (ticker && p > 0) db.upsertMarketPrice(ticker.toUpperCase(), Math.round(p * 1000));
    }
  }
  res.redirect('/admin/portfolio?success=Đã cập nhật giá thị trường');
});

// ─── Start ────────────────────────────────────────────────────────────────────

app.listen(PORT, () => {
  console.log(`\n  Trade Mõm Neverdie`);
  console.log(`  http://localhost:${PORT}`);
  console.log(`  Admin: admin / admin123\n`);
});
