const express = require('express');
const session = require('express-session');
const bcrypt = require('bcryptjs');
const path = require('path');
const db = require('./db');
const locales = require('./locales');
const dnse = require('./dnse');

const app = express();
const PORT = process.env.PORT || 3000;

db.initDB();

// Initialize DNSE market data feed
dnse.init(db.getDatabase());
dnse.startAutoRefresh();

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(express.static(path.join(__dirname, 'public'), { maxAge: 0, etag: false }));
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
  const lang = req.session.lang || 'vi';
  res.locals.lang = lang;
  res.locals.t = locales[lang];
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
  const t = locales[req.session.lang || 'vi'];
  const user = db.getUserByUsername(username?.trim());
  if (!user || !bcrypt.compareSync(password, user.password)) {
    return res.render('login', { error: t.err_wrong_credentials });
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
  const t = locales[req.session.lang || 'vi'];
  const u = username?.trim();
  if (!u || u.length < 3) return res.render('register', { error: t.err_username_short });
  if (!password || password.length < 6) return res.render('register', { error: t.err_password_short });
  if (password !== confirmPassword) return res.render('register', { error: t.err_password_mismatch });
  if (db.getUserByUsername(u)) return res.render('register', { error: t.err_username_taken });

  const user = db.createUser(u, bcrypt.hashSync(password, 10));
  req.session.userId = user.id;
  req.session.role = user.role;
  res.redirect('/dashboard');
});

app.get('/logout', (req, res) => { req.session.destroy(); res.redirect('/login'); });

app.post('/lang', (req, res) => {
  req.session.lang = (req.session.lang === 'en') ? 'vi' : 'en';
  res.redirect(req.headers.referer || '/');
});;

// ─── Dashboard ────────────────────────────────────────────────────────────────

app.get('/dashboard', requireAuth, (req, res) => {
  const user = db.getUserById(req.session.userId);
  const holdings = db.getHoldings(req.session.userId, 'stock');
  const recentOrders = db.getUserOrders(req.session.userId, 8, 'stock');
  const availableBalance = db.getAvailableBalance(req.session.userId, 'stock');
  const holdingsValue = holdings.reduce((s, h) => s + h.avg_cost * h.total_quantity, 0);
  const dnseStatus = dnse.getStatus();
  res.render('dashboard', { user, holdings, recentOrders, availableBalance, holdingsValue, market: 'stock', dnseStatus, startingBalance: 1000000000 });
});

// ─── Derivatives Board ───────────────────────────────────────────────────────

app.get('/derivative', requireAuth, (req, res) => {
  const user = db.getUserById(req.session.userId);
  const holdings = db.getHoldings(req.session.userId, 'derivative');
  const recentOrders = db.getUserOrders(req.session.userId, 8, 'derivative');
  const availableBalance = db.getAvailableBalance(req.session.userId, 'derivative');
  const holdingsValue = holdings.reduce((s, h) => s + h.avg_cost * h.total_quantity, 0);
  const dnseStatus = dnse.getStatus();
  const watchlist = dnse.getDerivativeWatchlist();
  const quotes = dnse.getQuotesCache();
  res.render('derivative-board', {
    user, holdings, recentOrders, availableBalance, holdingsValue,
    market: 'derivative', dnseStatus, startingBalance: 1000000000,
    watchlist, quotes,
  });
});

app.post('/derivative/watch', requireAuth, (req, res) => {
  const ticker = req.body.ticker?.trim().toUpperCase();
  if (ticker) dnse.addToWatchlist(ticker);
  res.redirect('/derivative');
});

// ─── Orders ───────────────────────────────────────────────────────────────────

app.get('/orders', requireAuth, (req, res) => {
  const market = req.query.market || 'stock';
  const orders = db.getUserOrders(req.session.userId, 0, market);
  res.render('orders', { orders, market });
});

app.get('/orders/new', requireAuth, (req, res) => {
  const market = req.query.market || 'stock';
  const user = db.getUserById(req.session.userId);
  const holdings = db.getHoldings(req.session.userId, market);
  const availableBalance = db.getAvailableBalance(req.session.userId, market);
  res.render('new-order', {
    user, holdings, availableBalance, market,
    error: null, success: null,
    prefill: { ticker: req.query.ticker || '', type: req.query.type || 'buy' },
  });
});

app.post('/orders', requireAuth, (req, res) => {
  const market = req.body.market || 'stock';
  const isDerivative = market === 'derivative';
  const user = db.getUserById(req.session.userId);
  const holdings = db.getHoldings(req.session.userId, market);
  const availableBalance = db.getAvailableBalance(req.session.userId, market);
  const { ticker, order_type, quantity, price } = req.body;
  const t = locales[req.session.lang || 'vi'];

  const renderErr = (error) => res.render('new-order', {
    user, holdings, availableBalance, market, error, success: null,
    prefill: { ticker: ticker || '', type: order_type || 'buy' },
  });

  const tickerClean = ticker?.trim().toUpperCase();
  if (!tickerClean || tickerClean.length < 1 || tickerClean.length > 10) return renderErr(t.err_invalid_ticker);
  if (!['buy', 'sell'].includes(order_type)) return renderErr(t.err_invalid_order_type);

  const qty  = parseInt(quantity);
  const prcK = parseFloat(price);
  const prc  = Math.round(prcK * 1000);
  if (!qty || qty <= 0 || !Number.isInteger(qty)) return renderErr(t.err_invalid_qty);
  // Stocks: must be multiple of 100. Derivatives: any integer >= 1
  if (!isDerivative && qty % 100 !== 0) return renderErr(t.err_qty_lot);
  if (!prcK || prcK <= 0) return renderErr(t.err_invalid_price);

  const totalValue = qty * prc;
  const fee = Math.round(totalValue * 0.001 * 100) / 100; // 0.1%

  if (order_type === 'buy') {
    const totalCost = totalValue + fee;
    if (availableBalance < totalCost) {
      return renderErr(t.err_insuf_balance(formatVND(totalCost), formatVND(availableBalance)));
    }
  } else {
    const avail = db.getAvailableQuantity(req.session.userId, tickerClean, market);
    if (avail < qty) {
      return renderErr(isDerivative ? t.err_insuf_contracts(tickerClean, avail) : t.err_insuf_shares(tickerClean, avail));
    }
  }

  db.createOrder({ user_id: req.session.userId, ticker: tickerClean, order_type, quantity: qty, price: prc, fee, total_value: totalValue, market });

  res.render('new-order', {
    user: db.getUserById(req.session.userId),
    holdings: db.getHoldings(req.session.userId, market),
    availableBalance: db.getAvailableBalance(req.session.userId, market),
    market,
    error: null,
    success: t.success_order(order_type, tickerClean),
    prefill: { ticker: '', type: 'buy' },
  });
});

// ─── Leaderboard ─────────────────────────────────────────────────────────────

app.get('/leaderboard', requireAuth, (req, res) => {
  const user = db.getUserById(req.session.userId);
  if (!user.tournament_id) {
    return res.render('leaderboard', { users: [], tournament: null });
  }
  const tournament = db.getTournamentById(user.tournament_id);
  const users = db.getUsersByTournament(user.tournament_id);
  res.render('leaderboard', { users, tournament });
});

// ─── Public Profile ───────────────────────────────────────────────────────────

app.get('/player/:username', requireAuth, (req, res) => {
  const viewer = db.getUserById(req.session.userId);
  const profile = db.getUserPublicProfile(req.params.username);
  if (!profile) return res.redirect('/leaderboard');

  // Only same tournament can view
  if (!viewer.tournament_id || viewer.tournament_id !== profile.tournament_id) {
    return res.redirect('/leaderboard');
  }

  const stockHoldings = db.getHoldings(profile.id, 'stock');
  const derivHoldings = db.getHoldings(profile.id, 'derivative');
  const orders = db.getUserOrders(profile.id, 0, 'stock');
  const navHistory = db.getNAVHistory(profile.id, 30);
  res.render('profile', { profile, stockHoldings, derivHoldings, orders, navHistory });
});

app.post('/nav/record', requireAuth, (req, res) => {
  const note = req.body.note?.trim() || '';
  const market = req.body.market || 'stock';
  db.recordNAV(req.session.userId, note, market);
  const redirect = market === 'derivative' ? '/derivative' : '/dashboard';
  res.redirect(redirect + '?success=' + encodeURIComponent(locales[req.session.lang || 'vi'].nav_recorded));
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

// ─── Admin Tournaments ────────────────────────────────────────────────────────

app.get('/admin/tournaments', requireAdmin, (req, res) => {
  const tournaments = db.getAllTournaments();
  const users = db.getAllUsers();
  res.render('admin-tournaments', { tournaments, users });
});

app.post('/admin/tournaments', requireAdmin, (req, res) => {
  const t = locales[req.session.lang || 'vi'];
  const name = req.body.name?.trim();
  if (!name) return res.redirect('/admin/tournaments?error=' + encodeURIComponent(t.err_tournament_name));
  try {
    db.createTournament(name);
    res.redirect('/admin/tournaments?success=' + encodeURIComponent(name));
  } catch {
    res.redirect('/admin/tournaments?error=' + encodeURIComponent(t.err_tournament_exists));
  }
});

app.post('/admin/users/:id/tournament', requireAdmin, (req, res) => {
  const userId = parseInt(req.params.id);
  const tournamentId = req.body.tournament_id ? parseInt(req.body.tournament_id) : null;
  db.setUserTournament(userId, tournamentId);
  res.redirect('/admin/tournaments');
});

// ─── Admin Portfolio ──────────────────────────────────────────────────────────

app.get('/admin/portfolio', requireAdmin, (req, res) => {
  const tickers = db.getHeldTickers();
  const portfolios = db.getAllPortfolios();
  res.render('admin-portfolio', { tickers, portfolios });
});

app.post('/admin/prices', requireAdmin, (req, res) => {
  const { prices } = req.body;
  if (prices && typeof prices === 'object') {
    for (const [ticker, val] of Object.entries(prices)) {
      const p = parseFloat(val);
      if (ticker && p > 0) db.upsertMarketPrice(ticker.toUpperCase(), Math.round(p * 1000));
    }
  }
  res.redirect('/admin/portfolio?success=Đã cập nhật giá thủ công');
});

app.post('/admin/fetch-prices', requireAdmin, async (req, res) => {
  try {
    const result = await dnse.fetchOnce(12000);
    const msg = `DNSE: ${result.updated.length} ma da cap nhat` +
      (result.errors.length ? ` (${result.errors.length} khong co du lieu: ${result.errors.map(e => e.ticker).join(', ')})` : '');
    res.redirect('/admin/portfolio?success=' + encodeURIComponent(msg));
  } catch (err) {
    res.redirect('/admin/portfolio?error=' + encodeURIComponent('Loi fetch DNSE: ' + (err.message || '').slice(0, 200)));
  }
});

// ─── API: DNSE Status & Prices (for frontend auto-refresh) ──────────────────

app.get('/api/prices', requireAuth, (req, res) => {
  const prices = db.getMarketPrices();
  const status = dnse.getStatus();
  res.json({ prices, dnse: status });
});

// Candle data for chart - returns cached or fetches historical from VNDirect
app.get('/api/candles/:ticker', requireAuth, async (req, res) => {
  const ticker = req.params.ticker.toUpperCase();
  const resolution = req.query.resolution || '1';
  const days = parseInt(req.query.days) || 3;
  let candles = dnse.getCandles(ticker);
  if (candles.length === 0) {
    candles = await dnse.fetchHistoricalCandles(ticker, resolution, days);
  }
  res.json(candles);
});

// SSE endpoint for real-time quote streaming
app.get('/api/sse/quotes', requireAuth, (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
  });
  res.write('\n');
  dnse.addSSEClient(res);
  // Keep alive every 30s
  const ka = setInterval(() => { try { res.write(': keepalive\n\n'); } catch { clearInterval(ka); } }, 30000);
  req.on('close', () => clearInterval(ka));
});

app.post('/admin/dnse/connect', requireAdmin, (req, res) => {
  dnse.connect();
  dnse.refreshSubscriptions();
  res.redirect('/admin/portfolio?success=DNSE: ket noi thu cong');
});

app.post('/admin/dnse/disconnect', requireAdmin, (req, res) => {
  dnse.disconnect();
  res.redirect('/admin/portfolio?success=DNSE: da ngat ket noi');
});

// ─── Start ────────────────────────────────────────────────────────────────────

app.listen(PORT, () => {
  console.log(`\n  Trade Mõm Neverdie`);
  console.log(`  http://localhost:${PORT}`);
  console.log(`  Admin: admin / admin123\n`);
});
