# Trade Mom Neverdie

A paper trading web app simulating the Vietnam stock market. Players start with ₫1,000,000,000 and compete to grow their portfolio. Trades follow real VN market rules: T+2 settlement, standard lots (100 shares), and a 0.1% transaction fee. All orders require admin approval.

---

## Features

- **T+2 settlement** — bought shares are locked for 2 business days before they can be sold
- **Standard lot enforcement** — quantity must be a multiple of 100
- **0.1% fee** per order (applied on both buy and sell)
- **Admin approval workflow** — all orders are pending until an admin reviews them
- **Market price tracking** — auto-fetch from VNStock API or manual override
- **Leaderboard** — ranked by total assets (cash + holdings at market price)
- **Light / dark mode** — persisted in localStorage
- **Vietnamese / English UI** — toggle persisted in server session

---

## Screenshots

### Admin — Orders & Users Overview
Pending orders with approve/reject actions. User table shows balance, market value of holdings, total assets, P&L, and pending buy cost.

### Admin — Portfolio
Per-user portfolio cards with full holdings detail (avg cost, market price, invested, market value, P&L). Market prices can be auto-fetched from VNStock or edited manually.

### User — Dashboard
Stats bar with available balance, holdings value (unrealized P&L), total assets, and P&L vs starting capital. Holdings table with T+2 lock status and one-click sell button.

---

## Tech Stack

- **Runtime** — Node.js v22+
- **Framework** — Express.js
- **Templating** — EJS
- **Database** — SQLite via better-sqlite3
- **Auth** — express-session + bcryptjs
- **Market data** — vnstock (Python, v3.4.2+)
- **Styling** — Vanilla CSS, system-ui font, dark/light theme via CSS custom properties

---

## Getting Started

### Prerequisites

- Node.js v22+
- Python 3 + pip (for VNStock price fetching)

### Install

```bash
git clone <repo-url>
cd trade_mom_neverdie
npm install
pip install vnstock
```

### Run

```bash
node index.js
```

App runs at `http://localhost:3000`.

Default admin account: `admin` / `admin123` — **change this in production**.

---

## Database

SQLite database is stored as `trade_mom.db` in the project root. It is **not committed to git**.

On first run, the database is created automatically with all tables and a default admin account.

### Schema

| Table | Description |
|---|---|
| `users` | Player accounts with balance and role |
| `orders` | All buy/sell orders with status (pending/approved/rejected) |
| `holdings_lots` | Individual purchase lots with FIFO tracking and T+2 settlement date |
| `market_prices` | Current market prices per ticker, updated manually or via VNStock |

### Changing the admin password

```bash
node -e "const b=require('bcryptjs');console.log(b.hashSync('newpassword',10));"
```
```sql
UPDATE users SET password = '<hash>' WHERE username = 'admin';
```

### Editing an entry price

Holdings are stored as individual lots in `holdings_lots`. Prices are in actual VND (thousands × 1000):

```sql
SELECT * FROM holdings_lots WHERE ticker = 'NTL';
UPDATE holdings_lots SET cost_price = 19000 WHERE id = <id>;
-- e.g. 19.0 thousand = 19000
```

---

## Deployment

### First deploy

```bash
# On server
git clone <repo-url>
cd trade_mom_neverdie
npm install        # compiles native SQLite bindings for Linux
node index.js
```

### Deploy with existing data

1. **Local** — flush WAL to main db file:
   ```bash
   sqlite3 trade_mom.db "PRAGMA wal_checkpoint(TRUNCATE);"
   ```
2. Upload `trade_mom.db` to the server via SFTP (e.g. Termius), overwriting the existing file.
3. Restart the app.

### Running with PM2

```bash
npm install -g pm2
pm2 start index.js --name trade-mom
pm2 save
pm2 startup
```

---

## Market Price Fetching

The **Fetch from VNStock** button in Admin > Portfolio spawns a Python subprocess that pulls the latest closing price for all held tickers via VNStock (VCI source).

The script is at `fetch_prices.py`. It reads tickers from the database and updates `market_prices`. Prices can also be set manually from the same page.

---

## Order Flow

```
User places order → status: pending
         ↓
Admin reviews → Approve or Reject
         ↓ (Approve)
Buy:  balance deducted, holding lot created (locked until T+2)
Sell: lots consumed FIFO, proceeds credited to balance
```

**Available balance** = actual balance − sum of all pending buy costs
**Available quantity** = settled lots (settlement_date ≤ today) − pending sell orders

---

## Language

Toggle between Vietnamese (default) and English using the `VI` / `EN` button in the nav bar. Preference is saved in the server session per user.
