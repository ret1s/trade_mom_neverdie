#!/usr/bin/env python3
"""
Fetch latest market prices for all held tickers via vnstock.
Uses price_board (single batch call) for speed and real-time data.
Falls back to quote.history per-ticker if price_board fails.

Usage:
  python3 fetch_prices.py               # fetch all tickers in holdings
  python3 fetch_prices.py VCB MBB VIC   # fetch specific tickers only
"""

import sys
import json
import sqlite3
import os
from datetime import date, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), 'trade_mom.db')


def get_tickers_from_db(conn, overrides=None):
    if overrides:
        return [t.upper() for t in overrides]
    rows = conn.execute(
        "SELECT DISTINCT ticker FROM holdings_lots WHERE remaining_quantity > 0"
    ).fetchall()
    return [r[0] for r in rows]


def normalize_price(price):
    """
    vnstock sources may return price in actual VND (28400) or thousands (28.4).
    Vietnamese stocks trade between ~1,000 and ~500,000 VND.
    If value <= 500, assume thousands and multiply by 1000.
    """
    if price is None:
        return None
    return round(price * 1000) if price <= 500 else round(price)


def fetch_price_board(tickers):
    """Fetch all tickers in one call via price_board. Returns {ticker: price_vnd}."""
    from vnstock import Vnstock
    stock = Vnstock().stock(symbol=tickers[0], source='VCI')
    df = stock.trading.price_board(symbols=tickers)
    if df is None or df.empty:
        return {}

    result = {}
    # Normalise column names (vnstock may use different casing)
    df.columns = ['_'.join(str(c).lower().split()) for c in df.columns]

    # Try common column names for latest price
    price_col = None
    for candidate in ['match_price', 'close', 'price', 'last_price', 'basic_price']:
        if candidate in df.columns:
            price_col = candidate
            break
    # Also try tuple-style columns flattened
    if price_col is None:
        for col in df.columns:
            if 'match' in col or 'close' in col or 'price' in col:
                price_col = col
                break

    if price_col is None:
        return {}

    # Find ticker column
    ticker_col = None
    for candidate in ['ticker', 'symbol', 'code', 'stock']:
        if candidate in df.columns:
            ticker_col = candidate
            break
    if ticker_col is None:
        for col in df.columns:
            if 'ticker' in col or 'symbol' in col or 'code' in col:
                ticker_col = col
                break

    if ticker_col is None:
        # Try positional — first column is usually the ticker
        ticker_col = df.columns[0]

    for _, row in df.iterrows():
        try:
            ticker = str(row[ticker_col]).upper().strip()
            raw = float(row[price_col])
            if raw > 0:
                result[ticker] = normalize_price(raw)
        except Exception:
            continue

    return result


def fetch_price_history(ticker):
    """Fallback: fetch last close via quote.history for a single ticker."""
    from vnstock import Vnstock
    end   = date.today().strftime('%Y-%m-%d')
    start = (date.today() - timedelta(days=10)).strftime('%Y-%m-%d')
    df = Vnstock().stock(symbol=ticker, source='VCI').quote.history(
        start=start, end=end, interval='1D'
    )
    if df is None or df.empty:
        return None
    return normalize_price(float(df['close'].iloc[-1]))


def upsert_price(conn, ticker, price_vnd):
    conn.execute("""
        INSERT INTO market_prices (ticker, price, updated_at)
        VALUES (?, ?, datetime('now', 'localtime'))
        ON CONFLICT(ticker) DO UPDATE SET
            price = excluded.price,
            updated_at = excluded.updated_at
    """, (ticker, price_vnd))


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS market_prices (
            ticker TEXT PRIMARY KEY,
            price REAL NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
        )
    """)

    tickers = get_tickers_from_db(conn, sys.argv[1:] if len(sys.argv) > 1 else None)
    if not tickers:
        print(json.dumps({"ok": True, "updated": [], "errors": [], "message": "No tickers to fetch"}))
        conn.close()
        return

    updated, errors = [], []

    # ── Attempt 1: price_board (batch, fast, real-time) ──────────────────────
    board_prices = {}
    try:
        board_prices = fetch_price_board(tickers)
    except Exception as e:
        pass  # will fall back per-ticker

    for ticker in tickers:
        if ticker in board_prices and board_prices[ticker]:
            price_vnd = board_prices[ticker]
            upsert_price(conn, ticker, price_vnd)
            updated.append({"ticker": ticker, "price_vnd": price_vnd, "source": "price_board"})
        else:
            # ── Fallback: quote.history per ticker ───────────────────────────
            try:
                price_vnd = fetch_price_history(ticker)
                if price_vnd is None:
                    errors.append({"ticker": ticker, "error": "No data"})
                    continue
                upsert_price(conn, ticker, price_vnd)
                updated.append({"ticker": ticker, "price_vnd": price_vnd, "source": "history"})
            except Exception as e:
                errors.append({"ticker": ticker, "error": str(e)})

    conn.commit()
    conn.close()

    print(json.dumps({
        "ok": True,
        "updated": updated,
        "errors": errors,
        "message": f"Updated {len(updated)} tickers, {len(errors)} errors"
    }))


if __name__ == '__main__':
    main()
