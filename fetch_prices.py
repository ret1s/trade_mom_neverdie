#!/usr/bin/env python3
"""
Fetch current market prices for all held tickers via vnstock
and update the market_prices table in trade_mom.db.

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

def fetch_price(ticker, start, end):
    from vnstock import Vnstock
    df = Vnstock().stock(symbol=ticker, source='VCI').quote.history(
        start=start, end=end, interval='1D'
    )
    if df is None or df.empty:
        return None
    return float(df['close'].iloc[-1])

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
        return

    end   = date.today().strftime('%Y-%m-%d')
    start = (date.today() - timedelta(days=10)).strftime('%Y-%m-%d')

    updated, errors = [], []

    for ticker in tickers:
        try:
            price_k = fetch_price(ticker, start, end)
            if price_k is None:
                errors.append({"ticker": ticker, "error": "No data returned"})
                continue
            # vnstock returns price in thousands VND; store as actual VND
            price_vnd = round(price_k * 1000)
            conn.execute("""
                INSERT INTO market_prices (ticker, price, updated_at)
                VALUES (?, ?, datetime('now', 'localtime'))
                ON CONFLICT(ticker) DO UPDATE SET
                    price = excluded.price,
                    updated_at = excluded.updated_at
            """, (ticker, price_vnd))
            updated.append({"ticker": ticker, "price_k": price_k, "price_vnd": price_vnd})
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
