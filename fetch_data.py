#!/usr/bin/env python3
"""Standalone data fetcher — downloads historical prices for all ETFs
using direct Yahoo Finance v8 chart API with proper headers.
Saves to SQLite database. Run this once to populate the DB."""

import json
import sqlite3
import time
from pathlib import Path
from datetime import datetime, timedelta

import requests

DB_FILE = Path(__file__).parent / ".etf_data.db"

TICKERS = ["SPY", "VTI", "QQQ", "IWM", "IWD", "EFA", "VEA", "EEM", "AGG", "TLT", "GLD", "IYR", "DBC"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

def init_db():
    conn = sqlite3.connect(str(DB_FILE))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS weekly_prices (
        ticker TEXT, date TEXT, close REAL,
        PRIMARY KEY (ticker, date))""")
    conn.execute("""CREATE TABLE IF NOT EXISTS ticker_info (
        ticker TEXT PRIMARY KEY, info_json TEXT, updated_at REAL)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS cph_housing (
        date TEXT PRIMARY KEY, index_val REAL)""")
    conn.commit()
    conn.close()

def save_prices(rows):
    conn = sqlite3.connect(str(DB_FILE))
    conn.executemany(
        "INSERT OR REPLACE INTO weekly_prices (ticker, date, close) VALUES (?, ?, ?)",
        rows
    )
    conn.commit()
    conn.close()

def count_prices(ticker):
    conn = sqlite3.connect(str(DB_FILE))
    r = conn.execute("SELECT COUNT(*) FROM weekly_prices WHERE ticker=?", (ticker,)).fetchone()
    conn.close()
    return r[0] if r else 0

def save_info(ticker, info_dict):
    conn = sqlite3.connect(str(DB_FILE))
    conn.execute(
        "INSERT OR REPLACE INTO ticker_info (ticker, info_json, updated_at) VALUES (?, ?, ?)",
        (ticker, json.dumps(info_dict), time.time())
    )
    conn.commit()
    conn.close()

def fetch_chart(ticker, period1, period2, interval="1wk"):
    """Fetch price data from Yahoo Finance v8 chart API."""
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {
        "period1": int(period1),
        "period2": int(period2),
        "interval": interval,
        "includeAdjustedClose": "true",
    }
    resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    result = data.get("chart", {}).get("result")
    if not result:
        return []

    chart = result[0]
    timestamps = chart.get("timestamp", [])
    closes = chart.get("indicators", {}).get("adjclose", [{}])[0].get("adjclose", [])

    if not closes:
        closes = chart.get("indicators", {}).get("quote", [{}])[0].get("close", [])

    rows = []
    for ts, close in zip(timestamps, closes):
        if close is not None and close > 0:
            date_str = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
            rows.append((ticker, date_str, round(float(close), 4)))

    return rows

def fetch_quote_summary(ticker):
    """Fetch ticker metadata from Yahoo Finance v10 quoteSummary API."""
    url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
    params = {
        "modules": "financialData,quoteType,defaultKeyStatistics,assetProfile,summaryDetail",
    }
    resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    result = data.get("quoteSummary", {}).get("result")
    if not result:
        return None

    # Flatten all modules into a single dict
    info = {}
    for module_data in result:
        for module_name, module_vals in module_data.items():
            if isinstance(module_vals, dict):
                for k, v in module_vals.items():
                    if isinstance(v, dict) and "raw" in v:
                        info[k] = v["raw"]
                    elif not isinstance(v, dict):
                        info[k] = v
    return info

def wait_for_api():
    """Wait until Yahoo API is accessible (not rate limited)."""
    url = "https://query2.finance.yahoo.com/v8/finance/chart/SPY?range=5d&interval=1d"
    print("  Checking Yahoo Finance API availability...")
    for attempt in range(10):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                print(f"  ✓ API is available (attempt {attempt+1})")
                return True
            elif resp.status_code == 429:
                wait = 60 * (attempt + 1)
                print(f"  Rate limited (429). Waiting {wait}s... (attempt {attempt+1}/10)")
                time.sleep(wait)
            else:
                print(f"  HTTP {resp.status_code}. Waiting 30s...")
                time.sleep(30)
        except Exception as e:
            print(f"  Error: {e}. Waiting 30s...")
            time.sleep(30)
    print("  ✗ Could not reach Yahoo Finance API after all retries.")
    return False

def fetch_one_ticker(ticker, period1, period2, interval="1wk"):
    """Fetch one ticker with generous retry logic."""
    for attempt in range(5):
        try:
            rows = fetch_chart(ticker, period1, period2, interval=interval)
            if rows:
                return rows
            else:
                print(f"    no data on attempt {attempt+1}")
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait = 120 * (attempt + 1)  # 2min, 4min, 6min, 8min, 10min
                print(f"    rate limited (429), waiting {wait}s (attempt {attempt+1}/5)...")
                time.sleep(wait)
            else:
                print(f"    HTTP error: {e} (attempt {attempt+1})")
                time.sleep(15)
        except Exception as e:
            print(f"    error: {e} (attempt {attempt+1})")
            time.sleep(15)
    return None

def main():
    init_db()
    now = datetime.now()
    period2 = int(now.timestamp())
    # 35 years back
    period1_35y = int((now - timedelta(days=35 * 365.25)).timestamp())

    print(f"\n  ═══════════════════════════════════════════════════════")
    print(f"  ETF Data Fetcher — {len(TICKERS)} tickers")
    print(f"  35-year window: {datetime.utcfromtimestamp(period1_35y).strftime('%Y-%m-%d')} to {now.strftime('%Y-%m-%d')}")
    print(f"  Database: {DB_FILE}")
    print(f"  ═══════════════════════════════════════════════════════\n")

    # Pre-flight check
    if not wait_for_api():
        print("  Aborting — Yahoo Finance API not reachable.\n")
        return

    # ── Phase 1: Weekly historical prices ──────────────────────────
    print("\n  Phase 1: Weekly historical prices\n")
    successes = 0

    for i, ticker in enumerate(TICKERS):
        existing = count_prices(ticker)
        if existing > 500:
            print(f"  [{i+1}/{len(TICKERS)}] {ticker}: already has {existing} rows — skipping.")
            successes += 1
            continue

        if i > 0:
            delay = 15
            print(f"  ... waiting {delay}s ...")
            time.sleep(delay)

        print(f"  [{i+1}/{len(TICKERS)}] {ticker}: downloading weekly history...")
        rows = fetch_one_ticker(ticker, period1_35y, period2, interval="1wk")
        if rows:
            save_prices(rows)
            first_date = rows[0][1]
            last_date = rows[-1][1]
            print(f"  [{i+1}/{len(TICKERS)}] {ticker}: ✓ {len(rows)} weekly rows ({first_date} to {last_date})")
            successes += 1
        else:
            print(f"  [{i+1}/{len(TICKERS)}] {ticker}: ✗ all attempts failed")

    print(f"\n  Weekly history: {successes}/{len(TICKERS)} succeeded.\n")

    # ── Phase 2: YTD daily prices ──────────────────────────────────
    print("  Phase 2: YTD daily prices\n")
    ytd_start = int(datetime(now.year, 1, 1).timestamp())

    for i, ticker in enumerate(TICKERS):
        if i > 0:
            time.sleep(8)
        try:
            rows = fetch_chart(ticker, ytd_start, period2, interval="1d")
            if rows:
                save_prices(rows)
                print(f"  [{i+1}/{len(TICKERS)}] {ticker}: {len(rows)} YTD daily rows")
        except Exception as e:
            print(f"  [{i+1}/{len(TICKERS)}] {ticker}: YTD failed: {e}")

    # ── Phase 3: Ticker metadata ───────────────────────────────────
    print(f"\n  Phase 3: Ticker metadata\n")
    for i, ticker in enumerate(TICKERS):
        if i > 0:
            time.sleep(10)
        for attempt in range(3):
            try:
                info = fetch_quote_summary(ticker)
                if info:
                    save_info(ticker, info)
                    price = info.get("regularMarketPrice") or info.get("currentPrice")
                    name = info.get("shortName") or info.get("longName") or "?"
                    print(f"  [{i+1}/{len(TICKERS)}] {ticker}: ✓ {name} (${price})")
                    break
                else:
                    print(f"  [{i+1}/{len(TICKERS)}] {ticker}: no metadata returned (attempt {attempt+1})")
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 429:
                    wait = 120
                    print(f"  [{i+1}/{len(TICKERS)}] {ticker}: rate limited, waiting {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"  [{i+1}/{len(TICKERS)}] {ticker}: HTTP error: {e}")
                    break
            except Exception as e:
                print(f"  [{i+1}/{len(TICKERS)}] {ticker}: error: {e}")
                break

    # ── Summary ────────────────────────────────────────────────────
    conn = sqlite3.connect(str(DB_FILE))
    total = conn.execute("SELECT COUNT(*) FROM weekly_prices").fetchone()[0]
    print(f"\n  ═══════════════════════════════════════════════════════")
    print(f"  COMPLETE — {total} total price rows in database")
    print(f"  ═══════════════════════════════════════════════════════\n")
    print(f"  Per-ticker breakdown:")
    for ticker in TICKERS:
        r = conn.execute(
            "SELECT MIN(date), MAX(date), COUNT(*) FROM weekly_prices WHERE ticker=?",
            (ticker,)
        ).fetchone()
        if r and r[2] > 0:
            print(f"    {ticker:6s}: {r[2]:5d} rows  |  {r[0]} to {r[1]}")
        else:
            print(f"    {ticker:6s}:     0 rows  |  NO DATA")
    conn.close()
    print()

if __name__ == "__main__":
    main()
