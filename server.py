#!/usr/bin/env python3
"""ETF Compare — Flask backend with Yahoo Finance live data + fallback."""

import json
import math
import time
import threading
import traceback
from datetime import datetime, timedelta
from pathlib import Path

import yfinance as yf
from flask import Flask, jsonify, send_from_directory

app = Flask(__name__, static_folder=".", static_url_path="")

# ── Tickers & static metadata ───────────────────────────────────────
ETF_META = {
    "SPY":  {"index": "S&P 500",                     "category": "Large Cap Blend",     "description": "Tracks the S&P 500 Index — the benchmark for U.S. large-cap equities and the world's most traded ETF."},
    "IVV":  {"index": "S&P 500",                     "category": "Large Cap Blend",     "description": "Low-cost iShares S&P 500 tracker offering broad exposure to 500 of the largest U.S. companies."},
    "VOO":  {"index": "S&P 500",                     "category": "Large Cap Blend",     "description": "Vanguard's flagship S&P 500 ETF, combining ultra-low fees with full large-cap U.S. equity coverage."},
    "VTI":  {"index": "CRSP US Total Market",         "category": "Total Market",        "description": "Covers the entire U.S. equity market — large, mid, and small caps — in a single low-cost fund."},
    "QQQ":  {"index": "Nasdaq-100",                   "category": "Large Cap Growth",    "description": "Tracks the Nasdaq-100 Index, heavily weighted toward technology, communication services, and consumer discretionary."},
    "IWM":  {"index": "Russell 2000",                 "category": "Small Cap Blend",     "description": "The largest Russell 2000 tracker, providing exposure to ~2,000 U.S. small-cap stocks."},
    "VEA":  {"index": "FTSE Developed All Cap ex US", "category": "International ex-US", "description": "Diversified exposure to developed markets outside the U.S., including Europe, Japan, and Australia."},
    "IEFA": {"index": "MSCI EAFE IMI",               "category": "International ex-US", "description": "Low-cost exposure to developed-market equities in Europe, Australasia, and the Far East."},
    "VTV":  {"index": "CRSP US Large Cap Value",     "category": "Large Cap Value",     "description": "Focuses on large-cap U.S. value stocks, offering higher dividend yields with a value tilt."},
    "AGG":  {"index": "Bloomberg U.S. Aggregate Bond","category": "U.S. Aggregate Bond", "description": "Broad exposure to U.S. investment-grade bonds including treasuries, corporates, and securitized debt."},
}

TICKERS = list(ETF_META.keys())

# ── Fallback data (approximate, used when Yahoo is unavailable) ──────
FALLBACK_DATA = [
    {"rank":1,"ticker":"QQQ","name":"Invesco QQQ Trust","issuer":"Invesco","marketCap":320.6,"price":527.18,"expenseRatio":0.20,"ytdReturn":5.1,"oneYearReturn":28.4,"threeYearReturn":11.2,"fifteenYearReturn":18.5,"fifteenYearNote":"","maxDrawdown":-36.4,"drawdownPeriod":"Jan–Oct 2022","dividendYield":0.55,"avgVolume":42300000,"holdings":101,"inceptionDate":"1999-03-10","category":"Large Cap Growth","index":"Nasdaq-100","description":"Tracks the Nasdaq-100 Index, heavily weighted toward technology, communication services, and consumer discretionary."},
    {"rank":2,"ticker":"VOO","name":"Vanguard S&P 500 ETF","issuer":"Vanguard","marketCap":560.8,"price":563.21,"expenseRatio":0.03,"ytdReturn":4.22,"oneYearReturn":22.9,"threeYearReturn":10.5,"fifteenYearReturn":14.6,"fifteenYearNote":"Since inception (~15.4Y)","maxDrawdown":-33.9,"drawdownPeriod":"Feb–Mar 2020","dividendYield":1.25,"avgVolume":5200000,"holdings":503,"inceptionDate":"2010-09-07","category":"Large Cap Blend","index":"S&P 500","description":"Vanguard's flagship S&P 500 ETF, combining ultra-low fees with full large-cap U.S. equity coverage."},
    {"rank":3,"ticker":"SPY","name":"SPDR S&P 500 ETF Trust","issuer":"State Street","marketCap":630.5,"price":612.34,"expenseRatio":0.0945,"ytdReturn":4.2,"oneYearReturn":22.8,"threeYearReturn":10.4,"fifteenYearReturn":14.5,"fifteenYearNote":"","maxDrawdown":-33.9,"drawdownPeriod":"Feb–Mar 2020","dividendYield":1.22,"avgVolume":68400000,"holdings":503,"inceptionDate":"1993-01-22","category":"Large Cap Blend","index":"S&P 500","description":"Tracks the S&P 500 Index — the benchmark for U.S. large-cap equities and the world's most traded ETF."},
    {"rank":4,"ticker":"IVV","name":"iShares Core S&P 500 ETF","issuer":"BlackRock","marketCap":580.2,"price":614.87,"expenseRatio":0.03,"ytdReturn":4.25,"oneYearReturn":23.0,"threeYearReturn":10.5,"fifteenYearReturn":14.5,"fifteenYearNote":"","maxDrawdown":-33.9,"drawdownPeriod":"Feb–Mar 2020","dividendYield":1.24,"avgVolume":5900000,"holdings":503,"inceptionDate":"2000-05-15","category":"Large Cap Blend","index":"S&P 500","description":"Low-cost iShares S&P 500 tracker offering broad exposure to 500 of the largest U.S. companies."},
    {"rank":5,"ticker":"VTI","name":"Vanguard Total Stock Market ETF","issuer":"Vanguard","marketCap":440.3,"price":292.56,"expenseRatio":0.03,"ytdReturn":3.8,"oneYearReturn":21.5,"threeYearReturn":9.8,"fifteenYearReturn":14.0,"fifteenYearNote":"","maxDrawdown":-34.5,"drawdownPeriod":"Feb–Mar 2020","dividendYield":1.28,"avgVolume":3800000,"holdings":3637,"inceptionDate":"2001-05-24","category":"Total Market","index":"CRSP US Total Market","description":"Covers the entire U.S. equity market — large, mid, and small caps — in a single low-cost fund."},
    {"rank":6,"ticker":"VTV","name":"Vanguard Value ETF","issuer":"Vanguard","marketCap":130.5,"price":174.62,"expenseRatio":0.04,"ytdReturn":3.5,"oneYearReturn":18.2,"threeYearReturn":9.1,"fifteenYearReturn":11.2,"fifteenYearNote":"","maxDrawdown":-36.8,"drawdownPeriod":"Feb–Mar 2020","dividendYield":2.32,"avgVolume":2100000,"holdings":335,"inceptionDate":"2004-01-26","category":"Large Cap Value","index":"CRSP US Large Cap Value","description":"Focuses on large-cap U.S. value stocks, offering higher dividend yields with a value tilt."},
    {"rank":7,"ticker":"IWM","name":"iShares Russell 2000 ETF","issuer":"BlackRock","marketCap":75.3,"price":224.56,"expenseRatio":0.19,"ytdReturn":2.4,"oneYearReturn":15.3,"threeYearReturn":4.8,"fifteenYearReturn":9.8,"fifteenYearNote":"","maxDrawdown":-41.5,"drawdownPeriod":"Feb–Mar 2020","dividendYield":1.12,"avgVolume":23500000,"holdings":1976,"inceptionDate":"2000-05-22","category":"Small Cap Blend","index":"Russell 2000","description":"The largest Russell 2000 tracker, providing exposure to ~2,000 U.S. small-cap stocks."},
    {"rank":8,"ticker":"VEA","name":"Vanguard FTSE Developed Markets ETF","issuer":"Vanguard","marketCap":136.4,"price":52.87,"expenseRatio":0.05,"ytdReturn":6.3,"oneYearReturn":12.1,"threeYearReturn":5.4,"fifteenYearReturn":5.8,"fifteenYearNote":"","maxDrawdown":-34.3,"drawdownPeriod":"Feb–Mar 2020","dividendYield":3.02,"avgVolume":9100000,"holdings":4048,"inceptionDate":"2007-07-20","category":"International ex-US","index":"FTSE Developed All Cap ex US","description":"Diversified exposure to developed markets outside the U.S., including Europe, Japan, and Australia."},
    {"rank":9,"ticker":"IEFA","name":"iShares Core MSCI EAFE ETF","issuer":"BlackRock","marketCap":125.7,"price":79.34,"expenseRatio":0.07,"ytdReturn":6.0,"oneYearReturn":11.8,"threeYearReturn":5.1,"fifteenYearReturn":5.5,"fifteenYearNote":"Since inception (~13.3Y)","maxDrawdown":-34.0,"drawdownPeriod":"Feb–Mar 2020","dividendYield":2.85,"avgVolume":10500000,"holdings":2800,"inceptionDate":"2012-10-18","category":"International ex-US","index":"MSCI EAFE IMI","description":"Low-cost exposure to developed-market equities in Europe, Australasia, and the Far East."},
    {"rank":10,"ticker":"AGG","name":"iShares Core U.S. Aggregate Bond ETF","issuer":"BlackRock","marketCap":120.8,"price":98.42,"expenseRatio":0.03,"ytdReturn":0.8,"oneYearReturn":4.2,"threeYearReturn":-1.2,"fifteenYearReturn":1.5,"fifteenYearNote":"","maxDrawdown":-17.2,"drawdownPeriod":"Jan–Oct 2022","dividendYield":4.35,"avgVolume":7200000,"holdings":11822,"inceptionDate":"2003-09-22","category":"U.S. Aggregate Bond","index":"Bloomberg U.S. Aggregate Bond","description":"Broad exposure to U.S. investment-grade bonds including treasuries, corporates, and securitized debt."},
]

# ── Cache ────────────────────────────────────────────────────────────
_cache = {
    "data": FALLBACK_DATA,
    "time": 0,
    "fetched_at": None,
    "source": "fallback",
    "updating": False,
}
CACHE_TTL = 300  # 5 minutes
CACHE_FILE = Path(__file__).parent / ".etf_cache.json"


def load_disk_cache():
    """Load cached data from disk if available and fresh."""
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE) as f:
                disk = json.load(f)
            age = time.time() - disk.get("time", 0)
            if age < 3600:  # accept disk cache up to 1 hour old
                _cache["data"] = disk["data"]
                _cache["time"] = disk["time"]
                _cache["fetched_at"] = disk.get("fetched_at")
                _cache["source"] = "disk_cache"
                print(f"  Loaded {len(disk['data'])} ETFs from disk cache ({age/60:.0f}min old).")
                return True
        except Exception as e:
            print(f"  Could not read disk cache: {e}")
    return False


def save_disk_cache():
    """Persist cache to disk."""
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump({
                "data": _cache["data"],
                "time": _cache["time"],
                "fetched_at": _cache["fetched_at"],
            }, f)
    except Exception as e:
        print(f"  Could not write disk cache: {e}")


# ── Yahoo Finance helpers ────────────────────────────────────────────
def annualized_return(start_price, end_price, years):
    if not start_price or start_price == 0 or years <= 0:
        return 0.0
    return (math.pow(end_price / start_price, 1.0 / years) - 1) * 100


def calc_max_drawdown(close_series):
    if close_series is None or len(close_series) < 2:
        return 0.0, "N/A"
    peak = close_series.iloc[0]
    max_dd = 0.0
    dd_peak_date = close_series.index[0]
    dd_trough_date = close_series.index[0]
    cur_peak_date = close_series.index[0]
    for date, price in close_series.items():
        if price > peak:
            peak = price
            cur_peak_date = date
        dd = (price - peak) / peak
        if dd < max_dd:
            max_dd = dd
            dd_peak_date = cur_peak_date
            dd_trough_date = date
    pct = round(max_dd * 100, 2)
    fmt = lambda d: d.strftime("%b %Y")
    period = f"{fmt(dd_peak_date)}–{fmt(dd_trough_date)}" if max_dd < 0 else "N/A"
    return pct, period


def fetch_live_data():
    """Fetch live data from Yahoo Finance with conservative rate limiting."""
    _cache["updating"] = True
    try:
        print("\n  [Yahoo Finance] Starting live data fetch...")
        now = datetime.now()
        start_15y = (now - timedelta(days=15 * 365.25)).strftime("%Y-%m-%d")
        start_ytd = f"{now.year}-01-01"

        # ── Step 1: Batch download historical prices ────────────────
        print("  [Yahoo Finance] Downloading 15Y weekly history...")
        time.sleep(2)
        hist_all = yf.download(
            TICKERS, start=start_15y, interval="1wk",
            group_by="ticker", auto_adjust=True,
            threads=False, progress=False,
        )

        print("  [Yahoo Finance] Downloading YTD daily history...")
        time.sleep(3)
        hist_ytd_all = yf.download(
            TICKERS, start=start_ytd, interval="1d",
            group_by="ticker", auto_adjust=True,
            threads=False, progress=False,
        )

        # Check if batch downloads succeeded
        hist_ok = hist_all is not None and not hist_all.empty
        ytd_ok = hist_ytd_all is not None and not hist_ytd_all.empty
        if not hist_ok:
            print("  [Yahoo Finance] Historical download failed — will use fallback data.")

        # ── Step 2: Fetch individual info with 5s delays ────────────
        results = []
        for i, ticker in enumerate(TICKERS):
            meta = ETF_META.get(ticker, {})
            fallback = next((e for e in FALLBACK_DATA if e["ticker"] == ticker), {})

            # Generous delay to avoid 429
            if i > 0:
                time.sleep(5)

            print(f"  [Yahoo Finance] [{i+1}/{len(TICKERS)}] Fetching {ticker}...")
            info = {}
            for attempt in range(3):
                try:
                    t = yf.Ticker(ticker)
                    info = t.info or {}
                    if info and info.get("regularMarketPrice"):
                        break
                except Exception as e:
                    print(f"    Attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))

            # ── Extract quote fields (with fallback) ────────────────
            price = info.get("regularMarketPrice") or info.get("previousClose") or fallback.get("price", 0)
            mkt_raw = info.get("totalAssets") or info.get("marketCap") or 0
            market_cap = round(mkt_raw / 1e9, 1) if mkt_raw else fallback.get("marketCap", 0)
            avg_volume = info.get("averageDailyVolume10Day") or info.get("averageVolume") or fallback.get("avgVolume", 0)

            expense_ratio = None
            for key in ["annualReportExpenseRatio", "expenseRatio"]:
                val = info.get(key)
                if val is not None:
                    expense_ratio = round(val * 100, 4) if val < 1 else round(val, 4)
                    break
            if expense_ratio is None:
                expense_ratio = fallback.get("expenseRatio")

            div_yield = info.get("yield")
            if div_yield is not None:
                div_yield = round(div_yield * 100, 2)
            else:
                dy = info.get("trailingAnnualDividendYield")
                div_yield = round(dy * 100, 2) if dy else fallback.get("dividendYield", 0)

            holdings = info.get("totalHoldings") or fallback.get("holdings")
            inception = info.get("fundInceptionDate")
            if inception:
                try:
                    inception = datetime.utcfromtimestamp(inception).strftime("%Y-%m-%d")
                except Exception:
                    inception = fallback.get("inceptionDate")
            else:
                inception = fallback.get("inceptionDate")

            issuer = info.get("fundFamily") or fallback.get("issuer", "")
            name = info.get("shortName") or info.get("longName") or fallback.get("name", ticker)

            # ── Historical calculations ─────────────────────────────
            hist = None
            hist_ytd = None
            try:
                if hist_ok:
                    hist = hist_all[ticker]["Close"].dropna()
            except Exception:
                pass
            try:
                if ytd_ok:
                    hist_ytd = hist_ytd_all[ticker]["Close"].dropna()
            except Exception:
                pass

            # 15-year return
            fifteen_yr = fallback.get("fifteenYearReturn", 0)
            fifteen_yr_note = fallback.get("fifteenYearNote", "")
            if hist is not None and len(hist) > 10:
                years = (hist.index[-1] - hist.index[0]).days / 365.25
                fifteen_yr = round(annualized_return(hist.iloc[0], hist.iloc[-1], years), 2)
                fifteen_yr_note = f"Since inception (~{years:.1f}Y)" if years < 14.5 else ""

            # Max drawdown
            max_dd = fallback.get("maxDrawdown", 0)
            dd_period = fallback.get("drawdownPeriod", "N/A")
            if hist is not None and len(hist) > 10:
                max_dd, dd_period = calc_max_drawdown(hist)

            # YTD return
            ytd_ret = fallback.get("ytdReturn", 0)
            if hist_ytd is not None and len(hist_ytd) >= 2:
                ytd_ret = round(((hist_ytd.iloc[-1] - hist_ytd.iloc[0]) / hist_ytd.iloc[0]) * 100, 2)

            # 1Y & 3Y returns
            one_yr = fallback.get("oneYearReturn", 0)
            three_yr = fallback.get("threeYearReturn", 0)
            if hist is not None and len(hist) > 10:
                last = hist.iloc[-1]
                idx_1y = hist.index.searchsorted(now - timedelta(days=365))
                idx_1y = max(0, min(idx_1y, len(hist) - 1))
                idx_3y = hist.index.searchsorted(now - timedelta(days=3 * 365))
                idx_3y = max(0, min(idx_3y, len(hist) - 1))
                one_yr = round(((last - hist.iloc[idx_1y]) / hist.iloc[idx_1y]) * 100, 2)
                three_yr = round(annualized_return(hist.iloc[idx_3y], last, 3), 2)

            results.append({
                "ticker": ticker, "name": name, "issuer": issuer,
                "marketCap": market_cap, "price": round(price, 2),
                "expenseRatio": expense_ratio,
                "ytdReturn": ytd_ret, "oneYearReturn": one_yr,
                "threeYearReturn": three_yr,
                "fifteenYearReturn": fifteen_yr, "fifteenYearNote": fifteen_yr_note,
                "maxDrawdown": max_dd, "drawdownPeriod": dd_period,
                "dividendYield": div_yield, "avgVolume": avg_volume,
                "holdings": holdings, "inceptionDate": inception,
                "category": meta.get("category", ""), "index": meta.get("index", ""),
                "description": meta.get("description", ""),
            })

        # Sort by 15-year return descending, assign ranks
        results.sort(key=lambda e: e.get("fifteenYearReturn", 0), reverse=True)
        for i, etf in enumerate(results):
            etf["rank"] = i + 1

        # Determine if we got any live data
        has_live = any(r["price"] != fallback.get("price", 0)
                       for r, fallback in zip(results, FALLBACK_DATA))

        _cache["data"] = results
        _cache["time"] = time.time()
        _cache["fetched_at"] = datetime.utcnow().isoformat() + "Z"
        _cache["source"] = "yahoo_finance" if has_live else "fallback_enhanced"

        save_disk_cache()
        print(f"  [Yahoo Finance] Done. {len(results)} ETFs loaded (source: {_cache['source']}).\n")

    except Exception as e:
        print(f"  [Yahoo Finance] Fetch failed: {e}")
        traceback.print_exc()
    finally:
        _cache["updating"] = False


# ── API endpoint ─────────────────────────────────────────────────────
@app.route("/api/etfs")
def api_etfs():
    return jsonify({
        "data": _cache["data"],
        "cached": _cache["source"] != "yahoo_finance",
        "source": _cache["source"],
        "fetchedAt": _cache.get("fetched_at") or datetime.utcnow().isoformat() + "Z",
        "updating": _cache["updating"],
    })


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    if _cache["updating"]:
        return jsonify({"status": "already_updating"})
    thread = threading.Thread(target=fetch_live_data, daemon=True)
    thread.start()
    return jsonify({"status": "refresh_started"})


# ── Serve frontend ───────────────────────────────────────────────────
@app.route("/")
def index_page():
    return send_from_directory(".", "index.html")


# ── Startup ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n  ETF Compare dashboard starting...\n")

    # Try disk cache first
    if not load_disk_cache():
        print("  Using built-in fallback data (instant load).")

    print(f"  Dashboard ready with {len(_cache['data'])} ETFs.\n")
    print("  → http://localhost:3000\n")
    print("  Live Yahoo Finance fetch will begin in the background.\n")

    # Start background fetch after a short delay
    def delayed_fetch():
        time.sleep(3)  # let server start first
        fetch_live_data()

    bg = threading.Thread(target=delayed_fetch, daemon=True)
    bg.start()

    app.run(host="0.0.0.0", port=3000, debug=False)
