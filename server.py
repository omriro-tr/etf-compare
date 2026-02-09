#!/usr/bin/env python3
"""ETF Compare — Flask backend with SQLite persistence, Yahoo Finance,
Alpha Vantage fallback, and Statistics Denmark live housing data."""

import json
import math
import os
import sqlite3
import time
import threading
import traceback
from datetime import datetime, timedelta
from pathlib import Path

import requests as http_requests
import yfinance as yf
from flask import Flask, jsonify, request, send_from_directory

app = Flask(__name__, static_folder=".", static_url_path="")

# ── Configuration ─────────────────────────────────────────────────────
ALPHA_VANTAGE_API_KEY = os.environ.get("ALPHA_VANTAGE_API_KEY", "")
DB_FILE = Path(__file__).parent / ".etf_data.db"
CACHE_FILE = Path(__file__).parent / ".etf_cache.json"
CACHE_TTL = 604800  # 7 days — skip background fetch if DB data is less than 1 week old

# ── Tickers & static metadata ───────────────────────────────────────
ETF_META = {
    # ── U.S. Equities ────────────────────────────────────────────────
    "SPY":  {"index": "S&P 500",                     "category": "Large Cap Blend",     "description": "Tracks the S&P 500 Index — the benchmark for U.S. large-cap equities and the world's most traded ETF. Inception 1993 — the longest-running U.S. equity ETF."},
    "VTI":  {"index": "CRSP US Total Market",         "category": "Total Market",        "description": "Covers the entire U.S. equity market — large, mid, and small caps — in a single low-cost fund."},
    "QQQ":  {"index": "Nasdaq-100",                   "category": "Large Cap Growth",    "description": "Tracks the Nasdaq-100 Index, heavily weighted toward technology, communication services, and consumer discretionary."},
    "IWM":  {"index": "Russell 2000",                 "category": "Small Cap Blend",     "description": "The largest Russell 2000 tracker, providing exposure to ~2,000 U.S. small-cap stocks."},
    "IWD":  {"index": "Russell 1000 Value",           "category": "Large Cap Value",     "description": "Tracks the Russell 1000 Value Index — large-cap U.S. stocks with value characteristics. Chosen over VTV (2004) for 4 extra years of history (inception 2000)."},
    # ── International Equities ───────────────────────────────────────
    "EFA":  {"index": "MSCI EAFE",                    "category": "International Developed", "description": "Tracks the MSCI EAFE Index — developed-market equities in Europe, Australasia, and the Far East. Inception 2001 — the longest-running developed international ETF."},
    "VEA":  {"index": "FTSE Developed All Cap ex US",  "category": "International Developed", "description": "Tracks the FTSE Developed All Cap ex US Index — broad developed-market equities outside the U.S. including small caps. Lower expense ratio than EFA. Inception 2007."},
    "EEM":  {"index": "MSCI Emerging Markets",        "category": "Emerging Markets",    "description": "Tracks the MSCI Emerging Markets Index — China, India, Brazil, Taiwan, South Korea, and more. Chosen over VWO (2005) for 2 extra years of history (inception 2003)."},
    # ── Bonds ────────────────────────────────────────────────────────
    "TLT":  {"index": "ICE U.S. Treasury 20+ Year",  "category": "Long-Term Treasury",  "description": "Exposure to long-duration U.S. Treasury bonds (20+ years). Often negatively correlated with equities during market stress — a classic flight-to-safety asset."},
    # ── Real Assets & Alternatives ───────────────────────────────────
    "GLD":  {"index": "Gold Spot Price (LBMA)",       "category": "Commodity — Gold",    "description": "Physically-backed gold ETF. Gold has near-zero long-term correlation with equities and serves as an inflation hedge and safe-haven asset."},
    "IYR":  {"index": "Dow Jones U.S. Real Estate",   "category": "U.S. Real Estate (REITs)", "description": "Tracks the Dow Jones U.S. Real Estate Index — diversified REIT exposure. Chosen over VNQ (2004) for 4 extra years of history (inception 2000)."},
    "DBC":  {"index": "DBIQ Optimum Yield Diversified Commodity", "category": "Broad Commodities", "description": "Diversified commodity futures — energy, precious metals, industrial metals, and agriculture. Low correlation to stocks; natural inflation hedge."},
    # ── Non-ETF / Static ─────────────────────────────────────────────
    "CPH-RE": {"index": "Copenhagen Apartment Index", "category": "Real Estate — Copenhagen", "description": "Copenhagen residential apartment prices (price per m²) + estimated ~3.5% annual gross rent yield. Price data from Statistics Denmark / Finance Denmark. Rent yield is an approximation.", "static": True},
}

TICKERS = [t for t in ETF_META if not ETF_META[t].get("static")]
STATIC_TICKERS = [t for t in ETF_META if ETF_META[t].get("static")]

# ── Index backers: older tickers to extend ETF data before inception ──
# Maps ETF ticker → ordered list of (yahoo_ticker, description, includes_dividends).
# They are chain-spliced in order: first backer fills gap closest to ETF inception,
# second backer fills the gap before that, etc.  Prefer total-return (mutual fund)
# sources over price-only indices so that dividends are included.
INDEX_BACKERS = {
    "SPY": [("VFINX", "Vanguard 500 Fund (total return)", True),        # 1980+
            ("^GSPC", "S&P 500 Index (price only)", False)],             # 1970+
    "VTI": [("VTSMX", "Vanguard Total Mkt Fund (total return)", True),  # 1992+
            ("VFINX", "Vanguard 500 Fund (total return)", True),         # 1980+
            ("^GSPC", "S&P 500 Index (price only)", False)],             # 1970+
    "QQQ": [("^IXIC", "Nasdaq Composite (price only, ~0.5% div gap)", False)],  # 1971+
    "IWM": [("^RUT", "Russell 2000 (price only, ~1% div gap)", False)],  # 1987+
    "IWD": [("^DJI", "Dow Jones Ind. Avg (price only, ~2% div gap)", False)],  # 1992+
    "TLT": [("VUSTX", "Vanguard LT Treasury Fund (total return)", True)],  # 1986+
    "GLD": [("GC=F", "Gold Futures (no dividends for gold)", True)],     # 2000+
    "EEM": [("VEIEX", "Vanguard EM Index Fund (total return)", True)],   # 1994+
    "VEA": [("EFA", "iShares MSCI EAFE ETF (adj. close)", True)],       # 2001+
    "IYR": [],  # already from 2000
    "DBC": [],  # already from 2006, no good commodity index on Yahoo
    "EFA": [],  # already from 2001
}

# ── Fallback data (used when DB is empty and network unavailable) ────
FALLBACK_DATA = [
    # ── U.S. Equities ────────────────────────────────────────────────
    {"rank":1,"ticker":"QQQ","name":"Invesco QQQ Trust","issuer":"Invesco","marketCap":320.6,"price":527.18,"expenseRatio":0.20,"ytdReturn":5.1,"oneYearReturn":28.4,"threeYearReturn":11.2,"fiveYearReturn":18.0,"fiveYearNote":"","tenYearReturn":18.8,"tenYearNote":"","fifteenYearReturn":18.5,"fifteenYearNote":"","twentyYearReturn":15.5,"twentyYearNote":"","twentyFiveYearReturn":12.5,"twentyFiveYearNote":"","maxDrawdown":-83.0,"drawdownPeriod":"Mar 2000–Oct 2002","drawdownLabel":"Dot-com bust","secondDrawdown":-56.2,"secondDrawdownPeriod":"Oct 2007–Mar 2009","secondDrawdownLabel":"Global financial crisis","dataStart":"1999-03-01","dividendYield":0.55,"avgVolume":42300000,"holdings":101,"inceptionDate":"1999-03-10","category":"Large Cap Growth","index":"Nasdaq-100","description":"Tracks the Nasdaq-100 Index, heavily weighted toward technology, communication services, and consumer discretionary."},
    {"rank":2,"ticker":"SPY","name":"SPDR S&P 500 ETF Trust","issuer":"State Street","marketCap":630.5,"price":612.34,"expenseRatio":0.0945,"ytdReturn":4.2,"oneYearReturn":22.8,"threeYearReturn":10.4,"fiveYearReturn":14.5,"fiveYearNote":"","tenYearReturn":13.0,"tenYearNote":"","fifteenYearReturn":14.5,"fifteenYearNote":"","twentyYearReturn":10.5,"twentyYearNote":"","twentyFiveYearReturn":8.2,"twentyFiveYearNote":"","maxDrawdown":-56.8,"drawdownPeriod":"Oct 2007–Mar 2009","drawdownLabel":"Global financial crisis","secondDrawdown":-49.1,"secondDrawdownPeriod":"Mar 2000–Oct 2002","secondDrawdownLabel":"Dot-com bust","dataStart":"1993-02-01","dividendYield":1.22,"avgVolume":68400000,"holdings":503,"inceptionDate":"1993-01-22","category":"Large Cap Blend","index":"S&P 500","description":"Tracks the S&P 500 Index — the benchmark for U.S. large-cap equities and the world's most traded ETF. Inception 1993 — the longest-running U.S. equity ETF."},
    {"rank":3,"ticker":"VTI","name":"Vanguard Total Stock Market ETF","issuer":"Vanguard","marketCap":440.3,"price":292.56,"expenseRatio":0.03,"ytdReturn":3.8,"oneYearReturn":21.5,"threeYearReturn":9.8,"fiveYearReturn":14.0,"fiveYearNote":"","tenYearReturn":12.5,"tenYearNote":"","fifteenYearReturn":14.0,"fifteenYearNote":"","twentyYearReturn":10.2,"twentyYearNote":"","twentyFiveYearReturn":None,"twentyFiveYearNote":"","maxDrawdown":-55.9,"drawdownPeriod":"Oct 2007–Mar 2009","drawdownLabel":"Global financial crisis","secondDrawdown":-33.8,"secondDrawdownPeriod":"Feb 2020–Mar 2020","secondDrawdownLabel":"COVID-19 crash","dataStart":"2001-06-01","dividendYield":1.28,"avgVolume":3800000,"holdings":3637,"inceptionDate":"2001-05-24","category":"Total Market","index":"CRSP US Total Market","description":"Covers the entire U.S. equity market — large, mid, and small caps — in a single low-cost fund."},
    {"rank":4,"ticker":"IWD","name":"iShares Russell 1000 Value ETF","issuer":"BlackRock","marketCap":62.0,"price":185.40,"expenseRatio":0.19,"ytdReturn":3.2,"oneYearReturn":17.5,"threeYearReturn":8.8,"fiveYearReturn":11.0,"fiveYearNote":"","tenYearReturn":9.5,"tenYearNote":"","fifteenYearReturn":10.8,"fifteenYearNote":"","twentyYearReturn":8.5,"twentyYearNote":"","twentyFiveYearReturn":7.8,"twentyFiveYearNote":"","maxDrawdown":-59.5,"drawdownPeriod":"Oct 2007–Mar 2009","drawdownLabel":"Global financial crisis","secondDrawdown":-33.5,"secondDrawdownPeriod":"Feb 2020–Mar 2020","secondDrawdownLabel":"COVID-19 crash","dataStart":"2000-06-01","dividendYield":2.0,"avgVolume":3500000,"holdings":849,"inceptionDate":"2000-05-22","category":"Large Cap Value","index":"Russell 1000 Value","description":"Tracks the Russell 1000 Value Index — large-cap U.S. stocks with value characteristics. Chosen over VTV (2004) for 4 extra years of history (inception 2000)."},
    {"rank":5,"ticker":"IWM","name":"iShares Russell 2000 ETF","issuer":"BlackRock","marketCap":75.3,"price":224.56,"expenseRatio":0.19,"ytdReturn":2.4,"oneYearReturn":15.3,"threeYearReturn":4.8,"fiveYearReturn":9.0,"fiveYearNote":"","tenYearReturn":8.5,"tenYearNote":"","fifteenYearReturn":9.8,"fifteenYearNote":"","twentyYearReturn":8.5,"twentyYearNote":"","twentyFiveYearReturn":8.0,"twentyFiveYearNote":"","maxDrawdown":-59.9,"drawdownPeriod":"Oct 2007–Mar 2009","drawdownLabel":"Global financial crisis","secondDrawdown":-41.6,"secondDrawdownPeriod":"Feb 2020–Mar 2020","secondDrawdownLabel":"COVID-19 crash","dataStart":"2000-06-01","dividendYield":1.12,"avgVolume":23500000,"holdings":1976,"inceptionDate":"2000-05-22","category":"Small Cap Blend","index":"Russell 2000","description":"The largest Russell 2000 tracker, providing exposure to ~2,000 U.S. small-cap stocks."},
    # ── International Equities ───────────────────────────────────────
    {"rank":6,"ticker":"EFA","name":"iShares MSCI EAFE ETF","issuer":"BlackRock","marketCap":100.0,"price":82.50,"expenseRatio":0.32,"ytdReturn":5.8,"oneYearReturn":11.5,"threeYearReturn":5.0,"fiveYearReturn":7.0,"fiveYearNote":"","tenYearReturn":5.5,"tenYearNote":"","fifteenYearReturn":5.5,"fifteenYearNote":"","twentyYearReturn":4.5,"twentyYearNote":"","twentyFiveYearReturn":None,"twentyFiveYearNote":"","maxDrawdown":-62.4,"drawdownPeriod":"Oct 2007–Mar 2009","drawdownLabel":"Global financial crisis","secondDrawdown":-33.5,"secondDrawdownPeriod":"Feb 2020–Mar 2020","secondDrawdownLabel":"COVID-19 crash","dataStart":"2001-08-01","dividendYield":2.8,"avgVolume":18000000,"holdings":782,"inceptionDate":"2001-08-14","category":"International Developed","index":"MSCI EAFE","description":"Tracks the MSCI EAFE Index — developed-market equities in Europe, Australasia, and the Far East. Inception 2001 — the longest-running developed international ETF."},
    {"rank":7,"ticker":"VEA","name":"Vanguard FTSE Developed Markets ETF","issuer":"Vanguard","marketCap":130.0,"price":51.20,"expenseRatio":0.05,"ytdReturn":5.5,"oneYearReturn":10.8,"threeYearReturn":4.5,"fiveYearReturn":6.5,"fiveYearNote":"","tenYearReturn":5.0,"tenYearNote":"","fifteenYearReturn":5.0,"fifteenYearNote":"","twentyYearReturn":None,"twentyYearNote":"","twentyFiveYearReturn":None,"twentyFiveYearNote":"","maxDrawdown":-58.5,"drawdownPeriod":"Oct 2007–Mar 2009","drawdownLabel":"Global financial crisis","secondDrawdown":-34.0,"secondDrawdownPeriod":"Feb 2020–Mar 2020","secondDrawdownLabel":"COVID-19 crash","dataStart":"2007-07-01","dividendYield":3.0,"avgVolume":9500000,"holdings":4048,"inceptionDate":"2007-07-20","category":"International Developed","index":"FTSE Developed All Cap ex US","description":"Tracks the FTSE Developed All Cap ex US Index — broad developed-market equities outside the U.S. including small caps. Lower expense ratio than EFA. Inception 2007."},
    {"rank":8,"ticker":"EEM","name":"iShares MSCI Emerging Markets ETF","issuer":"BlackRock","marketCap":19.0,"price":43.80,"expenseRatio":0.70,"ytdReturn":2.0,"oneYearReturn":8.0,"threeYearReturn":1.2,"fiveYearReturn":3.5,"fiveYearNote":"","tenYearReturn":2.0,"tenYearNote":"","fifteenYearReturn":2.5,"fifteenYearNote":"","twentyYearReturn":5.0,"twentyYearNote":"","twentyFiveYearReturn":None,"twentyFiveYearNote":"","maxDrawdown":-65.1,"drawdownPeriod":"Oct 2007–Mar 2009","drawdownLabel":"Global financial crisis","secondDrawdown":-36.0,"secondDrawdownPeriod":"Feb 2020–Mar 2020","secondDrawdownLabel":"COVID-19 crash","dataStart":"2003-04-01","dividendYield":2.5,"avgVolume":16000000,"holdings":1280,"inceptionDate":"2003-04-07","category":"Emerging Markets","index":"MSCI Emerging Markets","description":"Tracks the MSCI Emerging Markets Index — China, India, Brazil, Taiwan, South Korea, and more. Chosen over VWO (2005) for 2 extra years of history (inception 2003)."},
    # ── Bonds ────────────────────────────────────────────────────────
    {"rank":10,"ticker":"TLT","name":"iShares 20+ Year Treasury Bond ETF","issuer":"BlackRock","marketCap":50.5,"price":88.60,"expenseRatio":0.15,"ytdReturn":-1.2,"oneYearReturn":-3.5,"threeYearReturn":-8.0,"fiveYearReturn":-6.0,"fiveYearNote":"","tenYearReturn":-1.5,"tenYearNote":"","fifteenYearReturn":1.5,"fifteenYearNote":"","twentyYearReturn":2.8,"twentyYearNote":"","twentyFiveYearReturn":None,"twentyFiveYearNote":"","maxDrawdown":-52.3,"drawdownPeriod":"Aug 2020–Oct 2023","drawdownLabel":"Rate hikes / inflation","secondDrawdown":-12.5,"secondDrawdownPeriod":"Jul 2016–Nov 2018","secondDrawdownLabel":"Fed tightening / trade war","dataStart":"2002-08-01","dividendYield":4.0,"avgVolume":25000000,"holdings":40,"inceptionDate":"2002-07-22","category":"Long-Term Treasury","index":"ICE U.S. Treasury 20+ Year","description":"Exposure to long-duration U.S. Treasury bonds (20+ years). Often negatively correlated with equities during market stress — a classic flight-to-safety asset."},
    # ── Real Assets & Alternatives ───────────────────────────────────
    {"rank":11,"ticker":"GLD","name":"SPDR Gold Shares","issuer":"State Street","marketCap":72.0,"price":243.50,"expenseRatio":0.40,"ytdReturn":4.2,"oneYearReturn":26.5,"threeYearReturn":12.8,"fiveYearReturn":12.0,"fiveYearNote":"","tenYearReturn":9.0,"tenYearNote":"","fifteenYearReturn":8.2,"fifteenYearNote":"","twentyYearReturn":9.5,"twentyYearNote":"","twentyFiveYearReturn":None,"twentyFiveYearNote":"","maxDrawdown":-45.5,"drawdownPeriod":"Sep 2011–Dec 2015","drawdownLabel":"China / commodity selloff","secondDrawdown":-18.5,"secondDrawdownPeriod":"Aug 2020–Mar 2021","secondDrawdownLabel":"","dataStart":"2004-11-01","dividendYield":0.0,"avgVolume":7200000,"holdings":None,"inceptionDate":"2004-11-18","category":"Commodity — Gold","index":"Gold Spot Price (LBMA)","description":"Physically-backed gold ETF. Gold has near-zero long-term correlation with equities and serves as an inflation hedge and safe-haven asset."},
    {"rank":12,"ticker":"IYR","name":"iShares U.S. Real Estate ETF","issuer":"BlackRock","marketCap":4.5,"price":90.20,"expenseRatio":0.39,"ytdReturn":1.2,"oneYearReturn":5.0,"threeYearReturn":1.8,"fiveYearReturn":4.0,"fiveYearNote":"","tenYearReturn":5.5,"tenYearNote":"","fifteenYearReturn":7.5,"fifteenYearNote":"","twentyYearReturn":7.0,"twentyYearNote":"","twentyFiveYearReturn":8.0,"twentyFiveYearNote":"","maxDrawdown":-76.5,"drawdownPeriod":"Feb 2007–Mar 2009","drawdownLabel":"Global financial crisis","secondDrawdown":-31.5,"secondDrawdownPeriod":"Feb 2020–Mar 2020","secondDrawdownLabel":"COVID-19 crash","dataStart":"2000-07-01","dividendYield":3.0,"avgVolume":5000000,"holdings":70,"inceptionDate":"2000-06-12","category":"U.S. Real Estate (REITs)","index":"Dow Jones U.S. Real Estate","description":"Tracks the Dow Jones U.S. Real Estate Index — diversified REIT exposure. Chosen over VNQ (2004) for 4 extra years of history (inception 2000)."},
    {"rank":13,"ticker":"DBC","name":"Invesco DB Commodity Index ETF","issuer":"Invesco","marketCap":3.2,"price":22.80,"expenseRatio":0.85,"ytdReturn":2.5,"oneYearReturn":5.0,"threeYearReturn":3.0,"fiveYearReturn":8.0,"fiveYearNote":"","tenYearReturn":0.5,"tenYearNote":"","fifteenYearReturn":0.5,"fifteenYearNote":"","twentyYearReturn":None,"twentyYearNote":"","twentyFiveYearReturn":None,"twentyFiveYearNote":"","maxDrawdown":-55.0,"drawdownPeriod":"Jun 2008–Feb 2009","drawdownLabel":"Global financial crisis","secondDrawdown":-48.0,"secondDrawdownPeriod":"Jun 2014–Jan 2016","secondDrawdownLabel":"China / commodity selloff","dataStart":"2006-02-01","dividendYield":0.0,"avgVolume":2000000,"holdings":None,"inceptionDate":"2006-02-03","category":"Broad Commodities","index":"DBIQ Optimum Yield Diversified Commodity","description":"Diversified commodity futures — energy, precious metals, industrial metals, and agriculture. Low correlation to stocks; natural inflation hedge."},
    # ── Non-ETF / Static ─────────────────────────────────────────────
    {"rank":14,"ticker":"CPH-RE","name":"Copenhagen Apartments (price/m² + rent)","issuer":"Statistics Denmark","marketCap":None,"price":None,"expenseRatio":None,"ytdReturn":7.0,"oneYearReturn":8.8,"threeYearReturn":7.7,"fiveYearReturn":8.5,"fiveYearNote":"incl. ~3.5% rent (price only: 5.0%)","tenYearReturn":10.0,"tenYearNote":"incl. ~3.5% rent (price only: 6.5%)","fifteenYearReturn":11.3,"fifteenYearNote":"incl. ~3.5% rent (price only: 7.8%)","twentyYearReturn":10.0,"twentyYearNote":"incl. ~3.5% rent (price only: 6.5%)","twentyFiveYearReturn":9.4,"twentyFiveYearNote":"incl. ~3.5% rent (price only: 5.9%)","maxDrawdown":-30.0,"drawdownPeriod":"2007–2012","drawdownLabel":"Global financial crisis","secondDrawdown":None,"secondDrawdownPeriod":None,"secondDrawdownLabel":"","dataStart":"1992-01-01","dividendYield":3.5,"avgVolume":None,"holdings":None,"inceptionDate":"1992-01-01","category":"Real Estate — Copenhagen","index":"Copenhagen Apartment Index","description":"Copenhagen residential apartment prices (price per m²) + estimated ~3.5% annual gross rent yield. Price data from Statistics Denmark / Finance Denmark. Rent yield is an approximation."},
]

# ── In-memory cache ──────────────────────────────────────────────────
_cache = {
    "data": FALLBACK_DATA,
    "time": 0,
    "fetched_at": None,
    "source": "fallback",
    "updating": False,
}


# ══════════════════════════════════════════════════════════════════════
#  SQLite Database Layer
# ══════════════════════════════════════════════════════════════════════

def _db():
    """Get a new SQLite connection (thread-safe — one connection per call)."""
    conn = sqlite3.connect(str(DB_FILE))
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    """Create tables if they don't exist."""
    conn = _db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS weekly_prices (
            ticker TEXT NOT NULL,
            date   TEXT NOT NULL,
            close  REAL NOT NULL,
            PRIMARY KEY (ticker, date)
        );
        CREATE TABLE IF NOT EXISTS ticker_info (
            ticker     TEXT PRIMARY KEY,
            data       TEXT NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS cph_housing (
            date        TEXT PRIMARY KEY,
            index_value REAL NOT NULL
        );
    """)
    conn.commit()
    conn.close()


def db_last_date(ticker):
    """Most recent price date for a ticker, or None."""
    conn = _db()
    r = conn.execute("SELECT MAX(date) FROM weekly_prices WHERE ticker=?", (ticker,)).fetchone()
    conn.close()
    return r[0] if r and r[0] else None


def db_price_count(ticker):
    """Number of stored price rows for a ticker."""
    conn = _db()
    r = conn.execute("SELECT COUNT(*) FROM weekly_prices WHERE ticker=?", (ticker,)).fetchone()
    conn.close()
    return r[0] if r else 0


def db_total_prices():
    """Total price rows across all tickers."""
    conn = _db()
    r = conn.execute("SELECT COUNT(*) FROM weekly_prices").fetchone()
    conn.close()
    return r[0] if r else 0


def db_newest_date():
    """Most recent price date across ALL tickers (not backers), or None."""
    conn = _db()
    placeholders = ",".join("?" for _ in TICKERS)
    r = conn.execute(
        f"SELECT MAX(date) FROM weekly_prices WHERE ticker IN ({placeholders})",
        list(TICKERS)
    ).fetchone()
    conn.close()
    return r[0] if r and r[0] else None


def db_data_age_days():
    """How many days old is the most recent price row? Returns float or None."""
    newest = db_newest_date()
    if not newest:
        return None
    try:
        newest_dt = datetime.strptime(newest[:10], "%Y-%m-%d")
        return (datetime.now() - newest_dt).days
    except Exception:
        return None


def db_save_prices(rows):
    """Save list of (ticker, date_str, close) to weekly_prices."""
    if not rows:
        return
    conn = _db()
    conn.executemany("INSERT OR REPLACE INTO weekly_prices VALUES (?,?,?)", rows)
    conn.commit()
    conn.close()


def db_load_prices(ticker):
    """Load all (date_str, close) for a ticker, ordered by date."""
    conn = _db()
    rows = conn.execute(
        "SELECT date, close FROM weekly_prices WHERE ticker=? ORDER BY date",
        (ticker,)
    ).fetchall()
    conn.close()
    return rows


def db_save_info(ticker, info_dict):
    """Save ticker info dict as JSON with timestamp."""
    conn = _db()
    conn.execute(
        "INSERT OR REPLACE INTO ticker_info VALUES (?,?,?)",
        (ticker, json.dumps(info_dict, default=str), time.time())
    )
    conn.commit()
    conn.close()


def db_load_info(ticker):
    """Load ticker info. Returns (dict, updated_at) or (None, 0)."""
    conn = _db()
    r = conn.execute("SELECT data, updated_at FROM ticker_info WHERE ticker=?", (ticker,)).fetchone()
    conn.close()
    if r:
        try:
            return json.loads(r[0]), r[1]
        except Exception:
            return None, 0
    return None, 0


def db_save_housing(rows):
    """Save list of (date_str, index_value) to cph_housing."""
    if not rows:
        return
    conn = _db()
    conn.executemany("INSERT OR REPLACE INTO cph_housing VALUES (?,?)", rows)
    conn.commit()
    conn.close()


def db_load_housing():
    """Load all (date_str, index_value), ordered by date."""
    conn = _db()
    rows = conn.execute("SELECT date, index_value FROM cph_housing ORDER BY date").fetchall()
    conn.close()
    return rows


def db_last_housing_date():
    conn = _db()
    r = conn.execute("SELECT MAX(date) FROM cph_housing").fetchone()
    conn.close()
    return r[0] if r and r[0] else None


# ══════════════════════════════════════════════════════════════════════
#  Math / Computation Helpers
# ══════════════════════════════════════════════════════════════════════

def annualized_return(start_price, end_price, years):
    if not start_price or start_price == 0 or years <= 0:
        return 0.0
    return (math.pow(end_price / start_price, 1.0 / years) - 1) * 100


def cumulative_return(annualized_pct, years):
    """Convert an annualized return (%) to a cumulative return (%) over *years*.
    E.g. 10% annualized over 10 years → ((1.10)^10 − 1) × 100 ≈ 159.37%"""
    if annualized_pct is None:
        return None
    return round((math.pow(1 + annualized_pct / 100, years) - 1) * 100, 1)


def best_annualized_return(entry):
    """Pick the longest-horizon annualized return available for an entry.
    Prefers 25Y > 20Y > 15Y > 10Y > 5Y."""
    for key in ("twentyFiveYearReturn", "twentyYearReturn", "fifteenYearReturn",
                "tenYearReturn", "fiveYearReturn"):
        val = entry.get(key)
        if val is not None:
            return val
    return None


RISK_FREE_RATE = 4.0  # annualized %, approximate current T-bill yield

# Approximate annualized standard deviations (%) for fallback use.
# Based on long-term (10Y+) historical weekly-return volatility.
FALLBACK_STDDEV = {
    "SPY": 15.2, "QQQ": 20.5, "VTI": 15.5, "IWM": 20.0, "IWD": 15.0,
    "EFA": 16.5, "VEA": 16.5, "EEM": 21.0,
    "TLT": 17.0,
    "GLD": 16.0, "IYR": 21.0, "DBC": 18.0,
    "CPH-RE": 8.0,
}

# Approximate annual gross rent yield for Copenhagen apartments (% of property value).
# Source: various Danish real-estate reports — typical range 3–4% gross.
# Net of maintenance/taxes/vacancy it's lower (~2–3%), but we use gross to approximate
# "total return" comparable to dividend-reinvesting ETF indices.
CPH_RENT_YIELD = 3.5  # annualized %


def calc_annualized_stddev(prices):
    """Compute annualized standard deviation from a list of (date, price) pairs.
    Uses weekly returns, annualized by √52."""
    if len(prices) < 52:
        return None
    # Weekly returns
    returns = []
    for k in range(1, len(prices)):
        _, c = prices[k]
        _, prev_c = prices[k - 1]
        if prev_c > 0:
            returns.append((c - prev_c) / prev_c)
    if len(returns) < 26:
        return None
    mean = sum(returns) / len(returns)
    var = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    weekly_std = math.sqrt(var)
    return round(weekly_std * math.sqrt(52) * 100, 2)  # annualized %


def calc_sharpe(annualized_return_pct, stddev_pct, risk_free=RISK_FREE_RATE):
    """Sharpe ratio = (return - risk-free) / std dev."""
    if annualized_return_pct is None or stddev_pct is None or stddev_pct == 0:
        return None
    return round((annualized_return_pct - risk_free) / stddev_pct, 2)


# Enrich FALLBACK_DATA with computed cumulative returns for each period
for _fb_entry in FALLBACK_DATA:
    _fb_entry["fiveYearCumulativeReturn"] = cumulative_return(_fb_entry.get("fiveYearReturn"), 5)
    _fb_entry["tenYearCumulativeReturn"] = cumulative_return(_fb_entry.get("tenYearReturn"), 10)
    _fb_entry["fifteenYearCumulativeReturn"] = cumulative_return(_fb_entry.get("fifteenYearReturn"), 15)
    _fb_entry["twentyYearCumulativeReturn"] = cumulative_return(_fb_entry.get("twentyYearReturn"), 20)
    _fb_entry["twentyFiveYearCumulativeReturn"] = cumulative_return(_fb_entry.get("twentyFiveYearReturn"), 25)
    _fb_entry["fortyYearCumulativeReturn"] = cumulative_return(best_annualized_return(_fb_entry), 40)
    _fb_std = FALLBACK_STDDEV.get(_fb_entry["ticker"])
    _fb_entry["annualizedStdDev"] = _fb_std
    _fb_entry["sharpeRatio"] = calc_sharpe(_fb_entry.get("tenYearReturn"), _fb_std)
    # Estimate since-inception: use best available return as proxy
    _fb_entry["sinceInceptionReturn"] = best_annualized_return(_fb_entry)
    _fb_ds = _fb_entry.get("dataStart")
    if _fb_ds:
        try:
            _fb_yrs = (datetime.now() - datetime.strptime(str(_fb_ds)[:10], "%Y-%m-%d")).days / 365.25
            _fb_entry["sinceInceptionYears"] = round(_fb_yrs, 1)
        except Exception:
            _fb_entry["sinceInceptionYears"] = 0
    else:
        _fb_entry["sinceInceptionYears"] = 0
    # Estimate since-1990 return: use since-inception if data starts before 1990
    _fb_entry["since1990Return"] = None
    _fb_entry["since1990Years"] = 0
    _fb_entry["since1990CumulativeReturn"] = None
    if _fb_ds and str(_fb_ds)[:4] <= "1990":
        _fb_entry["since1990Return"] = best_annualized_return(_fb_entry)
        try:
            _1990_yrs = (datetime.now() - datetime(1990, 1, 1)).days / 365.25
            _fb_entry["since1990Years"] = round(_1990_yrs, 1)
            _fb_entry["since1990CumulativeReturn"] = cumulative_return(
                _fb_entry["since1990Return"], _fb_entry["since1990Years"])
        except Exception:
            pass


def _fmt_dd_date(d):
    try:
        return datetime.strptime(d[:10], "%Y-%m-%d").strftime("%b %Y")
    except Exception:
        return d

# ── Known drawdown event labels (based on trough year) ──────────────
DRAWDOWN_LABELS = {
    (2001, 2003): "Dot-com bust",
    (2002, 2003): "Dot-com bust",
    (2008, 2009): "Global financial crisis",
    (2009, 2009): "Global financial crisis",
    (2011, 2012): "European debt crisis",
    (2015, 2016): "China / commodity selloff",
    (2018, 2019): "Fed tightening / trade war",
    (2020, 2020): "COVID-19 crash",
    (2022, 2023): "Rate hikes / inflation",
    (2023, 2024): "Rate hikes / inflation",
}

def _label_drawdown(trough_date_str):
    """Try to assign a known event label to a drawdown based on the trough date."""
    try:
        yr = int(trough_date_str[:4])
    except Exception:
        return ""
    for (y1, y2), label in DRAWDOWN_LABELS.items():
        if y1 <= yr <= y2:
            return label
    return ""


def calc_drawdowns(prices):
    """Compute the two largest peak-to-trough drawdowns at least 2 years apart.
    Returns dict with max/second drawdown info."""
    result = {
        "maxDrawdown": 0.0, "drawdownPeriod": "N/A", "drawdownLabel": "",
        "maxDdPeak": None, "maxDdTrough": None,
        "secondDrawdown": None, "secondDrawdownPeriod": None, "secondDrawdownLabel": "",
    }
    if len(prices) < 2:
        return result

    # Step 1: identify ALL drawdown events (peak → trough pairs)
    drawdown_events = []
    peak = prices[0][1]
    cur_peak_date = prices[0][0]
    cur_dd = 0.0
    cur_trough_date = prices[0][0]

    for date_str, price in prices:
        if price > peak:
            # When a new peak is hit, record the previous drawdown if significant
            if cur_dd < -0.05:  # Only track drawdowns > 5%
                drawdown_events.append((cur_dd, cur_peak_date, cur_trough_date))
            peak = price
            cur_peak_date = date_str
            cur_dd = 0.0
            cur_trough_date = date_str
        dd = (price - peak) / peak
        if dd < cur_dd:
            cur_dd = dd
            cur_trough_date = date_str

    # Record the last drawdown if we haven't recovered
    if cur_dd < -0.05:
        drawdown_events.append((cur_dd, cur_peak_date, cur_trough_date))

    if not drawdown_events:
        return result

    # Sort by drawdown severity (most negative first)
    drawdown_events.sort(key=lambda x: x[0])

    # Best (worst) drawdown
    best = drawdown_events[0]
    result["maxDrawdown"] = round(best[0] * 100, 2)
    result["drawdownPeriod"] = f"{_fmt_dd_date(best[1])}–{_fmt_dd_date(best[2])}"
    result["drawdownLabel"] = _label_drawdown(best[2])
    result["maxDdPeak"] = best[1]
    result["maxDdTrough"] = best[2]

    # Second-largest drawdown: must be at least 2 years apart from the first trough
    try:
        best_trough_year = int(best[2][:4])
    except Exception:
        best_trough_year = 0

    for ev in drawdown_events[1:]:
        try:
            ev_trough_year = int(ev[2][:4])
        except Exception:
            continue
        if abs(ev_trough_year - best_trough_year) >= 2:
            result["secondDrawdown"] = round(ev[0] * 100, 2)
            result["secondDrawdownPeriod"] = f"{_fmt_dd_date(ev[1])}–{_fmt_dd_date(ev[2])}"
            result["secondDrawdownLabel"] = _label_drawdown(ev[2])
            break

    return result


def calc_period_return(prices, target_years):
    """prices: list of (date_str, close). Returns (return_pct, note) or (None, '')."""
    if len(prices) < 10:
        return None, ""
    last_date_str, last_close = prices[-1]
    first_date_str = prices[0][0]
    last_date = datetime.strptime(last_date_str[:10], "%Y-%m-%d")
    first_date = datetime.strptime(first_date_str[:10], "%Y-%m-%d")
    available_years = (last_date - first_date).days / 365.25
    if available_years < target_years - 0.5:
        return None, ""
    target_date_str = (last_date - timedelta(days=target_years * 365.25)).strftime("%Y-%m-%d")
    # Find closest date >= target
    idx = 0
    for i, (d, _) in enumerate(prices):
        if d >= target_date_str:
            idx = i
            break
    start_close = prices[idx][1]
    actual_start = datetime.strptime(prices[idx][0][:10], "%Y-%m-%d")
    actual_years = (last_date - actual_start).days / 365.25
    if actual_years < 1:
        return None, ""
    ret = round(annualized_return(start_close, last_close, actual_years), 2)
    note = ""
    if actual_years < target_years - 0.5:
        note = f"Since inception (~{available_years:.1f}Y)"
    return ret, note


def calc_since_inception_return(prices):
    """Compute annualized return using the full available price series.
    Returns (return_pct, years_of_data) or (None, 0)."""
    if len(prices) < 10:
        return None, 0
    first_date_str, first_close = prices[0]
    last_date_str, last_close = prices[-1]
    first_date = datetime.strptime(first_date_str[:10], "%Y-%m-%d")
    last_date = datetime.strptime(last_date_str[:10], "%Y-%m-%d")
    total_years = (last_date - first_date).days / 365.25
    if total_years < 1 or first_close <= 0:
        return None, 0
    ret = round(annualized_return(first_close, last_close, total_years), 2)
    return ret, round(total_years, 1)


def calc_since_date_return(prices, since_date_str):
    """Compute annualized return from a specific start date (e.g. '1990-01-01').
    Returns (return_pct, years) or (None, 0) if data doesn't reach that date."""
    if len(prices) < 10:
        return None, 0
    first_available = prices[0][0]
    if first_available > since_date_str:
        return None, 0  # data doesn't go back far enough
    # Find closest date >= since_date_str
    idx = 0
    for i, (d, _) in enumerate(prices):
        if d >= since_date_str:
            idx = i
            break
    start_close = prices[idx][1]
    if start_close <= 0:
        return None, 0
    last_date_str, last_close = prices[-1]
    start_date = datetime.strptime(prices[idx][0][:10], "%Y-%m-%d")
    end_date = datetime.strptime(last_date_str[:10], "%Y-%m-%d")
    years = (end_date - start_date).days / 365.25
    if years < 1:
        return None, 0
    ret = round(annualized_return(start_close, last_close, years), 2)
    return ret, round(years, 1)


def _compute_short_returns(prices, fallback, now):
    """Compute YTD, 1Y, 3Y returns from a price list. Returns (ytd, one_yr, three_yr)."""
    ytd_start = f"{now.year}-01-01"
    if len(prices) < 10:
        return (fallback.get("ytdReturn", 0),
                fallback.get("oneYearReturn", 0),
                fallback.get("threeYearReturn", 0))

    last_close = prices[-1][1]

    # YTD
    ytd_prices = [(d, c) for d, c in prices if d >= ytd_start]
    if len(ytd_prices) >= 2:
        ytd_ret = round(((ytd_prices[-1][1] - ytd_prices[0][1]) / ytd_prices[0][1]) * 100, 2)
    else:
        ytd_ret = fallback.get("ytdReturn", 0)

    # 1Y
    one_yr_cutoff = (now - timedelta(days=365)).strftime("%Y-%m-%d")
    one_yr_price = next((c for d, c in prices if d >= one_yr_cutoff), None)
    one_yr = round(((last_close - one_yr_price) / one_yr_price) * 100, 2) if one_yr_price else fallback.get("oneYearReturn", 0)

    # 3Y
    three_yr_cutoff = (now - timedelta(days=3 * 365)).strftime("%Y-%m-%d")
    three_yr_price = next((c for d, c in prices if d >= three_yr_cutoff), None)
    three_yr = round(annualized_return(three_yr_price, last_close, 3), 2) if three_yr_price else fallback.get("threeYearReturn", 0)

    return ytd_ret, one_yr, three_yr


# ══════════════════════════════════════════════════════════════════════
#  Correlation Matrix & Diversification Sort
# ══════════════════════════════════════════════════════════════════════

# Fallback approximate correlation matrix (weekly returns, well-known values)
# Sources: historical analysis of these ETFs/asset classes over 10+ years.
_ALL_TICKERS_ORDER = [
    "SPY", "QQQ", "VTI", "IWM", "IWD",
    "EFA", "VEA", "EEM",
    "TLT",
    "GLD", "IYR", "DBC", "CPH-RE",
]

_FALLBACK_CORR = [
    # SPY   QQQ   VTI   IWM   IWD   EFA   VEA   EEM   TLT   GLD   IYR   DBC   CPH
    [ 1.00, 0.90, 0.99, 0.88, 0.93, 0.87, 0.86, 0.75,-0.40, 0.05, 0.65, 0.35, 0.15],  # SPY
    [ 0.90, 1.00, 0.92, 0.80, 0.78, 0.80, 0.79, 0.72,-0.42, 0.02, 0.55, 0.28, 0.12],  # QQQ
    [ 0.99, 0.92, 1.00, 0.92, 0.94, 0.87, 0.86, 0.76,-0.40, 0.05, 0.67, 0.35, 0.15],  # VTI
    [ 0.88, 0.80, 0.92, 1.00, 0.88, 0.82, 0.82, 0.76,-0.38, 0.05, 0.72, 0.40, 0.15],  # IWM
    [ 0.93, 0.78, 0.94, 0.88, 1.00, 0.87, 0.86, 0.73,-0.32, 0.08, 0.75, 0.42, 0.18],  # IWD
    [ 0.87, 0.80, 0.87, 0.82, 0.87, 1.00, 0.98, 0.85,-0.30, 0.15, 0.65, 0.45, 0.25],  # EFA
    [ 0.86, 0.79, 0.86, 0.82, 0.86, 0.98, 1.00, 0.85,-0.30, 0.15, 0.64, 0.44, 0.25],  # VEA
    [ 0.75, 0.72, 0.76, 0.76, 0.73, 0.85, 0.85, 1.00,-0.22, 0.20, 0.55, 0.52, 0.20],  # EEM
    [-0.40,-0.42,-0.40,-0.38,-0.32,-0.30,-0.30,-0.22, 1.00, 0.25, 0.05,-0.20, 0.05],  # TLT
    [ 0.05, 0.02, 0.05, 0.05, 0.08, 0.15, 0.15, 0.20, 0.25, 1.00, 0.10, 0.35, 0.15],  # GLD
    [ 0.65, 0.55, 0.67, 0.72, 0.75, 0.65, 0.64, 0.55, 0.05, 0.10, 1.00, 0.30, 0.30],  # IYR
    [ 0.35, 0.28, 0.35, 0.40, 0.42, 0.45, 0.44, 0.52,-0.20, 0.35, 0.30, 1.00, 0.20],  # DBC
    [ 0.15, 0.12, 0.15, 0.15, 0.18, 0.25, 0.25, 0.20, 0.05, 0.15, 0.30, 0.20, 1.00],  # CPH-RE
]

# Build {ticker: {ticker: corr}} lookup from the matrix
FALLBACK_CORR_MAP = {}
for i, t1 in enumerate(_ALL_TICKERS_ORDER):
    FALLBACK_CORR_MAP[t1] = {}
    for j, t2 in enumerate(_ALL_TICKERS_ORDER):
        FALLBACK_CORR_MAP[t1][t2] = _FALLBACK_CORR[i][j]


def compute_correlation_map():
    """Compute pairwise Pearson correlations from DB weekly prices.
    Falls back to hardcoded matrix if insufficient data."""
    all_tickers = TICKERS + STATIC_TICKERS  # includes CPH-RE
    price_series = {}

    # Load weekly returns for ETFs
    for ticker in TICKERS:
        prices = db_load_prices(ticker)
        if len(prices) < 52:  # need at least ~1 year of weekly data
            continue
        # Build {date: return} from weekly prices
        returns = {}
        for k in range(1, len(prices)):
            d, c = prices[k]
            _, prev_c = prices[k - 1]
            if prev_c > 0:
                returns[d] = (c - prev_c) / prev_c
        if returns:
            price_series[ticker] = returns

    # Load quarterly returns for CPH-RE
    housing = db_load_housing()
    if len(housing) >= 8:
        returns = {}
        for k in range(1, len(housing)):
            d, c = housing[k]
            _, prev_c = housing[k - 1]
            if prev_c > 0:
                returns[d] = (c - prev_c) / prev_c
        if returns:
            price_series["CPH-RE"] = returns

    # Need at least 5 tickers with data to compute meaningful correlations
    if len(price_series) < 5:
        return FALLBACK_CORR_MAP

    # Compute pairwise Pearson correlation
    corr_map = {}
    tickers_with_data = list(price_series.keys())

    for t1 in tickers_with_data:
        corr_map[t1] = {}
        for t2 in tickers_with_data:
            if t1 == t2:
                corr_map[t1][t2] = 1.0
                continue
            # Find overlapping dates
            common = set(price_series[t1].keys()) & set(price_series[t2].keys())
            if len(common) < 26:  # need at least ~6 months overlap
                # Use fallback for this pair
                corr_map[t1][t2] = FALLBACK_CORR_MAP.get(t1, {}).get(t2, 0.5)
                continue
            dates = sorted(common)
            r1 = [price_series[t1][d] for d in dates]
            r2 = [price_series[t2][d] for d in dates]
            # Pearson correlation
            n = len(r1)
            mean1 = sum(r1) / n
            mean2 = sum(r2) / n
            cov = sum((a - mean1) * (b - mean2) for a, b in zip(r1, r2))
            std1 = math.sqrt(sum((a - mean1) ** 2 for a in r1))
            std2 = math.sqrt(sum((b - mean2) ** 2 for b in r2))
            if std1 > 0 and std2 > 0:
                corr_map[t1][t2] = round(cov / (std1 * std2), 4)
            else:
                corr_map[t1][t2] = 0.0

    # Fill in any tickers that had no data from fallback
    for t in all_tickers:
        if t not in corr_map:
            corr_map[t] = {}
        for t2 in all_tickers:
            if t2 not in corr_map[t]:
                corr_map[t][t2] = FALLBACK_CORR_MAP.get(t, {}).get(t2, 0.5)

    return corr_map


def diversification_sort(results, corr_map, top_n=4):
    """Sort results: top_n by 10Y return, then greedily pick least correlated.

    For positions 1–top_n: highest 10Y annualized return.
    For position top_n+1 onwards: pick the remaining asset whose average
    absolute correlation with all already-selected assets is lowest.
    """
    if len(results) <= top_n:
        results.sort(key=lambda e: e.get("tenYearReturn") or 0, reverse=True)
        return results

    # Step 1: top_n by 10Y return
    by_return = sorted(results, key=lambda e: e.get("tenYearReturn") or 0, reverse=True)
    selected = list(by_return[:top_n])
    remaining = list(by_return[top_n:])

    # Step 2: greedily pick least correlated
    while remaining:
        best_candidate = None
        best_avg_corr = float("inf")

        for candidate in remaining:
            ct = candidate["ticker"]
            # Average absolute correlation with all selected assets
            corrs = []
            for sel in selected:
                st = sel["ticker"]
                c = corr_map.get(ct, {}).get(st, corr_map.get(st, {}).get(ct, 0.5))
                corrs.append(abs(c))
            avg_corr = sum(corrs) / len(corrs) if corrs else 1.0

            if avg_corr < best_avg_corr:
                best_avg_corr = avg_corr
                best_candidate = candidate

        if best_candidate:
            selected.append(best_candidate)
            remaining.remove(best_candidate)

    return selected


def _avg(values):
    """Average of non-None values, or None if empty."""
    vals = [v for v in values if v is not None]
    return round(sum(vals) / len(vals), 2) if vals else None


def _avg_dd(values):
    """Average drawdown (already negative), or 0."""
    vals = [v for v in values if v is not None and v != 0]
    return round(sum(vals) / len(vals), 2) if vals else 0


def build_portfolio_entry(assets, n=7, corr_map=None):
    """Build a synthetic equally-weighted portfolio from the first *n* assets.
    Computes averaged returns and cumulative growth.  Returns a dict."""
    pool = assets[:n]
    tickers_str = ", ".join(a["ticker"] for a in pool)

    # Averaged annualized returns for each period
    ytd    = _avg([a.get("ytdReturn") for a in pool])
    one_yr = _avg([a.get("oneYearReturn") for a in pool])
    three_yr = _avg([a.get("threeYearReturn") for a in pool])
    five_yr  = _avg([a.get("fiveYearReturn") for a in pool])
    ten_yr   = _avg([a.get("tenYearReturn") for a in pool])
    fifteen_yr = _avg([a.get("fifteenYearReturn") for a in pool])
    twenty_yr  = _avg([a.get("twentyYearReturn") for a in pool])
    twentyfive_yr = _avg([a.get("twentyFiveYearReturn") for a in pool])
    since_inc = _avg([a.get("sinceInceptionReturn") for a in pool])
    since_inc_yrs = _avg([a.get("sinceInceptionYears") for a in pool])
    since_1990 = _avg([a.get("since1990Return") for a in pool])
    since_1990_yrs = _avg([a.get("since1990Years") for a in pool if a.get("since1990Return") is not None])

    # Drawdown estimate — average of constituents (conservative; real portfolio
    # benefits from diversification so actual drawdown would be lower)
    max_dd = _avg_dd([a.get("maxDrawdown") for a in pool])

    # Expense ratio — average of ETFs that have one
    er = _avg([a.get("expenseRatio") for a in pool])

    # Dividend yield — average of available
    dy = _avg([a.get("dividendYield") for a in pool])

    entry = {
        "ticker": "PORT-7",
        "name": f"Equal-Weight Portfolio ({n} assets)",
        "issuer": "Synthetic",
        "marketCap": None,
        "price": None,
        "expenseRatio": er,
        "ytdReturn": ytd,
        "oneYearReturn": one_yr,
        "threeYearReturn": three_yr,
        "fiveYearReturn": five_yr,   "fiveYearNote": "Equal-weight avg",
        "tenYearReturn": ten_yr,     "tenYearNote": "Equal-weight avg",
        "fifteenYearReturn": fifteen_yr, "fifteenYearNote": "Equal-weight avg",
        "twentyYearReturn": twenty_yr,   "twentyYearNote": "Equal-weight avg",
        "twentyFiveYearReturn": twentyfive_yr, "twentyFiveYearNote": "Equal-weight avg",
        "sinceInceptionReturn": since_inc, "sinceInceptionYears": since_inc_yrs,
        "since1990Return": since_1990, "since1990Years": since_1990_yrs,
        "maxDrawdown": max_dd,
        "drawdownPeriod": "Estimated avg",
        "drawdownLabel": "Avg of constituents (actual lower)",
        "secondDrawdown": None,
        "secondDrawdownPeriod": None,
        "secondDrawdownLabel": "",
        "dataStart": None,
        "dividendYield": dy,
        "avgVolume": None,
        "holdings": None,
        "inceptionDate": None,
        "category": "Diversified Portfolio",
        "index": tickers_str,
        "description": (
            f"Equally-weighted portfolio of {n} asset classes: {tickers_str}. "
            "Returns are the simple average of each constituent's annualized return "
            "(approximates annual rebalancing). Drawdown is the average of constituent "
            "drawdowns — actual portfolio drawdown would be lower due to diversification."
        ),
        "rank": 0,
        "rankReason": f"Portfolio of top {n}",
    }

    # Cumulative returns
    entry["fiveYearCumulativeReturn"] = cumulative_return(five_yr, 5)
    entry["tenYearCumulativeReturn"] = cumulative_return(ten_yr, 10)
    entry["fifteenYearCumulativeReturn"] = cumulative_return(fifteen_yr, 15)
    entry["twentyYearCumulativeReturn"] = cumulative_return(twenty_yr, 20)
    entry["twentyFiveYearCumulativeReturn"] = cumulative_return(twentyfive_yr, 25)
    entry["since1990CumulativeReturn"] = cumulative_return(since_1990, since_1990_yrs) if since_1990 else None
    entry["fortyYearCumulativeReturn"] = cumulative_return(best_annualized_return(entry), 40)

    # Portfolio volatility estimate: σ_p = sqrt( (1/n²) * Σ Σ ρ_ij * σ_i * σ_j )
    stds = [a.get("annualizedStdDev") for a in pool]
    if all(s is not None for s in stds) and corr_map:
        var_sum = 0.0
        for i_idx, a in enumerate(pool):
            for j_idx, b in enumerate(pool):
                si = stds[i_idx] / 100
                sj = stds[j_idx] / 100
                rho = corr_map.get(a["ticker"], {}).get(b["ticker"], 0.5)
                var_sum += si * sj * rho
        port_std = round(math.sqrt(var_sum / (n * n)) * 100, 2)
    else:
        # Simple fallback: avg std / sqrt(n)
        valid_stds = [s for s in stds if s is not None]
        port_std = round(sum(valid_stds) / len(valid_stds) / math.sqrt(n), 2) if valid_stds else None

    entry["annualizedStdDev"] = port_std
    entry["sharpeRatio"] = calc_sharpe(ten_yr, port_std)

    return entry


# ══════════════════════════════════════════════════════════════════════
#  Compute dashboard from DB (no network needed)
# ══════════════════════════════════════════════════════════════════════

def compute_from_db():
    """Load all data from SQLite and compute the full dashboard."""
    now = datetime.now()
    results = []

    for ticker in TICKERS:
        meta = ETF_META.get(ticker, {})
        fallback = next((e for e in FALLBACK_DATA if e["ticker"] == ticker), {})

        # Load from DB
        prices = db_load_prices(ticker)
        info, _info_updated = db_load_info(ticker)
        if info is None:
            info = {}

        # ── Extract fields from info (with fallback) ─────────────
        price_val = info.get("regularMarketPrice") or info.get("previousClose")
        if not price_val and prices:
            price_val = prices[-1][1]
        price_val = price_val or fallback.get("price", 0)

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

        # ── Compute returns from stored prices ───────────────────
        if len(prices) > 10:
            five_yr, five_yr_note = calc_period_return(prices, 5)
            ten_yr, ten_yr_note = calc_period_return(prices, 10)
            fifteen_yr, fifteen_yr_note = calc_period_return(prices, 15)
            twenty_yr, twenty_yr_note = calc_period_return(prices, 20)
            twentyfive_yr, twentyfive_yr_note = calc_period_return(prices, 25)
            since_inc_ret, since_inc_years = calc_since_inception_return(prices)
            since_1990_ret, since_1990_years = calc_since_date_return(prices, "1990-01-01")
            dd_info = calc_drawdowns(prices)
            ytd_ret, one_yr, three_yr = _compute_short_returns(prices, fallback, now)
        else:
            five_yr = fallback.get("fiveYearReturn")
            five_yr_note = fallback.get("fiveYearNote", "")
            ten_yr = fallback.get("tenYearReturn")
            ten_yr_note = fallback.get("tenYearNote", "")
            fifteen_yr = fallback.get("fifteenYearReturn", 0)
            fifteen_yr_note = fallback.get("fifteenYearNote", "")
            twenty_yr = fallback.get("twentyYearReturn")
            twenty_yr_note = fallback.get("twentyYearNote", "")
            twentyfive_yr = fallback.get("twentyFiveYearReturn")
            twentyfive_yr_note = fallback.get("twentyFiveYearNote", "")
            dd_info = {
                "maxDrawdown": fallback.get("maxDrawdown", 0),
                "drawdownPeriod": fallback.get("drawdownPeriod", "N/A"),
                "drawdownLabel": fallback.get("drawdownLabel", ""),
                "secondDrawdown": fallback.get("secondDrawdown"),
                "secondDrawdownPeriod": fallback.get("secondDrawdownPeriod"),
                "secondDrawdownLabel": fallback.get("secondDrawdownLabel", ""),
            }
            ytd_ret = fallback.get("ytdReturn", 0)
            one_yr = fallback.get("oneYearReturn", 0)
            three_yr = fallback.get("threeYearReturn", 0)
            since_inc_ret = fallback.get("sinceInceptionReturn")
            since_inc_years = fallback.get("sinceInceptionYears", 0)
            since_1990_ret = fallback.get("since1990Return")
            since_1990_years = fallback.get("since1990Years", 0)

        # Fall back for any None long-term returns
        if five_yr is None:
            five_yr = fallback.get("fiveYearReturn")
            five_yr_note = fallback.get("fiveYearNote", "")
        if ten_yr is None:
            ten_yr = fallback.get("tenYearReturn")
            ten_yr_note = fallback.get("tenYearNote", "")
        if fifteen_yr is None:
            fifteen_yr = fallback.get("fifteenYearReturn", 0)
            fifteen_yr_note = fallback.get("fifteenYearNote", "")
        if twenty_yr is None:
            twenty_yr = fallback.get("twentyYearReturn")
            twenty_yr_note = fallback.get("twentyYearNote", "")
        if twentyfive_yr is None:
            twentyfive_yr = fallback.get("twentyFiveYearReturn")
            twentyfive_yr_note = fallback.get("twentyFiveYearNote", "")

        # Determine earliest data date + note if extended by index backer
        data_start = prices[0][0] if prices else fallback.get("dataStart")
        backer_note = ""
        if ticker in INDEX_BACKERS and INDEX_BACKERS[ticker] and data_start and inception:
            try:
                ds = datetime.strptime(str(data_start)[:10], "%Y-%m-%d")
                inc = datetime.strptime(str(inception)[:10], "%Y-%m-%d")
                if ds < inc - timedelta(days=180):
                    descs = [b[1] for b in INDEX_BACKERS[ticker]]
                    backer_note = "Extended via " + " → ".join(descs)
            except Exception:
                pass

        entry = {
            "ticker": ticker, "name": name, "issuer": issuer,
            "marketCap": market_cap, "price": round(price_val, 2) if price_val else 0,
            "expenseRatio": expense_ratio,
            "ytdReturn": ytd_ret, "oneYearReturn": one_yr,
            "threeYearReturn": three_yr,
            "fiveYearReturn": five_yr, "fiveYearNote": five_yr_note or "",
            "tenYearReturn": ten_yr, "tenYearNote": ten_yr_note or "",
            "fifteenYearReturn": fifteen_yr, "fifteenYearNote": fifteen_yr_note or "",
            "twentyYearReturn": twenty_yr, "twentyYearNote": twenty_yr_note or "",
            "twentyFiveYearReturn": twentyfive_yr, "twentyFiveYearNote": twentyfive_yr_note or "",
            "sinceInceptionReturn": since_inc_ret, "sinceInceptionYears": since_inc_years,
            "since1990Return": since_1990_ret, "since1990Years": since_1990_years,
            **dd_info,
            "dataStart": data_start,
            "dividendYield": div_yield, "avgVolume": avg_volume,
            "holdings": holdings, "inceptionDate": inception,
            "category": meta.get("category", ""), "index": meta.get("index", ""),
            "description": meta.get("description", ""),
            "backerNote": backer_note,
        }
        # Cumulative returns for each period
        entry["fiveYearCumulativeReturn"] = cumulative_return(five_yr, 5)
        entry["tenYearCumulativeReturn"] = cumulative_return(ten_yr, 10)
        entry["fifteenYearCumulativeReturn"] = cumulative_return(fifteen_yr, 15)
        entry["twentyYearCumulativeReturn"] = cumulative_return(twenty_yr, 20)
        entry["twentyFiveYearCumulativeReturn"] = cumulative_return(twentyfive_yr, 25)
        entry["since1990CumulativeReturn"] = cumulative_return(since_1990_ret, since_1990_years) if since_1990_ret else None
        entry["fortyYearCumulativeReturn"] = cumulative_return(best_annualized_return(entry), 40)
        # Volatility & Sharpe
        std = calc_annualized_stddev(prices) if len(prices) > 52 else FALLBACK_STDDEV.get(ticker)
        if std is None:
            std = FALLBACK_STDDEV.get(ticker)
        entry["annualizedStdDev"] = std
        entry["sharpeRatio"] = calc_sharpe(ten_yr, std)
        results.append(entry)

    # ── Compute CPH-RE from housing DB ───────────────────────────
    housing = db_load_housing()
    cph_fallback = next((e for e in FALLBACK_DATA if e["ticker"] == "CPH-RE"), {})
    cph_meta = ETF_META.get("CPH-RE", {})

    if len(housing) > 10:
        # Price-only returns from DST data
        five_yr_price, _ = calc_period_return(housing, 5)
        ten_yr_price, _ = calc_period_return(housing, 10)
        fifteen_yr_price, _ = calc_period_return(housing, 15)
        twenty_yr_price, _ = calc_period_return(housing, 20)
        twentyfive_yr_price, _ = calc_period_return(housing, 25)
        cph_since_price, cph_since_years = calc_since_inception_return(housing)
        cph_1990_price, cph_1990_years = calc_since_date_return(housing, "1990-01-01")
        dd_info = calc_drawdowns(housing)
        ytd_price, one_yr_price, three_yr_price = _compute_short_returns(housing, cph_fallback, now)

        # Total return ≈ price appreciation + rent yield (simple addition of annualized rates)
        _ry = CPH_RENT_YIELD
        def _add_rent(price_ret):
            return round(price_ret + _ry, 2) if price_ret is not None else None

        five_yr = _add_rent(five_yr_price)
        ten_yr = _add_rent(ten_yr_price)
        fifteen_yr = _add_rent(fifteen_yr_price)
        twenty_yr = _add_rent(twenty_yr_price)
        twentyfive_yr = _add_rent(twentyfive_yr_price)
        cph_since_ret = _add_rent(cph_since_price)
        cph_1990_ret = _add_rent(cph_1990_price)
        ytd_ret = _add_rent(ytd_price)
        one_yr = _add_rent(one_yr_price)
        three_yr = _add_rent(three_yr_price)

        def _note(price_ret):
            return f"incl. ~{_ry}% rent (price only: {price_ret:.1f}%)" if price_ret is not None else ""

        cph_data_start = housing[0][0] if housing else "1992-01-01"
        cph_entry = {
            "ticker": "CPH-RE", "name": "Copenhagen Apartments (price/m² + rent)",
            "issuer": "Statistics Denmark",
            "marketCap": None, "price": None, "expenseRatio": None,
            "ytdReturn": ytd_ret, "oneYearReturn": one_yr,
            "threeYearReturn": three_yr,
            "fiveYearReturn": five_yr,
            "fiveYearNote": _note(five_yr_price),
            "tenYearReturn": ten_yr,
            "tenYearNote": _note(ten_yr_price),
            "fifteenYearReturn": fifteen_yr,
            "fifteenYearNote": _note(fifteen_yr_price),
            "twentyYearReturn": twenty_yr,
            "twentyYearNote": _note(twenty_yr_price),
            "twentyFiveYearReturn": twentyfive_yr,
            "twentyFiveYearNote": _note(twentyfive_yr_price),
            "sinceInceptionReturn": cph_since_ret, "sinceInceptionYears": cph_since_years,
            "since1990Return": cph_1990_ret, "since1990Years": cph_1990_years,
            "dividendYield": _ry,  # rent yield shown as "dividend yield"
            **dd_info,
            "dataStart": cph_data_start,
            "avgVolume": None, "holdings": None,
            "inceptionDate": "1992-01-01",
            "category": cph_meta.get("category", "Real Estate — Copenhagen"),
            "index": cph_meta.get("index", "Copenhagen Apartment Index"),
            "description": (
                f"Copenhagen residential apartment prices (price per m²) + estimated "
                f"~{_ry}% annual gross rent yield. Price data from Statistics Denmark / "
                f"Finance Denmark. Rent yield is an approximation."
            ),
        }
        cph_entry["fiveYearCumulativeReturn"] = cumulative_return(five_yr, 5)
        cph_entry["tenYearCumulativeReturn"] = cumulative_return(ten_yr, 10)
        cph_entry["fifteenYearCumulativeReturn"] = cumulative_return(fifteen_yr, 15)
        cph_entry["twentyYearCumulativeReturn"] = cumulative_return(twenty_yr, 20)
        cph_entry["twentyFiveYearCumulativeReturn"] = cumulative_return(twentyfive_yr, 25)
        cph_entry["since1990CumulativeReturn"] = cumulative_return(cph_1990_ret, cph_1990_years) if cph_1990_ret else None
        cph_entry["fortyYearCumulativeReturn"] = cumulative_return(best_annualized_return(cph_entry), 40)
        # CPH-RE volatility: quarterly data → annualize with √4
        cph_std = FALLBACK_STDDEV.get("CPH-RE")
        if len(housing) >= 8:
            rets = []
            for k in range(1, len(housing)):
                _, c = housing[k]
                _, pc = housing[k - 1]
                if pc > 0:
                    rets.append((c - pc) / pc)
            if len(rets) >= 4:
                m = sum(rets) / len(rets)
                v = sum((r - m) ** 2 for r in rets) / (len(rets) - 1)
                cph_std = round(math.sqrt(v) * math.sqrt(4) * 100, 2)
        cph_entry["annualizedStdDev"] = cph_std
        cph_entry["sharpeRatio"] = calc_sharpe(ten_yr, cph_std)
        results.append(cph_entry)
    else:
        cph_fb = dict(cph_fallback)
        cph_fb["fiveYearCumulativeReturn"] = cumulative_return(cph_fb.get("fiveYearReturn"), 5)
        cph_fb["tenYearCumulativeReturn"] = cumulative_return(cph_fb.get("tenYearReturn"), 10)
        cph_fb["fifteenYearCumulativeReturn"] = cumulative_return(cph_fb.get("fifteenYearReturn"), 15)
        cph_fb["twentyYearCumulativeReturn"] = cumulative_return(cph_fb.get("twentyYearReturn"), 20)
        cph_fb["twentyFiveYearCumulativeReturn"] = cumulative_return(cph_fb.get("twentyFiveYearReturn"), 25)
        cph_fb["fortyYearCumulativeReturn"] = cumulative_return(best_annualized_return(cph_fb), 40)
        results.append(cph_fb)

    # Sort: top 4 by 10Y return, then by lowest correlation (diversification)
    corr_map = compute_correlation_map()
    results = diversification_sort(results, corr_map, top_n=4)
    for i, etf in enumerate(results):
        etf["rank"] = i + 1
        # Add ranking reason label
        if i < 4:
            etf["rankReason"] = "Top 10Y return"
        else:
            # Find avg correlation to assets above
            ct = etf["ticker"]
            corrs = []
            for prev in results[:i]:
                pt = prev["ticker"]
                c = corr_map.get(ct, {}).get(pt, corr_map.get(pt, {}).get(ct, 0.5))
                corrs.append(c)
            avg_c = sum(corrs) / len(corrs) if corrs else 0
            etf["rankReason"] = f"Diversifier (avg corr {avg_c:+.2f})"

    # Build equal-weight portfolio of top 7 and insert as first entry
    portfolio = build_portfolio_entry(results, n=7, corr_map=corr_map)
    results.insert(0, portfolio)

    return results


# ══════════════════════════════════════════════════════════════════════
#  Data Fetchers — Yahoo Finance (primary)
# ══════════════════════════════════════════════════════════════════════

def _yahoo_chart_api(ticker, interval="1wk", since_date=None):
    """Fetch history for *ticker* via Yahoo Finance v8 chart API.
    If since_date (str 'YYYY-MM-DD') is given, only fetches from that date onward.
    Returns list of (date_str, close) sorted by date, or [] on failure."""
    import urllib.parse
    encoded = urllib.parse.quote(ticker, safe='')
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}"
    if since_date:
        try:
            p1 = int(datetime.strptime(since_date, "%Y-%m-%d").timestamp())
        except Exception:
            p1 = 0
    else:
        p1 = 0
    params = {"period1": str(p1), "period2": "9999999999", "interval": interval}
    headers = {"User-Agent": "Mozilla/5.0 (ETFCompare/1.0)"}
    for attempt in range(3):
        try:
            resp = http_requests.get(url, params=params, headers=headers, timeout=20)
            data = resp.json()
            result = (data.get("chart") or {}).get("result")
            if result:
                ts = result[0].get("timestamp", [])
                closes = result[0]["indicators"]["quote"][0].get("close", [])
                points = []
                for i, t in enumerate(ts):
                    c = closes[i] if i < len(closes) else None
                    if c is not None and c > 0:
                        d = datetime.utcfromtimestamp(t).strftime("%Y-%m-%d")
                        points.append((d, round(float(c), 4)))
                return points
            else:
                err_msg = (data.get("chart") or {}).get("error", {}).get("description", "no result")
                if attempt == 2:
                    print(f"    [{ticker}] Yahoo chart API: {err_msg}")
        except Exception as e:
            if attempt == 2:
                print(f"    [{ticker}] Yahoo chart API error: {e}")
        time.sleep(2 * (attempt + 1))
    return []


def _is_data_current(ticker, max_age_days=3):
    """Check if a ticker's most recent price is within max_age_days of today.
    Accounts for weekends/holidays — data from last Friday is 'current' on Monday."""
    last = db_last_date(ticker)
    if not last:
        return False
    try:
        last_dt = datetime.strptime(last[:10], "%Y-%m-%d")
        age = (datetime.now() - last_dt).days
        return age <= max_age_days
    except Exception:
        return False


def fetch_yahoo_history():
    """Download weekly prices from Yahoo using direct chart API.
    Incremental: skips tickers whose data is already current (≤3 days old).
    For existing tickers, only fetches data from the last stored date onward.
    Returns True if at least some data was fetched."""
    success_count = 0
    skipped_count = 0
    all_tickers = list(TICKERS)

    # Collect unique backer tickers we also need to fetch
    backer_tickers_needed = set()
    for etf_ticker in TICKERS:
        for backer_entry in INDEX_BACKERS.get(etf_ticker, []):
            backer_tickers_needed.add(backer_entry[0])

    # ── Step 1: Fetch all ETF tickers ──────────────────────────────────
    for i, ticker in enumerate(all_tickers):
        existing = db_price_count(ticker)
        last = db_last_date(ticker)

        # Already current — skip entirely (no network call)
        if existing > 200 and _is_data_current(ticker, max_age_days=3):
            print(f"  [Yahoo] [{i+1}/{len(all_tickers)}] {ticker}: ✓ {existing} rows, "
                  f"last={last} (current), skip.")
            skipped_count += 1
            success_count += 1
            continue

        # Has substantial data but needs a recent update — incremental fetch
        if existing > 200 and last:
            print(f"  [Yahoo] [{i+1}/{len(all_tickers)}] {ticker}: {existing} rows, "
                  f"last={last} (stale), fetching update...")
            if i > 0:
                time.sleep(1)
            recent = _yahoo_chart_api(ticker, "1d", since_date=last)
            new_rows = [(ticker, d, c) for d, c in recent if d > last]
            if new_rows:
                db_save_prices(new_rows)
                print(f"    → Added {len(new_rows)} new rows (up to {new_rows[-1][1]}).")
            else:
                print(f"    → Already up to date.")
            success_count += 1
            continue

        # No data or very little — full weekly history fetch
        print(f"  [Yahoo] [{i+1}/{len(all_tickers)}] {ticker}: "
              f"{'empty' if existing == 0 else f'{existing} rows'}, fetching full weekly history...")
        if i > 0:
            time.sleep(2)

        points = _yahoo_chart_api(ticker, "1wk")
        if points:
            rows = [(ticker, d, c) for d, c in points]
            db_save_prices(rows)
            print(f"  [Yahoo] [{i+1}/{len(all_tickers)}] {ticker}: saved {len(rows)} weekly rows "
                  f"({points[0][0]} → {points[-1][0]}).")
            success_count += 1
        else:
            print(f"  [Yahoo] [{i+1}/{len(all_tickers)}] {ticker}: no data returned.")

    # ── Step 2: Fetch backer (index) tickers ───────────────────────────
    if backer_tickers_needed:
        print(f"\n  [Yahoo] Fetching {len(backer_tickers_needed)} index backer ticker(s)...")
    for j, bt in enumerate(sorted(backer_tickers_needed)):
        existing = db_price_count(bt)
        last = db_last_date(bt)

        # Backer data is historical — if we have a good amount, skip entirely
        # (backers are index data that doesn't change retroactively)
        if existing > 200:
            # Still do a quick incremental update for recent data points
            if _is_data_current(bt, max_age_days=7):
                print(f"  [Yahoo] [{j+1}] {bt}: ✓ {existing} rows (current), skip.")
                continue
            # Stale backer — incremental update
            if last:
                time.sleep(1)
                print(f"  [Yahoo] [{j+1}] {bt}: {existing} rows, last={last}, updating...")
                recent = _yahoo_chart_api(bt, "1d", since_date=last)
                new_rows = [(bt, d, c) for d, c in recent if d > last]
                if new_rows:
                    db_save_prices(new_rows)
                    print(f"    → Added {len(new_rows)} new rows.")
                else:
                    print(f"    → Already up to date.")
                continue

        # No data — full fetch
        time.sleep(2)
        print(f"  [Yahoo] [{j+1}] Fetching backer {bt}...")
        points = _yahoo_chart_api(bt, "1wk")
        if points:
            rows = [(bt, d, c) for d, c in points]
            db_save_prices(rows)
            print(f"  [Yahoo] [{j+1}] {bt}: saved {len(rows)} rows ({points[0][0]} → {points[-1][0]}).")
        else:
            print(f"  [Yahoo] [{j+1}] {bt}: no data.")

    # ── Step 3: Splice backer data into ETF tickers ────────────────────
    _splice_backer_data()

    print(f"  [Yahoo] History fetch complete: {success_count}/{len(all_tickers)} ETF tickers OK "
          f"({skipped_count} already current, {success_count - skipped_count} updated).")
    return success_count > 0


def _splice_backer_data():
    """For each ETF with INDEX_BACKERS, chain-splice backer data before the ETF's earliest date.
    Backers are processed in listed order (first = closest to ETF inception, last = oldest).
    Each layer is scaled so its last price matches the next layer's first price."""
    for etf_ticker, backers in INDEX_BACKERS.items():
        if not backers:
            continue

        # First, delete any previously spliced rows (backer dates before ETF's real inception)
        # We detect the ETF's own earliest date from the original fetch
        # by finding the earliest non-backer date. Since we INSERT OR REPLACE,
        # we just re-splice from scratch using the original ETF data.
        # Load current full series for this ETF (may include old spliced data)
        all_prices = db_load_prices(etf_ticker)
        if not all_prices:
            continue

        # The ETF's "real" start is the inception date from meta, or we approximate
        # by finding where backer data would have ended
        # Simpler: reload the original ETF data by checking what Yahoo returned
        # for this ticker directly. Since backers are stored under their own ticker,
        # we need to identify the ETF's native range.
        # Approach: find the earliest backer date and remove ETF rows before that
        # Actually, let's just delete all ETF rows that came from backers
        # (i.e., before the ETF's real Yahoo data start) and re-splice.
        # We know the ETF's real data starts at its Yahoo fetch start.
        # Heuristic: the ETF's own data starts where the first backer ends.
        # Better: just re-do. Delete rows before the latest backer's last date
        # and rebuild.

        # Get earliest date from the ETF ticker's own Yahoo data
        # by loading the backer tickers and finding the boundary
        backer_tickers_set = set()
        for b in backers:
            backer_tickers_set.add(b[0])

        # Find the ETF's native start: the first date that ISN'T from a backer splice
        # We stored backer data under the ETF ticker, so we can't distinguish.
        # Solution: just re-do the splice. Delete all data before what we know
        # is the ETF's own inception and re-insert from backers.
        etf_inception = None
        for e in FALLBACK_DATA:
            if e.get("ticker") == etf_ticker:
                etf_inception = e.get("inceptionDate") or e.get("dataStart")
                break
        if not etf_inception:
            continue

        # Delete previously spliced rows (those before inception)
        conn = _db()
        conn.execute("DELETE FROM weekly_prices WHERE ticker=? AND date < ?",
                     (etf_ticker, etf_inception))
        conn.commit()
        conn.close()

        # Reload the ETF's native data
        etf_prices = db_load_prices(etf_ticker)
        if not etf_prices:
            continue

        # Chain-splice backers in order: each fills the gap before current earliest
        current_first_date = etf_prices[0][0]
        current_first_price = etf_prices[0][1]
        total_spliced = 0
        splice_desc_parts = []

        for backer_entry in backers:
            backer_yt, backer_desc = backer_entry[0], backer_entry[1]
            backer_prices = db_load_prices(backer_yt)
            if not backer_prices:
                continue

            # Only keep backer data BEFORE current earliest date
            pre = [(d, c) for d, c in backer_prices if d < current_first_date and c > 0]
            if not pre:
                continue

            # Scale: last backer price → current first price
            backer_last_price = pre[-1][1]
            if backer_last_price <= 0:
                continue
            scale = current_first_price / backer_last_price

            rows = [(etf_ticker, d, round(c * scale, 4)) for d, c in pre]
            db_save_prices(rows)
            total_spliced += len(rows)
            splice_desc_parts.append(f"{backer_desc} ({pre[0][0]}→{pre[-1][0]})")

            # Update anchor for next layer
            current_first_date = pre[0][0]
            current_first_price = pre[0][1] * scale

        if total_spliced > 0:
            print(f"  [Splice] {etf_ticker}: +{total_spliced} rows via {' → '.join(splice_desc_parts)}")


def fetch_yahoo_info():
    """Fetch ticker metadata from Yahoo. Only re-fetches if stored info > 3 days old.
    Most metadata (market cap, price) changes daily, but expense ratio, name, etc. are stable."""
    INFO_TTL = 259200  # 3 days in seconds
    for i, ticker in enumerate(TICKERS):
        stored_info, updated_at = db_load_info(ticker)
        age = time.time() - updated_at
        if stored_info and age < INFO_TTL:
            print(f"  [Yahoo] [{i+1}/{len(TICKERS)}] {ticker} info fresh ({age/3600:.1f}h old), skip.")
            continue

        if i > 0:
            time.sleep(6)

        print(f"  [Yahoo] [{i+1}/{len(TICKERS)}] Fetching {ticker} info...")
        for attempt in range(3):
            try:
                t = yf.Ticker(ticker)
                info = t.info or {}
                if info and info.get("regularMarketPrice"):
                    db_save_info(ticker, info)
                    print(f"  [Yahoo] [{i+1}/{len(TICKERS)}] {ticker} info saved.")
                    break
            except Exception as e:
                print(f"    Attempt {attempt+1} failed: {e}")
            if attempt < 2:
                time.sleep(8 * (attempt + 1))


# ══════════════════════════════════════════════════════════════════════
#  Data Fetchers — Alpha Vantage (fallback)
# ══════════════════════════════════════════════════════════════════════

def fetch_alpha_vantage_prices(ticker):
    """Fetch weekly adjusted prices for one ticker from Alpha Vantage."""
    if not ALPHA_VANTAGE_API_KEY:
        return False
    print(f"  [AlphaVantage] Fetching weekly prices for {ticker}...")
    try:
        resp = http_requests.get("https://www.alphavantage.co/query", params={
            "function": "TIME_SERIES_WEEKLY_ADJUSTED",
            "symbol": ticker,
            "outputsize": "full",
            "apikey": ALPHA_VANTAGE_API_KEY,
        }, timeout=30)
        data = resp.json()

        series = data.get("Weekly Adjusted Time Series")
        if not series:
            msg = data.get("Note") or data.get("Information") or "no data"
            print(f"  [AlphaVantage] {ticker}: {msg}")
            return False

        rows = []
        for date_str, vals in series.items():
            close = float(vals.get("5. adjusted close", vals.get("4. close", 0)))
            if close > 0:
                rows.append((ticker, date_str, close))

        if rows:
            db_save_prices(rows)
            print(f"  [AlphaVantage] Saved {len(rows)} prices for {ticker}.")
            return True
    except Exception as e:
        print(f"  [AlphaVantage] Failed for {ticker}: {e}")
    return False


def fetch_alpha_vantage_info(ticker):
    """Fetch ETF/stock overview from Alpha Vantage."""
    if not ALPHA_VANTAGE_API_KEY:
        return False
    print(f"  [AlphaVantage] Fetching overview for {ticker}...")
    try:
        resp = http_requests.get("https://www.alphavantage.co/query", params={
            "function": "OVERVIEW",
            "symbol": ticker,
            "apikey": ALPHA_VANTAGE_API_KEY,
        }, timeout=30)
        data = resp.json()

        if not data or "Symbol" not in data:
            return False

        # Map AV fields to our expected format
        info = {}
        if data.get("Name"):
            info["shortName"] = data["Name"]
        if data.get("MarketCapitalization") and data["MarketCapitalization"] != "None":
            info["marketCap"] = int(data["MarketCapitalization"])
        if data.get("DividendYield") and data["DividendYield"] != "None":
            info["trailingAnnualDividendYield"] = float(data["DividendYield"])

        if info:
            # Merge with any existing stored info
            existing, _ = db_load_info(ticker)
            if existing:
                existing.update(info)
                info = existing
            db_save_info(ticker, info)
            print(f"  [AlphaVantage] Saved overview for {ticker}.")
            return True
    except Exception as e:
        print(f"  [AlphaVantage] Overview failed for {ticker}: {e}")
    return False


# ══════════════════════════════════════════════════════════════════════
#  Data Fetchers — Statistics Denmark (Copenhagen housing)
# ══════════════════════════════════════════════════════════════════════

def _parse_dst_quarter(time_str):
    """Convert DST quarter format ('2024Q1', '2024K1') to 'YYYY-MM-DD'."""
    try:
        time_str = time_str.strip()
        if "Q" in time_str or "K" in time_str:
            parts = time_str.replace("K", "Q").split("Q")
            year = int(parts[0])
            quarter = int(parts[1])
            month = (quarter - 1) * 3 + 1
            return f"{year}-{month:02d}-01"
        elif "M" in time_str:
            parts = time_str.split("M")
            return f"{int(parts[0])}-{int(parts[1]):02d}-01"
        else:
            return f"{int(time_str)}-01-01"
    except Exception:
        return None


def fetch_dst_housing():
    """Fetch Copenhagen apartment price index from Statistics Denmark API."""
    # Skip if data is recent (quarterly updates)
    last_date = db_last_housing_date()
    if last_date:
        try:
            last_dt = datetime.strptime(last_date, "%Y-%m-%d")
            if (datetime.now() - last_dt).days < 80:
                print(f"  [DST] Housing data recent ({last_date}), skipping.")
                return True
        except Exception:
            pass

    print("  [DST] Fetching Copenhagen housing price index...")

    # Try EJEN5 first (property sales price index by municipality)
    tables_to_try = [
        {
            "table": "EJEN5",
            "variables": [
                {"code": "EJDTYPE", "values": ["6"]},
                {"code": "OMRÅDE", "values": ["101"]},
                {"code": "Tid", "values": ["*"]},
            ],
        },
        {
            "table": "EJENEU",
            "variables": [
                {"code": "BODO3", "values": ["TOT"]},
                {"code": "BEBY", "values": ["1"]},
                {"code": "Tid", "values": ["*"]},
            ],
        },
    ]

    for attempt_cfg in tables_to_try:
        try:
            payload = {
                "table": attempt_cfg["table"],
                "format": "JSON",
                "variables": attempt_cfg["variables"],
            }
            print(f"  [DST] Trying table {attempt_cfg['table']}...")
            resp = http_requests.post(
                "https://api.statbank.dk/v1/data",
                json=payload, timeout=30,
            )

            if resp.status_code != 200:
                print(f"  [DST] {attempt_cfg['table']} returned HTTP {resp.status_code}")
                continue

            data = resp.json()
            if not data:
                continue

            rows = []
            for entry in data:
                # DST JSON format: list of dicts with "key" and "values"
                time_val = None
                for k in entry.get("key", []):
                    code = (k.get("code") or "").upper()
                    if code == "TID" or code == "Tid":
                        time_val = k.get("value", "")

                idx_val = None
                for v in entry.get("values", []):
                    try:
                        idx_val = float(v)
                        break
                    except (ValueError, TypeError):
                        continue

                if time_val and idx_val and idx_val > 0:
                    date_str = _parse_dst_quarter(time_val)
                    if date_str:
                        rows.append((date_str, idx_val))

            if rows:
                db_save_housing(rows)
                print(f"  [DST] Saved {len(rows)} quarterly housing data points from {attempt_cfg['table']}.")
                return True
            else:
                print(f"  [DST] No parseable rows from {attempt_cfg['table']}.")

        except Exception as e:
            print(f"  [DST] {attempt_cfg['table']} failed: {e}")

    print("  [DST] All table attempts failed — using fallback housing data.")
    return False


# ══════════════════════════════════════════════════════════════════════
#  Main Fetch Orchestrator
# ══════════════════════════════════════════════════════════════════════

def fetch_live_data():
    """Orchestrate all data fetching, save to SQLite, then recompute dashboard.
    Incremental: only fetches data that is missing or stale."""
    _cache["updating"] = True
    try:
        data_age = db_data_age_days()
        total = db_total_prices()
        print(f"\n  ═══ Starting live data fetch ═══")
        print(f"      DB has {total} price rows, data age: {data_age or '?'} day(s)")

        # Step 1: Historical prices — Yahoo (primary, incremental)
        yahoo_ok = fetch_yahoo_history()

        # Step 1b: If Yahoo failed, try Alpha Vantage for tickers with no/little data
        if not yahoo_ok and ALPHA_VANTAGE_API_KEY:
            print("  [Fallback] Yahoo history failed — trying Alpha Vantage...")
            for ticker in TICKERS:
                if db_price_count(ticker) < 10:
                    fetch_alpha_vantage_prices(ticker)
                    time.sleep(1)

        # Step 2: Ticker info — Yahoo (primary), Alpha Vantage (fallback)
        fetch_yahoo_info()

        if ALPHA_VANTAGE_API_KEY:
            for ticker in TICKERS:
                info, _ = db_load_info(ticker)
                if not info or not info.get("regularMarketPrice"):
                    fetch_alpha_vantage_info(ticker)
                    time.sleep(1)

        # Step 3: Copenhagen housing data from Statistics Denmark
        fetch_dst_housing()

        # Step 4: Recompute dashboard from DB
        results = compute_from_db()

        _cache["data"] = results
        _cache["time"] = time.time()
        _cache["fetched_at"] = datetime.utcnow().isoformat() + "Z"
        _cache["source"] = "live"

        save_disk_cache()
        total = db_total_prices()
        print(f"  ═══ Done. {len(results)} entries, {total} prices in DB. ═══\n")

    except Exception as e:
        print(f"  ═══ Fetch failed: {e} ═══")
        traceback.print_exc()
    finally:
        _cache["updating"] = False


# ══════════════════════════════════════════════════════════════════════
#  JSON disk cache (fast read layer for API)
# ══════════════════════════════════════════════════════════════════════

def _enrich_stddev_sharpe(entries):
    """Ensure every entry has annualizedStdDev and sharpeRatio.
    Fills in from FALLBACK_STDDEV when missing (e.g. loaded from old cache)."""
    for e in entries:
        if e.get("annualizedStdDev") is None:
            e["annualizedStdDev"] = FALLBACK_STDDEV.get(e.get("ticker"))
        if e.get("sharpeRatio") is None:
            std = e.get("annualizedStdDev")
            ret = e.get("tenYearReturn")
            e["sharpeRatio"] = calc_sharpe(ret, std)
    # Ensure since1990Return field exists (may be missing in old caches)
    for e in entries:
        if "since1990Return" not in e:
            e["since1990Return"] = None
            e["since1990Years"] = 0
            e["since1990CumulativeReturn"] = None
    # Re-compute portfolio entry if present
    for e in entries:
        if e.get("ticker", "").startswith("PORT-") and e.get("annualizedStdDev") is None:
            # gather constituents' stds
            pool = [x for x in entries if x.get("ticker") != e["ticker"]]
            stds = [x.get("annualizedStdDev") for x in pool[:7] if x.get("annualizedStdDev") is not None]
            if stds:
                port_std = round(sum(stds) / len(stds) / math.sqrt(len(stds)), 2)
                e["annualizedStdDev"] = port_std
                e["sharpeRatio"] = calc_sharpe(e.get("tenYearReturn"), port_std)


def load_disk_cache():
    """Load JSON cache from disk (any age — always prefer over fallback)."""
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE) as f:
                disk = json.load(f)
            age = time.time() - disk.get("time", 0)
            if disk.get("data"):
                _enrich_stddev_sharpe(disk["data"])
                _cache["data"] = disk["data"]
                _cache["time"] = disk["time"]
                _cache["fetched_at"] = disk.get("fetched_at")
                _cache["source"] = "disk_cache"
                if age < 60:
                    age_str = f"{age:.0f}s"
                elif age < 3600:
                    age_str = f"{age/60:.0f}min"
                elif age < 86400:
                    age_str = f"{age/3600:.1f}h"
                else:
                    age_str = f"{age/86400:.1f}d"
                print(f"  Loaded {len(disk['data'])} entries from JSON cache ({age_str} old).")
                return True
        except Exception as e:
            print(f"  Could not read JSON cache: {e}")
    return False


def save_disk_cache():
    """Persist in-memory cache to JSON disk file."""
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump({
                "data": _cache["data"],
                "time": _cache["time"],
                "fetched_at": _cache["fetched_at"],
            }, f)
    except Exception as e:
        print(f"  Could not write JSON cache: {e}")


# ══════════════════════════════════════════════════════════════════════
#  API Endpoints
# ══════════════════════════════════════════════════════════════════════

@app.route("/api/etfs")
def api_etfs():
    return jsonify({
        "data": _cache["data"],
        "cached": _cache["source"] != "live",
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


@app.route("/api/growth")
def api_growth():
    """Return historical growth-of-$10K series for requested tickers.
    All series are aligned to start from the earliest common date where
    ALL selected assets have data, so the comparison is apples-to-apples.
    Query params:  tickers=SPY,QQQ,PORT-7  (comma-separated, max 5)
                   years=10|max             (lookback, default 10; 'max' = full overlap)
    Uses DB weekly prices when available, otherwise synthesizes from annualized returns."""
    raw = request.args.get("tickers", "")
    tickers_req = [t.strip() for t in raw.split(",") if t.strip()][:5]
    years_raw = request.args.get("years", "10")
    use_max = years_raw.lower() == "max"
    years = 60 if use_max else min(int(years_raw), 60)
    base = 10_000

    # Determine the set of portfolio constituent tickers (for PORT-7)
    port_constituents = []
    for e in _cache.get("data", []):
        if e.get("ticker", "").startswith("PORT-"):
            port_constituents = [t.strip() for t in e.get("index", "").split(",") if t.strip()]
            break

    now = datetime.now()
    cutoff = (now - timedelta(days=years * 365.25)).strftime("%Y-%m-%d")

    # ── Step 1: Collect raw price series for each ticker ──────────────
    raw_series = {}  # ticker -> list of (date, price)

    for ticker in tickers_req:
        if ticker.startswith("PORT-"):
            # For portfolio, build from constituents
            constituent_series = {}
            for ct in port_constituents:
                rows = _get_raw_prices(ct, cutoff)
                if rows:
                    constituent_series[ct] = rows
            if constituent_series:
                # Find the common start date for all constituents
                const_start = max(rows[0][0] for rows in constituent_series.values())
                # Build equal-weight portfolio series
                all_by_date = {}
                for ct, rows in constituent_series.items():
                    # Filter to common start and normalize
                    filtered = [(d, p) for d, p in rows if d >= const_start]
                    if not filtered:
                        continue
                    base_p = filtered[0][1]
                    if base_p <= 0:
                        continue
                    for d, p in filtered:
                        all_by_date.setdefault(d, []).append(p / base_p)
                n_const = len(constituent_series)
                port_raw = []
                for d in sorted(all_by_date.keys()):
                    ratios = all_by_date[d]
                    avg_ratio = sum(ratios) / n_const
                    port_raw.append((d, avg_ratio))
                if port_raw:
                    raw_series[ticker] = port_raw
            else:
                # Synthetic fallback
                synth = _synthetic_growth(ticker, years, base)
                if synth:
                    raw_series[ticker] = [(p[0], p[1] / base) for p in synth]
        else:
            rows = _get_raw_prices(ticker, cutoff)
            if rows:
                # Store as (date, price) — we'll normalize after alignment
                raw_series[ticker] = rows
            else:
                synth = _synthetic_growth(ticker, years, base)
                if synth:
                    raw_series[ticker] = [(p[0], p[1] / base) for p in synth]

    if not raw_series:
        return jsonify({})

    # ── Step 2: Find common start date (latest earliest date) ─────────
    common_start = max(series[0][0] for series in raw_series.values())

    # ── Step 3: Trim, normalize to $10K from common start, and build output
    result = {}
    for ticker in tickers_req:
        series = raw_series.get(ticker)
        if not series:
            result[ticker] = []
            continue
        # Filter to common start
        filtered = [(d, p) for d, p in series if d >= common_start]
        if len(filtered) < 2:
            result[ticker] = []
            continue
        base_val = filtered[0][1]
        if base_val <= 0:
            result[ticker] = []
            continue
        result[ticker] = [[d, round(base * p / base_val, 2)] for d, p in filtered]

    # Add metadata about the common period
    if result:
        first_dates = [pts[0][0] for pts in result.values() if pts]
        last_dates = [pts[-1][0] for pts in result.values() if pts]
        if first_dates and last_dates:
            result["_meta"] = {
                "commonStart": min(first_dates),
                "commonEnd": max(last_dates),
            }

    return jsonify(result)


def _get_raw_prices(ticker, cutoff):
    """Load raw (date, price) pairs for a ticker, filtered to >= cutoff.  Returns list or None."""
    if ticker == "CPH-RE":
        rows = db_load_housing()
    else:
        rows = db_load_prices(ticker)
    if len(rows) < 4:
        return None
    filtered = [(d, p) for d, p in rows if d >= cutoff and p > 0]
    if len(filtered) < 2:
        return None
    return filtered


def _synthetic_growth(ticker, years, base):
    """Generate a synthetic monthly growth curve from cached annualized return data."""
    entry = None
    for e in _cache.get("data", []):
        if e.get("ticker") == ticker:
            entry = e
            break
    if not entry:
        return []

    # Pick the best available annualized return
    ann = best_annualized_return(entry)
    if ann is None:
        return []

    monthly_rate = math.pow(1 + ann / 100, 1 / 12)
    now = datetime.now()
    total_months = years * 12
    points = []
    for m in range(total_months + 1):
        d = (now - timedelta(days=(total_months - m) * 30.44)).strftime("%Y-%m-%d")
        val = round(base * math.pow(monthly_rate, m), 2)
        points.append([d, val])
    return points


@app.route("/")
def index_page():
    return send_from_directory(".", "index.html")


# ══════════════════════════════════════════════════════════════════════
#  Startup
# ══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n  ETF Compare dashboard starting...\n")

    # 1. Initialize SQLite database
    init_db()
    total = db_total_prices()
    print(f"  SQLite database: {total} price rows stored.")

    # 2. If DB has data, compute dashboard from it (instant, no network)
    if total > 0:
        print("  Computing dashboard from stored data...")
        results = compute_from_db()
        _cache["data"] = results
        _cache["time"] = time.time()
        _cache["fetched_at"] = datetime.utcnow().isoformat() + "Z"
        _cache["source"] = "database"
        save_disk_cache()
        print(f"  Computed {len(results)} entries from database.")
    else:
        # Fall back to JSON cache or hardcoded fallback
        if not load_disk_cache():
            print("  No stored data — using hardcoded fallback.")
            # Apply diversification sort to fallback data
            fb = diversification_sort(list(FALLBACK_DATA), FALLBACK_CORR_MAP, top_n=4)
            for i, etf in enumerate(fb):
                etf["rank"] = i + 1
                if i < 4:
                    etf["rankReason"] = "Top 10Y return"
                else:
                    ct = etf["ticker"]
                    corrs = [FALLBACK_CORR_MAP.get(ct, {}).get(prev["ticker"], 0.5) for prev in fb[:i]]
                    avg_c = sum(corrs) / len(corrs) if corrs else 0
                    etf["rankReason"] = f"Diversifier (avg corr {avg_c:+.2f})"
            # Build equal-weight portfolio of top 7 and insert as first entry
            portfolio = build_portfolio_entry(fb, n=7, corr_map=FALLBACK_CORR_MAP)
            fb.insert(0, portfolio)
            _cache["data"] = fb
            _cache["time"] = time.time()
            _cache["fetched_at"] = datetime.utcnow().isoformat() + "Z"
            save_disk_cache()

    data_age = db_data_age_days()
    newest = db_newest_date()
    print(f"\n  Dashboard ready with {len(_cache['data'])} entries.")
    if newest:
        print(f"  SQLite data: most recent price date = {newest} ({data_age:.0f} day(s) ago)")
    if ALPHA_VANTAGE_API_KEY:
        print("  Alpha Vantage fallback: enabled")
    else:
        print("  Alpha Vantage fallback: disabled (set ALPHA_VANTAGE_API_KEY to enable)")
    print(f"\n  → http://localhost:3000\n")

    # 3. Background fetch if DB is empty or data is stale
    #    Uses SQLite data freshness (not JSON cache age) as the primary check.
    #    Data older than 3 days triggers an incremental update.
    need_fetch = False
    if total == 0:
        print(f"  DB is empty — will fetch prices in background.\n")
        need_fetch = True
    elif data_age is not None and data_age > 3:
        print(f"  Data is {data_age:.0f} day(s) old — will fetch incremental update in background.\n")
        need_fetch = True
    else:
        print(f"  Data is current ({data_age:.0f} day(s) old) — no fetch needed.\n")

    if need_fetch:
        print("  Background data fetch starting in 3s...\n")
        def delayed_fetch():
            time.sleep(3)
            fetch_live_data()
        bg = threading.Thread(target=delayed_fetch, daemon=True)
        bg.start()

    app.run(host="0.0.0.0", port=3000, debug=False)
