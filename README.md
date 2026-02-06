# ETF Compare — Top 10 ETFs by Market Cap

An interactive dashboard comparing the 10 largest ETFs by market capitalization, ranked by 15-year average annual return. **Live data from Yahoo Finance.**

## Features

- **Live Yahoo Finance data** — Prices, returns, drawdowns, and metrics fetched in real-time
- **15-Year performance ranking** — ETFs ranked by annualized return over the last 15 years
- **Max drawdown analysis** — Worst peak-to-trough decline with date ranges
- **Index coverage** — S&P 500, Nasdaq-100, Russell 2000, International ex-US, and U.S. Bonds
- **Interactive charts** — Toggle between 15Y Return, Max Drawdown, Market Cap, Expense Ratio, YTD Return, and Dividend Yield
- **Sortable table** — Click any column header to sort
- **Search/filter** — Instantly filter by ticker, name, issuer, or category
- **Detail modal** — Click any row for full details
- **5-minute cache** — Data refreshes automatically; click Refresh for immediate update

## Quick Start

```bash
# Install dependencies
pip3 install --user yfinance flask

# Run the server
python3 server.py
```

Then open **http://localhost:3000** in your browser.

## Tech Stack

- **Python / Flask** — Backend server + Yahoo Finance data fetching
- **yfinance** — Unofficial Yahoo Finance API wrapper
- **HTML / CSS / JavaScript** — Frontend dashboard (no build step)
- **Chart.js 4** — Interactive bar charts

## ETFs Covered

| Index | ETF(s) |
|-------|--------|
| S&P 500 | SPY, IVV, VOO |
| Nasdaq-100 | QQQ |
| Russell 2000 | IWM |
| U.S. Total Market | VTI |
| Large Cap Value | VTV |
| International ex-US | VEA, IEFA |
| U.S. Aggregate Bond | AGG |
