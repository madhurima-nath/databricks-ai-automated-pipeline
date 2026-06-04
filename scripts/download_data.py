"""
Download raw market data locally and save as CSV files to data/raw/.

Run this once from your laptop before committing — Databricks reads from
these files instead of calling external APIs (which are blocked on serverless compute).

Usage:
    source .venv/bin/activate
    python scripts/download_data.py

Requires FRED_API_KEY in .env
"""

import datetime
import os
import sys

import pandas as pd

# Load .env
env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

fred_api_key = os.environ.get("FRED_API_KEY", "")
if not fred_api_key:
    sys.exit("FRED_API_KEY not set. Add it to .env and try again.")

import yfinance as yf
from fredapi import Fred

START_DATE = "2010-01-01"
END_DATE   = datetime.date.today().isoformat()
OUT_DIR    = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
os.makedirs(OUT_DIR, exist_ok=True)

fred = Fred(api_key=fred_api_key)


def _save(df: pd.DataFrame, name: str):
    path = os.path.join(OUT_DIR, f"{name}.csv")
    df.to_csv(path, index=False)
    print(f"  {name}.csv — {len(df):,} rows")


def _close_only(ticker: str, col: str) -> pd.DataFrame:
    raw = yf.download(ticker, start=START_DATE, end=END_DATE, auto_adjust=True, progress=False)
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [c[0] for c in raw.columns]
    df = raw[["Close"]].reset_index()
    df.columns = ["date", col]
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    return df


def _fred_series(series_id: str, col: str) -> pd.DataFrame:
    s = fred.get_series(series_id, observation_start=START_DATE, observation_end=END_DATE)
    df = s.reset_index()
    df.columns = ["date", col]
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    return df


print(f"Downloading data {START_DATE} → {END_DATE}\n")

# S&P 500 — full OHLCV
raw = yf.download("^GSPC", start=START_DATE, end=END_DATE, auto_adjust=True, progress=False)
if isinstance(raw.columns, pd.MultiIndex):
    raw.columns = [c[0] for c in raw.columns]
sp500 = raw.reset_index()
sp500.columns = [c.lower().replace(" ", "_") for c in sp500.columns]
date_col = next(c for c in sp500.columns if c in ("date", "datetime", "price"))
sp500 = sp500.rename(columns={date_col: "date"})
sp500["date"] = pd.to_datetime(sp500["date"]).dt.date.astype(str)
_save(sp500, "sp500")

_save(_close_only("^STOXX50E", "eurostoxx_close"), "eurostoxx")
_save(_close_only("^VIX",      "vix_close"),       "vix")
_save(_fred_series("FEDFUNDS", "fed_rate"),         "fed_rate")
_save(_fred_series("ECBDFR",   "ecb_rate"),         "ecb_rate")

print("\nDone. Now run:")
print("  git add data/raw/ && git commit -m 'add raw market data CSVs' && git push")
