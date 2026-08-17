"""
Nifty 50 - Live Opening Range Dashboard (backend)
===================================================

Serves:
  GET  /            -> the dashboard (static/index.html)
  GET  /api/status   -> current live status for all 50 stocks (JSON)

Live status logic (per stock, per request):
  - Candle 1 = first 5-min candle of today's session (09:15-09:20 IST) -- fixed once known.
  - Latest   = most recent completed 5-min candle available right now.
  - current price = Latest candle's Close.

  status = "bullish"  if current price > Candle1 High   (price currently above opening range)
  status = "bearish"  if current price < Candle1 Low    (price currently below opening range)
  status = "inside"   otherwise

  Additionally reports whether the original fixed signal fired:
  candle2_signal = "bullish_cross" if Candle2.High > Candle1.High
                   "bearish_cross" if Candle2.Low  < Candle1.Low
                   "none"          otherwise
  (Candle 2 = 09:20-09:25 IST, only known once >= 2 candles exist.)

Data source: yfinance (Yahoo Finance). ~15 min delayed, not tick-level.
Results are cached for CACHE_TTL_SECONDS to avoid hammering Yahoo on every
frontend poll (multiple browser tabs / fast refresh intervals all share
one cache).

Run locally:
    uvicorn app:app --reload --port 8000

Deploy: see README.md (Render.com free web service).
"""

import time
import threading
from datetime import datetime, time as dtime
from pathlib import Path

import pandas as pd
import pytz
import yfinance as yf
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from nifty50_list import NIFTY_50

IST = pytz.timezone("Asia/Kolkata")
MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)

CACHE_TTL_SECONDS = 60      # min time between real Yahoo fetches
BATCH_SIZE = 15
RETRY_PER_BATCH = 2

app = FastAPI(title="Nifty 50 Live Opening Range Dashboard")

STATIC_DIR = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

_cache_lock = threading.Lock()
_cache = {"timestamp": 0, "payload": None}


def is_market_open(now_ist: datetime) -> bool:
    if now_ist.weekday() >= 5:  # Sat/Sun
        return False
    t = now_ist.time()
    return MARKET_OPEN <= t <= MARKET_CLOSE


def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def fetch_all_candles():
    """Returns {ticker: dataframe_of_todays_5m_candles}, skipped_list"""
    results, skipped = {}, []

    for batch in chunk(NIFTY_50, BATCH_SIZE):
        data = None
        for attempt in range(RETRY_PER_BATCH):
            try:
                data = yf.download(
                    tickers=batch, period="1d", interval="5m",
                    group_by="ticker", threads=True, progress=False,
                    auto_adjust=False,
                )
                if data is not None and not data.empty:
                    break
            except Exception:
                pass
            time.sleep(1)

        if data is None or data.empty:
            skipped.extend(batch)
            continue

        for tkr in batch:
            try:
                if isinstance(data.columns, pd.MultiIndex):
                    if tkr not in data.columns.get_level_values(0):
                        skipped.append(tkr)
                        continue
                    df = data[tkr].dropna(how="all")
                else:
                    df = data.dropna(how="all")

                if df is None or len(df) < 1:
                    skipped.append(tkr)
                    continue

                results[tkr] = df.sort_index()
            except Exception:
                skipped.append(tkr)

    return results, skipped


def build_status_payload():
    now_ist = datetime.now(IST)
    candles, skipped = fetch_all_candles()

    bullish, bearish, inside = [], [], []

    for tkr, df in candles.items():
        symbol = tkr.replace(".NS", "")
        if len(df) < 1:
            continue

        c1 = df.iloc[0]
        latest = df.iloc[-1]

        try:
            c1_high, c1_low = float(c1["High"]), float(c1["Low"])
            current_price = float(latest["Close"])
        except Exception:
            continue

        candle2_signal = "none"
        if len(df) >= 2:
            c2 = df.iloc[1]
            c2_high, c2_low = float(c2["High"]), float(c2["Low"])
            if c2_high > c1_high:
                candle2_signal = "bullish_cross"
            elif c2_low < c1_low:
                candle2_signal = "bearish_cross"

        row = {
            "symbol": symbol,
            "current_price": round(current_price, 2),
            "c1_high": round(c1_high, 2),
            "c1_low": round(c1_low, 2),
            "candle2_signal": candle2_signal,
            "last_candle_time": df.index[-1].tz_convert(IST).strftime("%H:%M"),
        }

        if current_price > c1_high:
            row["pct"] = round((current_price - c1_high) / c1_high * 100, 2)
            bullish.append(row)
        elif current_price < c1_low:
            row["pct"] = round((c1_low - current_price) / c1_low * 100, 2)
            bearish.append(row)
        else:
            inside.append(row)

    bullish.sort(key=lambda r: r["pct"], reverse=True)
    bearish.sort(key=lambda r: r["pct"], reverse=True)

    return {
        "generated_at": now_ist.strftime("%Y-%m-%d %H:%M:%S IST"),
        "market_open": is_market_open(now_ist),
        "bullish": bullish,
        "bearish": bearish,
        "inside_count": len(inside),
        "skipped": [s.replace(".NS", "") for s in skipped],
        "total_tracked": len(candles),
    }


@app.get("/")
def root():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/status")
def api_status():
    now = time.time()
    with _cache_lock:
        if _cache["payload"] is not None and (now - _cache["timestamp"]) < CACHE_TTL_SECONDS:
            return JSONResponse(_cache["payload"])

    payload = build_status_payload()

    with _cache_lock:
        _cache["timestamp"] = time.time()
        _cache["payload"] = payload

    return JSONResponse(payload)


@app.get("/api/health")
def health():
    return {"ok": True, "time": datetime.now(IST).isoformat()}
