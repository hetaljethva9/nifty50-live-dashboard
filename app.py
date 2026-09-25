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
  candle2_signal = "bullish_cross" if Candle2.Close > Candle1.High
                   "bearish_cross" if Candle2.Close < Candle1.Low
                   "none"          otherwise
  (Candle 2 = 09:20-09:25 IST, only known once >= 2 candles exist. The
  signal is based on the candle's CLOSE, i.e. it only counts if the
  09:20-09:25 candle closes beyond Candle 1's range, not just wicks
  through it intrabar.)

  Separately, for all 50 stocks (regardless of bullish/bearish/inside
  status), also computes a Supertrend indicator (period=10, multiplier=3)
  on the 5-min chart using 5 days of history for a stable ATR, and logs
  every Supertrend direction flip (bullish<->bearish) that has occurred
  so far TODAY for each stock -- a running log for the day, not just the
  latest candle. A stock can appear multiple times if it flipped more
  than once. Reported as "supertrend_flips" in the API response.

  Each flip is further filtered/annotated per the dual-timeframe strategy
  in the user's trading notes: each stock's current 15-min Supertrend
  (10,3) is computed as the "macro bias," and a flip is only kept if its
  direction matches that bias (aligned_with_15m) -- counter-trend flips
  are dropped entirely, not just flagged. Kept flips also carry
  "after_945" (the strategy's rule to wait for the first 15-min candle to
  close before acting); strategy_signal = "valid_entry" once both
  conditions hold, else "aligned_too_early". This is a Supertrend
  confirmation signal only -- it does not replicate the
  original strategy's expiry-day scheduling, OI-wall checks, strike
  selection, or hedge sizing, since those require options-chain data
  this dashboard doesn't have.

Data source: yfinance (Yahoo Finance). ~15 min delayed, not tick-level.
Results are cached for CACHE_TTL_SECONDS to avoid hammering Yahoo on every
frontend poll (multiple browser tabs / fast refresh intervals all share
one cache). The 15-min trend is cached separately for longer
(CACHE_TTL_15M_SECONDS) since it changes far less often.

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

CACHE_TTL_SECONDS = 150    # min time between real Yahoo fetches (raised to ease rate limiting)
BATCH_SIZE = 10            # smaller batches = smaller/less bursty requests to Yahoo
RETRY_PER_BATCH = 3
BATCH_DELAY_SECONDS = 3    # pause between batches so we don't hammer Yahoo back-to-back
RETRY_BACKOFF_BASE = 5     # seconds; doubles each retry (5, 10, 20...) on rate-limit errors

SUPERTREND_PERIOD = 10      # ATR lookback (in 5-min candles)
SUPERTREND_MULTIPLIER = 3   # standard default

app = FastAPI(title="Nifty 50 Live Opening Range Dashboard")

STATIC_DIR = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

EXECUTION_CUTOFF = dtime(9, 45)   # strategy rule: ignore signals before the first 15-min candle closes

_cache_lock = threading.Lock()
_cache = {"timestamp": 0, "payload": None}

CACHE_TTL_15M_SECONDS = 900   # 15-min trend changes slowly; refresh far less often than the 5-min data
_cache_15m_lock = threading.Lock()
_cache_15m = {"timestamp": 0, "trends": None}


def is_market_open(now_ist: datetime) -> bool:
    if now_ist.weekday() >= 5:  # Sat/Sun
        return False
    t = now_ist.time()
    return MARKET_OPEN <= t <= MARKET_CLOSE


def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def fetch_candles(interval, period):
    """
    Generic batched/backed-off fetch of {ticker: dataframe}, skipped_list
    for a given yfinance interval+period. Used for both the 5-min candles
    (opening range + Supertrend flips) and the 15-min candles (macro trend
    filter). Batches are kept small, spaced out, and back off on failure
    to avoid tripping Yahoo Finance's rate limiter (YFRateLimitError). A
    ticker that still fails after all retries is skipped rather than
    failing the whole request.
    """
    results, skipped = {}, []
    batches = list(chunk(NIFTY_50, BATCH_SIZE))

    for batch_num, batch in enumerate(batches):
        data = None
        for attempt in range(RETRY_PER_BATCH):
            try:
                data = yf.download(
                    tickers=batch, period=period, interval=interval,
                    group_by="ticker", threads=True, progress=False,
                    auto_adjust=False,
                )
                if data is not None and not data.empty:
                    break
            except Exception:
                pass
            # Exponential backoff before retrying this same batch.
            time.sleep(RETRY_BACKOFF_BASE * (2 ** attempt))

        if data is None or data.empty:
            skipped.extend(batch)
        else:
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

        # Pause between batches (not after the last one) to spread out
        # requests instead of firing them back-to-back.
        if batch_num < len(batches) - 1:
            time.sleep(BATCH_DELAY_SECONDS)

    return results, skipped


def fetch_all_candles():
    """
    Returns {ticker: dataframe_of_5m_candles}, skipped_list.

    Fetches 5 trading days of 5-min candles (not just today) because the
    Supertrend indicator needs a lookback of SUPERTREND_PERIOD candles of
    history to compute a stable ATR; today's candles alone (esp. early in
    the session) aren't enough. Callers that only care about today's
    session (Candle 1 / Candle 2 logic) should filter to today's rows
    themselves -- see split_today().
    """
    return fetch_candles(interval="5m", period="5d")


def split_today(df, now_ist):
    """Returns the subset of df whose candles fall on today's IST date."""
    today = now_ist.date()
    mask = df.index.tz_convert(IST).date == today
    return df[mask]


def compute_supertrend(df, period=SUPERTREND_PERIOD, multiplier=SUPERTREND_MULTIPLIER):
    """
    Standard Supertrend indicator. Returns a pandas Series of trend
    direction per candle: 1 = uptrend, -1 = downtrend. Uses Wilder-style
    ATR smoothing. First `period` rows are unreliable (ATR still warming
    up) -- callers should only trust trend values after that point, which
    is why fetch_all_candles() pulls multiple days of history.
    """
    high, low, close = df["High"], df["Low"], df["Close"]
    prev_close = close.shift(1)

    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

    hl2 = (high + low) / 2
    raw_upper = hl2 + multiplier * atr
    raw_lower = hl2 - multiplier * atr

    final_upper = raw_upper.copy()
    final_lower = raw_lower.copy()
    trend = pd.Series(1, index=df.index, dtype="int64")

    for i in range(1, len(df)):
        if pd.isna(atr.iloc[i]):
            continue

        if pd.isna(final_upper.iloc[i - 1]):
            # First candle with a valid ATR: nothing to ratchet against yet,
            # so bootstrap directly from the raw bands.
            final_upper.iloc[i] = raw_upper.iloc[i]
            final_lower.iloc[i] = raw_lower.iloc[i]
            trend.iloc[i] = trend.iloc[i - 1]
            continue

        final_upper.iloc[i] = (
            raw_upper.iloc[i]
            if (raw_upper.iloc[i] < final_upper.iloc[i - 1] or close.iloc[i - 1] > final_upper.iloc[i - 1])
            else final_upper.iloc[i - 1]
        )
        final_lower.iloc[i] = (
            raw_lower.iloc[i]
            if (raw_lower.iloc[i] > final_lower.iloc[i - 1] or close.iloc[i - 1] < final_lower.iloc[i - 1])
            else final_lower.iloc[i - 1]
        )

        prev_trend = trend.iloc[i - 1]
        if prev_trend == 1 and close.iloc[i] < final_lower.iloc[i]:
            trend.iloc[i] = -1
        elif prev_trend == -1 and close.iloc[i] > final_upper.iloc[i]:
            trend.iloc[i] = 1
        else:
            trend.iloc[i] = prev_trend

    return trend


def get_15m_trends():
    """
    Returns {ticker: "bullish"|"bearish"} = each stock's CURRENT 15-min
    Supertrend direction (10, 3) -- the "macro bias" from the dual
    timeframe strategy. Cached separately from the 5-min data (much
    longer TTL) since a 15-min trend only updates every 15 minutes and
    fetching it doesn't need to happen on the same cadence as the 5-min
    poll. Uses 60 days of 15-min history (yfinance's max for this
    interval) so the ATR is well warmed up.
    """
    now = time.time()
    with _cache_15m_lock:
        if _cache_15m["trends"] is not None and (now - _cache_15m["timestamp"]) < CACHE_TTL_15M_SECONDS:
            return _cache_15m["trends"]

    candles, _skipped = fetch_candles(interval="15m", period="60d")
    trends = {}
    for tkr, df in candles.items():
        if len(df) < SUPERTREND_PERIOD + 2:
            continue
        trend = compute_supertrend(df).dropna()
        if trend.empty:
            continue
        trends[tkr] = "bullish" if trend.iloc[-1] == 1 else "bearish"

    with _cache_15m_lock:
        _cache_15m["timestamp"] = time.time()
        _cache_15m["trends"] = trends

    return trends


def get_supertrend_flips_today(full_df, now_ist):
    """
    Runs Supertrend on the full (multi-day) candle history for a stock and
    returns every candle-to-candle flip (bullish<->bearish) whose flip
    candle falls on today's IST date -- i.e. a running log of all
    Supertrend direction changes so far today, not just the latest one.
    A stock can appear more than once if it flipped several times today.
    Returns [] if there's not enough history yet for a stable ATR.
    """
    if len(full_df) < SUPERTREND_PERIOD + 2:
        return []

    trend = compute_supertrend(full_df)
    if trend.isna().all():
        return []

    today = now_ist.date()
    flips = []

    for i in range(1, len(full_df)):
        t_i, t_prev = trend.iloc[i], trend.iloc[i - 1]
        if pd.isna(t_i) or pd.isna(t_prev) or t_i == t_prev:
            continue

        candle_time = full_df.index[i].tz_convert(IST)
        if candle_time.date() != today:
            continue

        flips.append({
            "new_trend": "bullish" if t_i == 1 else "bearish",
            "prev_trend": "bullish" if t_prev == 1 else "bearish",
            "price_at_flip": round(float(full_df["Close"].iloc[i]), 2),
            "flip_time": (candle_time + pd.Timedelta(minutes=5)).strftime("%H:%M"),
        })

    return flips


def find_first_breakout_time(df, c1_high, c1_low, direction):
    """
    Scans candles from index 1 (Candle 2) onward for the first candle whose
    High broke above c1_high (direction='bullish') or whose Low broke below
    c1_low (direction='bearish'). Returns that candle's CLOSE time (its open
    time + 5 min) as "HH:MM" IST, or None if not found (shouldn't happen for
    a row already classified into that direction, since the latest candle's
    own high/low will satisfy it by construction).
    """
    for i in range(1, len(df)):
        candle = df.iloc[i]
        try:
            if direction == "bullish" and float(candle["High"]) > c1_high:
                return (df.index[i].tz_convert(IST) + pd.Timedelta(minutes=5)).strftime("%H:%M")
            if direction == "bearish" and float(candle["Low"]) < c1_low:
                return (df.index[i].tz_convert(IST) + pd.Timedelta(minutes=5)).strftime("%H:%M")
        except Exception:
            continue
    return None


def build_status_payload():
    now_ist = datetime.now(IST)
    candles, skipped = fetch_all_candles()
    trends_15m = get_15m_trends()

    bullish, bearish, inside, supertrend_flips = [], [], [], []

    for tkr, full_df in candles.items():
        symbol = tkr.replace(".NS", "")
        trend_15m = trends_15m.get(tkr)

        st_flips = get_supertrend_flips_today(full_df, now_ist)
        for f in st_flips:
            flip_h, flip_m = (int(x) for x in f["flip_time"].split(":"))
            after_cutoff = dtime(flip_h, flip_m) >= EXECUTION_CUTOFF
            aligned = trend_15m is not None and f["new_trend"] == trend_15m

            if aligned and after_cutoff:
                strategy_signal = "valid_entry"
            elif aligned and not after_cutoff:
                strategy_signal = "aligned_too_early"
            else:
                # Counter-trend vs. the 15-min bias (or 15-min bias unknown) --
                # not an actionable signal per the strategy, so skip it.
                continue

            supertrend_flips.append({
                "symbol": symbol,
                "trend_15m": trend_15m,
                "aligned_with_15m": aligned,
                "after_945": after_cutoff,
                "strategy_signal": strategy_signal,
                **f,
            })

        df = split_today(full_df, now_ist)
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
        confirmed_at = None
        if len(df) >= 2:
            c2 = df.iloc[1]
            c2_close = float(c2["Close"])
            if c2_close > c1_high:
                candle2_signal = "bullish_cross"
            elif c2_close < c1_low:
                candle2_signal = "bearish_cross"

            if candle2_signal != "none":
                # Candle 2 covers [c2_open, c2_open + 5min); the signal is only
                # known once that candle closes, so "confirmed at" = its close time.
                c2_close_time = df.index[1].tz_convert(IST) + pd.Timedelta(minutes=5)
                confirmed_at = c2_close_time.strftime("%H:%M")

        row = {
            "symbol": symbol,
            "current_price": round(current_price, 2),
            "c1_high": round(c1_high, 2),
            "c1_low": round(c1_low, 2),
            "candle2_signal": candle2_signal,
            "confirmed_at": confirmed_at,
            "last_candle_time": df.index[-1].tz_convert(IST).strftime("%H:%M"),
        }

        if current_price > c1_high:
            row["pct"] = round((current_price - c1_high) / c1_high * 100, 2)
            row["first_breakout_time"] = find_first_breakout_time(df, c1_high, c1_low, "bullish")
            bullish.append(row)
        elif current_price < c1_low:
            row["pct"] = round((c1_low - current_price) / c1_low * 100, 2)
            row["first_breakout_time"] = find_first_breakout_time(df, c1_high, c1_low, "bearish")
            bearish.append(row)
        else:
            inside.append(row)

    bullish.sort(key=lambda r: r["pct"], reverse=True)
    bearish.sort(key=lambda r: r["pct"], reverse=True)
    supertrend_flips.sort(key=lambda r: r["flip_time"], reverse=True)

    strategy_context = {
        "day_name": now_ist.strftime("%A"),
        "after_945": now_ist.time() >= EXECUTION_CUTOFF,
        "note": (
            "Treat 'Valid Entry' below as a Supertrend dual-timeframe "
            "confirmation signal only, not a full trade recommendation "
            "(no OI/strike/hedge data here)."
        ),
    }

    return {
        "generated_at": now_ist.strftime("%Y-%m-%d %H:%M:%S IST"),
        "market_open": is_market_open(now_ist),
        "bullish": bullish,
        "bearish": bearish,
        "inside_count": len(inside),
        "supertrend_flips": supertrend_flips,
        "strategy_context": strategy_context,
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
