# Nifty 50 — Live Opening Range Dashboard

A live, auto-refreshing (1–5 min) dashboard tracking all 50 Nifty stocks
against their own opening range (first 5-min candle, 09:15–09:20 IST).

- **Bullish table:** stocks currently trading *above* their Candle 1 High
- **Bearish table:** stocks currently trading *below* their Candle 1 Low
- A green **confirmed** tag means the original fixed rule also fired
  (Candle 2 crossed Candle 1's high/low) — the rest are live moves that
  happened later in the day, beyond the original 2-candle check.

Data source: Yahoo Finance via `yfinance` — free, no API key, but
**~15 minutes delayed and not tick-level**. Good for a monitoring dashboard,
not for split-second execution. For true real-time you'd need a broker
API/websocket feed (see "Going truly real-time" below).

## How it works

- `app.py` — a small FastAPI backend. On each request to `/api/status` it
  fetches today's 5-min candles for all 50 stocks, compares latest price to
  the opening range, and returns JSON. Responses are cached for 60 seconds
  so multiple browser refreshes don't hammer Yahoo Finance.
- `static/index.html` — the dashboard page. It polls `/api/status` on
  whatever interval you pick (1/2/5 min) and re-renders the tables — no
  page reload needed.

Because the backend fetches fresh data **on-demand** (triggered by the
page polling it) rather than running a background loop, it works fine on
free hosting tiers that spin down when idle — the moment your browser tab
asks for data, it wakes up and fetches it.

## Deploy to the cloud (Render.com, free tier)

1. Push this folder to a new GitHub repo.
2. Go to [render.com](https://render.com) → New → **Blueprint** → connect
   the repo. Render will read `render.yaml` and configure everything
   automatically (or do it manually: New → Web Service → Environment:
   Python 3 → Build command `pip install -r requirements.txt` → Start
   command `uvicorn app:app --host 0.0.0.0 --port $PORT`).
3. Deploy. Render gives you a URL like
   `https://nifty50-live-dashboard.onrender.com` — open it, that's your
   live dashboard.
4. Bookmark it / open it on your phone. Leave the tab open during market
   hours and it keeps refreshing itself.

**Free tier note:** Render's free web services sleep after ~15 min of no
traffic and take ~30-50 seconds to wake on the next request. That just
means: if nobody has the dashboard open, it naturally pauses; the moment
you open the tab it wakes up and starts refreshing normally. If you want
zero cold-start delay, upgrade that one service to Render's cheapest paid
tier (~$7/mo).

## Run it locally first (recommended before deploying)

```bash
pip install -r requirements.txt
uvicorn app:app --reload --port 8000
```

Open http://localhost:8000 in your browser.

## Going truly real-time (optional upgrade path)

To move beyond "auto-refreshing every few minutes with slightly delayed
data" to genuine tick-by-tick updates, you'd swap the `fetch_all_candles()`
function in `app.py` to pull from a broker's websocket feed instead of
yfinance (e.g. Zerodha Kite Connect, Upstox, or Angel One — all require a
paid API subscription and your trading account credentials). The rest of
the dashboard (caching layer, frontend, tables) stays the same — happy to
build that swap if/when you have API access to one of those.

## Notes

- Nifty 50 constituents change periodically — update `nifty50_list.py`
  when NSE rebalances the index.
- `/api/health` is a simple health-check endpoint if you want to wire up
  an uptime monitor.
- Want SMS/Telegram/email alerts the moment a stock flips bullish/bearish,
  instead of having to watch the dashboard? That's a straightforward
  addition — just ask.
