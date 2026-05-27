# Plan & Progress — Polymarket BTC 5m Edge Detection

Phase 1 = measurement only. No order placement, no backtester, no ML, no dashboard. Decision to move to Phase 2 depends on whether the data shows a real edge.

## Done

### PR 1 — Scaffold + Binance WS collector (commit `1c241cc`)
- `uv`-managed Python project (3.11+), minimal deps: `websockets`, `python-dotenv`, `httpx`; dev: `pytest`, `pytest-asyncio`, `ruff`.
- `src/btc_edge/` package: `config.py`, `logging_setup.py` (stdlib + JSON formatter, ms UTC).
- `BronzeWriter` (`storage/bronze.py`): gzipped JSONL, UTC daily rotation, flush-per-write.
- Binance WS collector (`collectors/binance.py`): combined stream `btcusdt@trade/btcusdt@bookTicker`, ms timestamps, reconnect with exponential backoff (cap 30s), fail-loud on parse/write errors.
- Tests: bronze round-trip, trade/bookTicker parse (`event_ts_ms = None` for bookTicker because Binance does not include event time there).
- Live smoke (office network): 3,237 records in ~110s. Trade latency p50=401ms, **p95=4781ms** — concerning, likely corporate proxy. To be re-measured on personal PC.

### PR 2 — Polymarket Gamma poller (commit `9068b12`)
- `collectors/polymarket_gamma.py`: 30s poll of `https://gamma-api.polymarket.com/markets?active=true&closed=false`, slug filter `^btc-updown-5m-\d+$`, one bronze record per matched market per poll.
- Record captures: `conditionId`, both `clobTokenIds` (Up/Down), `outcomes`, `start_date`, `end_date`, full raw `payload`.
- **Plan correction:** the approved plan said *hourly* markets; real Gamma data shows the actual cadence is **5m** (with 15m also published). Scoped to 5m per the original project ask; 15m is a one-line regex change later.
- Tests: slug regex, filter behavior, record shape, missing-token fallback.
- **Live smoke blocked** by corporate SSL inspection (httpx certificate verify failed). To be repeated on personal PC.

### Plan-vs-reality deltas worth remembering
- Hourly → 5m (corrected; see PR 2 note).
- p95 trade latency higher than the oracle-lag hypothesis assumes — needs re-measurement off the office network before treating the substrate as trustworthy.

## In flight / pending

- **Personal-PC validation runs** (no code change, just rerun):
  - Binance collector ~5 min; recheck p50/p95 of `collector_ts_ms - event_ts_ms` for trades.
  - Gamma poller ~5 min; confirm SSL works and slug filter picks up the live BTC 5m stream.

## Up next

### PR 3 — Polymarket CLOB WebSocket collector
- Endpoint: `wss://ws-subscriptions-clob.polymarket.com/ws/market`.
- Subscribe to the union of `clobTokenIds` from currently-active BTC 5m markets (driven off the Gamma poller output, or a live REST seed at startup).
- Persist book-diff and trade events through `BronzeWriter("polymarket_clob")`.
- Open question: refresh the subscription set as new markets roll in (re-subscribe every N minutes, or on Gamma-poller signal). Keep it as a periodic re-subscribe initially; integrate signal channel only if needed.

### PR 4 — Silver layer + DuckDB views
- Parse bronze JSONL into typed Parquet per source per day.
- Unified ms timeline view joining Binance trades, Binance book ticker, Polymarket book/trade, and Gamma snapshots on `collector_ts_ms`.
- DuckDB queries used by the analysis notebook.

### PR 5 — Analysis notebook (after 24–48h of data)
For each closed market window, compute and plot:
- BTC spot trajectory + Polymarket YES/NO mid-price.
- Fair value from a Brownian / barrier approximation given current spot, strike, time remaining, and realized vol.
- Both **rolling realized vol** (lookback window TBD) and **static vol** as side-by-side benchmarks.
- Empirical distribution of `Polymarket mid − fair value`.
- Deviation magnitude vs time-to-resolution; persistence (time to revert within 2¢).
- Final-30s focus: how often does mid deviate from fair value by >10¢ with <30s left?

## Out of scope for Phase 1
- Order placement.
- Backtester.
- ML / learned models (fair value is closed-form math only).
- UI / dashboard (notebooks are sufficient).
- Live trading signals.

## Open questions worth answering before PR 5
- Which Chainlink/Pyth feed actually settles these markets? Confirms whether Binance is the right reference price or whether a feed-of-feeds composite would be closer.
- Does Polymarket publish resolution timestamps via Gamma, or only via on-chain? Affects how cleanly we can label "close" prices for the analysis.
