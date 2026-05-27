# btc-edge

Phase 1 measurement project: collect Binance spot + Polymarket BTC 5-min market data, then quantify whether Polymarket prices deviate from a fair-value model. No trading logic.

## Setup

Requires Python 3.11+ and [uv](https://github.com/astral-sh/uv).

```bash
uv sync
cp .env.example .env   # optional: edit if you want a custom DATA_DIR
```

## Run

### Binance collector

```bash
uv run python -m btc_edge.collectors.binance
```

Persists every `btcusdt@trade` and `btcusdt@bookTicker` event from Binance to `data/bronze/binance/YYYY-MM-DD.jsonl.gz` (UTC date). Runs until Ctrl-C.

Each line is a JSON record:

```json
{
  "source": "binance",
  "stream": "trade",
  "symbol": "BTCUSDT",
  "collector_ts_ms": 1716800000123,
  "event_ts_ms": 1716800000100,
  "payload": { ... }
}
```

`event_ts_ms` is `null` for `bookTicker` (Binance does not include event time on that stream).

Inspect output (Git Bash):

```bash
zcat data/bronze/binance/$(date -u +%F).jsonl.gz | head -5
```

## Tests

```bash
uv run pytest
```

## Project layout

```
src/btc_edge/
  collectors/binance.py    # Binance WS collector
  storage/bronze.py        # gzipped JSONL writer, daily UTC rotation
  config.py                # env + paths
  logging_setup.py         # stdlib logging + JSON formatter
tests/                     # unit tests, no network
data/                      # bronze/silver/gold (gitignored, created at runtime)
notebooks/                 # analysis (later PR)
```

## Scope

Phase 1 = measurement only. No order placement, no backtester, no ML, no dashboard. See `CLAUDE.md` for behavioral guidelines and the plan file under `.claude/plans/` for PR-level scope.
