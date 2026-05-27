"""Polymarket Gamma poller for BTC 5-minute "Up or Down" markets.

Every 30 seconds, fetches the active+open market list from Gamma and
emits one bronze record per market matching the ``btc-updown-5m-*``
slug pattern. Bronze keeps each snapshot raw — deduping and silver-layer
typing happens later.

Discovered slug patterns (empirically, May 2026):
    btc-updown-5m-<unix_end_ts>       5-minute Up/Down markets
    btc-updown-15m-<unix_end_ts>      15-minute Up/Down markets

Phase 1 scope: 5m only. Extend later by widening ``BTC_UPDOWN_SLUG``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import signal
import time
from typing import Any

import httpx

from btc_edge.logging_setup import configure_logging, log_with
from btc_edge.storage.bronze import BronzeWriter

LOG = logging.getLogger("btc_edge.polymarket_gamma")

GAMMA_URL = "https://gamma-api.polymarket.com/markets"
BTC_UPDOWN_SLUG = re.compile(r"^btc-updown-5m-\d+$")
POLL_INTERVAL_S = 30.0
HTTP_TIMEOUT_S = 10.0
PAGE_LIMIT = 500


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def is_btc_5m(market: dict[str, Any]) -> bool:
    """True if the market slug matches the BTC 5m Up/Down pattern."""
    slug = market.get("slug") or ""
    return bool(BTC_UPDOWN_SLUG.match(slug))


def _token_ids(market: dict[str, Any]) -> list[str]:
    """Decode the JSON-encoded ``clobTokenIds`` field. Returns ``[]`` if absent."""
    raw = market.get("clobTokenIds")
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(t) for t in raw]
    return [str(t) for t in json.loads(raw)]


def _outcomes(market: dict[str, Any]) -> list[str]:
    raw = market.get("outcomes")
    if not raw:
        return []
    if isinstance(raw, list):
        return list(raw)
    return list(json.loads(raw))


def build_record(market: dict[str, Any], collector_ts_ms: int) -> dict[str, Any]:
    """Build one bronze record from a Gamma market dict."""
    return {
        "source": "polymarket_gamma",
        "stream": "market_snapshot",
        "collector_ts_ms": collector_ts_ms,
        "event_ts_ms": None,  # Gamma doesn't expose a server event time
        "condition_id": market.get("conditionId"),
        "slug": market.get("slug"),
        "token_ids": _token_ids(market),
        "outcomes": _outcomes(market),
        "start_date": market.get("startDate"),
        "end_date": market.get("endDate"),
        "payload": market,
    }


async def fetch_markets(client: httpx.AsyncClient) -> list[dict[str, Any]]:
    """Fetch one page of active+open markets from Gamma."""
    resp = await client.get(
        GAMMA_URL,
        params={
            "active": "true",
            "closed": "false",
            "limit": PAGE_LIMIT,
            "order": "startDate",
            "ascending": "false",
        },
    )
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise ValueError(f"unexpected Gamma response shape: {type(data).__name__}")
    return data


async def run() -> None:
    """Poll loop: fetch -> filter -> write -> sleep 30s. Runs until SIGINT/SIGTERM."""
    configure_logging()
    writer = BronzeWriter("polymarket_gamma")

    stop = asyncio.Event()

    def _stop(*_: object) -> None:
        stop.set()

    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _stop)
    except NotImplementedError:
        pass

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_S) as client:
            while not stop.is_set():
                try:
                    markets = await fetch_markets(client)
                    ts = _now_ms()
                    matched = [m for m in markets if is_btc_5m(m)]
                    for m in matched:
                        writer.write(build_record(m, ts))
                    log_with(
                        LOG, logging.INFO, "gamma.poll",
                        total=len(markets), matched=len(matched),
                    )
                except httpx.HTTPError as exc:
                    log_with(LOG, logging.WARNING, "gamma.http_error", err=repr(exc))
                # Sleep but wake early on stop signal.
                try:
                    await asyncio.wait_for(stop.wait(), timeout=POLL_INTERVAL_S)
                except asyncio.TimeoutError:
                    pass
    finally:
        writer.close()
        log_with(LOG, logging.INFO, "gamma.stopped")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
