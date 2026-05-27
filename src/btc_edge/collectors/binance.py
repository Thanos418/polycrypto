"""Binance WebSocket collector for BTCUSDT trade + bookTicker streams.

Runs an asyncio loop that:

1. Connects to Binance's combined-stream endpoint.
2. Parses each frame, stamps it with a millisecond collector timestamp.
3. Writes one bronze JSONL record per event.
4. Reconnects with exponential backoff (capped at 30s) on disconnect.

Parse and writer errors propagate — fail loud, no silent skip. Only
``ConnectionClosed`` is caught to drive reconnection.
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed

from btc_edge.logging_setup import configure_logging, log_with
from btc_edge.storage.bronze import BronzeWriter

LOG = logging.getLogger("btc_edge.binance")

SYMBOL = "BTCUSDT"
WS_URL = (
    "wss://stream.binance.com:9443/stream"
    "?streams=btcusdt@trade/btcusdt@bookTicker"
)
PING_INTERVAL_S = 20
PING_TIMEOUT_S = 20
RECONNECT_BACKOFF_INITIAL_S = 1.0
RECONNECT_BACKOFF_MAX_S = 30.0


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _stream_kind(stream_name: str) -> str:
    """Map Binance combined-stream name to our short kind tag.

    ``btcusdt@trade`` -> ``trade``; ``btcusdt@bookTicker`` -> ``bookTicker``.
    """
    if "@" not in stream_name:
        raise ValueError(f"unexpected stream name: {stream_name!r}")
    return stream_name.split("@", 1)[1]


def parse_frame(raw: str | bytes, collector_ts_ms: int) -> dict[str, Any]:
    """Parse one Binance combined-stream frame into a bronze record.

    Expects the envelope ``{"stream": "btcusdt@xxx", "data": {...}}``.
    ``event_ts_ms`` is taken from ``data.E`` (trade has it, bookTicker does
    not — falls back to ``None``).
    """
    frame = json.loads(raw)
    stream_name = frame["stream"]
    data = frame["data"]
    kind = _stream_kind(stream_name)
    event_ts = data.get("E")
    return {
        "source": "binance",
        "stream": kind,
        "symbol": SYMBOL,
        "collector_ts_ms": collector_ts_ms,
        "event_ts_ms": int(event_ts) if event_ts is not None else None,
        "payload": data,
    }


async def _consume(ws: Any, writer: BronzeWriter) -> None:
    async for raw in ws:
        record = parse_frame(raw, _now_ms())
        writer.write(record)


async def run() -> None:
    """Main loop: connect, consume, reconnect with exponential backoff."""
    configure_logging()
    writer = BronzeWriter("binance")
    backoff = RECONNECT_BACKOFF_INITIAL_S

    stop = asyncio.Event()

    def _stop(*_: object) -> None:
        stop.set()

    # SIGINT/SIGTERM: set the stop event so the outer loop exits cleanly.
    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _stop)
    except NotImplementedError:
        # Windows: add_signal_handler is unsupported on ProactorEventLoop.
        # KeyboardInterrupt still works to terminate the process.
        pass

    try:
        while not stop.is_set():
            try:
                log_with(LOG, logging.INFO, "binance.connect", url=WS_URL)
                async with websockets.connect(
                    WS_URL,
                    ping_interval=PING_INTERVAL_S,
                    ping_timeout=PING_TIMEOUT_S,
                ) as ws:
                    log_with(LOG, logging.INFO, "binance.connected")
                    backoff = RECONNECT_BACKOFF_INITIAL_S
                    consumer = asyncio.create_task(_consume(ws, writer))
                    stopper = asyncio.create_task(stop.wait())
                    done, pending = await asyncio.wait(
                        {consumer, stopper}, return_when=asyncio.FIRST_COMPLETED
                    )
                    for task in pending:
                        task.cancel()
                    for task in done:
                        # Re-raise any exception from the consumer (parse/write errors).
                        if task is consumer:
                            task.result()
            except ConnectionClosed as exc:
                log_with(
                    LOG, logging.WARNING, "binance.disconnect",
                    code=exc.code, reason=str(exc.reason), backoff_s=backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, RECONNECT_BACKOFF_MAX_S)
            except OSError as exc:
                # Network-level failure before WS handshake completes.
                log_with(
                    LOG, logging.WARNING, "binance.network_error",
                    err=repr(exc), backoff_s=backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, RECONNECT_BACKOFF_MAX_S)
    finally:
        writer.close()
        log_with(LOG, logging.INFO, "binance.stopped")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
