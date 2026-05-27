"""Parser tests for Binance combined-stream frames.

Payload shapes are taken from Binance's public WebSocket documentation:
- @trade:      https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#trade-streams
- @bookTicker: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#individual-symbol-book-ticker-streams
"""

from __future__ import annotations

import json

from btc_edge.collectors.binance import parse_frame


TRADE_FRAME = json.dumps({
    "stream": "btcusdt@trade",
    "data": {
        "e": "trade",
        "E": 1716800000100,
        "s": "BTCUSDT",
        "t": 12345,
        "p": "68000.50",
        "q": "0.001",
        "T": 1716800000099,
        "m": False,
        "M": True,
    },
})

# bookTicker frames have no "E" event-time field.
BOOK_TICKER_FRAME = json.dumps({
    "stream": "btcusdt@bookTicker",
    "data": {
        "u": 400900217,
        "s": "BTCUSDT",
        "b": "68000.00",
        "B": "1.5",
        "a": "68000.10",
        "A": "2.0",
    },
})


def test_parse_trade_frame() -> None:
    record = parse_frame(TRADE_FRAME, collector_ts_ms=1716800000200)

    assert record["source"] == "binance"
    assert record["stream"] == "trade"
    assert record["symbol"] == "BTCUSDT"
    assert record["collector_ts_ms"] == 1716800000200
    assert record["event_ts_ms"] == 1716800000100
    assert record["payload"]["p"] == "68000.50"


def test_parse_book_ticker_frame_has_null_event_ts() -> None:
    record = parse_frame(BOOK_TICKER_FRAME, collector_ts_ms=1716800000300)

    assert record["stream"] == "bookTicker"
    assert record["event_ts_ms"] is None
    assert record["collector_ts_ms"] == 1716800000300
    assert record["payload"]["b"] == "68000.00"
    assert record["payload"]["a"] == "68000.10"
