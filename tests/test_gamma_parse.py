"""Parser/filter tests for the Polymarket Gamma poller.

Canned market dicts are simplified versions of real Gamma responses
captured against ``https://gamma-api.polymarket.com/markets`` in May 2026.
"""

from __future__ import annotations

from btc_edge.collectors.polymarket_gamma import (
    BTC_UPDOWN_SLUG,
    build_record,
    is_btc_5m,
)


BTC_5M_MARKET = {
    "id": "2369179",
    "conditionId": "0xe035001dedf2ba34e95104fcf99fd32664ea8fd501d0f2a66d6a9c07c0347c57",
    "slug": "btc-updown-5m-1779964500",
    "question": "Bitcoin Up or Down - May 28, 6:35AM-6:40AM ET",
    "startDate": "2026-05-27T10:42:44.541580Z",
    "endDate": "2026-05-28T10:40:00Z",
    "outcomes": '["Up", "Down"]',
    "clobTokenIds": '["1385688350252415520260929435539029846067699253436312565976094800371129994277", "73685113065917254405742934230294480861989723632637587262744395635954502682761"]',
    "active": True,
    "closed": False,
}

BTC_15M_MARKET = {**BTC_5M_MARKET, "slug": "btc-updown-15m-1779964200"}
DAILY_BTC_MARKET = {**BTC_5M_MARKET, "slug": "bitcoin-above-76600-on-may-27-2026-8am-et"}
NON_BTC_MARKET = {**BTC_5M_MARKET, "slug": "will-israel-launch-a-major-ground-offensive"}


def test_slug_regex_accepts_only_btc_5m() -> None:
    assert BTC_UPDOWN_SLUG.match("btc-updown-5m-1779964500")
    assert not BTC_UPDOWN_SLUG.match("btc-updown-15m-1779964500")
    assert not BTC_UPDOWN_SLUG.match("btc-updown-5m-")
    assert not BTC_UPDOWN_SLUG.match("eth-updown-5m-1779964500")


def test_is_btc_5m_filter() -> None:
    assert is_btc_5m(BTC_5M_MARKET) is True
    assert is_btc_5m(BTC_15M_MARKET) is False
    assert is_btc_5m(DAILY_BTC_MARKET) is False
    assert is_btc_5m(NON_BTC_MARKET) is False
    assert is_btc_5m({}) is False


def test_build_record_shape() -> None:
    record = build_record(BTC_5M_MARKET, collector_ts_ms=1779878465320)

    assert record["source"] == "polymarket_gamma"
    assert record["stream"] == "market_snapshot"
    assert record["collector_ts_ms"] == 1779878465320
    assert record["event_ts_ms"] is None
    assert record["condition_id"] == BTC_5M_MARKET["conditionId"]
    assert record["slug"] == BTC_5M_MARKET["slug"]
    assert record["outcomes"] == ["Up", "Down"]
    assert record["token_ids"] == [
        "1385688350252415520260929435539029846067699253436312565976094800371129994277",
        "73685113065917254405742934230294480861989723632637587262744395635954502682761",
    ]
    assert record["start_date"] == BTC_5M_MARKET["startDate"]
    assert record["end_date"] == BTC_5M_MARKET["endDate"]
    assert record["payload"] is BTC_5M_MARKET


def test_build_record_handles_missing_tokens() -> None:
    market = {"slug": "btc-updown-5m-1", "conditionId": "0xabc"}
    record = build_record(market, collector_ts_ms=1)
    assert record["token_ids"] == []
    assert record["outcomes"] == []
