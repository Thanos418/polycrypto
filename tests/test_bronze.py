"""Round-trip tests for BronzeWriter."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

from btc_edge.storage.bronze import BronzeWriter


def test_round_trip_preserves_order_and_content(tmp_path: Path) -> None:
    writer = BronzeWriter("test_source", base_dir=tmp_path)
    records = [
        {"i": 0, "msg": "alpha"},
        {"i": 1, "msg": "beta"},
        {"i": 2, "msg": "gamma"},
    ]
    for rec in records:
        writer.write(rec)
    writer.close()

    files = list((tmp_path / "test_source").glob("*.jsonl.gz"))
    assert len(files) == 1, f"expected exactly one daily file, got {files}"

    with gzip.open(files[0], "rt", encoding="utf-8") as fh:
        lines = [json.loads(line) for line in fh]

    assert lines == records


def test_appends_across_writer_instances(tmp_path: Path) -> None:
    """Re-opening the same day's file should append, not overwrite."""
    BronzeWriter("test_source", base_dir=tmp_path).write({"n": 1})
    w2 = BronzeWriter("test_source", base_dir=tmp_path)
    w2.write({"n": 2})
    w2.close()

    files = list((tmp_path / "test_source").glob("*.jsonl.gz"))
    assert len(files) == 1
    with gzip.open(files[0], "rt", encoding="utf-8") as fh:
        lines = [json.loads(line) for line in fh]
    assert lines == [{"n": 1}, {"n": 2}]
