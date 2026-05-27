"""Bronze-layer writer: append-only gzipped JSONL with UTC daily rotation."""

from __future__ import annotations

import gzip
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO

from btc_edge.config import BRONZE_DIR


class BronzeWriter:
    """Append JSON records to ``{BRONZE_DIR}/{source}/{UTC-date}.jsonl.gz``.

    Files are opened in binary-append mode; gzip's concatenated-member format
    means later appends remain valid gzip streams that ``gzip``, ``zcat``, and
    DuckDB read transparently.

    Rotation is checked on every write — if the UTC date has changed since the
    current file was opened, the file is closed and a new one is opened.

    Each ``write`` flushes immediately. At Binance trade rates (<100 evt/s)
    flush-per-event is well within budget and prevents data loss on hard kill.
    """

    def __init__(self, source: str, base_dir: Path | None = None) -> None:
        self.source = source
        self.base_dir = (base_dir if base_dir is not None else BRONZE_DIR) / source
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._fh: BinaryIO | None = None
        self._current_date: str | None = None

    @staticmethod
    def _utc_date() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _path_for(self, date: str) -> Path:
        return self.base_dir / f"{date}.jsonl.gz"

    def _ensure_open(self) -> None:
        date = self._utc_date()
        if self._fh is None or date != self._current_date:
            self.close()
            self._fh = gzip.open(self._path_for(date), "ab")  # type: ignore[assignment]
            self._current_date = date

    def write(self, record: dict[str, Any]) -> None:
        """Append a single JSON record as one line."""
        self._ensure_open()
        assert self._fh is not None
        line = json.dumps(record, separators=(",", ":")).encode("utf-8") + b"\n"
        self._fh.write(line)
        self._fh.flush()

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None
            self._current_date = None

    def __enter__(self) -> "BronzeWriter":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
