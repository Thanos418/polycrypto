"""Stdlib logging configured with a JSON formatter and ms-precision UTC timestamps."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    """Emit log records as single-line JSON with ISO UTC ms timestamps."""

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc)
        payload: dict[str, object] = {
            "ts": ts.isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        for key, value in record.__dict__.get("extra_fields", {}).items():
            payload[key] = value
        return json.dumps(payload, separators=(",", ":"))


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logger to emit JSON to stdout. Safe to call multiple times."""
    root = logging.getLogger()
    root.setLevel(level)
    for handler in list(root.handlers):
        root.removeHandler(handler)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)


def log_with(logger: logging.Logger, level: int, msg: str, /, **fields: object) -> None:
    """Helper to attach structured fields to a log record."""
    logger.log(level, msg, extra={"extra_fields": fields})
