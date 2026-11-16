"""Structured JSON logging with run_id propagation for all platform services."""

from __future__ import annotations

import contextlib
import contextvars
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from typing import Iterator

_run_id_var: ContextVar = contextvars.ContextVar("run_id", default="unassigned")
_EXTRA_FIELDS = ("domain", "layer", "job", "duration_ms", "row_count", "error_type")


def get_run_id() -> str:
    """Return the active run_id in the current execution context."""
    return _run_id_var.get()


def set_run_id(run_id: str) -> contextvars.Token[str]:
    """Bind an explicit run_id to the current execution context."""
    return _run_id_var.set(run_id)


def new_run_id(prefix: str = "run") -> str:
    """Generate, bind, and return a short unique run_id."""
    rid = f"{prefix}-{uuid.uuid4().hex[:12]}"
    _run_id_var.set(rid)
    return rid


@contextlib.contextmanager
def run_context(prefix: str = "run", run_id: str | None = None) -> Iterator[str]:
    """Scope a run_id to a block and restore the previous value on exit."""
    rid = run_id or f"{prefix}-{uuid.uuid4().hex[:12]}"
    token = _run_id_var.set(rid)
    try:
        yield rid
    finally:
        _run_id_var.reset(token)


class JsonFormatter(logging.Formatter):
    """Format log records as single-line JSON compatible with Vector and VictoriaLogs."""

    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        obj: dict[str, object] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": self.service,
            "run_id": getattr(record, "run_id", None) or get_run_id(),
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "funcName": record.funcName,
            "lineno": record.lineno,
        }
        for field in _EXTRA_FIELDS:
            val = getattr(record, field, None)
            if val is not None:
                obj[field] = val

        if record.exc_info and record.exc_info[0]:
            obj["error_type"] = obj.get("error_type") or record.exc_info[0].__name__
            obj["exception"] = self.formatException(record.exc_info)

        return json.dumps(obj, default=str)


def setup_json_logging(service: str, level: int = logging.INFO) -> None:
    """Configure root logger to emit structured JSON lines to stdout."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service))

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)
    root.addHandler(handler)
