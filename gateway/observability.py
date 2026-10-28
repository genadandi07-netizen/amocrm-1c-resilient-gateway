"""Structured logging with a request-id correlation context.

The gateway never formats f-strings into a flat log line: every record carries
``request_id`` (from ``X-Request-ID`` or a generated UUID) and is emitted as a
single JSON object by the default formatter. This makes it possible to follow
one webhook across ingress, the SQLite outbox, and the egress worker using the
same correlation identifier.

The module is intentionally framework-agnostic; FastAPI installs the context in
``gateway.app`` and the background worker configures it directly.
"""

from __future__ import annotations

import contextvars
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

__all__ = [
    "JsonFormatter",
    "RequestIdFilter",
    "configure_logging",
    "current_request_id",
    "new_request_id",
    "request_id_var",
    "set_request_id",
]

REQUEST_ID_HEADER = "X-Request-ID"

request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "request_id", default="-"
)


def new_request_id() -> str:
    return uuid.uuid4().hex


def current_request_id() -> str:
    return request_id_var.get()


def set_request_id(value: str | None) -> contextvars.Token[str]:
    """Bind a (sanitized) request id to the current context."""
    candidate = str(value or "").strip()
    if not candidate or len(candidate) > 128:
        candidate = new_request_id()
    return request_id_var.set(candidate)


class RequestIdFilter(logging.Filter):
    """Ensure ``record.request_id`` exists even outside an HTTP request."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "request_id"):
            record.request_id = current_request_id()
        return True


class JsonFormatter(logging.Formatter):
    """One JSON object per log line, including request correlation fields."""

    _EXTRA_FIELDS = (
        "event_id",
        "idempotency_key",
        "destination",
        "attempt",
        "duration_ms",
        "status_code",
    )

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "request_id": getattr(record, "request_id", current_request_id()),
            "message": record.getMessage(),
        }
        for field in self._EXTRA_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging(
    level: str = "INFO",
    *,
    json_logs: bool = True,
    force: bool = False,
    stream=sys.stdout,
) -> None:
    """Configure the root logger for structured output.

    ``force=False`` is used by the web app so an embedding process/test runner
    keeps its own handlers; ``force=True`` is used by the standalone worker.
    """
    root = logging.getLogger()
    root.setLevel(getattr(logging, str(level).upper(), logging.INFO))

    if not force and root.handlers:
        for handler in root.handlers:
            handler.addFilter(RequestIdFilter())
        return

    handler = logging.StreamHandler(stream)
    handler.addFilter(RequestIdFilter())
    if json_logs:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s [%(name)s] "
                "request_id=%(request_id)s %(message)s"
            )
        )
    if force:
        root.handlers.clear()
    root.addHandler(handler)
