"""Egress dispatcher: 1C/amoCRM clients, circuit breaker, backoff+jitter, DLQ."""

from __future__ import annotations

import json
import logging
import random
import threading
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from .amocrm_client import AmoClient, AmoError, comment_with_idempotency_key
from .config import (
    DEFAULT_ONEC_COUNTERPARTY_PATH,
    DEFAULT_ONEC_ORDER_PATH,
    BackoffConfig,
    CircuitBreakerConfig,
    Settings,
)
from .contracts import (
    OneCCounterpartySchema,
    OneCOrderCreateSchema,
    OneCOrderStatusSchema,
)
from .errors import (
    ErrorShape,
    GatewayError,
    make_error,
)
from .resilience import (
    TokenBucketRateLimiter,
    classify_status,
    parse_retry_after,
    retry_after_floor,
)
from .state_store import (
    DeliveryRow,
    EventRow,
    claim_event,
    finish_delivery,
    get_delivery_for,
    get_event,
    list_deliveries,
    mark_delivery_in_flight,
    record_attempt,
    set_delivery_status,
    transition_status,
)
from .transport import transport_accepts_headers

LOG = logging.getLogger("gateway.egress")

ORDER_PATH = DEFAULT_ONEC_ORDER_PATH
COUNTERPARTY_PATH = DEFAULT_ONEC_COUNTERPARTY_PATH

IDEMPOTENCY_HEADER = "X-Idempotency-Key"


class OneCError(GatewayError):
    """Raised when the 1C OData transport reports a non-2xx response."""

    error_type = "dependency"
    location = "egress.onec"
    how_to_fix = "Check 1C OData availability and payload correctness, then retry."

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        retry_after: float | None = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        # RFC 9110 ``Retry-After`` hint (seconds) parsed from the response
        # headers; ``None`` when the server did not send one.
        self.retry_after = retry_after


class CircuitBreaker:
    """Three-state circuit breaker (closed / open / half-open), thread-safe."""

    def __init__(
        self,
        cfg: CircuitBreakerConfig,
        clock: Callable[[], float] | None = None,
    ):
        self._cfg = cfg
        self._clock = clock if clock is not None else time.monotonic
        self._lock = threading.Lock()
        self._state = "closed"
        self._failures = 0
        self._opened_at: float | None = None
        self._last_probe_at: float | None = None

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    def allow(self) -> bool:
        with self._lock:
            now = self._clock()
            if self._state == "closed":
                return True
            if self._state == "open":
                if (
                    self._opened_at is not None
                    and (now - self._opened_at) >= self._cfg.cooldown_seconds
                ):
                    self._state = "half_open"
                    self._last_probe_at = now
                    return True
                return False
            # half-open: allow exactly one probe per interval
            if (
                self._last_probe_at is None
                or (now - self._last_probe_at) >= self._cfg.half_open_probe_interval
            ):
                self._last_probe_at = now
                return True
            return False

    def record_success(self) -> None:
        with self._lock:
            self._state = "closed"
            self._failures = 0
            self._opened_at = None
            self._last_probe_at = None

    def record_failure(self) -> None:
        with self._lock:
            now = self._clock()
            if self._state == "half_open":
                self._state = "open"
                self._opened_at = now
                self._failures = 0
                return
            self._failures += 1
            if self._state == "closed" and self._failures >= self._cfg.failure_threshold:
                self._state = "open"
                self._opened_at = now


def compute_backoff(attempt: int, cfg: BackoffConfig, rng: random.Random) -> float | None:
    """Full-jitter exponential backoff; ``None`` when attempts are exhausted."""
    attempt = max(attempt, 1)
    if attempt > cfg.max_attempts:
        return None
    base = cfg.base_seconds * (cfg.multiplier ** (attempt - 1))
    capped = min(base, cfg.max_seconds)
    delay = capped * (1.0 - cfg.jitter_ratio * rng.random())
    if delay < 0.0:
        return 0.0
    return min(delay, cfg.max_seconds)


class OneCClient:
    """1C OData client with an injected transport (offline in tests)."""

    def __init__(
        self,
        base_url: str,
        transport: Callable[[str, str, dict], dict],
        headers: dict | None = None,
        *,
        rate_limiter: TokenBucketRateLimiter | None = None,
        sleep_fn: Callable[[float], None] | None = None,
        order_path: str = ORDER_PATH,
        counterparty_path: str = COUNTERPARTY_PATH,
    ):
        self.base_url = str(base_url).rstrip("/")
        self.transport = transport
        self.headers = headers or {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        # Endpoints come from Settings in production; the constructor defaults
        # keep direct/offline use possible without hardcoding in call sites.
        self.order_path = order_path
        self.counterparty_path = counterparty_path
        # Optional token-bucket *pacer*: it sleeps before every outbound call
        # so the 1C leg stays below the configured request rate.
        self.rate_limiter = rate_limiter
        self.sleep_fn = sleep_fn if sleep_fn is not None else time.sleep
        self._transport_accepts_headers = transport_accepts_headers(transport)

    def _post(self, path: str, body: dict, headers: dict | None = None) -> dict:
        url = f"{self.base_url}{path}"
        request_headers = dict(headers or {})
        # Pace the outbound call before touching the transport.
        if self.rate_limiter is not None:
            self.rate_limiter.acquire()
        try:
            if self._transport_accepts_headers:
                response = self.transport(
                    url, "POST", body, headers=request_headers
                )
            else:
                # Transport without a headers parameter (legacy/offline fixtures).
                response = self.transport(url, "POST", body)
        except OneCError:
            raise
        except Exception as exc:  # noqa: BLE001 - any transport failure is domain
            LOG.warning("1C transport failure: %s", exc, exc_info=True)
            raise OneCError(f"1C transport failure: {exc}") from exc
        return self._check(response)

    @staticmethod
    def _check(response: Any) -> dict:
        if isinstance(response, dict):
            status = response.get("status_code", response.get("status", 200))
            body = response.get("body", response.get("json", {}))
            raw_headers = response.get("headers", {}) or {}
        else:
            status = getattr(response, "status_code", 200)
            body = getattr(response, "body", {})
            raw_headers = getattr(response, "headers", {}) or {}
        if isinstance(status, int) and status >= 400:
            retry_after = (
                parse_retry_after(raw_headers)
                if hasattr(raw_headers, "items")
                else None
            )
            raise OneCError(
                f"1C OData returned HTTP {status}",
                status_code=status,
                retry_after=retry_after,
            )
        return body if isinstance(body, dict) else {"body": body}

    def create_order(
        self,
        order: OneCOrderCreateSchema,
        idempotency_key: str | None = None,
    ) -> dict:
        body = order.model_dump(mode="json")
        headers: dict[str, str] = {}
        if idempotency_key:
            headers[IDEMPOTENCY_HEADER] = str(idempotency_key)
            # Mirror the dedup token into the natural 1C key (comment field) so
            # 1C can de-duplicate a retried request even without the header.
            body["comment"] = comment_with_idempotency_key(
                body.get("comment"), str(idempotency_key)
            )
        return self._post(self.order_path, body, headers=headers)

    def create_counterparty(self, counterparty: OneCCounterpartySchema) -> dict:
        return self._post(
            self.counterparty_path, counterparty.model_dump(mode="json")
        )


def _decode_payload(text: str) -> Any:
    try:
        return json.loads(text)
    except (TypeError, ValueError):
        return {"raw": text}


def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def send_to_dlq(
    event: EventRow,
    error: ErrorShape,
    attempts: int,
    transport: Callable[[dict], Any],
    now: datetime,
) -> bool:
    """POST a failed event to the n8n Dead Letter Queue Webhook."""
    payload = {
        "event_id": event.id,
        "idempotency_key": event.idempotency_key,
        "payload": _decode_payload(event.payload),
        "error": {
            "type": error["type"],
            "message": error["message"],
            "severity": error["severity"],
            "location": error["location"],
            "how_to_fix": error["how_to_fix"],
        },
        "attempts": attempts,
        "failed_at": _iso(now),
    }
    try:
        transport(payload)
        return True
    except Exception as exc:  # noqa: BLE001 - DLQ is best effort by contract
        LOG.error(
            "DLQ delivery failed for event_id=%s key=%s: %s",
            event.id,
            event.idempotency_key,
            exc,
            exc_info=True,
        )
        return False


def _status(conn, event_id: int) -> str | None:
    row = get_event(conn, event_id)
    return row.status if row is not None else None


def _decode_envelope(text: str) -> tuple[str, dict]:
    try:
        data = json.loads(text)
    except (TypeError, ValueError):
        return "unknown", {}
    if isinstance(data, dict):
        return str(data.get("event_type", "unknown")), (data.get("data") or {})
    return "unknown", {}


def _exc_to_error(exc: Exception) -> ErrorShape:
    if isinstance(exc, GatewayError):
        return exc.to_error_shape()
    if isinstance(exc, (OneCError, AmoError)):  # pragma: no cover - legacy path
        return exc.to_error_shape()
    LOG.error("unexpected egress failure: %s", exc, exc_info=True)
    return make_error(
        "dependency",
        f"{type(exc).__name__}: {exc}",
        "egress",
        "Check the dependency connectivity and payload correctness.",
        "critical",
    )


def _resolve_breaker(breaker, destination: str):
    """Accept either a single breaker (legacy) or a ``{destination: breaker}`` map."""
    if isinstance(breaker, dict):
        return breaker.get(destination)
    return breaker


def _build_destination_call(client, destination: str, event, event_type: str, data: dict):
    """Return the callable that pushes one event to ``destination``.

    ``client`` is a :class:`OneCClient` for the ``1c`` leg; for the ``amocrm``
    leg it may be an :class:`AmoClient` or expose an ``amocrm`` attribute
    (typically a ``SimpleNamespace(one_c=..., amocrm=...)`` bundle).
    """
    if destination == "amocrm":
        amo_client = client
        if not isinstance(amo_client, AmoClient):
            amo_client = getattr(client, "amocrm", None) or getattr(client, "amo", None)
        if amo_client is None:
            raise AmoError("No amoCRM client configured for amocrm delivery")
        if event_type == "onec_order_status":
            status_model = OneCOrderStatusSchema.model_validate(data)
            return lambda: amo_client.update_order_status(
                status_model, idempotency_key=event.idempotency_key
            )
        lead_id = data.get("lead_id") or data.get("order_id") or event.id
        status_id = str(data.get("status_id") or data.get("status") or "")
        return lambda: amo_client.update_lead_status(
            lead_id, status_id, idempotency_key=event.idempotency_key
        )

    onec_client = client
    if not isinstance(onec_client, OneCClient):
        onec_client = getattr(client, "one_c", None) or getattr(client, "onec", None)
    if onec_client is None:
        raise OneCError("No 1C client configured for 1c delivery")
    if event_type == "order":
        model = OneCOrderCreateSchema.model_validate(data)
        return lambda: onec_client.create_order(
            model, idempotency_key=event.idempotency_key
        )
    if event_type == "counterparty":
        counterparty = OneCCounterpartySchema.model_validate(data)
        return lambda: onec_client.create_counterparty(counterparty)
    return None


def _deliver_one(
    delivery: DeliveryRow | None,
    call: Callable[[], Any],
    settings: Settings,
    dlq_transport: Callable[[dict], Any],
    sleep_fn: Callable[[float], None],
    breaker: CircuitBreaker | None,
    rng: random.Random,
    conn,
    event: EventRow,
) -> str:
    """Run one outbox delivery with retries; mark FAILED and DLQ on exhaustion."""
    attempt = 0
    last_error: ErrorShape | None = None
    while attempt < settings.backoff.max_attempts:
        if breaker is not None and not breaker.allow():
            break
        attempt += 1
        try:
            call()
            if breaker is not None:
                breaker.record_success()
            if delivery is not None:
                finish_delivery(
                    conn, delivery.id, "DELIVERED", attempt_count=attempt
                )
            return "DELIVERED"
        except Exception as exc:  # noqa: BLE001
            status_code = getattr(exc, "status_code", None)
            status_class = (
                classify_status(status_code) if isinstance(status_code, int) else "error"
            )
            if breaker is not None and status_class not in ("retry", "auth"):
                # 429 (rate limit) and 401/403 (auth) are transient/throttling
                # signals, not dependency failures: they must never trip the
                # circuit breaker.
                breaker.record_failure()
            last_error = _exc_to_error(exc)
            if attempt < settings.backoff.max_attempts:
                delay = compute_backoff(attempt, settings.backoff, rng)
                if delay is not None:
                    # RFC 9110: ``Retry-After`` is a floor, not a ceiling.
                    delay = retry_after_floor(
                        delay, getattr(exc, "retry_after", None)
                    )
                    sleep_fn(delay)

    if last_error is None:
        last_error = make_error(
            "dependency",
            "Circuit breaker is open",
            "egress.onec",
            "Wait for the cooldown period and retry delivery.",
            "critical",
        )
    if delivery is not None:
        finish_delivery(
            conn,
            delivery.id,
            "FAILED",
            attempt_count=attempt,
            last_error=last_error["message"],
        )
    send_to_dlq(event, last_error, attempt, dlq_transport, datetime.now(timezone.utc))
    return "FAILED"


def dispatch_event(
    conn,
    event: EventRow,
    settings: Settings,
    client: OneCClient,
    dlq_transport: Callable[[dict], Any],
    sleep_fn: Callable[[float], None],
    breaker: CircuitBreaker | None = None,
    clock: Callable[[], float] | None = None,
    rng: random.Random | None = None,
    lease_seconds: float | None = None,
) -> str:
    """Deliver every outbox row for one event, routing by ``delivery.destination``.

    The event is first *claimed* with an atomic ``VALIDATED -> DISPATCHED``
    conditional update. Two workers that fetch the same row therefore deliver
    it exactly once; the loser returns the current status without calling any
    transport. ``DISPATCHED`` rows older than ``lease_seconds`` are reclaimed
    (crashed worker recovery) instead of being stranded.
    """
    rng = rng if rng is not None else random.Random()
    lease = lease_seconds if lease_seconds is not None else settings.dispatch_lease_seconds

    if not claim_event(conn, event.id, lease_seconds=lease):
        current = _status(conn, event.id)
        return current or "UNKNOWN"

    event_type, data = _decode_envelope(event.payload)

    deliveries = list_deliveries(conn, event.id)
    if not deliveries:
        # Legacy event without an outbox row (created outside init_db migration).
        destinations = ("amocrm" if event_type == "onec_order_status" else "1c",)
    else:
        destinations = tuple(d.destination for d in deliveries)

    all_ok = True
    last_error: ErrorShape | None = None
    attempt_total = 0
    for destination in destinations:
        delivery = get_delivery_for(conn, event.id, destination)
        if delivery is not None and delivery.status == "DELIVERED":
            continue

        if event_type not in ("order", "counterparty", "onec_order_status"):
            # Unknown/unroutable type: accept and mark delivered (legacy behavior).
            if delivery is not None:
                set_delivery_status(conn, delivery.id, "DELIVERED")
            continue

        try:
            call = _build_destination_call(
                client, destination, event, event_type, data
            )
        except Exception as exc:  # noqa: BLE001 - schema/transport setup failure
            last_error = _exc_to_error(exc)
            all_ok = False
            attempt_total += 1
            if delivery is not None:
                finish_delivery(
                    conn,
                    delivery.id,
                    "FAILED",
                    attempt_count=1,
                    last_error=last_error["message"],
                )
            continue

        if call is None:
            if delivery is not None:
                set_delivery_status(conn, delivery.id, "DELIVERED")
            continue

        if delivery is not None:
            mark_delivery_in_flight(conn, delivery.id)
        destination_breaker = _resolve_breaker(breaker, destination)
        result = _deliver_one(
            delivery,
            call,
            settings,
            dlq_transport,
            sleep_fn,
            destination_breaker,
            rng,
            conn,
            event,
        )
        attempt_total += 1
        if result != "DELIVERED":
            all_ok = False

    if all_ok:
        record_attempt(conn, event.id, attempt_total, None)
    transition_status(
        conn, event.id, "DISPATCHED", "DELIVERED" if all_ok else "FAILED"
    )
    return "DELIVERED" if all_ok else "FAILED"
