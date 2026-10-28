#!/usr/bin/env python3
"""Background egress worker: drains the SQLite delivery queue into 1C OData.

The FastAPI gateway only *accepts* webhooks (its 1C/DLQ transports are stubbed
out in ``gateway/app.py``); this process closes the loop by picking up every
``VALIDATED`` / ``FAILED`` event and delivering it through
``gateway.egress_dispatcher.dispatch_event`` — the same logic that backs
``gateway.pipeline.run_delivery_pass``, extended with per-event fault
isolation so one poisoned row can never stall or crash the worker.

Runs as a long-lived process (Docker service ``egress-worker``):

    python -m scripts.egress_worker          # or: python scripts/egress_worker.py

Environment (every value is read through ``Settings.from_env()`` and validated
at startup, see ``.env.example``):

    GATEWAY_DB_PATH                  SQLite file shared with the gateway (WAL)
    GATEWAY_ONEC_BASE_URL            1C OData root URL
    GATEWAY_ONEC_ORDER_PATH          order entity path (appended to the root)
    GATEWAY_ONEC_COUNTERPARTY_PATH   counterparty entity path
    GATEWAY_ONEC_USER/PASSWORD       HTTP Basic credentials for 1C
    GATEWAY_ONEC_TIMEOUT_SECONDS     per-request 1C/amoCRM timeout, default 15
    GATEWAY_AMOCRM_BASE_URL          amoCRM root URL
    GATEWAY_AMOCRM_LEADS_PATH        leads API path
    GATEWAY_N8N_DLQ_URL              n8n Dead Letter Queue webhook (optional)
    GATEWAY_POLL_INTERVAL            queue poll interval in seconds, default 2
    GATEWAY_DISPATCH_BATCH_LIMIT     events per pass, default 50
    GATEWAY_DISPATCH_LEASE_SECONDS   crashed-worker lease, default 300
    GATEWAY_ONEC_RATE_LIMIT_RPS      1C pacer rate, requests per second, default 2
    GATEWAY_ONEC_RATE_LIMIT_BURST    1C pacer burst, default 5
    GATEWAY_AMOCRM_RATE_LIMIT_RPS    amoCRM pacer rate, requests per second, default 2
    GATEWAY_AMOCRM_RATE_LIMIT_BURST  amoCRM pacer burst, default 5
    GATEWAY_AMOCRM_ACCESS_TOKEN      amoCRM bearer token; re-read on a 401 so a
                                     rotated token is picked up (single flight)
    GATEWAY_LOG_LEVEL                DEBUG/INFO/WARNING, default INFO
    GATEWAY_LOG_JSON                 true = JSON lines, false = text, default true

Compatibility aliases: ``POLL_INTERVAL`` and ``GATEWAY_WORKER_INTERVAL_SECONDS``
are accepted for the poll interval (resolved by ``Settings.from_env``).
"""

from __future__ import annotations

import base64
import logging
import os
import signal
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx

# Allow `python scripts/egress_worker.py` from any CWD: put the project root
# (which holds the `gateway` package) on sys.path before importing it.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from gateway.amocrm_client import AmoClient  # noqa: E402
from gateway.config import Settings  # noqa: E402
from gateway.egress_dispatcher import (  # noqa: E402
    CircuitBreaker,
    OneCClient,
    dispatch_event,
)
from gateway.observability import (  # noqa: E402
    configure_logging as configure_structured_logging,
)
from gateway.resilience import TokenBucketRateLimiter  # noqa: E402
from gateway.state_store import (  # noqa: E402
    EventRow,
    fetch_dispatchable,
    get_event,
    init_db,
)

LOG = logging.getLogger("gateway.egress_worker")


# ---------------------------------------------------------------------------
# transports (injected into OneCClient / dispatch_event, same as in tests)
# ---------------------------------------------------------------------------
def build_onec_transport(
    timeout: float,
    credentials: tuple[str, str] | None = None,
) -> Callable[..., dict]:
    """HTTP transport for ``OneCClient``: pool + Basic auth + caller headers.

    The underlying ``httpx.Client`` is attached as ``transport.http_client``
    so the worker can close it during graceful shutdown; caller headers win.
    ``credentials`` is taken from ``Settings`` in ``main`` (validated at
    startup); keeping it an explicit argument makes the transport testable
    offline and avoids reading ambient environment variables here.
    """
    default_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if credentials is None:
        LOG.warning(
            "GATEWAY_ONEC_USER/GATEWAY_ONEC_PASSWORD are not set: "
            "1C requests will be sent without HTTP Basic credentials"
        )
    else:
        token = base64.b64encode(f"{credentials[0]}:{credentials[1]}".encode()).decode()
        default_headers["Authorization"] = f"Basic {token}"

    http = httpx.Client(timeout=httpx.Timeout(timeout), headers=default_headers)

    def transport(
        url: str,
        method: str,
        body: dict,
        headers: dict | None = None,
    ) -> dict:
        try:
            response = http.request(
                method,
                url,
                json=body,
                headers=headers or None,
                timeout=timeout,
            )
        except httpx.HTTPError as exc:
            # OneCClient wraps any non-OneCError into OneCError -> event FAILED.
            raise RuntimeError(f"1C transport failure: {exc}") from exc
        try:
            parsed: Any = response.json() if response.content else {}
        except ValueError:
            parsed = {"raw": response.text[:2048]}
        if not isinstance(parsed, dict):
            parsed = {"body": parsed}
        # The response ``headers`` carry the RFC 9110 ``Retry-After`` hint.
        return {
            "status_code": response.status_code,
            "body": parsed,
            "headers": dict(response.headers),
        }

    transport.http_client = http  # type: ignore[attr-defined]
    return transport


def build_amocrm_transport(
    settings: Settings, timeout: float
) -> Callable[..., dict]:
    """HTTP transport for :class:`AmoClient`.

    The transport deliberately carries **no** client-level ``Authorization``
    header: :class:`gateway.amocrm_client.AmoClient` is the single owner of
    the dynamic ``Authorization: Bearer <token>`` injection (and of the
    single-flight token rotation), so a rotated token reaches the wire on the
    very next request instead of being pinned to a stale client header.

    Per-request headers (including the fresh ``Authorization``) are forwarded
    to httpx, which merges them over the client defaults.
    """
    default_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if not (settings.amocrm_access_token or "").strip():
        LOG.warning(
            "GATEWAY_AMOCRM_ACCESS_TOKEN is not set: "
            "amoCRM requests will be sent without an Authorization header"
        )
    http = httpx.Client(timeout=httpx.Timeout(timeout), headers=default_headers)

    def transport(
        url: str, method: str, body: dict, headers: dict | None = None
    ) -> dict:
        try:
            response = http.request(method, url, json=body, headers=headers or None)
        except httpx.HTTPError as exc:
            # AmoClient wraps any unexpected error into AmoError -> event FAILED.
            raise RuntimeError(f"amoCRM transport failure: {exc}") from exc
        try:
            parsed: Any = response.json() if response.content else {}
        except ValueError:
            parsed = {"raw": response.text[:2048]}
        if not isinstance(parsed, dict):
            parsed = {"body": parsed}
        # ``headers`` carry the RFC 9110 ``Retry-After`` hint for the client.
        return {
            "status_code": response.status_code,
            "body": parsed,
            "headers": dict(response.headers),
        }

    transport.http_client = http  # type: ignore[attr-defined]
    return transport


def build_dlq_transport(
    settings: Settings, timeout: float
) -> Callable[[dict], Any]:
    """n8n Dead Letter Queue webhook transport (optional by configuration)."""
    url = (settings.n8n_dlq_url or "").strip()
    http = httpx.Client(timeout=httpx.Timeout(timeout))

    def transport(payload: dict) -> None:
        if not url:
            raise RuntimeError(
                "GATEWAY_N8N_DLQ_URL is not configured: dead-letter event kept in SQLite"
            )
        try:
            response = http.post(url, json=payload)
            response.raise_for_status()
        except httpx.HTTPError:
            LOG.exception(
                "DLQ delivery failed for event_id=%s (kept in SQLite for replay)",
                payload.get("event_id"),
            )
            raise

    transport.http_client = http  # type: ignore[attr-defined]
    return transport


def close_transports(*transports: Callable[..., Any]) -> None:
    """Close the httpx pools owned by the transports (best-effort)."""
    for transport in transports:
        http_client = getattr(transport, "http_client", None)
        if http_client is not None:
            try:
                http_client.close()
            except Exception:  # noqa: BLE001
                LOG.exception("failed to close an HTTP transport")


# ---------------------------------------------------------------------------
# delivery pass
# ---------------------------------------------------------------------------
def _log_failed(conn, event: EventRow) -> None:
    """Warn only when the failure actually changed.

    A pending ``FAILED`` event is re-visited on every poll (backoff window /
    circuit-breaker cooldown); logging the identical reason at WARNING each
    time would flood the container log without adding information.
    """
    try:
        fresh: EventRow | None = get_event(conn, event.id)
    except Exception as exc:  # noqa: BLE001 - logging must never break the pass
        LOG.debug(
            "cannot re-read event id=%s for failure logging: %s",
            event.id,
            exc,
            exc_info=True,
        )
        fresh = None

    changed = fresh is None or (
        fresh.attempt_count != event.attempt_count
        or fresh.last_error != event.last_error
    )
    if fresh is not None and changed:
        LOG.warning(
            "event id=%s key=%s -> FAILED (attempt=%d, error=%s)",
            event.id,
            event.idempotency_key,
            fresh.attempt_count,
            fresh.last_error,
        )
    else:
        LOG.debug(
            "event id=%s key=%s -> FAILED again, reason unchanged (error=%s)",
            event.id,
            event.idempotency_key,
            (fresh.last_error if fresh else None) or "unknown",
        )


def run_worker_pass(
    conn,
    settings: Settings,
    client,
    dlq_transport: Callable[[dict], Any],
    breaker,
) -> dict[str, int]:
    """Deliver one batch of dispatchable events in both directions.

    Same semantics as ``gateway.pipeline.run_delivery_pass`` (DELIVERED/FAILED
    counters), but every event is isolated: an unexpected exception (payload
    schema drift, transient SQLITE_BUSY, ...) is logged and the remaining batch
    keeps flowing instead of aborting the pass. ``breaker`` and ``client`` may
    be a single 1C value (legacy) or per-direction mappings/bundles.
    """
    counters = {"delivered": 0, "failed": 0, "errors": 0, "skipped": 0}
    try:
        batch = fetch_dispatchable(
            conn,
            limit=settings.dispatch_batch_limit,
            lease_seconds=settings.dispatch_lease_seconds,
        )
    except Exception:  # noqa: BLE001 - a broken read must not kill the process
        LOG.exception("cannot read the delivery queue; will retry after the poll interval")
        return counters

    for event in batch:
        try:
            result = dispatch_event(
                conn,
                event,
                settings,
                client,
                dlq_transport,
                time.sleep,
                breaker=breaker,
            )
        except Exception:  # noqa: BLE001 - isolate one bad event from the batch
            counters["errors"] += 1
            LOG.exception(
                "event id=%s key=%s: unexpected error, skipping this pass",
                event.id,
                event.idempotency_key,
            )
            continue

        if result == "DELIVERED":
            counters["delivered"] += 1
            LOG.info(
                "event id=%s key=%s -> DELIVERED",
                event.id,
                event.idempotency_key,
                extra={"event_id": event.id, "idempotency_key": event.idempotency_key},
            )
        elif result == "FAILED":
            counters["failed"] += 1
            _log_failed(conn, event)
        elif result in ("DISPATCHED", "IN_PROGRESS"):
            # Another worker holds the lease: not an error, just skip it.
            counters["skipped"] += 1
            LOG.debug(
                "event id=%s key=%s is leased by another worker (%s)",
                event.id,
                event.idempotency_key,
                result,
            )
        else:
            LOG.debug("event id=%s key=%s -> %s", event.id, event.idempotency_key, result)

    return counters


# ---------------------------------------------------------------------------
# process lifecycle
# ---------------------------------------------------------------------------
def configure_logging(settings: Settings | None = None) -> None:
    """Emit structured JSON logs (or a request-id-aware text format)."""
    resolved = settings if settings is not None else Settings.from_env()
    configure_structured_logging(
        resolved.log_level,
        json_logs=resolved.log_json,
        force=True,
    )
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)


def install_signal_handlers(stop: threading.Event) -> None:
    def handler(signum: int, _frame: Any) -> None:
        name = signal.Signals(signum).name
        if not stop.is_set():
            LOG.info("received %s: finishing the current pass, then shutting down", name)
        stop.set()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


def main() -> int:
    settings = Settings.from_env()
    configure_logging(settings)
    interval = settings.poll_interval_seconds
    timeout = settings.onec_timeout_seconds

    stop = threading.Event()
    install_signal_handlers(stop)

    # init_db creates the schema (WAL + busy_timeout) if the gateway has not
    # started yet, so both processes can win the race safely.
    conn = init_db(
        settings.db_path,
        busy_timeout_ms=settings.busy_timeout_ms,
        timeout=settings.db_connect_timeout_seconds,
    )
    credentials = (
        (settings.onec_user, settings.onec_password)
        if (settings.onec_user or settings.onec_password)
        else None
    )
    onec_transport = build_onec_transport(timeout, credentials)
    # Token-bucket pacer: it sleeps so the 1C leg stays under the configured
    # request rate (requests per second + burst).
    onec_client = OneCClient(
        settings.one_c_base_url,
        onec_transport,
        rate_limiter=TokenBucketRateLimiter(
            settings.onec_rate_limit_rps, settings.onec_rate_limit_burst
        ),
        order_path=settings.onec_order_path,
        counterparty_path=settings.onec_counterparty_path,
    )
    amocrm_transport = build_amocrm_transport(settings, timeout)
    amocrm_client = AmoClient(
        settings.amocrm_base_url,
        amocrm_transport,
        access_token=settings.amocrm_access_token,
        requests_per_second=settings.amocrm_requests_per_second,
        # Single-flight token rotation: on HTTP 401 the client asks for a fresh
        # token exactly once per rotation epoch, so N concurrent workers never
        # trigger N refreshes - they reuse the rotated token instead.
        token_refresher=lambda: (
            os.environ.get("GATEWAY_AMOCRM_ACCESS_TOKEN")
            or settings.amocrm_access_token
        ),
        # Dual limiter coexistence: the ingress-style RateLimiter keeps
        # rejecting over-budget calls, the pacer additionally sleeps.
        pacer=TokenBucketRateLimiter(
            settings.amocrm_rate_limit_rps, settings.amocrm_rate_limit_burst
        ),
        leads_path=settings.amocrm_leads_path,
    )
    # One bundle dispatches both directions by ``delivery.destination``.
    client = SimpleNamespace(one_c=onec_client, amocrm=amocrm_client)
    dlq_transport = build_dlq_transport(settings, timeout)
    breakers = {
        "1c": CircuitBreaker(settings.circuit_breaker),  # kept between passes
        "amocrm": CircuitBreaker(settings.circuit_breaker),
    }

    if not (settings.n8n_dlq_url or "").strip():
        LOG.warning(
            "GATEWAY_N8N_DLQ_URL is not configured: exhausted events stay in "
            "SQLite as FAILED for a later replay"
        )

    LOG.info(
        "egress worker started: db=%s one_c=%s amocrm=%s timeout=%.1fs poll=%.2fs "
        "batch=%d breaker(failure_threshold=%d, cooldown=%.1fs)",
        settings.db_path,
        settings.one_c_base_url,
        settings.amocrm_base_url,
        timeout,
        interval,
        settings.dispatch_batch_limit,
        settings.circuit_breaker.failure_threshold,
        settings.circuit_breaker.cooldown_seconds,
    )

    try:
        while not stop.is_set():
            try:
                counters = run_worker_pass(
                    conn, settings, client, dlq_transport, breakers
                )
                if counters["delivered"] or counters["failed"] or counters["errors"]:
                    LOG.info(
                        "pass finished: %(delivered)d delivered, %(failed)d failed, "
                        "%(errors)d errors",
                        counters,
                    )
                else:
                    LOG.debug("pass finished: queue is empty")
            except Exception:  # noqa: BLE001 - never let a pass kill the worker
                LOG.exception("delivery pass crashed; retrying after %.2fs", interval)

            # Event.wait() returns as soon as the signal handler calls set(),
            # so shutdown latency is bounded by an in-flight 1C request only.
            stop.wait(interval)
    finally:
        close_transports(onec_transport, amocrm_transport, dlq_transport)
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            LOG.exception("failed to close the SQLite connection")
        LOG.info("egress worker stopped")

    return 0


if __name__ == "__main__":
    sys.exit(main())
