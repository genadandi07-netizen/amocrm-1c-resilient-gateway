"""Stage 3: amoCRM single-flight token rotation.

Covers the HTTP 401 recovery path of :class:`gateway.amocrm_client.AmoClient`:

* exactly **one** token refresh per rotation epoch, even when many workers
  race on the same 401 (single flight);
* **at most one** retry per delivery request - a second 401 is propagated
  instead of looping;
* auth failures (401) never trip the circuit breaker.

Everything is strictly synchronous: ``threading.Barrier`` / ``threading.Lock``
are used to build deterministic races, no event loop is involved.
"""

from __future__ import annotations

import json
import threading
import time
from typing import ClassVar

import pytest

from gateway.amocrm_client import AmoClient, AmoError
from gateway.config import (
    DEFAULT_CONFIG_PATH,
    BackoffConfig,
    CircuitBreakerConfig,
    Settings,
)
from gateway.egress_dispatcher import CircuitBreaker, dispatch_event
from gateway.state_store import get_event, insert_event, transition_status

OLD_TOKEN = "old-token"
NEW_TOKEN = "new-token"

STATUS_DATA = {
    "order_id": "LEAD-7",
    "status": "processing",
    "payment_status": "unpaid",
    "updated_at": "2026-10-12T00:00:00+00:00",
    "comment": "в работе",
}


# --- helpers ---------------------------------------------------------------

def _ok_transport(recorded, status_code=200, body=None):
    """Always-answering transport that records the sent Authorization header."""

    def transport(url, method, payload, headers=None):
        headers = headers or {}
        recorded.append(headers.get("Authorization"))
        return {
            "status_code": status_code,
            "body": body if body is not None else {"ok": True},
        }

    return transport


def _token_transport(recorded):
    """401 for the stale token, 200 for the fresh one."""

    def transport(url, method, payload, headers=None):
        headers = headers or {}
        auth = headers.get("Authorization")
        recorded.append(auth)
        if auth == f"Bearer {NEW_TOKEN}":
            return {"status_code": 200, "body": {"ok": True}}
        return {"status_code": 401, "body": {"error": "unauthorized"}}

    return transport


def _always_401_transport(recorded):
    def transport(url, method, payload, headers=None):
        headers = headers or {}
        recorded.append(headers.get("Authorization"))
        return {"status_code": 401, "body": {"error": "unauthorized"}}

    return transport


def _client(transport, refreshes, token=OLD_TOKEN):
    """AmoClient whose refresher records every invocation."""

    def refresher():
        refreshes.append(NEW_TOKEN)
        return NEW_TOKEN

    return AmoClient(
        "https://amo.example",
        transport,
        access_token=token,
        token_refresher=refresher,
    )


def _dispatch_settings(max_attempts=3):
    base = Settings.from_file(DEFAULT_CONFIG_PATH)
    return base.model_copy(
        update={
            "backoff": BackoffConfig(
                base_seconds=0.01,
                multiplier=2.0,
                max_seconds=0.05,
                jitter_ratio=0.0,
                max_attempts=max_attempts,
            ),
            "circuit_breaker": CircuitBreakerConfig(
                failure_threshold=2,
                cooldown_seconds=10.0,
                half_open_probe_interval=5.0,
            ),
        }
    )


def _prepared(conn, key, event_type="onec_order_status", data=None, destinations=("amocrm",)):
    envelope = json.dumps(
        {"event_type": event_type, "data": data if data is not None else STATUS_DATA},
        ensure_ascii=False,
    )
    event_id, _ = insert_event(conn, key, envelope, destinations=destinations)
    transition_status(conn, event_id, "RECEIVED", "VALIDATED")
    return get_event(conn, event_id)


class _FakeResponse:
    """Minimal stand-in for ``httpx.Response``."""

    status_code = 200
    content = b"{}"
    headers: ClassVar[dict[str, str]] = {}
    text = "{}"

    def json(self):
        return {"ok": True}


class _FakeHTTPClient:
    """Stand-in for ``httpx.Client`` that records the outbound requests."""

    def __init__(self, timeout=None, headers=None):
        self.timeout = timeout
        self.headers = dict(headers or {})
        self.calls: list[dict] = []
        self.closed = False

    def request(self, method, url, json=None, headers=None):
        self.calls.append(
            {"method": method, "url": url, "json": json, "headers": headers}
        )
        return _FakeResponse()

    def close(self):
        self.closed = True


# --- 401 -> rotate -> retry ------------------------------------------------

def test_401_triggers_single_token_refresh_and_retries_successfully():
    """A 401 rotates the token once and the retry carries the new token."""
    recorded: list[str | None] = []
    refreshes: list[str] = []
    client = _client(_token_transport(recorded), refreshes)

    assert client.token == OLD_TOKEN
    assert client.token_version == 0

    result = client.update_lead_status("LEAD-7", "PAID")

    assert result == {"ok": True}
    # Exactly one refresh, one version bump, one retry with the new token.
    assert len(refreshes) == 1
    assert client.token == NEW_TOKEN
    assert client.token_version == 1
    assert recorded == [f"Bearer {OLD_TOKEN}", f"Bearer {NEW_TOKEN}"]


def test_concurrent_token_refreshes_are_single_flight():
    """8 racing workers trigger exactly one refresh and share the new token."""
    workers = 8
    start_barrier = threading.Barrier(workers)
    recorded: list[str | None] = []
    refreshes: list[str] = []
    observed_versions: list[int] = []
    failures: list[BaseException] = []

    # Every worker must issue its first request *before* any 401 is answered,
    # otherwise the late workers would already use the rotated token and the
    # race on the refresh would not happen at all.
    call_lock = threading.Lock()
    call_state = {"calls": 0}

    def transport(url, method, payload, headers=None):
        headers = headers or {}
        auth = headers.get("Authorization")
        with call_lock:
            call_state["calls"] += 1
            call_no = call_state["calls"]
        if call_no <= workers:
            deadline = time.monotonic() + 10.0
            while call_state["calls"] < workers and time.monotonic() < deadline:
                time.sleep(0.001)
        recorded.append(auth)
        if auth == f"Bearer {NEW_TOKEN}":
            return {"status_code": 200, "body": {"ok": True}}
        return {"status_code": 401, "body": {"error": "unauthorized"}}

    client = _client(transport, refreshes)

    def worker():
        try:
            start_barrier.wait(timeout=10)
            result = client.update_lead_status("LEAD-7", "PAID")
            assert result == {"ok": True}
            observed_versions.append(client.token_version)
        except BaseException as exc:  # noqa: BLE001 - reported to the main thread
            failures.append(exc)

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert not failures, f"worker failed: {failures!r}"
    assert all(not thread.is_alive() for thread in threads)
    # Single flight: one refresh for the whole race, one version bump.
    assert len(refreshes) == 1
    assert client.token == NEW_TOKEN
    assert client.token_version == 1
    assert observed_versions == [1] * workers
    # Every worker sent the stale token once and the fresh token once.
    assert recorded.count(f"Bearer {OLD_TOKEN}") == workers
    assert recorded.count(f"Bearer {NEW_TOKEN}") == workers


def test_refresh_without_seen_version_always_increments():
    """A direct refresh (no version guard) always bumps the version."""
    refreshes: list[str] = []
    counter = {"n": 0}

    def refresher():
        counter["n"] += 1
        refreshes.append(f"token-{counter['n']}")
        return refreshes[-1]

    client = AmoClient(
        "https://amo.example",
        _ok_transport([]),
        access_token=OLD_TOKEN,
        token_refresher=refresher,
    )
    assert client.token_version == 0

    assert client.refresh_token() == "token-1"
    assert client.token_version == 1
    assert client.token == "token-1"

    assert client.refresh_token() == "token-2"
    assert client.token_version == 2
    assert client.token == "token-2"

    assert len(refreshes) == 2


def test_persistent_401_rotates_at_most_once():
    """A permanently failing token is rotated once, then the 401 is raised."""
    recorded: list[str | None] = []
    refreshes: list[str] = []
    client = _client(_always_401_transport(recorded), refreshes)

    with pytest.raises(AmoError) as excinfo:
        client.update_lead_status("LEAD-7", "PAID")

    assert excinfo.value.status_code == 401
    # One rotation, one retry - and no infinite loop.
    assert len(refreshes) == 1
    assert len(recorded) == 2
    assert recorded == [f"Bearer {OLD_TOKEN}", f"Bearer {NEW_TOKEN}"]
    assert client.token_version == 1


def test_401_does_not_increment_circuit_breaker(conn):
    """Auth failures are not dependency failures: the breaker stays closed."""
    settings = _dispatch_settings(max_attempts=3)
    breaker = CircuitBreaker(settings.circuit_breaker)
    event = _prepared(conn, "amo-401-breaker")

    recorded: list[str | None] = []
    refreshes: list[str] = []
    amo = _client(_always_401_transport(recorded), refreshes)

    dlq: list[dict] = []
    result = dispatch_event(
        conn,
        event,
        settings,
        amo,
        lambda payload: dlq.append(payload) or {},
        lambda delay: None,
        breaker=breaker,
    )

    assert result == "FAILED"
    # The 401 never counted against the breaker.
    assert breaker.state == "closed"
    assert breaker.allow() is True
    # The client retried once per delivery attempt, but never looped inside
    # a single request.
    assert len(recorded) == 2 * settings.backoff.max_attempts


# --- single owner of the Authorization header ------------------------------

def test_client_owns_the_authorization_header():
    """AmoClient injects the bearer token; the transport must not pin one."""
    recorded: list[str | None] = []
    client = _client(_ok_transport(recorded), [])

    client.update_lead_status("LEAD-7", "PAID", idempotency_key="k1")

    assert recorded == [f"Bearer {OLD_TOKEN}"]
    assert client.headers.get("Authorization") is None


def test_egress_worker_transport_has_no_static_authorization_header():
    """The worker transport carries no client-level Authorization header."""
    from scripts.egress_worker import build_amocrm_transport

    settings = Settings.from_file(DEFAULT_CONFIG_PATH).model_copy(
        update={"amocrm_access_token": "static-token"}
    )
    transport = build_amocrm_transport(settings, timeout=1.0)
    try:
        http = transport.http_client
        assert "authorization" not in {k.lower() for k in http.headers}
        assert http.headers.get("content-type") == "application/json"
    finally:
        transport.http_client.close()


def test_worker_transport_forwards_the_rotated_authorization_header(monkeypatch):
    """The rotated token reaches the wire: the transport forwards headers."""
    from scripts import egress_worker

    monkeypatch.setattr(egress_worker.httpx, "Client", _FakeHTTPClient)
    settings = Settings.from_file(DEFAULT_CONFIG_PATH).model_copy(
        update={"amocrm_access_token": "static-token"}
    )
    transport = egress_worker.build_amocrm_transport(settings, timeout=1.0)
    transport(
        "https://amo.example/api/v4/leads/LEAD-7",
        "PATCH",
        {"status_id": "PAID"},
        headers={"Authorization": "Bearer rotated-token"},
    )
    http = transport.http_client
    try:
        assert http.calls[0]["headers"]["Authorization"] == "Bearer rotated-token"
        # ... and the client itself still holds no pinned Authorization header.
        assert "authorization" not in {k.lower() for k in http.headers}
    finally:
        http.close()
