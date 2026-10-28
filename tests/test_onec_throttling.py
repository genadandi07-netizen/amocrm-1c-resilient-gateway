"""Tests for the 1C egress throttling: pacing, Retry-After floor, breaker isolation.

The three invariants under test:

* the token bucket **paces** the outbound 1C call (it sleeps before the
  transport is invoked, it never rejects),
* RFC 9110 ``Retry-After`` is a **floor**: the effective delay is
  ``max(jitter_delay, retry_after)`` and is never clamped by the backoff cap,
* HTTP 429 / HTTP 401 **do not** increment the circuit-breaker failure
  counter, while a genuine 5xx does.
"""

from __future__ import annotations

import json

import pytest

from gateway.config import (
    DEFAULT_CONFIG_PATH,
    BackoffConfig,
    CircuitBreakerConfig,
    Settings,
)
from gateway.contracts import OneCOrderCreateSchema
from gateway.egress_dispatcher import (
    CircuitBreaker,
    OneCClient,
    OneCError,
    dispatch_event,
)
from gateway.resilience import TokenBucketRateLimiter
from gateway.state_store import get_event, insert_event, transition_status

ORDER_DATA = {
    "number": "SO-1",
    "date": "2026-10-12T00:00:00+00:00",
    "counterparty_ref": "cp-1",
    "items": [{"sku": "SKU-1", "quantity": 1, "price": 1.0}],
    "total": 1.0,
}


class VirtualClock:
    """Deterministic monotonic clock; sleeping advances it like a real sleep."""

    def __init__(self, start: float = 1000.0):
        self.t = start
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.t += seconds

    def advance(self, seconds: float) -> None:
        self.t += seconds


def _order():
    return OneCOrderCreateSchema.model_validate(ORDER_DATA)


def _settings(max_attempts=3, failure_threshold=10):
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
                failure_threshold=failure_threshold,
                cooldown_seconds=10.0,
                half_open_probe_interval=5.0,
            ),
        }
    )


def _prepared(conn, key, data=None, destinations=("1c",)):
    envelope = json.dumps(
        {"event_type": "order", "data": data or ORDER_DATA}, ensure_ascii=False
    )
    event_id, _ = insert_event(conn, key, envelope, destinations=destinations)
    transition_status(conn, event_id, "RECEIVED", "VALIDATED")
    return get_event(conn, event_id)


def _transport(response=None, recorded=None):
    """Offline transport returning a fixed payload (and recording the calls)."""

    def transport(url, method, body, headers=None):
        if recorded is not None:
            recorded.append({"url": url, "method": method, "body": body})
        if response is not None:
            return response
        return {"status_code": 200, "body": {"ok": True}}

    return transport


# --- pacing before the transport call --------------------------------------

def test_client_without_rate_limiter_does_not_pace():
    client = OneCClient("https://onec.example", _transport())
    assert client.rate_limiter is None
    assert client.create_order(_order()) == {"ok": True}


def test_client_stores_injected_rate_limiter_and_sleep_fn():
    limiter = TokenBucketRateLimiter(2.0, 5)
    client = OneCClient(
        "https://onec.example", _transport(), rate_limiter=limiter, sleep_fn=lambda s: None
    )
    assert client.rate_limiter is limiter
    assert callable(client.sleep_fn)


def test_pacer_sleeps_before_the_transport_is_called():
    clock = VirtualClock()
    log: list[tuple] = []

    def sleep_fn(seconds):
        log.append(("sleep", seconds))
        clock.sleep(seconds)

    def transport(url, method, body, headers=None):
        log.append(("transport", clock()))
        return {"status_code": 200, "body": {"ok": True}}

    limiter = TokenBucketRateLimiter(2.0, 1, clock=clock, sleep_fn=sleep_fn)
    client = OneCClient("https://onec.example", transport, rate_limiter=limiter)

    client.create_order(_order())  # first call: one token available, no sleep
    client.create_order(_order())  # second call: deficit of 1 token = 0.5 s

    assert [entry[0] for entry in log] == ["transport", "sleep", "transport"]
    assert log[1][1] == pytest.approx(0.5)
    # the transport was only invoked after the pacer had slept
    assert log[2][1] == pytest.approx(1000.5)


def test_pacer_waits_for_the_exact_deficit():
    clock = VirtualClock()
    limiter = TokenBucketRateLimiter(4.0, 2, clock=clock, sleep_fn=clock.sleep)
    client = OneCClient("https://onec.example", _transport(), rate_limiter=limiter)

    client.create_order(_order())
    client.create_order(_order())
    assert clock.slept == []
    client.create_order(_order())  # bucket is empty -> 1 token missing = 0.25 s
    assert clock.slept == [0.25]


def test_pacer_is_applied_on_every_retry_attempt(conn):
    settings = _settings(max_attempts=3)
    event = _prepared(conn, "pace-retries")
    clock = VirtualClock()
    limiter = TokenBucketRateLimiter(2.0, 1, clock=clock, sleep_fn=clock.sleep)
    client = OneCClient(
        "https://onec.example",
        _transport({"status_code": 500, "body": {}}),
        rate_limiter=limiter,
    )
    dispatch_event(conn, event, settings, client, lambda p: {}, lambda d: None)
    # 3 attempts, the bucket holds a single token -> 2 paced waits
    assert clock.slept == [0.5, 0.5]


# --- Retry-After: parsing into the client error -----------------------------

def test_retry_after_header_is_parsed_into_the_onec_error():
    client = OneCClient(
        "https://onec.example",
        _transport({"status_code": 429, "body": {}, "headers": {"Retry-After": "42"}}),
    )
    with pytest.raises(OneCError) as excinfo:
        client.create_order(_order())
    assert excinfo.value.status_code == 429
    assert excinfo.value.retry_after == 42.0


def test_retry_after_is_none_when_the_header_is_absent():
    client = OneCClient(
        "https://onec.example", _transport({"status_code": 429, "body": {}})
    )
    with pytest.raises(OneCError) as excinfo:
        client.create_order(_order())
    assert excinfo.value.retry_after is None


def test_retry_after_http_date_is_parsed():
    import email.utils
    import time as _time

    future = email.utils.formatdate(_time.time() + 60, usegmt=True)
    client = OneCClient(
        "https://onec.example",
        _transport({"status_code": 503, "body": {}, "headers": {"Retry-After": future}}),
    )
    with pytest.raises(OneCError) as excinfo:
        client.create_order(_order())
    assert excinfo.value.retry_after is not None
    assert 55.0 <= excinfo.value.retry_after <= 61.0


# --- Retry-After: floor application in the dispatcher -----------------------

def test_retry_after_floor_is_applied_to_the_backoff_delay(conn):
    settings = _settings(max_attempts=3)
    event = _prepared(conn, "retry-after-floor")
    sleeps: list[float] = []
    client = OneCClient(
        "https://onec.example",
        _transport({"status_code": 429, "body": {}, "headers": {"Retry-After": "10"}}),
    )
    result = dispatch_event(
        conn, event, settings, client, lambda p: {}, sleeps.append
    )
    assert result == "FAILED"
    # one sleep between the three attempts
    assert len(sleeps) == settings.backoff.max_attempts - 1
    # the floor wins over the (much shorter) jittered backoff ...
    assert all(delay >= 10.0 for delay in sleeps)
    # ... and it is not clamped by backoff.max_seconds
    assert max(sleeps) > settings.backoff.max_seconds


def test_retry_after_never_shortens_the_jittered_delay(conn):
    settings = _settings(max_attempts=3)
    event = _prepared(conn, "retry-after-small")
    sleeps: list[float] = []
    client = OneCClient(
        "https://onec.example",
        _transport({"status_code": 429, "body": {}, "headers": {"Retry-After": "0.001"}}),
    )
    dispatch_event(conn, event, settings, client, lambda p: {}, sleeps.append)
    # attempt 1 -> 0.01 s, attempt 2 -> 0.02 s (jitter_ratio = 0)
    assert sleeps == [pytest.approx(0.01), pytest.approx(0.02)]


def test_retry_after_floor_applies_to_server_errors_as_well(conn):
    settings = _settings(max_attempts=2)
    event = _prepared(conn, "retry-after-503")
    sleeps: list[float] = []
    client = OneCClient(
        "https://onec.example",
        _transport({"status_code": 503, "body": {}, "headers": {"Retry-After": "7"}}),
    )
    dispatch_event(conn, event, settings, client, lambda p: {}, sleeps.append)
    assert sleeps == [7.0]


# --- circuit breaker isolation ----------------------------------------------

def test_circuit_breaker_is_not_tripped_by_429(conn):
    settings = _settings(max_attempts=3, failure_threshold=1)
    event = _prepared(conn, "breaker-429")
    recorded = []
    breaker = CircuitBreaker(settings.circuit_breaker, clock=lambda: 0.0)
    client = OneCClient(
        "https://onec.example",
        _transport({"status_code": 429, "body": {}}, recorded=recorded),
    )
    result = dispatch_event(
        conn, event, settings, client, lambda p: {}, lambda d: None, breaker=breaker
    )
    assert result == "FAILED"
    # every attempt was executed: the breaker never blocked the retries
    assert len(recorded) == settings.backoff.max_attempts
    assert breaker.state == "closed"
    assert breaker.allow() is True
    assert breaker._failures == 0


def test_circuit_breaker_is_not_tripped_by_401(conn):
    settings = _settings(max_attempts=3, failure_threshold=1)
    event = _prepared(conn, "breaker-401")
    recorded = []
    breaker = CircuitBreaker(settings.circuit_breaker, clock=lambda: 0.0)
    client = OneCClient(
        "https://onec.example",
        _transport({"status_code": 401, "body": {}}, recorded=recorded),
    )
    dispatch_event(
        conn, event, settings, client, lambda p: {}, lambda d: None, breaker=breaker
    )
    assert len(recorded) == settings.backoff.max_attempts
    assert breaker.state == "closed"
    assert breaker.allow() is True
    assert breaker._failures == 0


def test_circuit_breaker_is_not_tripped_by_403(conn):
    settings = _settings(max_attempts=3, failure_threshold=1)
    event = _prepared(conn, "breaker-403")
    breaker = CircuitBreaker(settings.circuit_breaker, clock=lambda: 0.0)
    client = OneCClient(
        "https://onec.example", _transport({"status_code": 403, "body": {}})
    )
    dispatch_event(
        conn, event, settings, client, lambda p: {}, lambda d: None, breaker=breaker
    )
    assert breaker.state == "closed"
    assert breaker._failures == 0


def test_circuit_breaker_still_trips_on_server_errors(conn):
    settings = _settings(max_attempts=3, failure_threshold=1)
    event = _prepared(conn, "breaker-500")
    recorded = []
    breaker = CircuitBreaker(settings.circuit_breaker, clock=lambda: 0.0)
    client = OneCClient(
        "https://onec.example",
        _transport({"status_code": 500, "body": {}}, recorded=recorded),
    )
    dispatch_event(
        conn, event, settings, client, lambda p: {}, lambda d: None, breaker=breaker
    )
    assert breaker.state == "open"
    assert breaker.allow() is False
    # the breaker opened after the first failure and blocked the next attempt
    assert len(recorded) == 1


def test_circuit_breaker_counts_transport_failures(conn):
    settings = _settings(max_attempts=3, failure_threshold=1)
    event = _prepared(conn, "breaker-transport")

    def boom(url, method, body, headers=None):
        raise RuntimeError("network down")

    breaker = CircuitBreaker(settings.circuit_breaker, clock=lambda: 0.0)
    client = OneCClient("https://onec.example", boom)
    dispatch_event(
        conn, event, settings, client, lambda p: {}, lambda d: None, breaker=breaker
    )
    assert breaker.state == "open"
    assert breaker._failures == 1
