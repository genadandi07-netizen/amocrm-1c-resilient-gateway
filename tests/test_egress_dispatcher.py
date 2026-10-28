import json
import random
from datetime import datetime, timezone

import pytest

from gateway.config import (
    DEFAULT_CONFIG_PATH,
    BackoffConfig,
    CircuitBreakerConfig,
    Settings,
)
from gateway.contracts import OneCCounterpartySchema, OneCOrderCreateSchema
from gateway.egress_dispatcher import (
    CircuitBreaker,
    OneCClient,
    OneCError,
    compute_backoff,
    dispatch_event,
    send_to_dlq,
)
from gateway.errors import make_error
from gateway.state_store import EventRow, get_event, insert_event, transition_status

ORDER_DATA = {
    "number": "SO-1",
    "date": "2026-10-12T00:00:00+00:00",
    "counterparty_ref": "cp-1",
    "items": [{"sku": "SKU-1", "quantity": 1, "price": 1.0}],
    "total": 1.0,
}
CB = CircuitBreakerConfig(
    failure_threshold=3, cooldown_seconds=10.0, half_open_probe_interval=100.0
)
BACKOFF = BackoffConfig(
    base_seconds=0.5, multiplier=2.0, max_seconds=30.0, jitter_ratio=1.0, max_attempts=5
)
ERROR = make_error("dependency", "boom", "egress.onec", "retry later", "critical")


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t

    def advance(self, delta):
        self.t += delta


def _envelope(event_type="order", data=None):
    return json.dumps(
        {"event_type": event_type, "data": data if data is not None else ORDER_DATA},
        ensure_ascii=False,
    )


def _prepared_event(conn, key="e1", event_type="order", data=None):
    event_id, _ = insert_event(conn, key, _envelope(event_type, data))
    transition_status(conn, event_id, "RECEIVED", "VALIDATED")
    return get_event(conn, event_id)


def _recorder(recorded, response=None):
    def transport(url, method, body):
        recorded.append({"url": url, "method": method, "body": body})
        return response if response is not None else {"status_code": 200, "body": {"ok": True}}

    return transport


def _dispatch_settings():
    base = Settings.from_file(DEFAULT_CONFIG_PATH)
    return base.model_copy(
        update={
            "backoff": BackoffConfig(
                base_seconds=0.01,
                multiplier=2.0,
                max_seconds=0.05,
                jitter_ratio=0.0,
                max_attempts=3,
            ),
            "circuit_breaker": CircuitBreakerConfig(
                failure_threshold=10, cooldown_seconds=10.0, half_open_probe_interval=5.0
            ),
        }
    )


# --- CircuitBreaker ------------------------------------------------------

def test_circuit_breaker_initial_closed_allows():
    cb = CircuitBreaker(CB, clock=FakeClock())
    assert cb.allow() is True and cb.state == "closed"


def test_circuit_breaker_opens_after_threshold():
    cb = CircuitBreaker(CB, clock=FakeClock())
    for _ in range(3):
        cb.record_failure()
    assert cb.state == "open" and cb.allow() is False


def test_circuit_breaker_open_blocks_until_cooldown():
    clock = FakeClock()
    cb = CircuitBreaker(CB, clock=clock)
    for _ in range(3):
        cb.record_failure()
    clock.advance(9.0)
    assert cb.allow() is False


def test_circuit_breaker_half_open_after_cooldown():
    clock = FakeClock()
    cb = CircuitBreaker(CB, clock=clock)
    for _ in range(3):
        cb.record_failure()
    clock.advance(10.0)
    assert cb.allow() is True and cb.state == "half_open"


def test_circuit_breaker_half_open_success_closes():
    clock = FakeClock()
    cb = CircuitBreaker(CB, clock=clock)
    for _ in range(3):
        cb.record_failure()
    clock.advance(10.0)
    cb.allow()
    cb.record_success()
    assert cb.state == "closed" and cb.allow() is True


def test_circuit_breaker_half_open_failure_reopens():
    clock = FakeClock()
    cb = CircuitBreaker(CB, clock=clock)
    for _ in range(3):
        cb.record_failure()
    clock.advance(10.0)
    cb.allow()
    cb.record_failure()
    assert cb.state == "open" and cb.allow() is False


# --- compute_backoff -----------------------------------------------------

def test_compute_backoff_within_bounds():
    rng = random.Random(42)
    for attempt in range(1, BACKOFF.max_attempts + 1):
        for _ in range(1000):
            delay = compute_backoff(attempt, BACKOFF, rng)
            assert 0.0 <= delay <= BACKOFF.max_seconds


def test_compute_backoff_mean_increases():
    first = [compute_backoff(1, BACKOFF, random.Random(i)) for i in range(500)]
    second = [compute_backoff(2, BACKOFF, random.Random(i)) for i in range(500)]
    assert sum(second) / len(second) > sum(first) / len(first)


def test_compute_backoff_exhausted_returns_none():
    assert compute_backoff(BACKOFF.max_attempts + 1, BACKOFF, random.Random(1)) is None


def test_compute_backoff_zero_jitter_deterministic():
    cfg = BackoffConfig(
        base_seconds=1.0, multiplier=3.0, max_seconds=100.0, jitter_ratio=0.0, max_attempts=4
    )
    rng = random.Random(7)
    assert compute_backoff(1, cfg, rng) == 1.0
    assert compute_backoff(2, cfg, rng) == 3.0
    assert compute_backoff(3, cfg, rng) == 9.0


def test_compute_backoff_capped_at_max():
    cfg = BackoffConfig(
        base_seconds=1.0, multiplier=10.0, max_seconds=5.0, jitter_ratio=0.0, max_attempts=10
    )
    assert compute_backoff(5, cfg, random.Random(0)) == 5.0


# --- OneCClient ----------------------------------------------------------

def test_onec_client_create_order():
    recorded = []
    client = OneCClient("https://onec.example", _recorder(recorded))
    result = client.create_order(OneCOrderCreateSchema.model_validate(ORDER_DATA))
    assert recorded[0]["method"] == "POST"
    assert recorded[0]["url"].endswith("Document_ЗаказПокупателя")
    assert recorded[0]["body"]["number"] == "SO-1"
    assert result == {"ok": True}


def test_onec_client_create_counterparty():
    recorded = []
    client = OneCClient("https://onec.example", _recorder(recorded))
    cp = OneCCounterpartySchema.model_validate({"inn": "1234567890", "name": "X"})
    client.create_counterparty(cp)
    assert recorded[0]["url"].endswith("Catalog_Контрагенты")
    assert recorded[0]["body"]["inn"] == "1234567890"


def test_onec_client_non_2xx_raises():
    client = OneCClient(
        "https://onec.example", _recorder([], {"status_code": 500, "body": {}})
    )
    with pytest.raises(OneCError):
        client.create_order(OneCOrderCreateSchema.model_validate(ORDER_DATA))


def test_onec_client_uses_injected_transport():
    recorded = []
    transport = _recorder(recorded)
    client = OneCClient("https://onec.example", transport)
    assert client.transport is transport
    client.create_order(OneCOrderCreateSchema.model_validate(ORDER_DATA))
    assert len(recorded) == 1


# --- send_to_dlq ---------------------------------------------------------

def _row():
    return EventRow(
        id=1,
        idempotency_key="k1",
        payload=_envelope(),
        status="FAILED",
        attempt_count=2,
        last_error=None,
        created_at="t",
        updated_at="t",
    )


def test_send_to_dlq_payload_has_six_keys():
    captured = []
    ok = send_to_dlq(
        _row(), ERROR, 3, lambda payload: captured.append(payload) or {"status_code": 200},
        datetime.now(timezone.utc),
    )
    assert ok is True
    assert set(captured[0]) == {
        "event_id",
        "idempotency_key",
        "payload",
        "error",
        "attempts",
        "failed_at",
    }


def test_send_to_dlq_error_has_five_fields():
    captured = []
    send_to_dlq(
        _row(), ERROR, 3, lambda payload: captured.append(payload) or {},
        datetime.now(timezone.utc),
    )
    assert set(captured[0]["error"]) == {
        "type",
        "message",
        "severity",
        "location",
        "how_to_fix",
    }


def test_send_to_dlq_transport_failure_returns_false():
    def boom(payload):
        raise RuntimeError("dlq down")

    assert send_to_dlq(_row(), ERROR, 3, boom, datetime.now(timezone.utc)) is False


# --- dispatch_event ------------------------------------------------------

def test_dispatch_event_happy_path(conn):
    s = _dispatch_settings()
    event = _prepared_event(conn, "happy")
    recorded = []
    client = OneCClient("https://onec.example", _recorder(recorded))
    dlq = []
    result = dispatch_event(
        conn, event, s, client, lambda p: dlq.append(p) or {}, lambda d: None
    )
    assert result == "DELIVERED"
    assert len(recorded) == 1 and dlq == []


def test_dispatch_event_open_breaker_no_client_calls(conn):
    s = _dispatch_settings()
    event = _prepared_event(conn, "open")
    recorded = []
    client = OneCClient("https://onec.example", _recorder(recorded))
    dlq = []
    breaker = CircuitBreaker(s.circuit_breaker, clock=lambda: 0.0)
    for _ in range(s.circuit_breaker.failure_threshold):
        breaker.record_failure()
    result = dispatch_event(
        conn, event, s, client, lambda p: dlq.append(p) or {}, lambda d: None, breaker=breaker
    )
    assert result == "FAILED"
    assert recorded == [] and len(dlq) == 1
    status = conn.execute("SELECT status FROM events WHERE id=?", (event.id,)).fetchone()[0]
    assert status == "FAILED"


def test_dispatch_event_retries_exhausted(conn):
    s = _dispatch_settings()
    event = _prepared_event(conn, "retry")
    recorded = []
    client = OneCClient(
        "https://onec.example", _recorder(recorded, {"status_code": 500, "body": {}})
    )
    dlq = []
    result = dispatch_event(
        conn, event, s, client, lambda p: dlq.append(p) or {}, lambda d: None
    )
    assert result == "FAILED"
    assert len(recorded) == s.backoff.max_attempts
    assert len(dlq) == 1


def test_dispatch_event_call_count_within_limit(conn):
    s = _dispatch_settings()
    event = _prepared_event(conn, "count")
    recorded = []
    client = OneCClient(
        "https://onec.example", _recorder(recorded, {"status_code": 503, "body": {}})
    )
    dispatch_event(conn, event, s, client, lambda p: {}, lambda d: None)
    assert 0 < len(recorded) <= s.backoff.max_attempts


def test_dispatch_event_failed_status_set(conn):
    s = _dispatch_settings()
    event = _prepared_event(conn, "status")
    client = OneCClient(
        "https://onec.example", _recorder([], {"status_code": 500, "body": {}})
    )
    dispatch_event(conn, event, s, client, lambda p: {}, lambda d: None)
    assert get_event(conn, event.id).status == "FAILED"


def test_dispatch_event_backoff_sleep_called(conn):
    s = _dispatch_settings()
    event = _prepared_event(conn, "sleep")
    client = OneCClient(
        "https://onec.example", _recorder([], {"status_code": 500, "body": {}})
    )
    sleeps = []
    dispatch_event(conn, event, s, client, lambda p: {}, lambda d: sleeps.append(d))
    assert sleeps and all(0.0 <= d <= s.backoff.max_seconds for d in sleeps)
