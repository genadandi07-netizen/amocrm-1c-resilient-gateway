"""Tests for end-to-end idempotency: X-Idempotency-Key on the 1C transport."""

from __future__ import annotations

import json

from gateway.amocrm_client import comment_with_idempotency_key
from gateway.config import (
    DEFAULT_CONFIG_PATH,
    BackoffConfig,
    CircuitBreakerConfig,
    Settings,
)
from gateway.contracts import OneCOrderCreateSchema
from gateway.egress_dispatcher import (
    IDEMPOTENCY_HEADER,
    OneCClient,
    dispatch_event,
)
from gateway.state_store import (
    get_delivery_for,
    get_event,
    insert_event,
    transition_status,
)

ORDER_DATA = {
    "number": "SO-77",
    "date": "2026-10-12T00:00:00+00:00",
    "counterparty_ref": "cp-1",
    "items": [{"sku": "SKU-1", "quantity": 2, "price": 5.0}],
    "total": 10.0,
}


def _capturing_transport(recorded, response=None):
    def transport(url, method, body, headers=None):
        recorded.append({"url": url, "method": method, "body": body, "headers": headers or {}})
        return response if response is not None else {"status_code": 200, "body": {"ok": True}}

    return transport


def _settings(max_attempts=3):
    base = Settings.from_file(DEFAULT_CONFIG_PATH)
    return base.model_copy(
        update={
            "backoff": BackoffConfig(
                base_seconds=0.01, multiplier=2.0, max_seconds=0.05,
                jitter_ratio=0.0, max_attempts=max_attempts,
            ),
            "circuit_breaker": CircuitBreakerConfig(
                failure_threshold=10, cooldown_seconds=10.0, half_open_probe_interval=5.0
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


# --- OneCClient header plumbing -------------------------------------------

def test_create_order_sends_idempotency_header():
    recorded = []
    client = OneCClient("https://onec.example", _capturing_transport(recorded))
    order = OneCOrderCreateSchema.model_validate(ORDER_DATA)
    client.create_order(order, idempotency_key="idem-abc")
    assert recorded[0]["headers"][IDEMPOTENCY_HEADER] == "idem-abc"


def test_create_order_without_key_has_no_header():
    recorded = []
    client = OneCClient("https://onec.example", _capturing_transport(recorded))
    client.create_order(OneCOrderCreateSchema.model_validate(ORDER_DATA))
    assert IDEMPOTENCY_HEADER not in recorded[0]["headers"]


def test_create_order_mirrors_key_into_comment():
    recorded = []
    client = OneCClient("https://onec.example", _capturing_transport(recorded))
    order = OneCOrderCreateSchema.model_validate(ORDER_DATA)
    client.create_order(order, idempotency_key="idem-xyz")
    assert recorded[0]["body"]["comment"] == comment_with_idempotency_key(None, "idem-xyz")


def test_create_order_legacy_transport_without_headers():
    recorded = []

    def transport(url, method, body):  # positional, no headers kwarg
        recorded.append((url, method, body))
        return {"status_code": 200, "body": {}}

    client = OneCClient("https://onec.example", transport)
    client.create_order(OneCOrderCreateSchema.model_validate(ORDER_DATA), idempotency_key="k")
    assert recorded[0][0].endswith("Document_ЗаказПокупателя")


# --- dispatch propagation --------------------------------------------------

def test_dispatch_sends_event_idempotency_key(conn):
    s = _settings()
    event = _prepared(conn, "propagate-key")
    recorded = []
    client = OneCClient("https://onec.example", _capturing_transport(recorded))
    result = dispatch_event(conn, event, s, client, lambda p: {}, lambda d: None)
    assert result == "DELIVERED"
    assert len(recorded) == 1
    assert recorded[0]["headers"][IDEMPOTENCY_HEADER] == "propagate-key"


def test_dispatch_key_is_stable_across_all_retries(conn):
    s = _settings()
    event = _prepared(conn, "stable-key")
    recorded = []
    client = OneCClient(
        "https://onec.example",
        _capturing_transport(recorded, {"status_code": 500, "body": {}}),
    )
    result = dispatch_event(conn, event, s, client, lambda p: {}, lambda d: None)
    assert result == "FAILED"
    assert len(recorded) == s.backoff.max_attempts
    keys = {r["headers"].get(IDEMPOTENCY_HEADER) for r in recorded}
    assert keys == {"stable-key"}


def test_delivery_row_stores_idempotency_key(conn):
    event = _prepared(conn, "row-key")
    delivery = get_delivery_for(conn, event.id, "1c")
    assert delivery.idempotency_key == "row-key"
    assert delivery.idempotency_key == event.idempotency_key
