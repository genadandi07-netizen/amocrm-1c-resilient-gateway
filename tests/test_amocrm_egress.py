"""Tests for the outbound amoCRM leg: AmoClient, rate limiting and retries."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from gateway.amocrm_client import (
    AmoClient,
    AmoError,
    AmoRateLimitError,
    comment_with_idempotency_key,
    default_rate_limit_config,
    map_status_to_amo,
)
from gateway.config import (
    DEFAULT_CONFIG_PATH,
    BackoffConfig,
    CircuitBreakerConfig,
    RateLimitConfig,
    Settings,
)
from gateway.contracts import OneCOrderStatusSchema
from gateway.egress_dispatcher import dispatch_event
from gateway.ingress_guard import RateLimiter
from gateway.state_store import (
    get_delivery_for,
    get_event,
    insert_event,
    transition_status,
)

STATUS_DATA = {
    "order_id": "LEAD-7",
    "status": "processing",
    "payment_status": "unpaid",
    "updated_at": "2026-10-12T00:00:00+00:00",
    "comment": "в работе",
}


class FakeClock:
    def __init__(self, start=None):
        self.t = start or datetime(2026, 10, 12, tzinfo=timezone.utc)

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t = self.t + timedelta(seconds=seconds)


def _recorder(recorded, response=None):
    def transport(url, method, body, headers=None):
        recorded.append({"url": url, "method": method, "body": body, "headers": headers or {}})
        return response if response is not None else {"status_code": 200, "body": {"ok": True}}

    return transport


def _client(recorded=None, response=None, **kwargs):
    recorded = recorded if recorded is not None else []
    return AmoClient("https://amo.example", _recorder(recorded, response), **kwargs), recorded


# --- helpers ---------------------------------------------------------------

def test_comment_with_idempotency_key_new():
    assert comment_with_idempotency_key(None, "k1") == "[idempotency-key:k1]"


def test_comment_with_idempotency_key_appends():
    out = comment_with_idempotency_key("оплачено", "k1")
    assert out == "[idempotency-key:k1] оплачено"


def test_default_rate_limit_config_is_100rps():
    cfg = default_rate_limit_config(100)
    assert cfg.algorithm == "token_bucket"
    assert cfg.requests_per_window == 100
    assert cfg.window_seconds == 1


def test_map_status_basic():
    assert map_status_to_amo("processing") == "IN_PROGRESS"
    assert map_status_to_amo("cancelled") == "CANCELLED"
    assert map_status_to_amo("weird") == "weird"


def test_map_status_paid_overrides():
    assert map_status_to_amo("processing", "paid") == "PAID"


# --- AmoClient -------------------------------------------------------------

def test_update_lead_status_calls_correct_endpoint():
    client, recorded = _client()
    result = client.update_lead_status("LEAD-7", "IN_PROGRESS")
    assert result == {"ok": True}
    assert recorded[0]["method"] == "PATCH"
    assert recorded[0]["url"].endswith("/api/v4/leads/LEAD-7")
    assert recorded[0]["body"]["status_id"] == "IN_PROGRESS"


def test_update_lead_status_sends_idempotency_header():
    client, recorded = _client()
    client.update_lead_status("LEAD-7", "PAID", idempotency_key="key-1")
    assert recorded[0]["headers"]["X-Idempotency-Key"] == "key-1"


def test_update_lead_status_includes_comment():
    client, recorded = _client()
    client.update_lead_status("L", "PAID", comment="done")
    assert recorded[0]["body"]["comment"] == "done"


def test_update_order_status_maps_contract():
    client, recorded = _client()
    status = OneCOrderStatusSchema.model_validate(STATUS_DATA)
    client.update_order_status(status, idempotency_key="k9")
    sent = recorded[0]
    assert sent["url"].endswith("/api/v4/leads/LEAD-7")
    assert sent["body"]["status_id"] == "IN_PROGRESS"
    assert "payment:unpaid" in sent["body"]["comment"]
    assert sent["headers"]["X-Idempotency-Key"] == "k9"


def test_non_2xx_raises_amo_error():
    client, _ = _client(response={"status_code": 500, "body": {}})
    with pytest.raises(AmoError):
        client.update_lead_status("L", "X")


def test_transport_failure_wrapped_as_amo_error():
    def boom(*args, **kwargs):
        raise RuntimeError("network down")

    client = AmoClient("https://amo.example", boom)
    with pytest.raises(AmoError):
        client.update_lead_status("L", "X")


def test_legacy_transport_without_headers_still_works():
    recorded = []

    def transport(url, method, body):  # no headers param
        recorded.append((url, method, body))
        return {"status_code": 200, "body": {}}

    client = AmoClient("https://amo.example", transport)
    client.update_lead_status("L", "X")
    assert recorded[0][1] == "PATCH"


def test_http_client_style_transport_object():
    class Resp:
        status_code = 200

        def json(self):
            return {"ok": True}

    class Http:
        def __init__(self):
            self.calls = []

        def request(self, method, url, json=None, headers=None):
            self.calls.append((method, url, json, headers))
            return Resp()

    http = Http()
    client = AmoClient("https://amo.example", http)
    assert client.update_lead_status("L", "X", idempotency_key="k") == {"ok": True}
    assert http.calls[0][0] == "PATCH"


# --- client-side rate limiting --------------------------------------------

def test_rate_limit_blocks_after_budget():
    clock = FakeClock()
    limiter = RateLimiter(
        RateLimitConfig(algorithm="token_bucket", requests_per_window=1, window_seconds=1)
    )
    client, _ = _client(rate_limiter=limiter, clock=clock)
    client.update_lead_status("L", "1")  # consumes the single token
    with pytest.raises(AmoRateLimitError):
        client.update_lead_status("L", "2")


def test_rate_limit_recovers_after_window():
    clock = FakeClock()
    limiter = RateLimiter(
        RateLimitConfig(algorithm="token_bucket", requests_per_window=1, window_seconds=1)
    )
    client, _ = _client(rate_limiter=limiter, clock=clock)
    client.update_lead_status("L", "1")
    clock.advance(1.0)
    client.update_lead_status("L", "2")  # refilled


def test_rate_limit_allows_100rps_burst():
    clock = FakeClock()
    client, recorded = _client(requests_per_second=100, clock=clock)
    for i in range(100):
        client.update_lead_status("L", str(i))
    assert len(recorded) == 100
    with pytest.raises(AmoRateLimitError):
        client.update_lead_status("L", "101")


def test_rate_limit_error_shapes_as_warning():
    err = AmoRateLimitError("slow down")
    shape = err.to_error_shape()
    assert shape["severity"] == "warning"
    assert shape["type"] == "security"


# --- dispatcher routing + retries -----------------------------------------

def _status_envelope(data=None):
    return json.dumps(
        {"event_type": "onec_order_status", "data": data if data is not None else STATUS_DATA},
        ensure_ascii=False,
    )


def _prepared(conn, key, event_type, data=None, destinations=("amocrm",)):
    envelope = json.dumps(
        {"event_type": event_type, "data": data or STATUS_DATA}, ensure_ascii=False
    )
    event_id, _ = insert_event(conn, key, envelope, destinations=destinations)
    transition_status(conn, event_id, "RECEIVED", "VALIDATED")
    return get_event(conn, event_id)


def _dispatch_settings(max_attempts=3):
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


def test_dispatch_routes_to_amocrm(conn):
    s = _dispatch_settings()
    event = _prepared(conn, "amo-1", "onec_order_status")
    recorded = []
    amo, recorded = _client(recorded)
    dlq = []
    result = dispatch_event(conn, event, s, amo, lambda p: dlq.append(p) or {}, lambda d: None)
    assert result == "DELIVERED"
    assert recorded and recorded[0]["url"].endswith("LEAD-7")
    delivery = get_delivery_for(conn, event.id, "amocrm")
    assert delivery.status == "DELIVERED"
    assert delivery.attempt_count == 1


def test_dispatch_amocrm_retries_on_5xx(conn):
    s = _dispatch_settings()
    event = _prepared(conn, "amo-5xx", "onec_order_status")
    recorded = []
    amo, _ = _client(recorded, {"status_code": 500, "body": {}})
    dlq = []
    result = dispatch_event(conn, event, s, amo, lambda p: dlq.append(p) or {}, lambda d: None)
    assert result == "FAILED"
    assert len(recorded) == s.backoff.max_attempts
    assert len(dlq) == 1
    delivery = get_delivery_for(conn, event.id, "amocrm")
    assert delivery.status == "FAILED"
    assert delivery.attempt_count == s.backoff.max_attempts
    assert get_event(conn, event.id).status == "FAILED"


def test_dispatch_amocrm_retries_on_429(conn):
    s = _dispatch_settings()
    event = _prepared(conn, "amo-429", "onec_order_status")
    recorded = []
    amo, _ = _client(recorded, {"status_code": 429, "body": {}})
    dispatch_event(conn, event, s, amo, lambda p: {}, lambda d: None)
    assert len(recorded) == s.backoff.max_attempts


def test_dispatch_amocrm_recovers_after_transient_error(conn):
    s = _dispatch_settings()
    event = _prepared(conn, "amo-recover", "onec_order_status")
    calls = {"n": 0}

    def flaky(url, method, body, headers=None):
        calls["n"] += 1
        if calls["n"] < 2:
            return {"status_code": 503, "body": {}}
        return {"status_code": 200, "body": {"ok": True}}

    amo = AmoClient("https://amo.example", flaky)
    result = dispatch_event(conn, event, s, amo, lambda p: {}, lambda d: None)
    assert result == "DELIVERED"
    assert calls["n"] == 2


def test_dispatch_bundle_routes_both_directions(conn):
    """A SimpleNamespace bundle dispatches 1C and amoCRM rows independently."""
    from types import SimpleNamespace

    from gateway.egress_dispatcher import OneCClient

    s = _dispatch_settings()
    order_data = {
        "number": "SO-1", "date": "2026-10-12T00:00:00+00:00",
        "counterparty_ref": "cp-1",
        "items": [{"sku": "SKU-1", "quantity": 1, "price": 1.0}], "total": 1.0,
    }
    onec_recorded = []
    amo_recorded = []

    def onec_transport(url, method, body, headers=None):
        onec_recorded.append(headers or {})
        return {"status_code": 200, "body": {"ok": True}}

    onec = OneCClient("https://onec.example", onec_transport)
    amo, _ = _client(amo_recorded)
    bundle = SimpleNamespace(one_c=onec, amocrm=amo)

    event = _prepared(conn, "bundle-1", "order", order_data, destinations=("1c",))
    result = dispatch_event(conn, event, s, bundle, lambda p: {}, lambda d: None)
    assert result == "DELIVERED"
    assert onec_recorded[0]["X-Idempotency-Key"] == "bundle-1"
    assert get_delivery_for(conn, event.id, "1c").status == "DELIVERED"
