"""Tests for the inbound 1C leg: POST /webhook/onec (token / HMAC / validation)."""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from gateway.app import create_app
from gateway.config import DEFAULT_CONFIG_PATH, RateLimitConfig, Settings
from gateway.ingress_guard import API_TOKEN_HEADER, extract_api_token, verify_api_token
from gateway.state_store import (
    get_delivery_for,
    init_db,
    list_deliveries,
)

TEST_SECRET = "test-secret"
ONEC_TOKEN = "onec-token-xyz"

STATUS_DATA = {
    "order_id": "LEAD-42",
    "status": "paid",
    "payment_status": "paid",
    "updated_at": "2026-10-12T00:00:00+00:00",
    "comment": "оплачено",
}


def _settings(**overrides):
    base = Settings.from_file(DEFAULT_CONFIG_PATH)
    values = {
        "hmac_secret": TEST_SECRET,
        "one_c_api_token": ONEC_TOKEN,
    }
    values.update(overrides)
    return base.model_copy(update=values)


def _body(key="onec-1", event_type="onec_order_status", data=None):
    payload = {
        "idempotency_key": key,
        "event_type": event_type,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "nonce": key + "-nonce",
        "data": STATUS_DATA if data is None else data,
    }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _token_headers(body, token=ONEC_TOKEN, nonce="n-1"):
    return {
        API_TOKEN_HEADER: token,
        "X-Timestamp": datetime.now(timezone.utc).isoformat(),
        "X-Nonce": nonce,
        "Content-Type": "application/json",
    }


def _hmac_headers(body, secret=TEST_SECRET, nonce="n-1"):
    sig = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return {
        "X-Signature": sig,
        "X-Timestamp": datetime.now(timezone.utc).isoformat(),
        "X-Nonce": nonce,
        "Content-Type": "application/json",
    }


@pytest.fixture
def gateway(settings, db_path):
    conn = init_db(db_path)
    app = create_app(settings=_settings(), conn=conn)
    client = TestClient(app)
    yield SimpleGateway(client, conn)
    conn.close()


class SimpleGateway:
    def __init__(self, client, conn):
        self.client = client
        self.conn = conn

    def post(self, body, headers):
        return self.client.post("/webhook/onec", content=body, headers=headers)


# --- auth primitives ------------------------------------------------------

def test_extract_api_token_prefers_header():
    assert extract_api_token({API_TOKEN_HEADER: "abc"}) == "abc"


def test_extract_api_token_bearer():
    assert extract_api_token({"Authorization": "Bearer  abc"}) == "abc"


def test_extract_api_token_none_when_absent():
    assert extract_api_token({"Content-Type": "application/json"}) is None


def test_verify_api_token_constant_time():
    assert verify_api_token({API_TOKEN_HEADER: ONEC_TOKEN}, ONEC_TOKEN) is True
    assert verify_api_token({API_TOKEN_HEADER: "wrong"}, ONEC_TOKEN) is False
    assert verify_api_token({}, ONEC_TOKEN) is False
    assert verify_api_token({API_TOKEN_HEADER: ONEC_TOKEN}, "") is False


# --- endpoint -------------------------------------------------------------

def test_onec_token_accepted_200(gateway):
    body = _body()
    resp = gateway.post(body, _token_headers(body, nonce="ok-1"))
    assert resp.status_code == 200
    assert resp.json()["status"] == "validated"
    assert resp.json()["destinations"] == ["amocrm"]


def test_onec_bearer_accepted_200(settings, db_path):
    conn = init_db(db_path)
    client = TestClient(create_app(settings=_settings(), conn=conn))
    body = _body(key="bearer-1")
    headers = _token_headers(body, nonce="bearer-1")
    headers.pop(API_TOKEN_HEADER)
    headers["Authorization"] = f"Bearer {ONEC_TOKEN}"
    resp = client.post("/webhook/onec", content=body, headers=headers)
    assert resp.status_code == 200
    conn.close()


def test_onec_hmac_accepted_200(gateway):
    body = _body(key="hmac-1")
    resp = gateway.post(body, _hmac_headers(body, nonce="hmac-1"))
    assert resp.status_code == 200


def test_onec_invalid_token_401(gateway):
    body = _body(key="bad-token")
    headers = _token_headers(body, token="nope", nonce="bad-token")
    resp = gateway.post(body, headers)
    assert resp.status_code == 401
    assert gateway.conn.execute("SELECT count(*) FROM events").fetchone()[0] == 0


def test_onec_missing_token_401(gateway):
    body = _body(key="no-token")
    headers = _token_headers(body, nonce="no-token")
    headers.pop(API_TOKEN_HEADER)
    resp = gateway.post(body, headers)
    assert resp.status_code == 401


def test_onec_bad_hmac_401(gateway):
    body = _body(key="bad-hmac")
    headers = _hmac_headers(body, secret="wrong", nonce="bad-hmac")
    resp = gateway.post(body, headers)
    assert resp.status_code == 401


def test_onec_replay_403(gateway):
    body = _body(key="replay")
    assert gateway.post(body, _token_headers(body, nonce="rp-1")).status_code == 200
    resp = gateway.post(body, _token_headers(body, nonce="rp-1"))
    assert resp.status_code == 403


def test_onec_missing_nonce_403(gateway):
    body = _body(key="no-nonce")
    headers = _token_headers(body, nonce="x")
    del headers["X-Nonce"]
    resp = gateway.post(body, headers)
    assert resp.status_code == 403


def test_onec_broken_body_422(gateway):
    body = b"{broken"
    resp = gateway.post(body, _token_headers(body, nonce="broken"))
    assert resp.status_code == 422
    assert set(resp.json()) == {"type", "message", "severity", "location", "how_to_fix"}


def test_onec_schema_violation_422(gateway):
    body = _body(key="bad-schema", data={"order_id": "L", "status": "paid"})  # no updated_at
    resp = gateway.post(body, _token_headers(body, nonce="bad-schema"))
    assert resp.status_code == 422
    assert gateway.conn.execute("SELECT count(*) FROM events").fetchone()[0] == 0


def test_onec_rate_limit_429(settings, db_path):
    limited = _settings(
        rate_limit=RateLimitConfig(
            algorithm="token_bucket", requests_per_window=1, window_seconds=60
        )
    )
    conn = init_db(db_path)
    client = TestClient(create_app(settings=limited, conn=conn))
    b1 = _body(key="rl-1")
    resp = client.post("/webhook/onec", content=b1, headers=_token_headers(b1, nonce="rl-1"))
    assert resp.status_code == 200
    b2 = _body(key="rl-2")
    resp = client.post("/webhook/onec", content=b2, headers=_token_headers(b2, nonce="rl-2"))
    assert resp.status_code == 429
    conn.close()


# --- persistence / routing -------------------------------------------------

def test_onec_event_gets_amocrm_delivery(gateway):
    body = _body(key="route-1")
    resp = gateway.post(body, _token_headers(body, nonce="route-1"))
    event_id = resp.json()["event_id"]
    deliveries = list_deliveries(gateway.conn, event_id)
    assert len(deliveries) == 1
    assert deliveries[0].destination == "amocrm"
    assert get_delivery_for(gateway.conn, event_id, "1c") is None


def test_onec_envelope_persisted(gateway):
    body = _body(key="env-1")
    event_id = gateway.post(body, _token_headers(body, nonce="env-1")).json()["event_id"]
    row = gateway.conn.execute(
        "SELECT payload FROM events WHERE id=?", (event_id,)
    ).fetchone()
    envelope = json.loads(row["payload"])
    assert envelope["event_type"] == "onec_order_status"
    assert envelope["data"]["order_id"] == "LEAD-42"


def test_onec_duplicate_returns_duplicate(gateway):
    b1 = _body(key="dup-onec")
    gateway.post(b1, _token_headers(b1, nonce="d-1"))
    b2 = _body(key="dup-onec")
    resp = gateway.post(b2, _token_headers(b2, nonce="d-2"))
    assert resp.json()["status"] == "duplicate"
    assert gateway.conn.execute("SELECT count(*) FROM deliveries").fetchone()[0] == 1
