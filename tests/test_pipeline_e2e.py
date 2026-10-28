import hashlib
import hmac
import json
from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient

from gateway.app import create_app
from gateway.config import RateLimitConfig
from gateway.egress_dispatcher import OneCClient
from gateway.pipeline import run_delivery_pass
from gateway.state_store import get_event, init_db, insert_event, transaction, transition_status

TEST_SECRET = "test-secret"

ORDER_DATA = {
    "number": "SO-1",
    "date": "2026-10-12T00:00:00+00:00",
    "counterparty_ref": "cp-1",
    "items": [{"sku": "SKU-1", "quantity": 1, "price": 2.0}],
    "total": 2.0,
}


def _sign(body, secret=TEST_SECRET):
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def _headers(body, nonce="nonce-1", signature=None, secret=TEST_SECRET):
    return {
        "X-Signature": signature if signature is not None else _sign(body, secret),
        "X-Timestamp": datetime.now(timezone.utc).isoformat(),
        "X-Nonce": nonce,
        "Content-Type": "application/json",
    }


def _body(key="idem-1", event_type="order", nonce="nonce-1", data=None):
    payload = {
        "idempotency_key": key,
        "event_type": event_type,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "nonce": nonce,
        "data": data if data is not None else ORDER_DATA,
    }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _client(recorded, response=None):
    def transport(url, method, body):
        recorded.append({"url": url, "method": method, "body": body})
        return response if response is not None else {"status_code": 200, "body": {"ok": True}}

    return transport


def _gateway(settings, db_path, transport=None, dlq=None, sleep_fn=None):
    conn = init_db(db_path)
    recorded = []
    if transport is None:
        transport = _client(recorded)
    client = OneCClient("https://onec.example", transport)
    dlq_records = []
    real_dlq = dlq
    if real_dlq is None:
        def real_dlq(payload):
            dlq_records.append(payload)
            return {"status_code": 200}
    app = create_app(
        settings=settings,
        conn=conn,
        client=client,
        dlq_transport=real_dlq,
        sleep_fn=sleep_fn or (lambda d: None),
    )
    return SimpleNamespace(
        app=TestClient(app),
        conn=conn,
        client=client,
        recorded=recorded,
        dlq=dlq_records,
    )


def _post(gw, body, nonce="nonce-1", signature=None):
    return gw.app.post(
        "/webhook/amocrm", content=body, headers=_headers(body, nonce=nonce, signature=signature)
    )


def _status(conn, event_id):
    return conn.execute("SELECT status FROM events WHERE id=?", (event_id,)).fetchone()[0]


# --- ingress / persistence ----------------------------------------------

def test_valid_webhook_returns_200(settings, db_path):
    gw = _gateway(settings, db_path)
    resp = _post(gw, _body(nonce="ok-1"), nonce="ok-1")
    assert resp.status_code == 200
    assert resp.json()["status"] == "validated"


def test_event_status_is_validated(settings, db_path):
    gw = _gateway(settings, db_path)
    resp = _post(gw, _body(key="st-1", nonce="st-1"), nonce="st-1")
    assert _status(gw.conn, resp.json()["event_id"]) == "VALIDATED"


def test_duplicate_webhook_returns_duplicate(settings, db_path):
    gw = _gateway(settings, db_path)
    _post(gw, _body(key="dup", nonce="dup-1"), nonce="dup-1")
    resp = _post(gw, _body(key="dup", nonce="dup-2"), nonce="dup-2")
    assert resp.status_code == 200 and resp.json()["status"] == "duplicate"


def test_duplicate_single_row(settings, db_path):
    gw = _gateway(settings, db_path)
    _post(gw, _body(key="dup2", nonce="dup2-1"), nonce="dup2-1")
    _post(gw, _body(key="dup2", nonce="dup2-2"), nonce="dup2-2")
    count = gw.conn.execute("SELECT count(*) FROM events").fetchone()[0]
    assert count == 1


def test_bad_signature_401(settings, db_path):
    gw = _gateway(settings, db_path)
    body = _body(key="bad-sig", nonce="bad-sig")
    resp = _post(gw, body, nonce="bad-sig", signature="00" * 32)
    assert resp.status_code == 401
    assert gw.conn.execute("SELECT count(*) FROM events").fetchone()[0] == 0


def test_tampered_header_403(settings, db_path):
    gw = _gateway(settings, db_path)
    body = _body(key="tamper", nonce="tamper")
    headers = _headers(body, nonce="tamper")
    del headers["X-Timestamp"]
    resp = gw.app.post("/webhook/amocrm", content=body, headers=headers)
    assert resp.status_code == 403
    assert gw.conn.execute("SELECT count(*) FROM events").fetchone()[0] == 0


def test_replay_403(settings, db_path):
    gw = _gateway(settings, db_path)
    body = _body(key="replay", nonce="replay-1")
    assert _post(gw, body, nonce="replay-1").status_code == 200
    resp = _post(gw, body, nonce="replay-1")
    assert resp.status_code == 403
    assert gw.conn.execute("SELECT count(*) FROM events").fetchone()[0] == 1


def test_rate_limit_429(settings, db_path):
    limited = settings.model_copy(
        update={
            "rate_limit": RateLimitConfig(
                algorithm="token_bucket", requests_per_window=1, window_seconds=60
            )
        }
    )
    gw = _gateway(limited, db_path)
    assert _post(gw, _body(key="rl-1", nonce="rl-1"), nonce="rl-1").status_code == 200
    resp = _post(gw, _body(key="rl-2", nonce="rl-2"), nonce="rl-2")
    assert resp.status_code == 429


def test_broken_payload_422(settings, db_path):
    gw = _gateway(settings, db_path)
    body = b"{broken"
    resp = _post(gw, body, nonce="broken-1")
    assert resp.status_code == 422
    assert set(resp.json()) == {"type", "message", "severity", "location", "how_to_fix"}
    assert gw.conn.execute("SELECT count(*) FROM events").fetchone()[0] == 0


def test_empty_body_422(settings, db_path):
    gw = _gateway(settings, db_path)
    resp = _post(gw, b"", nonce="empty-1")
    assert resp.status_code == 422


# --- egress / delivery ---------------------------------------------------

def test_run_delivery_pass_delivered_count(settings, db_path):
    gw = _gateway(settings, db_path)
    _post(gw, _body(key="deliver", nonce="deliver-1"), nonce="deliver-1")
    counters = run_delivery_pass(gw.conn, settings, gw.client, gw.dlq.append, lambda d: None)
    assert counters["delivered"] == 1 and counters["failed"] == 0


def test_e2e_event_reaches_delivered(settings, db_path):
    gw = _gateway(settings, db_path)
    resp = _post(gw, _body(key="e2e", nonce="e2e-1"), nonce="e2e-1")
    run_delivery_pass(gw.conn, settings, gw.client, gw.dlq.append, lambda d: None)
    assert get_event(gw.conn, resp.json()["event_id"]).status == "DELIVERED"


def test_onec_unavailable_event_failed(settings, db_path):
    gw = _gateway(
        settings, db_path, transport=_client([], {"status_code": 500, "body": {}})
    )
    _post(gw, _body(key="fail", nonce="fail-1"), nonce="fail-1")
    counters = run_delivery_pass(gw.conn, settings, gw.client, gw.dlq.append, lambda d: None)
    assert counters["failed"] == 1
    assert gw.conn.execute("SELECT count(*) FROM events WHERE status='FAILED'").fetchone()[0] == 1


def test_dlq_payload_six_keys(settings, db_path):
    gw = _gateway(
        settings, db_path, transport=_client([], {"status_code": 500, "body": {}})
    )
    _post(gw, _body(key="dlq", nonce="dlq-1"), nonce="dlq-1")
    run_delivery_pass(gw.conn, settings, gw.client, gw.dlq.append, lambda d: None)
    assert len(gw.dlq) == 1
    assert set(gw.dlq[0]) == {
        "event_id",
        "idempotency_key",
        "payload",
        "error",
        "attempts",
        "failed_at",
    }


def test_resend_failed_without_revalidation(settings, db_path, monkeypatch):
    gw = _gateway(
        settings, db_path, transport=_client([], {"status_code": 500, "body": {}})
    )
    resp = _post(gw, _body(key="resend", nonce="resend-1"), nonce="resend-1")
    run_delivery_pass(gw.conn, settings, gw.client, gw.dlq.append, lambda d: None)
    assert get_event(gw.conn, resp.json()["event_id"]).status == "FAILED"

    from gateway import pipeline

    calls = []
    monkeypatch.setattr(pipeline, "validate_webhook", lambda raw: calls.append(raw))
    monkeypatch.setattr(pipeline, "parse_webhook_body", lambda raw: calls.append(raw))

    gw.client.transport = _client([])
    run_delivery_pass(gw.conn, settings, gw.client, gw.dlq.append, lambda d: None)
    assert calls == []
    assert get_event(gw.conn, resp.json()["event_id"]).status == "DELIVERED"


def test_state_machine_transitions_reproducible(settings, db_path):
    conn = init_db(db_path)
    pairs = [
        ("RECEIVED", "VALIDATED"),
        ("VALIDATED", "DISPATCHED"),
        ("DISPATCHED", "DELIVERED"),
        ("DISPATCHED", "FAILED"),
        ("FAILED", "VALIDATED"),
    ]
    for frm, to in pairs:
        event_id, _ = insert_event(conn, f"sm-{frm}-{to}", "{}")
        with transaction(conn):
            conn.execute("UPDATE events SET status=? WHERE id=?", (frm, event_id))
        assert transition_status(conn, event_id, frm, to) is True
    event_id, _ = insert_event(conn, "sm-forbidden", "{}")
    assert transition_status(conn, event_id, "RECEIVED", "DELIVERED") is False
