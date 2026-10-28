"""Regression tests for the CTO-hardening pass.

Covers the invariants that were previously implicit:

* settings are real ``pydantic-settings`` and fail closed on bad endpoints;
* async OAuth rotation is single-flight through ``asyncio.Lock``;
* an internal ``TypeError`` in a transport is not mistaken for a legacy
  signature and retried (which would duplicate an outbound HTTP request);
* two workers cannot deliver the same outbox event twice;
* a crashed ``DISPATCHED`` lease is reclaimed after expiry;
* the SQLite WAL/synchronous contract is actually applied;
* the HTTP boundary echoes the request correlation id.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
from pydantic_settings import BaseSettings

import gateway.state_store as state_store
from gateway.amocrm_client import AmoClient, AmoError
from gateway.config import DEFAULT_CONFIG_PATH, Settings
from gateway.egress_dispatcher import OneCClient, dispatch_event
from gateway.state_store import (
    claim_event,
    connect,
    fetch_dispatchable,
    get_event,
    init_db,
    insert_event,
    transition_status,
)

ORDER_DATA = {
    "number": "SO-RACE",
    "date": "2026-10-12T00:00:00+00:00",
    "counterparty_ref": "cp-1",
    "items": [{"sku": "SKU-1", "quantity": 1, "price": 1.0}],
    "total": 1.0,
}
OLD_TOKEN = "old-token"
NEW_TOKEN = "new-token"


def _settings():
    return Settings.from_file(DEFAULT_CONFIG_PATH)


def test_settings_is_pydantic_settings_and_validates_endpoints():
    assert issubclass(Settings, BaseSettings)
    settings = Settings.from_env({})
    assert settings.onec_order_path.startswith("/")
    assert settings.amocrm_leads_path.startswith("/")
    with pytest.raises(ValueError):
        Settings.from_env({"GATEWAY_ONEC_ORDER_PATH": "relative/path"})


def test_async_token_rotation_is_single_flight():
    refreshes: list[str] = []

    def refresher() -> str:
        time.sleep(0.02)  # widen the race window
        refreshes.append(NEW_TOKEN)
        return NEW_TOKEN

    client = AmoClient(
        "https://amo.example",
        lambda *args, **kwargs: {"status_code": 200, "body": {}},
        access_token=OLD_TOKEN,
        token_refresher=refresher,
    )

    async def run_race():
        return await asyncio.gather(
            *(client.async_refresh_token(0) for _ in range(16))
        )

    tokens = asyncio.run(run_race())

    assert tokens == [NEW_TOKEN] * 16
    assert len(refreshes) == 1
    assert client.token_version == 1


def test_internal_transport_typeerror_is_not_retried():
    calls: list[str] = []

    def transport(url, method, body, headers=None):
        calls.append(url)
        raise TypeError("bug inside the transport, not a legacy signature")

    client = AmoClient("https://amo.example", transport, access_token=OLD_TOKEN)
    with pytest.raises(AmoError):
        client.update_lead_status("LEAD-1", "PAID")
    assert len(calls) == 1


def _prepare_event(db_path: str) -> int:
    conn = init_db(db_path)
    envelope = json.dumps({"event_type": "order", "data": ORDER_DATA})
    event_id, _ = insert_event(conn, "race-event", envelope)
    transition_status(conn, event_id, "RECEIVED", "VALIDATED")
    conn.close()
    return event_id


def test_two_workers_deliver_an_event_exactly_once(db_path):
    event_id = _prepare_event(db_path)
    settings = _settings()
    calls: list[dict] = []
    call_lock = threading.Lock()
    barrier = threading.Barrier(2)

    def transport(url, method, body, headers=None):
        with call_lock:
            calls.append({"url": url, "body": body})
        time.sleep(0.05)
        return {"status_code": 200, "body": {"ok": True}}

    def worker():
        conn = connect(db_path)
        try:
            event = get_event(conn, event_id)
            assert event is not None
            barrier.wait(timeout=10)
            return dispatch_event(
                conn,
                event,
                settings,
                OneCClient("https://onec.example", transport),
                lambda payload: {},
                lambda delay: None,
            )
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: worker(), range(2)))

    assert "DELIVERED" in results
    assert len(calls) == 1

    check = connect(db_path)
    try:
        assert get_event(check, event_id).status == "DELIVERED"
    finally:
        check.close()


def test_stale_dispatched_lease_is_reclaimed_but_fresh_one_is_not(db_path):
    conn = init_db(db_path)
    try:
        envelope = json.dumps({"event_type": "order", "data": ORDER_DATA})
        stale_id, _ = insert_event(conn, "stale", envelope)
        transition_status(conn, stale_id, "RECEIVED", "VALIDATED")
        transition_status(conn, stale_id, "VALIDATED", "DISPATCHED")
        conn.execute(
            "UPDATE events SET updated_at=? WHERE id=?",
            ((datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(), stale_id),
        )

        fresh_id, _ = insert_event(conn, "fresh", envelope)
        transition_status(conn, fresh_id, "RECEIVED", "VALIDATED")
        transition_status(conn, fresh_id, "VALIDATED", "DISPATCHED")

        ids = {
            row.id
            for row in fetch_dispatchable(conn, lease_seconds=60)
        }
        assert stale_id in ids
        assert fresh_id not in ids

        stale = get_event(conn, stale_id)
        assert claim_event(conn, stale_id, lease_seconds=60) is True
        assert get_event(conn, stale_id).status == "DISPATCHED"
        assert stale is not None

        fresh = get_event(conn, fresh_id)
        assert claim_event(conn, fresh_id, lease_seconds=60) is False
        assert fresh is not None
    finally:
        conn.close()


def test_transient_sqlite_lock_is_retried_without_failing(monkeypatch):
    """``database is locked`` is contention, not a defect: retry it."""
    sleeps: list[float] = []
    attempts: list[int] = []
    monkeypatch.setattr(state_store, "LOCK_RETRY_ATTEMPTS", 4)
    monkeypatch.setattr(state_store, "LOCK_RETRY_BASE_SECONDS", 0.05)
    monkeypatch.setattr(state_store.time, "sleep", sleeps.append)

    def operation():
        attempts.append(1)
        if len(attempts) < 3:
            raise sqlite3.OperationalError("database is locked")
        return "committed"

    assert state_store._retry_on_lock(operation) == "committed"
    assert len(attempts) == 3
    assert sleeps == [0.05, 0.1]


def test_exhausted_sqlite_lock_retries_raise_the_original_error(monkeypatch):
    monkeypatch.setattr(state_store, "LOCK_RETRY_ATTEMPTS", 2)
    monkeypatch.setattr(state_store.time, "sleep", lambda _delay: None)

    def always_locked():
        raise sqlite3.OperationalError("database is busy")

    with pytest.raises(sqlite3.OperationalError, match="database is busy"):
        state_store._retry_on_lock(always_locked)


def test_sqlite_uses_wal_and_synchronous_normal(conn):
    assert str(conn.execute("PRAGMA journal_mode;").fetchone()[0]).lower() == "wal"
    assert conn.execute("PRAGMA synchronous;").fetchone()[0] == 1
    assert conn.execute("PRAGMA busy_timeout;").fetchone()[0] == 5000


def test_request_id_is_echoed_by_the_http_boundary():
    from fastapi.testclient import TestClient

    from gateway.app import app

    response = TestClient(app).get(
        "/health", headers={"X-Request-ID": "audit-request-42"}
    )
    assert response.status_code == 200
    assert response.headers["X-Request-ID"] == "audit-request-42"
