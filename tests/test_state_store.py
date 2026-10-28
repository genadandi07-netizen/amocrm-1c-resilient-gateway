import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from gateway.state_store import (
    ALLOWED_TRANSITIONS,
    connect,
    fetch_dispatchable,
    init_db,
    insert_event,
    resend_failed,
    transaction,
    transition_status,
)


class _Proxy:
    def __init__(self, conn):
        self._conn = conn
        self.sql = []

    def execute(self, sql, *args, **kwargs):
        self.sql.append(sql)
        return self._conn.execute(sql, *args, **kwargs)

    def __getattr__(self, item):
        return getattr(self._conn, item)


def test_init_db_wal_mode(conn):
    mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
    assert str(mode).lower() == "wal"


def test_init_db_creates_events_table(conn):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='events'"
    ).fetchone()
    assert row is not None


def test_init_db_creates_status_index(conn):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_events_status'"
    ).fetchone()
    assert row is not None


def test_init_db_busy_timeout_is_configurable(tmp_path):
    path = str(tmp_path / "configured.db")
    connection = init_db(path, busy_timeout_ms=1234, timeout=1.0)
    try:
        assert connection.execute("PRAGMA busy_timeout;").fetchone()[0] == 1234
    finally:
        connection.close()


def test_status_check_rejects_bogus(conn):
    with pytest.raises(sqlite3.IntegrityError), transaction(conn):
        conn.execute(
            "INSERT INTO events (idempotency_key, payload, status, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("k-bogus", "{}", "BOGUS", "now", "now"),
        )


def test_insert_event_creates_row(conn):
    event_id, created = insert_event(conn, "k1", "{}")
    assert created is True and isinstance(event_id, int)
    assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == 1


def test_insert_event_duplicate_idempotency_key(conn):
    insert_event(conn, "k1", "{}")
    _, created = insert_event(conn, "k1", "{}")
    assert created is False
    assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == 1


def test_transaction_uses_begin_immediate(conn):
    proxy = _Proxy(conn)
    with transaction(proxy):
        pass
    assert any(
        str(sql).strip().upper().startswith("BEGIN IMMEDIATE") for sql in proxy.sql
    )


def test_allowed_transitions_table():
    expected = {
        ("RECEIVED", "VALIDATED"),
        ("VALIDATED", "DISPATCHED"),
        ("DISPATCHED", "DELIVERED"),
        ("DISPATCHED", "FAILED"),
        ("FAILED", "VALIDATED"),
    }
    present = {(frm, to) for frm, targets in ALLOWED_TRANSITIONS.items() for to in targets}
    assert expected.issubset(present)
    assert ("RECEIVED", "DELIVERED") not in present


def test_forbidden_transition_returns_false(conn):
    event_id, _ = insert_event(conn, "k2", "{}")
    assert transition_status(conn, event_id, "RECEIVED", "DELIVERED") is False
    status = conn.execute("SELECT status FROM events WHERE id=?", (event_id,)).fetchone()[0]
    assert status == "RECEIVED"


def test_transition_updates_status(conn):
    event_id, _ = insert_event(conn, "k3", "{}")
    assert transition_status(conn, event_id, "RECEIVED", "VALIDATED") is True
    status = conn.execute("SELECT status FROM events WHERE id=?", (event_id,)).fetchone()[0]
    assert status == "VALIDATED"


def test_fetch_dispatchable_selects_validated_and_failed(conn):
    e1, _ = insert_event(conn, "d1", "{}")
    transition_status(conn, e1, "RECEIVED", "VALIDATED")
    e2, _ = insert_event(conn, "d2", "{}")
    transition_status(conn, e2, "RECEIVED", "VALIDATED")
    transition_status(conn, e2, "VALIDATED", "DISPATCHED")
    transition_status(conn, e2, "DISPATCHED", "FAILED")
    ids = {row.id for row in fetch_dispatchable(conn, limit=10)}
    assert e1 in ids and e2 in ids


def test_fetch_dispatchable_excludes_delivered(conn):
    event_id, _ = insert_event(conn, "x1", "{}")
    transition_status(conn, event_id, "RECEIVED", "VALIDATED")
    transition_status(conn, event_id, "VALIDATED", "DISPATCHED")
    transition_status(conn, event_id, "DISPATCHED", "DELIVERED")
    assert all(row.id != event_id for row in fetch_dispatchable(conn, limit=10))


def test_resend_failed_transitions_to_validated(conn):
    event_id, _ = insert_event(conn, "r1", "{}")
    transition_status(conn, event_id, "RECEIVED", "VALIDATED")
    transition_status(conn, event_id, "VALIDATED", "DISPATCHED")
    transition_status(conn, event_id, "DISPATCHED", "FAILED")
    assert resend_failed(conn, event_id) is True
    status = conn.execute("SELECT status FROM events WHERE id=?", (event_id,)).fetchone()[0]
    assert status == "VALIDATED"


def test_concurrent_workers_50_no_lock_errors(db_path):
    init_db(db_path)

    def worker(index):
        connection = connect(db_path)
        try:
            event_id, created = insert_event(connection, f"conc-{index}", "{}")
            assert created is True
            assert transition_status(connection, event_id, "RECEIVED", "VALIDATED")
            assert transition_status(connection, event_id, "VALIDATED", "DISPATCHED")
            assert transition_status(connection, event_id, "DISPATCHED", "DELIVERED")
            return True
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=50) as pool:
        results = list(pool.map(worker, range(50)))
    assert all(results)

    check = init_db(db_path)
    try:
        count = check.execute(
            "SELECT count(*) FROM events WHERE status='DELIVERED'"
        ).fetchone()[0]
        assert count == 50
    finally:
        check.close()
