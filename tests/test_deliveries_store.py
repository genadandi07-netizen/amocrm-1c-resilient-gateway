"""Tests for the ``deliveries`` Transactional Outbox table and its CRUD."""

from __future__ import annotations

import sqlite3

import pytest

from gateway.state_store import (
    DELIVERY_STATUSES,
    DeliveryRow,
    ensure_delivery,
    fetch_ready_deliveries,
    finish_delivery,
    get_delivery,
    get_delivery_for,
    get_event,
    init_db,
    insert_event,
    list_deliveries,
    mark_delivery_in_flight,
    migrate_legacy_deliveries,
    set_delivery_status,
    transaction,
    transition_status,
)

# --- schema / atomic creation --------------------------------------------

def test_init_db_creates_deliveries_table(conn):
    names = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert "deliveries" in names


def test_init_db_creates_ready_index(conn):
    indexes = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()
    }
    assert "idx_deliveries_ready" in indexes


def test_insert_event_creates_default_1c_delivery(conn):
    event_id, created = insert_event(conn, "k-default", "{}")
    assert created is True
    rows = list_deliveries(conn, event_id)
    assert len(rows) == 1
    assert rows[0].destination == "1c"
    assert rows[0].status == "PENDING"
    assert rows[0].attempt_count == 0
    assert rows[0].idempotency_key == "k-default"


def test_insert_event_creates_multiple_destinations(conn):
    event_id, _ = insert_event(conn, "k-multi", "{}", destinations=("1c", "amocrm"))
    rows = list_deliveries(conn, event_id)
    assert {r.destination for r in rows} == {"1c", "amocrm"}


def test_insert_event_rejects_unknown_destination(conn):
    with pytest.raises(ValueError):
        insert_event(conn, "k-bad", "{}", destinations=("telegram",))


def test_insert_event_atomic_no_partial_rows_on_failure(conn):
    # A duplicate idempotency key must not create any delivery row.
    insert_event(conn, "dup", "{}")
    before = conn.execute("SELECT count(*) FROM deliveries").fetchone()[0]
    _event_id, created = insert_event(conn, "dup", "{}")
    after = conn.execute("SELECT count(*) FROM deliveries").fetchone()[0]
    assert created is False and after == before == 1


def test_delivery_unique_event_destination(conn):
    event_id, _ = insert_event(conn, "k-uniq", "{}")
    with pytest.raises(sqlite3.IntegrityError), transaction(conn):
        conn.execute(
            "INSERT INTO deliveries (event_id, destination, status, attempt_count, "
            "idempotency_key, created_at, updated_at) "
            "VALUES (?, '1c', 'PENDING', 0, 'k-uniq', 't', 't')",
            (event_id,),
        )


def test_delivery_foreign_key_references_event(conn):
    with pytest.raises(sqlite3.IntegrityError):
        ensure_delivery(conn, 9999, "1c", "ghost")


# --- CRUD ------------------------------------------------------------------

def test_get_delivery_and_get_for(conn):
    event_id, _ = insert_event(conn, "k-get", "{}")
    row = list_deliveries(conn, event_id)[0]
    assert isinstance(row, DeliveryRow)
    assert get_delivery(conn, row.id).id == row.id
    assert get_delivery_for(conn, event_id, "1c").id == row.id
    assert get_delivery_for(conn, event_id, "amocrm") is None


def test_ensure_delivery_is_idempotent(conn):
    event_id, _ = insert_event(conn, "k-ensure", "{}")
    first_id, first_created = ensure_delivery(conn, event_id, "amocrm", "k-ensure")
    second_id, second_created = ensure_delivery(conn, event_id, "amocrm", "k-ensure")
    assert first_created is True and second_created is False
    assert first_id == second_id


def test_ensure_delivery_rejects_bad_status(conn):
    event_id, _ = insert_event(conn, "k-status", "{}")
    with pytest.raises(ValueError):
        ensure_delivery(conn, event_id, "1c", "k-status", status="SENT")


def test_fetch_ready_deliveries_only_pending(conn):
    a, _ = insert_event(conn, "k-a", "{}")
    b, _ = insert_event(conn, "k-b", "{}", destinations=("amocrm",))
    ready = fetch_ready_deliveries(conn)
    assert {d.event_id for d in ready} == {a, b}
    assert all(d.status == "PENDING" for d in ready)
    filtered = fetch_ready_deliveries(conn, destination="amocrm")
    assert [d.event_id for d in filtered] == [b]


def test_mark_delivery_in_flight_sets_status(conn):
    event_id, _ = insert_event(conn, "k-flight", "{}")
    row = list_deliveries(conn, event_id)[0]
    assert mark_delivery_in_flight(conn, row.id) is True
    fresh = get_delivery(conn, row.id)
    assert fresh.status == "IN_FLIGHT"
    assert fresh.attempt_count == 0  # counted when the delivery finishes
    # No longer offered to the ready fetch while in flight.
    assert fetch_ready_deliveries(conn) == []


def test_finish_delivery_records_attempts(conn):
    event_id, _ = insert_event(conn, "k-finish", "{}")
    row = list_deliveries(conn, event_id)[0]
    mark_delivery_in_flight(conn, row.id)
    assert finish_delivery(conn, row.id, "FAILED", attempt_count=3, last_error="boom")
    fresh = get_delivery(conn, row.id)
    assert fresh.status == "FAILED"
    assert fresh.attempt_count == 3
    assert fresh.last_error == "boom"


def test_set_delivery_status_delivered(conn):
    event_id, _ = insert_event(conn, "k-done", "{}")
    row = list_deliveries(conn, event_id)[0]
    assert set_delivery_status(conn, row.id, "DELIVERED", bump_attempt=True) is True
    fresh = get_delivery(conn, row.id)
    assert fresh.status == "DELIVERED" and fresh.attempt_count == 1


def test_set_delivery_status_failed_records_error(conn):
    event_id, _ = insert_event(conn, "k-fail", "{}")
    row = list_deliveries(conn, event_id)[0]
    set_delivery_status(conn, row.id, "FAILED", last_error="boom", bump_attempt=True)
    fresh = get_delivery(conn, row.id)
    assert fresh.status == "FAILED"
    assert fresh.last_error == "boom"


def test_set_delivery_status_rejects_unknown(conn):
    event_id, _ = insert_event(conn, "k-badst", "{}")
    row = list_deliveries(conn, event_id)[0]
    with pytest.raises(ValueError):
        set_delivery_status(conn, row.id, "PONDERING")


def test_delivery_status_isolation(conn):
    """Each delivery keeps an independent status/attempt count."""
    event_id, _ = insert_event(conn, "k-iso", "{}", destinations=("1c", "amocrm"))
    one_c = get_delivery_for(conn, event_id, "1c")
    amo = get_delivery_for(conn, event_id, "amocrm")
    set_delivery_status(conn, one_c.id, "DELIVERED", bump_attempt=True)
    fresh_amo = get_delivery(conn, amo.id)
    assert fresh_amo.status == "PENDING" and fresh_amo.attempt_count == 0


def test_delivery_statuses_constant():
    assert DELIVERY_STATUSES == ("PENDING", "IN_FLIGHT", "DELIVERED", "FAILED")


# --- transactionality ------------------------------------------------------

def test_transaction_rollback_removes_delivery(conn):
    event_id, _ = insert_event(conn, "k-rollback", "{}")
    with pytest.raises(RuntimeError), transaction(conn):
        conn.execute(
            "UPDATE deliveries SET status='DELIVERED' WHERE event_id=?", (event_id,)
        )
        raise RuntimeError("boom")
    assert get_delivery_for(conn, event_id, "1c").status == "PENDING"


# --- legacy migration ------------------------------------------------------

def test_migrate_links_validated_event_to_1c(db_path):
    conn = init_db(db_path)
    event_id, _ = insert_event(conn, "legacy-1", "{}")
    transition_status(conn, event_id, "RECEIVED", "VALIDATED")
    conn.execute("DELETE FROM deliveries WHERE event_id=?", (event_id,))

    created = migrate_legacy_deliveries(conn)
    assert created == 1
    row = get_delivery_for(conn, event_id, "1c")
    assert row is not None and row.status == "PENDING"
    conn.close()


def test_migrate_preserves_failed_state(db_path):
    conn = init_db(db_path)
    event_id, _ = insert_event(conn, "legacy-failed", "{}")
    transition_status(conn, event_id, "RECEIVED", "VALIDATED")
    transition_status(conn, event_id, "VALIDATED", "DISPATCHED")
    transition_status(conn, event_id, "DISPATCHED", "FAILED")
    conn.execute(
        "UPDATE events SET attempt_count=3, last_error='old' WHERE id=?", (event_id,)
    )
    conn.execute("DELETE FROM deliveries WHERE event_id=?", (event_id,))

    migrate_legacy_deliveries(conn)
    row = get_delivery_for(conn, event_id, "1c")
    assert row is not None
    assert row.status == "FAILED"
    assert row.attempt_count == 3
    assert row.last_error == "old"
    conn.close()


def test_migrate_is_idempotent(db_path):
    conn = init_db(db_path)
    event_id, _ = insert_event(conn, "legacy-idem", "{}")
    transition_status(conn, event_id, "RECEIVED", "VALIDATED")
    first = migrate_legacy_deliveries(conn)
    second = migrate_legacy_deliveries(conn)
    assert first == 0 and second == 0
    assert len(list_deliveries(conn, event_id)) == 1
    conn.close()


def test_migrate_runs_automatically_on_init(db_path):
    # Simulate an old DB: create schema, insert a VALIDATED event, drop the
    # deliveries table, then reopen through init_db().
    conn = init_db(db_path)
    event_id, _ = insert_event(conn, "auto-migrate", "{}")
    transition_status(conn, event_id, "RECEIVED", "VALIDATED")
    conn.execute("DROP TABLE deliveries")
    conn.close()

    reopened = init_db(db_path)
    row = get_delivery_for(reopened, event_id, "1c")
    assert row is not None and row.status == "PENDING"
    assert get_event(reopened, event_id).status == "VALIDATED"
    reopened.close()
