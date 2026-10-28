"""SQLite WAL state store: idempotency, deduplication, atomic state machine.

Besides the ``events`` journal the store maintains a ``deliveries`` table —
one row per (event, destination) pair implementing the Transactional Outbox
pattern: a delivery is created atomically with its event and is driven through
PENDING -> IN_FLIGHT -> DELIVERED/FAILED by the egress dispatcher.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TypeVar

from .config import (
    DEFAULT_BUSY_TIMEOUT_MS,
    DEFAULT_DB_CONNECT_TIMEOUT_SECONDS,
    DEFAULT_DISPATCH_BATCH_LIMIT,
    DEFAULT_LOCK_RETRY_ATTEMPTS,
    DEFAULT_LOCK_RETRY_BASE_SECONDS,
    DEFAULT_LOCK_RETRY_MAX_SECONDS,
)
from .errors import StorageError

STATUSES = ("RECEIVED", "VALIDATED", "DISPATCHED", "DELIVERED", "FAILED")

ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "RECEIVED": {"VALIDATED"},
    "VALIDATED": {"DISPATCHED"},
    # DISPATCHED -> VALIDATED is the crash/lease reaper path: a worker that
    # died after claiming an event must not strand it forever.
    "DISPATCHED": {"DELIVERED", "FAILED", "VALIDATED"},
    "DELIVERED": set(),
    "FAILED": {"VALIDATED"},
}

DESTINATIONS = ("1c", "amocrm")
DELIVERY_STATUSES = ("PENDING", "IN_FLIGHT", "DELIVERED", "FAILED")

DDL_EVENTS = """
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key TEXT NOT NULL UNIQUE,
    payload TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('RECEIVED','VALIDATED','DISPATCHED','DELIVERED','FAILED')
    ),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""

DDL_INDEX = "CREATE INDEX IF NOT EXISTS idx_events_status ON events(status);"

DDL_DELIVERIES = """
CREATE TABLE IF NOT EXISTS deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL REFERENCES events(id),
    destination TEXT NOT NULL CHECK (destination IN ('1c', 'amocrm')),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'IN_FLIGHT', 'DELIVERED', 'FAILED')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    next_attempt_at TEXT,
    idempotency_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(event_id, destination)
);
"""

DDL_DELIVERIES_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_deliveries_ready "
    "ON deliveries(destination, status, next_attempt_at);"
)


@dataclass
class EventRow:
    id: int
    idempotency_key: str
    payload: str
    status: str
    attempt_count: int
    last_error: str | None
    created_at: str
    updated_at: str


@dataclass
class DeliveryRow:
    id: int
    event_id: int
    destination: str
    status: str
    attempt_count: int
    last_error: str | None
    next_attempt_at: str | None
    idempotency_key: str
    created_at: str
    updated_at: str


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


T = TypeVar("T")

# ``SQLITE_BUSY`` ("database is locked") is a *contention* signal, not a
# defect: ``PRAGMA busy_timeout`` already waited for the write lock. Under
# heavy concurrency (many writers on a small host) that budget can be
# exhausted and the caller would see a spurious failure. Retrying the lock
# acquisition turns such a failure into a short wait — the transaction has not
# started yet, so nothing is ever left half-written.
LOCK_RETRY_ATTEMPTS = DEFAULT_LOCK_RETRY_ATTEMPTS
LOCK_RETRY_BASE_SECONDS = DEFAULT_LOCK_RETRY_BASE_SECONDS
LOCK_RETRY_MAX_SECONDS = DEFAULT_LOCK_RETRY_MAX_SECONDS

# The three pragmas are part of the storage contract, not tuning knobs:
#   * WAL lets readers and the single writer progress concurrently;
#   * busy_timeout makes lock contention wait instead of failing immediately;
#   * NORMAL is durable across process crashes (only an OS crash may lose the
#     last transaction) and avoids an fsync per commit.
REQUIRED_JOURNAL_MODE = "wal"
REQUIRED_SYNCHRONOUS = "normal"


def _is_locked(exc: sqlite3.OperationalError) -> bool:
    message = str(exc).lower()
    return "locked" in message or "busy" in message


def _retry_on_lock(operation: Callable[[], T]) -> T:
    """Run ``operation``, retrying while SQLite reports a locked database."""
    delay = LOCK_RETRY_BASE_SECONDS
    for attempt in range(LOCK_RETRY_ATTEMPTS):
        try:
            return operation()
        except sqlite3.OperationalError as exc:
            if not _is_locked(exc) or attempt == LOCK_RETRY_ATTEMPTS - 1:
                raise
            time.sleep(delay)
            delay = min(delay * 2, LOCK_RETRY_MAX_SECONDS)
    raise AssertionError("unreachable")  # pragma: no cover


def _apply_connection_pragmas(conn: sqlite3.Connection, busy_timeout_ms: int) -> None:
    """Apply the mandatory WAL/busy_timeout/synchronous contract.

    Fails closed: if the filesystem cannot provide WAL (for example a network
    share without proper locking) the process must not start with a silently
    degraded journal that can corrupt the outbox under concurrency.
    """
    conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)};")
    mode_row = _retry_on_lock(
        lambda: conn.execute("PRAGMA journal_mode=WAL;").fetchone()
    )
    mode = str(mode_row[0]).lower() if mode_row is not None else ""
    if mode != REQUIRED_JOURNAL_MODE:
        raise StorageError(
            f"SQLite refused WAL journal mode (got {mode or 'unknown'}); "
            "the outbox requires a local filesystem with WAL support"
        )
    conn.execute("PRAGMA synchronous=NORMAL;")
    synchronous = str(
        conn.execute("PRAGMA synchronous;").fetchone()[0]
    ).lower()
    # ``PRAGMA synchronous`` returns the numeric level (1 == NORMAL); accept
    # both the numeric and the symbolic form for forward compatibility.
    if synchronous not in (REQUIRED_SYNCHRONOUS, "1"):
        raise StorageError(
            f"SQLite refused synchronous=NORMAL (got {synchronous})"
        )
    conn.execute("PRAGMA foreign_keys=ON;")


def connect(
    db_path: str,
    *,
    busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
    timeout: float = DEFAULT_DB_CONNECT_TIMEOUT_SECONDS,
) -> sqlite3.Connection:
    conn = sqlite3.connect(
        db_path, timeout=timeout, isolation_level=None, check_same_thread=False
    )
    try:
        conn.row_factory = sqlite3.Row
        _apply_connection_pragmas(conn, busy_timeout_ms)
    except Exception:
        conn.close()
        raise
    return conn


def init_db(
    db_path: str,
    *,
    busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
    timeout: float = DEFAULT_DB_CONNECT_TIMEOUT_SECONDS,
) -> sqlite3.Connection:
    conn = connect(db_path, busy_timeout_ms=busy_timeout_ms, timeout=timeout)
    try:
        for statement in (
            DDL_EVENTS,
            DDL_INDEX,
            DDL_DELIVERIES,
            DDL_DELIVERIES_INDEX,
        ):
            _retry_on_lock(lambda sql=statement: conn.execute(sql))
        migrate_legacy_deliveries(conn)
    except Exception:
        conn.close()
        raise
    return conn


def migrate_legacy_deliveries(conn: sqlite3.Connection) -> int:
    """Link pre-existing VALIDATED/FAILED events to a default 1C delivery.

    Events persisted before the ``deliveries`` table existed (or created by an
    older gateway build) get one default delivery row addressed to 1C so the
    outbox can drain them. FAILED events keep the FAILED status (and their
    attempt count / error) so operators can replay them. Idempotent: re-running
    never duplicates rows thanks to ``UNIQUE(event_id, destination)``.
    """
    now = _now_iso()
    with transaction(conn):
        cursor = conn.execute(
            "INSERT INTO deliveries (event_id, destination, status, attempt_count, "
            "last_error, next_attempt_at, idempotency_key, created_at, updated_at) "
            "SELECT e.id, '1c', CASE WHEN e.status = 'FAILED' THEN 'FAILED' ELSE 'PENDING' END, "
            "CASE WHEN e.status = 'FAILED' THEN e.attempt_count ELSE 0 END, "
            "e.last_error, NULL, e.idempotency_key, e.created_at, ? "
            "FROM events e "
            "WHERE e.status IN ('VALIDATED', 'FAILED') "
            "AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.event_id = e.id)",
            (now,),
        )
        return cursor.rowcount


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    # The write lock is taken up-front (BEGIN IMMEDIATE) so two writers never
    # deadlock on an upgrade; the lock acquisition itself may still lose the
    # busy-timeout race under heavy concurrency — retry it, then give up.
    _retry_on_lock(lambda: conn.execute("BEGIN IMMEDIATE"))
    try:
        yield conn
        _retry_on_lock(lambda: conn.execute("COMMIT"))
    except Exception:
        # Best-effort cleanup: the original exception is the one that matters.
        with suppress(sqlite3.Error):  # pragma: no cover
            conn.execute("ROLLBACK")
        raise


def _row_to_event(row: sqlite3.Row) -> EventRow:
    return EventRow(
        id=row["id"],
        idempotency_key=row["idempotency_key"],
        payload=row["payload"],
        status=row["status"],
        attempt_count=row["attempt_count"],
        last_error=row["last_error"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def get_event(conn: sqlite3.Connection, event_id: int) -> EventRow | None:
    row = conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
    return _row_to_event(row) if row is not None else None


def _insert_delivery_row(
    conn: sqlite3.Connection,
    event_id: int,
    destination: str,
    idempotency_key: str,
    *,
    status: str = "PENDING",
    now: str,
) -> tuple[int | None, bool]:
    """Insert one delivery row (UNIQUE(event_id, destination)); no transaction."""
    cursor = conn.execute(
        "INSERT INTO deliveries (event_id, destination, status, attempt_count, "
        "last_error, next_attempt_at, idempotency_key, created_at, updated_at) "
        "VALUES (?, ?, ?, 0, NULL, NULL, ?, ?, ?) "
        "ON CONFLICT(event_id, destination) DO NOTHING",
        (event_id, destination, status, idempotency_key, now, now),
    )
    if cursor.rowcount == 1:
        return cursor.lastrowid, True
    row = conn.execute(
        "SELECT id FROM deliveries WHERE event_id=? AND destination=?",
        (event_id, destination),
    ).fetchone()
    return (row["id"] if row is not None else None), False


def ensure_delivery(
    conn: sqlite3.Connection,
    event_id: int,
    destination: str,
    idempotency_key: str,
    *,
    status: str = "PENDING",
) -> tuple[int | None, bool]:
    """Create (or fetch) the outbox delivery for one event destination."""
    if destination not in DESTINATIONS:
        raise ValueError(f"destination must be one of {DESTINATIONS}, got {destination!r}")
    if status not in DELIVERY_STATUSES:
        raise ValueError(f"status must be one of {DELIVERY_STATUSES}, got {status!r}")
    now = _now_iso()
    with transaction(conn):
        return _insert_delivery_row(
            conn, event_id, destination, idempotency_key, status=status, now=now
        )


def insert_event(
    conn: sqlite3.Connection,
    idempotency_key: str,
    payload: str,
    status: str = "RECEIVED",
    destinations: Iterable[str] = ("1c",),
) -> tuple[int | None, bool]:
    """Insert an event together with its delivery rows in ONE transaction.

    ``destinations`` controls which outbox rows are created atomically with the
    event (default: the 1C destination, i.e. the original amoCRM -> 1C flow).
    """
    destinations = tuple(destinations) or ("1c",)
    for destination in destinations:
        if destination not in DESTINATIONS:
            raise ValueError(
                f"destination must be one of {DESTINATIONS}, got {destination!r}"
            )
    now = _now_iso()
    with transaction(conn):
        cursor = conn.execute(
            "INSERT INTO events "
            "(idempotency_key, payload, status, attempt_count, created_at, updated_at) "
            "VALUES (?, ?, ?, 0, ?, ?) "
            "ON CONFLICT(idempotency_key) DO NOTHING",
            (idempotency_key, payload, status, now, now),
        )
        created = cursor.rowcount == 1
        if not created:
            row = conn.execute(
                "SELECT id FROM events WHERE idempotency_key=?", (idempotency_key,)
            ).fetchone()
            return (row["id"] if row is not None else None), False
        event_id = cursor.lastrowid
        for destination in destinations:
            _insert_delivery_row(conn, event_id, destination, idempotency_key, now=now)
        return event_id, True


def transition_status(
    conn: sqlite3.Connection,
    event_id: int,
    from_status: str,
    to_status: str,
) -> bool:
    if to_status not in ALLOWED_TRANSITIONS.get(from_status, set()):
        return False
    with transaction(conn):
        cursor = conn.execute(
            "UPDATE events SET status=?, updated_at=? WHERE id=? AND status=?",
            (to_status, _now_iso(), event_id, from_status),
        )
        return cursor.rowcount == 1


def record_attempt(
    conn: sqlite3.Connection,
    event_id: int,
    attempt_count: int,
    last_error: str | None = None,
) -> None:
    with transaction(conn):
        conn.execute(
            "UPDATE events SET attempt_count=?, last_error=?, updated_at=? WHERE id=?",
            (attempt_count, last_error, _now_iso(), event_id),
        )


def fetch_dispatchable(
    conn: sqlite3.Connection,
    limit: int = DEFAULT_DISPATCH_BATCH_LIMIT,
    *,
    lease_seconds: float | None = None,
    now: datetime | None = None,
) -> list[EventRow]:
    """Return events the dispatcher may attempt.

    ``VALIDATED`` and ``FAILED`` rows are always eligible. When
    ``lease_seconds`` is provided, a ``DISPATCHED`` row whose lease expired
    (its worker crashed mid-delivery) is also returned so it can be reclaimed
    instead of being stranded forever.
    """
    if lease_seconds is None:
        rows = conn.execute(
            "SELECT * FROM events WHERE status IN ('VALIDATED','FAILED') "
            "ORDER BY created_at, id LIMIT ?",
            (limit,),
        ).fetchall()
        return [_row_to_event(row) for row in rows]

    cutoff = (
        (now if now is not None else datetime.now(timezone.utc))
        - timedelta(seconds=lease_seconds)
    ).isoformat()
    rows = conn.execute(
        "SELECT * FROM events WHERE status IN ('VALIDATED','FAILED') "
        "OR (status='DISPATCHED' AND updated_at <= ?) "
        "ORDER BY created_at, id LIMIT ?",
        (cutoff, limit),
    ).fetchall()
    return [_row_to_event(row) for row in rows]


def resend_failed(conn: sqlite3.Connection, event_id: int) -> bool:
    """Return a FAILED event to the delivery queue without re-validation."""
    return transition_status(conn, event_id, "FAILED", "VALIDATED")


def _reclaim_stale_dispatch(
    conn: sqlite3.Connection, event_id: int, lease_seconds: float
) -> bool:
    """Atomically move an expired ``DISPATCHED`` lease back to ``VALIDATED``.

    The ``updated_at <= cutoff`` predicate is evaluated by SQLite while holding
    the write lock, so exactly one worker wins the reclaim even when several
    workers poll the same stale row simultaneously.
    """
    cutoff = (
        datetime.now(timezone.utc) - timedelta(seconds=lease_seconds)
    ).isoformat()
    with transaction(conn):
        cursor = conn.execute(
            "UPDATE events SET status='VALIDATED', updated_at=? "
            "WHERE id=? AND status='DISPATCHED' AND updated_at <= ?",
            (_now_iso(), event_id, cutoff),
        )
        return cursor.rowcount == 1


def claim_event(
    conn: sqlite3.Connection,
    event_id: int,
    *,
    lease_seconds: float,
) -> bool:
    """Atomically claim one event for delivery.

    Returns ``True`` only for the worker that won the conditional
    ``VALIDATED -> DISPATCHED`` update. A concurrent worker that observed the
    same row gets ``False`` and must skip it: this is what makes the outbox
    at-most-once *per attempt* even when several worker processes poll the same
    SQLite file.
    """
    row = get_event(conn, event_id)
    if row is None:
        return False

    if row.status == "FAILED":
        if not resend_failed(conn, event_id):
            # Another worker moved it first: re-read and decide from the new
            # state instead of acting on the stale snapshot.
            row = get_event(conn, event_id)
            if row is None:
                return False
        else:
            row = get_event(conn, event_id)
            if row is None:
                return False

    if row.status == "DISPATCHED":
        if not _reclaim_stale_dispatch(conn, event_id, lease_seconds):
            return False
        row = get_event(conn, event_id)
        if row is None:
            return False

    if row.status != "VALIDATED":
        return False
    return transition_status(conn, event_id, "VALIDATED", "DISPATCHED")


# ---------------------------------------------------------------------------
# deliveries (Transactional Outbox)
# ---------------------------------------------------------------------------


def _row_to_delivery(row: sqlite3.Row) -> DeliveryRow:
    return DeliveryRow(
        id=row["id"],
        event_id=row["event_id"],
        destination=row["destination"],
        status=row["status"],
        attempt_count=row["attempt_count"],
        last_error=row["last_error"],
        next_attempt_at=row["next_attempt_at"],
        idempotency_key=row["idempotency_key"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def get_delivery(conn: sqlite3.Connection, delivery_id: int) -> DeliveryRow | None:
    row = conn.execute("SELECT * FROM deliveries WHERE id=?", (delivery_id,)).fetchone()
    return _row_to_delivery(row) if row is not None else None


def get_delivery_for(
    conn: sqlite3.Connection, event_id: int, destination: str
) -> DeliveryRow | None:
    row = conn.execute(
        "SELECT * FROM deliveries WHERE event_id=? AND destination=?",
        (event_id, destination),
    ).fetchone()
    return _row_to_delivery(row) if row is not None else None


def list_deliveries(conn: sqlite3.Connection, event_id: int) -> list[DeliveryRow]:
    rows = conn.execute(
        "SELECT * FROM deliveries WHERE event_id=? ORDER BY id", (event_id,)
    ).fetchall()
    return [_row_to_delivery(row) for row in rows]


def set_delivery_status(
    conn: sqlite3.Connection,
    delivery_id: int,
    status: str,
    *,
    last_error: str | None = None,
    next_attempt_at: str | None = None,
    bump_attempt: bool = False,
) -> bool:
    """Move one delivery to ``status`` (optionally counting the attempt)."""
    if status not in DELIVERY_STATUSES:
        raise ValueError(f"status must be one of {DELIVERY_STATUSES}, got {status!r}")
    with transaction(conn):
        cursor = conn.execute(
            "UPDATE deliveries SET status=?, last_error=?, next_attempt_at=?, "
            "attempt_count=attempt_count + ?, updated_at=? WHERE id=?",
            (
                status,
                last_error,
                next_attempt_at,
                1 if bump_attempt else 0,
                _now_iso(),
                delivery_id,
            ),
        )
        return cursor.rowcount == 1


def mark_delivery_in_flight(conn: sqlite3.Connection, delivery_id: int) -> bool:
    """PENDING/IN_FLIGHT/FAILED -> IN_FLIGHT (attempt count set on completion)."""
    with transaction(conn):
        cursor = conn.execute(
            "UPDATE deliveries SET status='IN_FLIGHT', next_attempt_at=NULL, "
            "updated_at=? "
            "WHERE id=? AND status IN ('PENDING', 'IN_FLIGHT', 'FAILED')",
            (_now_iso(), delivery_id),
        )
        return cursor.rowcount == 1


def finish_delivery(
    conn: sqlite3.Connection,
    delivery_id: int,
    status: str,
    *,
    attempt_count: int,
    last_error: str | None = None,
) -> bool:
    """Finish a delivery with the total number of transport attempts made."""
    if status not in DELIVERY_STATUSES:
        raise ValueError(f"status must be one of {DELIVERY_STATUSES}, got {status!r}")
    with transaction(conn):
        cursor = conn.execute(
            "UPDATE deliveries SET status=?, last_error=?, next_attempt_at=NULL, "
            "attempt_count=?, updated_at=? WHERE id=?",
            (status, last_error, int(attempt_count), _now_iso(), delivery_id),
        )
        return cursor.rowcount == 1


def fetch_ready_deliveries(
    conn: sqlite3.Connection,
    destination: str | None = None,
    limit: int = DEFAULT_DISPATCH_BATCH_LIMIT,
) -> list[DeliveryRow]:
    """Pending outbox rows for a destination, oldest first."""
    if destination is None:
        rows = conn.execute(
            "SELECT * FROM deliveries WHERE status='PENDING' "
            "ORDER BY created_at, id LIMIT ?",
            (limit,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM deliveries WHERE status='PENDING' AND destination=? "
            "ORDER BY created_at, id LIMIT ?",
            (destination, limit),
        ).fetchall()
    return [_row_to_delivery(row) for row in rows]
