"""Pipeline orchestration: Ingress -> Validate -> Store -> Egress."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from .contract_validator import (
    delivery_destinations,
    parse_webhook_body,
    validate_webhook,
)
from .egress_dispatcher import OneCClient, dispatch_event
from .errors import ErrorShape
from .ingress_guard import authenticate_request
from .state_store import fetch_dispatchable, insert_event, transition_status


def process_webhook(
    raw_body: bytes,
    headers: Any,
    source_ip: str,
    conn,
    settings,
    now: datetime | None = None,
    *,
    require_signature: bool = True,
) -> tuple[int, dict | ErrorShape]:
    """Authenticate, validate and persist one webhook request.

    ``require_signature=False`` selects the 1C token ingress leg; either way the
    event is persisted together with its outbox delivery row(s) in one
    transaction (destination ``amocrm`` for 1C-originated status events).
    """
    now = now if now is not None else datetime.now(timezone.utc)

    auth = authenticate_request(
        raw_body, headers, source_ip, now, settings, require_signature=require_signature
    )
    if not auth.ok:
        return int(auth.status_code or 403), auth.error

    payload, perr = parse_webhook_body(raw_body)
    if perr is not None:
        return 422, perr

    domain, derr = validate_webhook(raw_body)
    if derr is not None:
        return 422, derr

    envelope = json.dumps(
        {
            "event_type": payload.event_type,
            "data": json.loads(domain.model_dump_json()),
        },
        ensure_ascii=False,
    )
    destinations = delivery_destinations(payload.event_type)
    event_id, created = insert_event(
        conn,
        payload.idempotency_key,
        envelope,
        status="RECEIVED",
        destinations=destinations,
    )
    if not created:
        return 200, {"status": "duplicate", "event_id": event_id}
    transition_status(conn, event_id, "RECEIVED", "VALIDATED")
    return 200, {
        "status": "validated",
        "event_id": event_id,
        "destinations": list(destinations),
    }


def run_delivery_pass(
    conn,
    settings,
    client: OneCClient,
    dlq_transport: Callable[[dict], Any],
    sleep_fn: Callable[[float], None],
    breaker=None,
    limit: int | None = None,
) -> dict[str, int]:
    """Deliver every dispatchable event once; return counters.

    Covers both outbox directions: the dispatcher routes ``deliveries`` rows to
    1C (``OneCClient.create_order``) or amoCRM (``AmoClient.update_lead_status``)
    based on ``delivery.destination``. ``breakers`` may be a single breaker (as
    historically) or a ``{destination: breaker}`` mapping.
    """
    if limit is None:
        limit = settings.dispatch_batch_limit
    delivered = 0
    failed = 0
    for event in fetch_dispatchable(
        conn,
        limit=limit,
        lease_seconds=settings.dispatch_lease_seconds,
    ):
        result = dispatch_event(
            conn, event, settings, client, dlq_transport, sleep_fn, breaker=breaker
        )
        if result == "DELIVERED":
            delivered += 1
        elif result == "FAILED":
            failed += 1
    return {"delivered": delivered, "failed": failed}
