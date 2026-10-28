import json
import re
import socket
from pathlib import Path
from typing import ClassVar

import pytest
from pydantic import ValidationError

from gateway.config import (
    BackoffConfig,
    CircuitBreakerConfig,
    RateLimitConfig,
    ReplayGuardConfig,
)
from gateway.contracts import (
    AmoLeadCreateSchema,
    OneCCounterpartySchema,
    OneCOrderCreateSchema,
    WebhookPayload,
)
from gateway.errors import make_error
from gateway.state_store import insert_event, transaction, transition_status

CODE_ROOT = Path(__file__).resolve().parent.parent

VALID_WEBHOOK = {
    "idempotency_key": "idem",
    "event_type": "order",
    "occurred_at": "2026-10-12T00:00:00+00:00",
    "nonce": "nonce",
    "data": {},
}
VALID_LEAD = {
    "amo_lead_id": 1,
    "name": "Lead",
    "budget": 1.0,
    "status_id": 1,
    "created_at": "2026-10-12T00:00:00+00:00",
}
VALID_ORDER = {
    "number": "SO-1",
    "date": "2026-10-12T00:00:00+00:00",
    "counterparty_ref": "cp-1",
    "items": [{"sku": "S", "quantity": 1, "price": 1.0}],
    "total": 1.0,
}
VALID_CP = {"inn": "1234567890", "name": "Romashka"}


def test_config_boundary_values():
    with pytest.raises(ValidationError):
        RateLimitConfig(algorithm="token_bucket", requests_per_window=0, window_seconds=60)
    with pytest.raises(ValidationError):
        BackoffConfig(
            base_seconds=0.1,
            multiplier=1.0,
            max_seconds=1.0,
            jitter_ratio=0.5,
            max_attempts=1,
        )
    with pytest.raises(ValidationError):
        ReplayGuardConfig(timestamp_window_seconds=0, nonce_journal_size=1)
    with pytest.raises(ValidationError):
        CircuitBreakerConfig(
            failure_threshold=0,
            cooldown_seconds=1.0,
            half_open_probe_interval=1.0,
        )


def test_four_schemas_valid():
    assert WebhookPayload.model_validate(VALID_WEBHOOK)
    assert AmoLeadCreateSchema.model_validate(VALID_LEAD)
    assert OneCOrderCreateSchema.model_validate(VALID_ORDER)
    assert OneCCounterpartySchema.model_validate(VALID_CP)


def test_four_schemas_invalid():
    cases = [
        (WebhookPayload, dict(VALID_WEBHOOK, extra=1)),
        (AmoLeadCreateSchema, dict(VALID_LEAD, extra=1)),
        (OneCOrderCreateSchema, dict(VALID_ORDER, extra=1)),
        (OneCCounterpartySchema, dict(VALID_CP, extra=1)),
    ]
    for model, data in cases:
        with pytest.raises(ValidationError):
            model.model_validate(data)


def test_all_five_transitions(conn):
    pairs = [
        ("RECEIVED", "VALIDATED"),
        ("VALIDATED", "DISPATCHED"),
        ("DISPATCHED", "DELIVERED"),
        ("DISPATCHED", "FAILED"),
        ("FAILED", "VALIDATED"),
    ]
    for frm, to in pairs:
        event_id, _ = insert_event(conn, f"vt-{frm}-{to}", "{}")
        with transaction(conn):
            conn.execute("UPDATE events SET status=? WHERE id=?", (frm, event_id))
        assert transition_status(conn, event_id, frm, to) is True


def test_response_codes_covered():
    from gateway.app import app

    responses = app.openapi()["paths"]["/webhook/amocrm"]["post"]["responses"]
    for code in ("200", "401", "403", "422", "429"):
        assert code in responses


def test_error_shape_five_fields_everywhere():
    err = make_error("schema", "m", "loc", "fix")
    assert set(err) == {"type", "message", "severity", "location", "how_to_fix"}
    with pytest.raises(ValueError):
        make_error("schema", "m", "loc", "")


def test_no_hardcoded_thresholds_in_modules():
    pattern = re.compile(
        r"(failure_threshold|cooldown_seconds|nonce_journal_size|max_attempts|requests_per_window)\s*=\s*[0-9]"
    )
    for name in ("ingress_guard.py", "state_store.py", "egress_dispatcher.py"):
        source = (CODE_ROOT / "gateway" / name).read_text(encoding="utf-8")
        assert not pattern.search(source), f"hardcoded threshold in {name}"


def test_strict_offline_blocks_sockets():
    with pytest.raises(RuntimeError):
        socket.socket().connect(("127.0.0.1", 9))


def test_openapi_spec_has_four_schemas():
    spec = json.loads((CODE_ROOT / "OPENAPI_SPEC.json").read_text(encoding="utf-8"))
    schemas = spec["components"]["schemas"]
    for name in (
        "WebhookPayload",
        "AmoLeadCreateSchema",
        "OneCOrderCreateSchema",
        "OneCCounterpartySchema",
    ):
        assert name in schemas


def test_documentation_files_present():
    for name in (
        "README.md",
        "OPENAPI_SPEC.json",
        "DEPLOYMENT_GUIDE.md",
        "ARCHITECTURE_REPORT.md",
        "docs/MAINTENANCE.md",
        "docs/INTEGRATION_MANUAL.md",
    ):
        assert (CODE_ROOT / name).is_file(), f"missing {name}"


def test_health_endpoint_returns_ok():
    from fastapi.testclient import TestClient

    from gateway.app import app

    response = TestClient(app).get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "amocrm-1c-gateway"}


def test_egress_worker_module_imports_cleanly():
    import scripts.egress_worker as worker

    assert hasattr(worker, "run_worker_pass")
    assert hasattr(worker, "fetch_dispatchable")


def test_build_onec_transport_forwards_custom_headers(monkeypatch):
    """D4: the 1C transport must forward caller headers (X-Idempotency-Key)."""
    import scripts.egress_worker as worker

    captured: dict = {}

    class _FakeResponse:
        status_code = 200
        content = b'{"ok": true}'
        text = '{"ok": true}'
        headers: ClassVar[dict[str, str]] = {"Retry-After": "7"}

        def json(self):
            return {"ok": True}

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            captured["client_kwargs"] = kwargs

        def request(self, method, url, **kwargs):
            captured["method"] = method
            captured["url"] = url
            captured["json"] = kwargs.get("json")
            captured["headers"] = kwargs.get("headers")
            captured["timeout"] = kwargs.get("timeout")
            return _FakeResponse()

    monkeypatch.setattr(worker.httpx, "Client", _FakeClient)

    transport = worker.build_onec_transport(7.5)
    response = transport(
        "https://onec.example/odata/standard.odata",
        "POST",
        {"name": "SO-1"},
        headers={"X-Idempotency-Key": "idem-42"},
    )

    assert captured["method"] == "POST"
    assert captured["url"] == "https://onec.example/odata/standard.odata"
    assert captured["json"] == {"name": "SO-1"}
    assert captured["headers"] == {"X-Idempotency-Key": "idem-42"}
    assert captured["timeout"] == 7.5
    assert response["status_code"] == 200
    assert response["body"] == {"ok": True}
    assert response["headers"] == {"Retry-After": "7"}


def test_importing_app_does_not_create_db(monkeypatch, tmp_path):
    db_file = tmp_path / "should-not-exist.db"
    monkeypatch.setenv("GATEWAY_DB_PATH", str(db_file))

    import importlib

    import gateway.app as app_module

    importlib.reload(app_module)
    assert not db_file.exists()
    assert app_module.app.state.db_path == str(db_file)
