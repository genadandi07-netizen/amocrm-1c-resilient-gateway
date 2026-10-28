import json

import pytest
from pydantic import ValidationError

from gateway.contract_validator import parse_webhook_body, validate_model, validate_webhook
from gateway.contracts import (
    AmoLeadCreateSchema,
    OneCCounterpartySchema,
    OneCOrderCreateSchema,
    WebhookPayload,
)

VALID_WEBHOOK = {
    "idempotency_key": "idem-1",
    "event_type": "order",
    "occurred_at": "2026-10-12T00:00:00+00:00",
    "nonce": "nonce-1",
    "data": {"order": True},
}

VALID_LEAD = {
    "amo_lead_id": 1,
    "name": "Lead",
    "budget": 10.0,
    "status_id": 2,
    "created_at": "2026-10-12T00:00:00+00:00",
}

VALID_ORDER = {
    "number": "SO-1",
    "date": "2026-10-12T00:00:00+00:00",
    "counterparty_ref": "cp-1",
    "items": [{"sku": "SKU-1", "quantity": 2, "price": 5.0}],
    "total": 10.0,
}

VALID_CP = {"inn": "1234567890", "name": "Romashka"}


def _raw(payload):
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _webhook(event_type="order", data=None):
    payload = dict(VALID_WEBHOOK)
    payload["event_type"] = event_type
    payload["data"] = data if data is not None else {}
    return payload


# --- WebhookPayload ------------------------------------------------------

def test_webhook_payload_valid():
    assert WebhookPayload.model_validate(VALID_WEBHOOK).idempotency_key == "idem-1"


def test_webhook_payload_extra_field_rejected():
    bad = dict(VALID_WEBHOOK, extra=1)
    with pytest.raises(ValidationError):
        WebhookPayload.model_validate(bad)


def test_webhook_payload_bad_date_rejected():
    bad = dict(VALID_WEBHOOK, occurred_at="not-a-date")
    with pytest.raises(ValidationError):
        WebhookPayload.model_validate(bad)


def test_webhook_payload_missing_field_rejected():
    bad = dict(VALID_WEBHOOK)
    del bad["nonce"]
    with pytest.raises(ValidationError):
        WebhookPayload.model_validate(bad)


def test_webhook_payload_json_serializable():
    dumped = WebhookPayload.model_validate(VALID_WEBHOOK).model_dump(mode="json")
    assert isinstance(dumped["occurred_at"], str)
    assert json.loads(json.dumps(dumped))


# --- AmoLeadCreateSchema --------------------------------------------------

def test_amo_lead_valid():
    assert AmoLeadCreateSchema.model_validate(VALID_LEAD).tags == []


def test_amo_lead_extra_field_rejected():
    with pytest.raises(ValidationError):
        AmoLeadCreateSchema.model_validate(dict(VALID_LEAD, extra=1))


def test_amo_lead_negative_budget_rejected():
    with pytest.raises(ValidationError):
        AmoLeadCreateSchema.model_validate(dict(VALID_LEAD, budget=-1))


def test_amo_lead_empty_name_rejected():
    with pytest.raises(ValidationError):
        AmoLeadCreateSchema.model_validate(dict(VALID_LEAD, name=""))


# --- OneCOrderCreateSchema -----------------------------------------------

def test_onec_order_valid():
    assert OneCOrderCreateSchema.model_validate(VALID_ORDER).number == "SO-1"


def test_onec_order_empty_items_rejected():
    with pytest.raises(ValidationError):
        OneCOrderCreateSchema.model_validate(dict(VALID_ORDER, items=[]))


def test_onec_order_zero_quantity_rejected():
    bad = dict(VALID_ORDER)
    bad["items"] = [{"sku": "S", "quantity": 0, "price": 1.0}]
    with pytest.raises(ValidationError):
        OneCOrderCreateSchema.model_validate(bad)


def test_onec_order_negative_price_rejected():
    bad = dict(VALID_ORDER)
    bad["items"] = [{"sku": "S", "quantity": 1, "price": -1.0}]
    with pytest.raises(ValidationError):
        OneCOrderCreateSchema.model_validate(bad)


def test_onec_order_extra_field_rejected():
    with pytest.raises(ValidationError):
        OneCOrderCreateSchema.model_validate(dict(VALID_ORDER, extra=1))


# --- OneCCounterpartySchema ----------------------------------------------

def test_counterparty_valid_inn10():
    assert OneCCounterpartySchema.model_validate(VALID_CP).inn == "1234567890"


def test_counterparty_valid_inn12():
    cp = {"inn": "123456789012", "name": "Romashka"}
    assert OneCCounterpartySchema.model_validate(cp).inn == "123456789012"


def test_counterparty_bad_inn_rejected():
    with pytest.raises(ValidationError):
        OneCCounterpartySchema.model_validate({"inn": "123456789", "name": "X"})


def test_counterparty_extra_field_rejected():
    with pytest.raises(ValidationError):
        OneCCounterpartySchema.model_validate(dict(VALID_CP, extra=1))


def test_counterparty_json_serializable():
    dumped = OneCCounterpartySchema.model_validate(VALID_CP).model_dump(mode="json")
    assert json.loads(json.dumps(dumped))["inn"] == "1234567890"


# --- parse_webhook_body --------------------------------------------------

def test_parse_webhook_body_valid():
    payload, err = parse_webhook_body(_raw(VALID_WEBHOOK))
    assert payload is not None and err is None


def test_parse_webhook_body_empty():
    payload, err = parse_webhook_body(b"")
    assert payload is None and err["type"] == "semantic"


def test_parse_webhook_body_broken_json():
    payload, err = parse_webhook_body(b"{broken")
    assert payload is None
    assert set(err) == {"type", "message", "severity", "location", "how_to_fix"}


def test_parse_webhook_body_non_object():
    payload, err = parse_webhook_body(b"123")
    assert payload is None and err["location"] == "body"


def test_parse_webhook_body_missing_field():
    bad = dict(VALID_WEBHOOK)
    del bad["event_type"]
    payload, err = parse_webhook_body(_raw(bad))
    assert payload is None and err["type"] == "schema"


# --- validate_model ------------------------------------------------------

def test_validate_model_valid():
    model, err = validate_model(WebhookPayload, VALID_WEBHOOK)
    assert model is not None and err is None


def test_validate_model_error_translated():
    model, err = validate_model(WebhookPayload, dict(VALID_WEBHOOK, extra=1))
    assert model is None and err["type"] == "schema"


def test_validate_model_error_location():
    _, err = validate_model(WebhookPayload, dict(VALID_WEBHOOK, occurred_at="nope"))
    assert err["location"].startswith("payload.")


def test_validate_model_how_to_fix_non_empty():
    _, err = validate_model(WebhookPayload, dict(VALID_WEBHOOK, extra=1))
    assert err["how_to_fix"]


# --- validate_webhook ----------------------------------------------------

def test_validate_webhook_lead():
    model, err = validate_webhook(_raw(_webhook("lead", VALID_LEAD)))
    assert isinstance(model, AmoLeadCreateSchema) and err is None


def test_validate_webhook_order():
    model, err = validate_webhook(_raw(_webhook("order", VALID_ORDER)))
    assert isinstance(model, OneCOrderCreateSchema) and err is None


def test_validate_webhook_counterparty():
    model, err = validate_webhook(_raw(_webhook("counterparty", VALID_CP)))
    assert isinstance(model, OneCCounterpartySchema) and err is None


def test_validate_webhook_unknown_event_type():
    model, err = validate_webhook(_raw(_webhook("widget", {})))
    assert model is None and err["type"] == "semantic"
