"""Generate OPENAPI_SPEC.json from the FastAPI app and Pydantic models."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pydantic import TypeAdapter  # noqa: E402

from gateway.app import app  # noqa: E402
from gateway.contracts import (  # noqa: E402
    AmoLeadCreateSchema,
    OneCCounterpartySchema,
    OneCOrderCreateSchema,
    OneCOrderStatusSchema,
    WebhookPayload,
)
from gateway.errors import ErrorShape  # noqa: E402

OUTPUT = ROOT / "OPENAPI_SPEC.json"

MODELS = (
    WebhookPayload,
    AmoLeadCreateSchema,
    OneCOrderCreateSchema,
    OneCCounterpartySchema,
    OneCOrderStatusSchema,
)

# Authentication headers accepted by both ingress legs. They are attached to
# the generated paths because the raw-body FastAPI handlers read the request
# directly (FastAPI cannot infer them from a signature).
AUTH_HEADER_PARAMETERS = [
    {
        "name": "Content-Type",
        "in": "header",
        "required": True,
        "description": "Must be application/json (charset allowed).",
        "schema": {"type": "string", "default": "application/json"},
    },
    {
        "name": "X-Signature",
        "in": "header",
        "required": False,
        "description": (
            "HMAC-SHA256 hex digest of the raw request body (prefix `sha256=` "
            "allowed). Mandatory for POST /webhook/amocrm; optional on "
            "POST /webhook/onec, where it selects the HMAC auth path."
        ),
        "schema": {"type": "string"},
    },
    {
        "name": "X-Api-Token",
        "in": "header",
        "required": False,
        "description": (
            "1C API token (GATEWAY_ONEC_API_TOKEN). Alternative to "
            "`Authorization: Bearer` on POST /webhook/onec when X-Signature "
            "is absent."
        ),
        "schema": {"type": "string"},
    },
    {
        "name": "Authorization",
        "in": "header",
        "required": False,
        "description": (
            "`Bearer <GATEWAY_ONEC_API_TOKEN>` on POST /webhook/onec. "
            "Equivalent to the X-Api-Token header."
        ),
        "schema": {"type": "string"},
    },
    {
        "name": "X-Timestamp",
        "in": "header",
        "required": True,
        "description": "ISO-8601 (with zone) or epoch seconds within the replay window.",
        "schema": {"type": "string"},
    },
    {
        "name": "X-Nonce",
        "in": "header",
        "required": True,
        "description": "Unique per request; reused values are rejected as replays.",
        "schema": {"type": "string"},
    },
]

# Envelope shared by both legs; only `data` differs per contract.
_ENVELOPE_FIELDS = {
    "idempotency_key": {"type": "string", "minLength": 1},
    "event_type": {"type": "string", "minLength": 1},
    "occurred_at": {"type": "string", "format": "date-time"},
    "nonce": {"type": "string", "minLength": 1},
}


def _envelope_schema(
    data_schema: dict, event_types: list[str], title: str
) -> dict:
    return {
        "title": title,
        "type": "object",
        "additionalProperties": False,
        "required": [
            "idempotency_key",
            "event_type",
            "occurred_at",
            "nonce",
            "data",
        ],
        "properties": {
            **_ENVELOPE_FIELDS,
            "event_type": {"type": "string", "enum": event_types},
            "data": data_schema,
        },
    }


def _enrich_paths(spec: dict) -> None:
    """Attach auth headers and request bodies that FastAPI cannot infer."""
    paths = spec.setdefault("paths", {})

    def _error_body(response: dict) -> None:
        response.setdefault("content", {})["application/json"] = {
            "schema": {"$ref": "#/components/schemas/ErrorShape"}
        }

    for path in ("/webhook/amocrm", "/webhook/onec"):
        operation = paths.get(path, {}).get("post")
        if operation is None:
            continue
        operation["parameters"] = [dict(param) for param in AUTH_HEADER_PARAMETERS]
        operation["tags"] = ["webhooks"]
        responses = operation.setdefault("responses", {})
        accepted = responses.setdefault("200", {"description": "Accepted"})
        accepted["description"] = "Event accepted (or duplicate)"
        accepted.setdefault("content", {})["application/json"] = {
            "schema": {
                "type": "object",
                "properties": {
                    "status": {"type": "string", "enum": ["validated", "duplicate"]},
                    "event_id": {"type": "integer"},
                    "destinations": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["1c", "amocrm"]},
                    },
                },
                "required": ["status", "event_id"],
            }
        }
        for code in ("401", "403", "422", "429"):
            response = responses.setdefault(code, {"description": ""})
            _error_body(response)

    amocrm = paths.get("/webhook/amocrm", {}).get("post")
    if amocrm is not None:
        amocrm["summary"] = "Inbound amoCRM relay webhook (amoCRM -> 1C)"
        amocrm["requestBody"] = {
            "required": True,
            "content": {
                "application/json": {
                    "schema": _envelope_schema(
                        {
                            "oneOf": [
                                {"$ref": "#/components/schemas/OneCOrderCreateSchema"},
                                {"$ref": "#/components/schemas/OneCCounterpartySchema"},
                                {"$ref": "#/components/schemas/AmoLeadCreateSchema"},
                            ]
                        },
                        ["lead", "order", "counterparty"],
                        "AmoCRMWebhookEnvelope",
                    ),
                    "examples": {
                        "order": {
                            "summary": "Lead budget -> 1C customer order",
                            "value": {
                                "idempotency_key": "amo-lead-123:2026-10-16T10:00:00+00:00",
                                "event_type": "order",
                                "occurred_at": "2026-10-16T10:00:00+00:00",
                                "nonce": "6f1c0e2a-8f52-4a7c-9e2b-2a3c44d1a9a1",
                                "data": {
                                    "number": "SO-123",
                                    "date": "2026-10-16T10:00:00+00:00",
                                    "counterparty_ref": "cp-123",
                                    "items": [
                                        {"sku": "SERVICE", "quantity": 1, "price": 15000.0}
                                    ],
                                    "total": 15000.0,
                                },
                            },
                        }
                    },
                }
            },
        }

    onec = paths.get("/webhook/onec", {}).get("post")
    if onec is not None:
        onec["summary"] = "Inbound 1C status webhook (1C -> amoCRM)"
        onec["requestBody"] = {
            "required": True,
            "content": {
                "application/json": {
                    "schema": _envelope_schema(
                        {"$ref": "#/components/schemas/OneCOrderStatusSchema"},
                        ["onec_order_status"],
                        "OneCWebhookEnvelope",
                    ),
                    "examples": {
                        "status": {
                            "summary": "Order status update routed to an amoCRM lead",
                            "value": {
                                "idempotency_key": "1c-order-42:paid:2026-10-16T10:00:00+00:00",
                                "event_type": "onec_order_status",
                                "occurred_at": "2026-10-16T10:00:00+00:00",
                                "nonce": "8d2b3c44-1f52-4a7c-9e2b-2a3c44d1a9a1",
                                "data": {
                                    "order_id": "LEAD-42",
                                    "status": "paid",
                                    "payment_status": "paid",
                                    "updated_at": "2026-10-12T00:00:00+00:00",
                                    "comment": "оплачено",
                                },
                            },
                        }
                    },
                }
            },
        }


def _hoist_defs(schema: dict, schemas: dict) -> dict:
    """Move nested ``$defs`` up into ``components/schemas``.

    Pydantic v2 emits nested models under a schema-local ``$defs`` block, but
    the ``ref_template`` renders their ``$ref`` targets as
    ``#/components/schemas/<name>``. Hoisting every ``$defs`` entry to the
    top-level keeps those references resolvable.
    """
    for name, sub in schema.pop("$defs", {}).items():
        schemas.setdefault(name, _hoist_defs(sub, schemas))
    return schema


def build_spec() -> dict:
    spec = app.openapi()
    components = spec.setdefault("components", {})
    schemas = components.setdefault("schemas", {})
    for model in MODELS:
        schema = _hoist_defs(
            model.model_json_schema(ref_template="#/components/schemas/{model}"),
            schemas,
        )
        schemas[model.__name__] = schema
    schemas["ErrorShape"] = TypeAdapter(ErrorShape).json_schema(
        ref_template="#/components/schemas/{model}"
    )
    _enrich_paths(spec)
    return spec


def main() -> None:
    spec = build_spec()
    OUTPUT.write_text(
        json.dumps(spec, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
