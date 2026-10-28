# Egress contracts research — 1C OData & n8n DLQ

> Frozen design note (2026-10). Authoritative runbooks are docs/INTEGRATION_MANUAL.md and docs/MAINTENANCE.md.

## OneC OData

Base URL is injected via `OneCClient(base_url, ...)`. Paths:

- `{base_url}/odata/standard.odata/Document_ЗаказПокупателя` — заказы покупателя
- `{base_url}/odata/standard.odata/Catalog_Контрагенты` — контрагенты

Method: `POST`. Headers:

| Header | Value |
|---|---|
| `Content-Type` | `application/json` |
| `Accept` | `application/json` |
| `Authorization` | `Basic <base64>` / `Bearer <token>` (deployment-specific) |

### Body example — Document_ЗаказПокупателя

```json
{
  "number": "SO-1",
  "date": "2026-10-12T00:00:00+00:00",
  "counterparty_ref": "cp-1",
  "items": [
    {"sku": "SKU-1", "quantity": 2, "price": 5.0}
  ],
  "total": 10.0
}
```

### Body example — Catalog_Контрагенты

```json
{
  "inn": "1234567890",
  "name": "Romashka",
  "legal_name": "ООО Ромашка",
  "phone": "+7-000-000-00-00",
  "email": "ops@example.com",
  "address": "Moscow"
}
```

Non-2xx responses raise `OneCError`; the dispatcher converts it into the unified
`ErrorShape` (`type=dependency`, `location=egress.onec`).

## n8n DLQ

The Dead Letter Queue transport is a bound callable invoked with a single JSON
payload. Expected payload:

```json
{
  "event_id": 42,
  "idempotency_key": "idem-1",
  "payload": {"event_type": "order", "data": {"number": "SO-1"}},
  "error": {
    "type": "dependency",
    "message": "1C OData returned HTTP 500",
    "severity": "critical",
    "location": "egress.onec",
    "how_to_fix": "Check 1C OData availability and payload correctness, then retry."
  },
  "attempts": 5,
  "failed_at": "2026-10-12T00:00:00+00:00"
}
```

The payload always contains exactly six top-level keys and a five-field error
object. A transport failure during DLQ delivery is logged and swallowed
(`send_to_dlq` returns `False`), never propagated to the caller.