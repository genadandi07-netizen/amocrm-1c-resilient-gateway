# Ingress signature research — AmoCRM webhook

> Frozen design note (2026-10). Authoritative runbooks are docs/INTEGRATION_MANUAL.md and docs/MAINTENANCE.md.

## Headers

| Header | Purpose | Format |
|---|---|---|
| `X-Signature` | HMAC-SHA256 signature over the raw request body | hex (optionally `sha256=<hex>`), case-insensitive |
| `X-Timestamp` | freshness marker for replay protection | ISO-8601 with timezone, or epoch seconds |
| `X-Nonce` | unique per request, prevents replay | opaque string, min length 1 |
| `Content-Type` | must be `application/json` | `application/json` (charset allowed) |

## Canonical string

The signature is computed over the **raw, unparsed request body bytes** (no
re-serialization):

```
signed_bytes = raw_body
X-Signature  = HMAC_SHA256(key = HMAC_SECRET, msg = signed_bytes).hexdigest()
```

Mantra: sign what you receive, verify what you signed. Re-encoding or
re-serializing the JSON before hashing will break verification.

## HMAC-SHA256 example

```python
import hashlib, hmac

secret = "shared-secret"
raw_body = b'{"idempotency_key":"k1","event_type":"order","occurred_at":"2026-10-12T00:00:00+00:00","nonce":"n1","data":{}}'

signature = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
headers = {
    "X-Signature": signature,          # also accepted: "sha256=" + signature
    "X-Timestamp": "2026-10-12T00:00:00+00:00",
    "X-Nonce": "n1",
    "Content-Type": "application/json",
}
```

Verification is constant-time (`hmac.compare_digest`). The signature header is
trimmed, an optional `sha256=` prefix is stripped and the hex is lowercased
before comparison.

## Recommended replay_guard config

```json
{
  "replay_guard": {
    "timestamp_window_seconds": 300,
    "nonce_journal_size": 10000
  }
}
```

- Reject any request whose timestamp differs from server time by more than
  `timestamp_window_seconds` (403).
- Keep a bounded sliding nonce journal of `nonce_journal_size` entries; a nonce
  seen again inside the journal window is rejected (403).
- Header tampering guard: duplicated or missing critical headers are rejected
  before the signature check (403).