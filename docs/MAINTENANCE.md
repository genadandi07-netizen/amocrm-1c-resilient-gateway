# MAINTENANCE — PRODUCTION RUNBOOK (amocrm-1c-resilient-gateway)

**AUDIENCE:** On-call / Tier-1 Engineer  
**SCOPE:** Local-only maintenance. Do not modify other files. Do not execute `git push`.  
**PREREQUISITES:** Access to host filesystem, `sqlite3`, `docker`, `docker compose`. DB path: `/data/gateway.db` (host) / `/app/data/gateway.db` (container).

---

## 1. 15-MINUTE ENGINEER ONBOARDING

### 1.1. Quick Start (dev/stage)
**From repo root (where `docker-compose.yml` exists):**
```bash
# Build and start
docker compose up -d --build

# Verify stack
docker compose ps

# Health (HTTP gateway)
curl -s http://localhost:8000/health
# Expected: {"status":"ok","service":"amocrm-1c-gateway"}
```

**Alternative: isolated Dockerfile (no compose):**
```bash
# Build image
docker build -t amocrm-1c-gateway:local .

# Run gateway (publish only if necessary)
docker run -d --name gateway \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/.env:/app/.env:ro \
  -p 127.0.0.1:8000:8000 \
  amocrm-1c-gateway:local \
  uvicorn gateway.app:app --host 0.0.0.0 --port 8000

# Run egress worker
docker run -d --name egress-worker \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/.env:/app/.env:ro \
  amocrm-1c-gateway:local \
  python -m scripts.egress_worker
```

### 1.2. Architectural Invariants
| Item | Value | Notes |
|---|---|---|
| Transactional Outbox DB | `/data/gateway.db` (host), `/app/data/gateway.db` (container) | SQLite WAL mode. Single source of truth. |
| Volume mount | `./data:/app/data` | Must be mounted for both `gateway` and `egress-worker`. |
| Structured JSON logs with correlation | `request_id` | Present in ingress/egress logs. Correlate via `event_id` / `delivery_id`. |
| Processes | `gateway` (ingress/validation only), `egress-worker` (outbox dispatcher) | No IPC/shared memory. Coordination only via DB WAL. |
| Scaling rule | **1x gateway, 1x egress-worker** | Multiple workers = duplicate deliveries (no cross-process claim lock). |
| Graceful shutdown | `stop_grace_period: 45s` | Worker finishes current pass on `SIGTERM`. |

---

## 2. SECRETS & AUTHORIZATION

### 2.1. Primary OAuth2 Binding (amoCRM)
**Setup (from repo root):**
Set `GATEWAY_AMOCRM_ACCESS_TOKEN` in `.env` (or in the environment). The token is read by `gateway/config.py` and used by `AmoClient` / the egress worker.
After updating the token in `.env`, recreate the worker container to pick up the new value (see §2.3).

### 2.2. Single-Flight Token Rotation
| Component | Role |
|---|---|
| `asyncio.Lock` (single-flight) | Guards token refresh; only one coroutine performs refresh at a time. |
| `token_version` (epoch) | Incremented on token refresh/update. Clients use current epoch to avoid reusing stale tokens after rotation. |
| `AmoClient` | On 401, performs single-flight refresh and retries exactly once with new token/version. |

**Rule:** Never manually edit tokens in DB. Rotation is in-memory coordinated via lock + epoch.

### 2.3. 1C Credentials Rotation (soft restart, no connection drop)
1. Update in `.env`:
```env
GATEWAY_ONEC_USER=<new_user>
GATEWAY_ONEC_PASSWORD=<new_password>
```
2. Recreate worker only (variables not hot-reloaded):
```bash
docker compose up -d --force-recreate egress-worker
```
3. Verify (no mass 401):
```bash
sqlite3 -header -column data/gateway.db "SELECT COUNT(*) AS auth_fail_15m FROM deliveries WHERE last_error LIKE '%401%' AND datetime(updated_at) > datetime('now','-15 minutes');"
```

---

## 3. DB SCHEMA & MIGRATIONS (SQLite WAL)

### 3.1. Transactional Migrations
- **Always use `BEGIN IMMEDIATE`.** Acquire write lock up front to avoid schema-deadlocks.
- **One transaction per migration.** DDL + `PRAGMA user_version` increment must be atomic.
- **Idempotent until runner exists.** `IF NOT EXISTS` / safe guards required.

```bash
sqlite3 data/gateway.db <<'SQL'
BEGIN IMMEDIATE;
-- DDL here
PRAGMA user_version = <N+1>;
COMMIT;
SQL
```

### 3.2. Safe Schema Changes
| Change | Allowed | Notes |
|---|---|---|
| `ALTER TABLE ... ADD COLUMN ... DEFAULT ...` | YES | Metadata-only; old code ignores new column. |
| Add table/index | YES | Idempotent with `IF NOT EXISTS`. |
| `RENAME COLUMN` | CAUTION | Affects triggers/views; verify SQLite version. |
| `DROP COLUMN` | NO (same release) | Remove only in later release after no readers. |
| Destructive rewrites | NO | Use shadow table + atomic `ALTER TABLE ... RENAME TO ...` only if strictly necessary and tested. |

**Versioning:** `PRAGMA user_version` is source of truth for migration count.

---

## 4. QUEUE INSPECTION & DEAD LETTER QUEUE (SQL RUNBOOK)

Run from repo root on host against `data/gateway.db`.

### 4.1. Distribution by Status
```bash
sqlite3 -header -column data/gateway.db "
SELECT 'events' AS tbl, status, COUNT(*) AS n, MIN(created_at) AS oldest, MAX(updated_at) AS newest
FROM events GROUP BY status
UNION ALL
SELECT 'deliveries', status, COUNT(*), MIN(created_at), MAX(updated_at)
FROM deliveries GROUP BY status
ORDER BY tbl, status;"
```

### 4.2. Last 10 Incidents in DLQ (Failed Deliveries)
```bash
sqlite3 -header -column data/gateway.db "
SELECT id, event_id, destination, attempt_count,
       substr(last_error,1,160) AS error_message,
       substr((SELECT payload FROM events e WHERE e.id = d.event_id),1,200) AS payload_snippet,
       updated_at AS created_at
FROM deliveries d
WHERE status='FAILED'
ORDER BY updated_at DESC
LIMIT 10;"
```

### 4.3. Manual Replay: Move DLQ batch back to VALIDATED
**Prerequisite:** Fix root cause (dependency/auth/circuit) first. Only replay after fix.

```bash
sqlite3 data/gateway.db <<'SQL'
BEGIN IMMEDIATE;
-- Replay up to N failed deliveries for a specific event/destination if needed
-- Default: replay all FAILED for events that can be reprocessed
UPDATE events
SET status = 'VALIDATED', updated_at = datetime('now')
WHERE status = 'FAILED'
  AND id IN (
    SELECT DISTINCT event_id FROM deliveries WHERE status = 'FAILED'
  );
COMMIT;
SQL
```

**Verify:**
```bash
sqlite3 -header -column data/gateway.db "SELECT status, COUNT(*) FROM events GROUP BY status;"
sqlite3 -header -column data/gateway.db "SELECT status, COUNT(*) FROM deliveries WHERE status='FAILED';"
```

**Note:** Worker will pick up `VALIDATED` on next pass. Do not set `deliveries` to `DELIVERED` manually.

---

## 5. TROUBLESHOOTING & INCIDENT RESPONSE

### 5.1. Symptom: WAL file grows (Checkpoint Starvation)
**Cause:** Long-running read transactions prevent WAL checkpoint.  
**Check:**
```bash
ls -lh data/gateway.db* 
sqlite3 data/gateway.db "PRAGMA journal_mode; PRAGMA wal_autocheckpoint;"
```
**Remedy (force checkpoint/truncate):**
```bash
sqlite3 data/gateway.db "PRAGMA wal_checkpoint(TRUNCATE);"
ls -lh data/gateway.db*
```
**Prevention:** Avoid long open transactions; keep `busy_timeout` set.

### 5.2. Symptom: Queue grows, no external requests (Circuit Breaker OPEN)
**Cause:** Circuit breaker opened due to 1C timeouts/5xx.  
**Check breaker state (logs):**
```bash
docker compose logs --tail=150 egress-worker | grep -i "circuit breaker"
```
**Check availability (from container):**
```bash
docker compose exec gateway sh -c 'curl -sS -o /dev/null -w "HTTP:%{http_code}\n" -m 10 -u "$GATEWAY_ONEC_USER:$GATEWAY_ONEC_PASSWORD" "$GATEWAY_ONEC_BASE_URL/odata/standard.odata/\$metadata"'
```
**Algorithm:**
1. Verify 1C reachable (auth/network).
2. If dependency down → wait for `cooldown_seconds` (default 30s). Breaker transitions `OPEN` → `HALF-OPEN` after cooldown; one probe allowed.
3. If dependency up but probe fails repeatedly → keep waiting/fix dependency.
4. Only restart worker if dependency fixed and you need immediate reset: `docker compose restart egress-worker` (graceful). BDB state unchanged.

**DB signal:** `deliveries` may show `attempt_count=0`, `last_error='Circuit breaker is open'`.

### 5.3. Symptom: Stuck events in DISPATCHED
**Cause:** Worker crash/OOM/SIGKILL during dispatch; lease expired.  
**Recovery:** Automatic by lease mechanism.
**Check lease/age:**
```bash
sqlite3 -header -column data/gateway.db "
SELECT id, event_id, status, attempt_count, updated_at,
       CAST(strftime('%s','now') - strftime('%s', updated_at) AS INTEGER) AS age_sec
FROM deliveries
WHERE status='IN_FLIGHT' AND strftime('%s','now') - strftime('%s', updated_at) > 300
ORDER BY updated_at;"
```
**Rule:** After `GATEWAY_DISPATCH_LEASE_SECONDS` expires, dispatcher reclaims `DISPATCHED`/stale `IN_FLIGHT` back to dispatchable state. Do **not** manually flip to `DELIVERED`.

**Also check worker health:**
```bash
docker compose ps egress-worker
docker inspect $(docker compose ps -q egress-worker) 2>/dev/null | grep -A2 OOMKilled
```

---

## APPENDIX A. QUICK COMMAND REFERENCE
```bash
# Stack
docker compose ps && curl -s http://localhost:8000/health

# Logs
docker compose logs --tail=200 -f egress-worker

# Queue snapshot
sqlite3 -header -column data/gateway.db "SELECT destination,status,COUNT(*) FROM deliveries GROUP BY 1,2;"

# DLQ last 10
sqlite3 -header -column data/gateway.db "SELECT id,event_id,destination,attempt_count,substr(last_error,1,120) AS err,updated_at FROM deliveries WHERE status='FAILED' ORDER BY updated_at DESC LIMIT 10;"

# Checkpoint
sqlite3 data/gateway.db "PRAGMA wal_checkpoint(TRUNCATE);"

# Restart worker (graceful)
docker compose restart egress-worker
```