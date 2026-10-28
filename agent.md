# amocrm-1c-resilient-gateway

## Назначение

**Двусторонний** шлюз-интеграция между **AmoCRM** и **1C**: принимает
HTTP-вебхуки с обеих сторон, аутентифицирует их (HMAC-SHA256 и/или API-token),
идемпотентно сохраняет события в **SQLite WAL** и доставляет их в зависимости от
направления:

- `amoCRM → 1C` (`POST /webhook/amocrm`) — заказы и контрагенты в
  **1C OData REST API**;
- `1C → amoCRM` (`POST /webhook/onec`) — статусы заказов в
  **amoCRM API** (`PATCH /api/v4/leads/<id>`) с client-side троттлингом 100 rps.

Доставки живут в транзакционном боксе `deliveries` (Transactional Outbox). При
недоступности зависимости срабатывают circuit breaker и exponential backoff, а
исчерпавшие попытки события уходят в **n8n Dead Letter Queue Webhook**.

## Pattern

**Single Agent (Pipeline «Ingress → Validate → Store → Egress») with Function Calling.**
Ациклический конвейер из четырёх атомарных модулей с маршрутизацией по
`deliveries.destination` (`1c` / `amocrm`); каждый переход статуса фиксируется в
конечном автомате хранилища.

## Роль

Оркестрирующий агент-разработчик, который реализует и сопровождает конвейер:

```
ingress_guard (HMAC / token / replay / rate-limit)
  → contract_validator (Pydantic v2, extra=forbid)
  → state_store (SQLite WAL, BEGIN IMMEDIATE, idempotency_keys, deliveries)
  → egress_dispatcher (маршрутизация 1c/amocrm, circuit breaker, backoff+jitter, n8n DLQ)
  → state_store (DELIVERED / FAILED)
```

## Навыки

| Компонент | Навык | Назначение |
|---|---|---|
| `ingress_guard` | `api-rate-limiting` | token bucket / sliding window для входящего трафика |
| `contract_validator` | `json-schema-validator` | JSON-контракты и классификация ошибок |
| `state_store` | `database-design` | WAL-схема, индекс, конкурентная запись, outbox `deliveries` |
| `egress_dispatcher` | `backend-development` | отказоустойчивые API-клиенты (1C OData + amoCRM), ретраи |

## Компоненты

1. **ingress_guard** — приём вебхуков с обеих сторон, гибридная авторизация (HMAC-SHA256 для `amoCRM`, API-token `X-Api-Token`/`Bearer` или HMAC для `1C`), replay-защита (окно timestamp + nonce-журнал), guard подмены заголовков, IP rate limiting. Коды ответа 401/403/429.
2. **contract_validator** — валидация Pydantic v2 (`extra=forbid`) схем `WebhookPayload`, `AmoLeadCreateSchema`, `OneCOrderCreateSchema`, `OneCCounterpartySchema`, `OneCOrderStatusSchema`; единый `ErrorShape` до записи в хранилище.
3. **state_store** — SQLite WAL, `BEGIN IMMEDIATE` для 50 параллельных воркеров, `idempotency_keys`, дедупликация, атомарный автомат событий `RECEIVED → VALIDATED → DISPATCHED → DELIVERED / FAILED` и Transactional Outbox `deliveries` (по строке на пару `(event_id, destination)`) с жизненным циклом `PENDING → IN_FLIGHT → DELIVERED | FAILED`.
4. **egress_dispatcher** — маршрутизация по `deliveries.destination`: клиент 1C OData (`Document_ЗаказПокупателя`, `Catalog_Контрагенты`) и вспомогательный исходящий amoCRM-клиент (`AmoClient`, `PATCH /api/v4/leads/<id>`); отдельный circuit breaker (closed/open/half-open) на каждое плечо, exponential backoff с jitter, fallback в n8n DLQ.

**Общая библиотека (не компонент): `gateway/resilience.py`** — `TokenBucketRateLimiter` (blocking pacer), `classify_status` (`ok`/`retry`/`auth`/`fail`/`error`), `parse_retry_after` и `retry_after_floor` (RFC 9110). Переиспользуется `egress_dispatcher` и воркером; инвариант четырёх компонентов сохраняется (`type: shared_library`).

## Ограничения

- Все пороги: 13 параметров в `config/gateway.json` (4 секции) + 4 env-only pacing scalar'а (`GATEWAY_ONEC_RATE_LIMIT_RPS/BURST`, `GATEWAY_AMOCRM_RATE_LIMIT_RPS/BURST`), не из кода.
- Единый формат ошибок: `type`, `message`, `severity`, `location`, `how_to_fix`.
- Строго offline: 1C OData, amoCRM API и n8n DLQ всегда замоканы в тестах (`network_mode=strict_offline`).
- `FAILED`-события переотправляются без повторной валидации.
- Сетевые вызовы инжектируются через `transport`-функции; тесты не открывают сокеты.
- Двусторонний контур: маршрутизация по `deliveries.destination`; у каждого плеча свой circuit breaker.

## Примеры задач

- Добавить новую схему/энтити 1C (например `Document_Реализация`) — расширить `gateway/contracts/` и `egress_dispatcher`.
- Изменить политику ретраев — только через `config/gateway.json` (`backoff.*`).
- Подключить внешний секрет HMAC — через переменную окружения `GATEWAY_HMAC_SECRET`.
- Настроить обратный вызов amoCRM — `GATEWAY_AMOCRM_BASE_URL` / `GATEWAY_AMOCRM_ACCESS_TOKEN` / `GATEWAY_AMOCRM_REQUESTS_PER_SECOND`.
- Диагностировать застрявшие события — `fetch_dispatchable()` + `resend_failed()` без повторной валидации.

## Верификация

300 тестов (100% pass, offline execution ~45–90s), `pytest -q`, `network_mode=strict_offline`.
Критерии приёмки A1–A16 покрыты в `tests/` (см. `ARCHITECTURE_REPORT.md`, §7):
A9 — входное плечо 1C (`token`/`Bearer`/HMAC), A10 — outbox `deliveries`,
A11 — исходящее плечо amoCRM, A12 — сквозная идемпотентность к 1C,
A13 — single-flight ротация токена, A14 — pacing, A15 — breaker bypass
429/401/403, A16 — RFC 9110 `Retry-After` floor. Guard импорта entrypoint —
`test_egress_worker_module_imports_cleanly`.

## Запуск

```
uvicorn gateway.app:app --host 0.0.0.0 --port 8000
pytest -q
```