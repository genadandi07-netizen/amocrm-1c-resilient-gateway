# ARCHITECTURE_REPORT — amocrm-1c-resilient-gateway

## 1. Принятый вариант

**Вариант A** — конвейер из четырёх атомарных модулей:
`ingress_guard → contract_validator → state_store → egress_dispatcher`.
Поток ацикличен, каждый переход статуса фиксируется в конечном автомате
хранилища. Конвейер обслуживает **два направления** (`amoCRM → 1C` и
`1C → amoCRM`): маршрутизацию выполняет `egress_dispatcher` по колонке
`deliveries.destination` (`1c` / `amocrm`), а исходящий amoCRM-клиент вынесен
во вспомогательный модуль `gateway/amocrm_client.py` (не пятый компонент —
транспорт уровня `egress_dispatcher`). Отклонены: Вариант B (отдельный
компонент планирования) и Вариант C (мультиагентная шина; модуль `IAPRouter`
допускается только при расширении и не засчитывается в лимит компонентов).

> `gateway/resilience.py` — **общая библиотека примитивов** (`shared_library`),
> а не пятый этап конвейера: `TokenBucketRateLimiter` (pacing),
> `classify_status`, `parse_retry_after` и `retry_after_floor`
> переиспользуются `egress_dispatcher` и воркером. Инвариант «ровно четыре
> компонента» сохраняется.

## 2. Компоненты (ровно 4)

| # | id | Роль | Файл |
|---|---|---|---|
| 1 | `ingress_guard` | Шлюз входа и безопасность | `gateway/ingress_guard.py` |
| 2 | `contract_validator` | Валидатор контрактов | `gateway/contract_validator.py` |
| 3 | `state_store` | Хранилище состояния и идемпотентности | `gateway/state_store.py` |
| 4 | `egress_dispatcher` | Диспетчер исходящих вызовов (маршрутизация `1c`/`amocrm`) | `gateway/egress_dispatcher.py` |
| — | `amocrm_client` | Вспомогательный egress-транспорт amoCRM (`PATCH /api/v4/leads/<id>`) | `gateway/amocrm_client.py` |
| — | `resilience` | Общая библиотека примитивов устойчивости (`shared_library`) | `gateway/resilience.py` |

## 3. Data Flow

```
amoCRM ─▶ ingress_guard (HMAC/replay/rate-limit)
1C     ─▶ ingress_guard (token/HMAC + replay/rate-limit)
        → contract_validator (Pydantic v2, extra=forbid)
        → state_store (events + deliveries, BEGIN IMMEDIATE)
        → egress_dispatcher (маршрутизация по deliveries.destination)
             ├─ TokenBucketRateLimiter: pacing RPS/BURST (blocking, оба плеча)
             ├─ circuit breaker: bypass для 429/401/403 (classify_status)
             ├─ Retry-After (RFC 9110): max(jitter_backoff, retry_after)
             ├─ destination=1c     → OneCClient  → 1C OData
             └─ destination=amocrm → AmoClient   → amoCRM API (≤100 rps)
                  AmoClient: single-flight ротация токена (_token_version, ≤1 retry)
        → state_store (DELIVERED)
```

`POST /webhook/amocrm` создаёт событие с delivery `1c` (`lead`/`order`/
`counterparty`), `POST /webhook/onec` — событие с delivery `amocrm`
(`onec_order_status`). При отказе `egress_dispatcher` помечает событие
`FAILED` и передаёт его в n8n DLQ. `FAILED`-события переотправляются через
`state_store` без повторной валидации.

## 4. State Machine

| From | To | Trigger |
|---|---|---|
| (новое) | `RECEIVED` | успешный приём ingress_guard |
| `RECEIVED` | `VALIDATED` | валидация + запись BEGIN IMMEDIATE |
| `VALIDATED` | `DISPATCHED` | egress принял событие |
| `DISPATCHED` | `DELIVERED` | успешный ответ 1C OData |
| `DISPATCHED` | `FAILED` | исчерпаны попытки / breaker open |
| `FAILED` | `VALIDATED` | повторный проход без валидации |

`ALLOWED_TRANSITIONS` в `gateway/state_store.py` содержит ровно эти 5 переходов
(строка «(новое) → `RECEIVED`» — это первичная вставка события, а не переход).

### 4.1. Жизненный цикл доставки (`deliveries`, Transactional Outbox)

Событие и его outbox-строки создаются **в одной транзакции**
(`insert_event(..., destinations=...)`), поэтому «событие принято, но
потеряно до постановки в очередь» невозможно. Одна строка на пару
`(event_id, destination)`, `UNIQUE(event_id, destination)`.

| From | To | Trigger |
|---|---|---|
| (новое) | `PENDING` | `insert_event` (atomic с записью события) |
| `PENDING` | `IN_FLIGHT` | `mark_delivery_in_flight()` перед вызовом транспорта |
| `IN_FLIGHT` | `DELIVERED` | `finish_delivery()` после успешного ответа |
| `IN_FLIGHT` | `FAILED` | `finish_delivery()` после исчерпания попыток / breaker open |
| `FAILED` | `IN_FLIGHT` | повторный проход воркера (retry без повторной валидации) |

`DELIVERY_STATUSES` в `gateway/state_store.py`: `PENDING`, `IN_FLIGHT`,
`DELIVERED`, `FAILED`. Легаси-события без строки (созданные старой сборкой)
подхватываются `migrate_legacy_deliveries()` и получают один delivery `1c`.

Все пишущие транзакции берут write-lock сразу (`BEGIN IMMEDIATE`), а сам
захват lock повторяется при `SQLITE_BUSY` (`_retry_on_lock()` в
`gateway/state_store.py`: 6 попыток, backoff 50→500 мс). Транзиентная
конкуренция превращается в короткое ожидание вместо `database is locked`
(критерий A3: 50 параллельных воркеров без блокировок).

## 5. Конфигурационные параметры (4 секции, 13 параметров)

`circuit_breaker`: `failure_threshold`, `cooldown_seconds`, `half_open_probe_interval`.
`replay_guard`: `timestamp_window_seconds`, `nonce_journal_size`.
`backoff`: `base_seconds`, `multiplier`, `max_seconds`, `jitter_ratio`, `max_attempts`.
`rate_limit`: `algorithm`, `requests_per_window`, `window_seconds`.

Пороги никогда не захардкожены в модулях — только `Settings` из
`config/gateway.json`/env. Двусторонний контур добавляет scalar-ключи
(не секции): `GATEWAY_ONEC_API_TOKEN`, `GATEWAY_AMOCRM_BASE_URL`,
`GATEWAY_AMOCRM_ACCESS_TOKEN`, `GATEWAY_AMOCRM_REQUESTS_PER_SECOND` (все —
`gateway/config.py`, кроме учётных данных воркера 1C).

Пейсинг задаётся **только через env** — четыре плоских scalar'а без секций в
`config/gateway.json`: `GATEWAY_ONEC_RATE_LIMIT_RPS` (2.0),
`GATEWAY_ONEC_RATE_LIMIT_BURST` (5), `GATEWAY_AMOCRM_RATE_LIMIT_RPS` (2.0),
`GATEWAY_AMOCRM_RATE_LIMIT_BURST` (5).

## 6. Единый формат ошибок

```json
{"type": "security|schema|semantic|dependency",
 "message": "human readable",
 "severity": "critical|warning",
 "location": "headers|body|signature|replay|rate_limit|egress.onec|egress.amocrm|egress.amocrm.rate_limit",
 "how_to_fix": "concrete corrective action"}
```

Ровно 5 полей, фабрика `make_error` в `gateway/errors.py`. Это и есть
канонический «единый лог ошибки» шлюза: поля `type`/`message`/`severity`/
`location`/`how_to_fix`. В сторонних описаниях тот же конверт иногда называют
`code`/`message`/`details`/`timestamp` — в коде используется именно
`ErrorShape` из `gateway/errors.py`, менять контракт нельзя (проверяется
`test_error_shape_five_fields_everywhere`).

## 7. Трассировка критериев A1–A16

| ID | Критерий | Файл теста | Тестируемая функция |
|---|---|---|---|
| A1 | неверная HMAC/подделанный заголовок → 401/403, нет записи | `tests/test_ingress_guard.py`, `tests/test_pipeline_e2e.py` | `verify_hmac_sha256`, `check_headers`, `test_bad_signature_401`, `test_tampered_header_403` |
| A2 | дубль не создаёт второй записи (idempotency_keys) | `tests/test_state_store.py`, `tests/test_pipeline_e2e.py` | `insert_event`, `test_insert_event_duplicate_idempotency_key`, `test_duplicate_single_row` |
| A3 | 50 параллельных воркеров BEGIN IMMEDIATE без блокировок | `tests/test_state_store.py` | `test_concurrent_workers_50_no_lock_errors`, `test_transaction_uses_begin_immediate` |
| A4 | breaker open → нет вызовов 1C, события в n8n DLQ | `tests/test_egress_dispatcher.py` | `test_circuit_breaker_opens_after_threshold`, `test_dispatch_event_open_breaker_no_client_calls` |
| A5 | backoff+jitter в границах, лимит попыток | `tests/test_egress_dispatcher.py` | `compute_backoff`, `test_compute_backoff_within_bounds`, `test_dispatch_event_retries_exhausted` |
| A6 | extra/тип → Pydantic, есть severity и how_to_fix | `tests/test_contract_validator.py` | `validate_model`, `test_validate_model_error_location` |
| A7 | 300 тестов (100% pass, offline execution), strict_offline | `tests/` (весь сьют) | `test_verification_suite.py` + `conftest.strict_offline` |
| A8 | пустой ввод/битый JSON без исключений | `tests/test_contract_validator.py`, `tests/test_pipeline_e2e.py` | `parse_webhook_body`, `test_parse_webhook_body_broken_json`, `test_empty_body_422` |
| A9 | входное плечо 1C: token / Bearer / HMAC, replay, 401/403/422/429 | `tests/test_onec_ingress.py` | `authenticate_request`, `extract_api_token`, `verify_api_token`, `test_onec_*` |
| A10 | Outbox `deliveries`: атомарность, статусы, миграция легаси | `tests/test_deliveries_store.py` | `insert_event`, `ensure_delivery`, `mark_delivery_in_flight`, `finish_delivery`, `fetch_ready_deliveries`, `migrate_legacy_deliveries` |
| A11 | исходящее плечо amoCRM: маршрутизация, throttling 100 rps, ретраи | `tests/test_amocrm_egress.py` | `AmoClient.update_lead_status`, `map_status_to_amo`, `dispatch_event`, `test_dispatch_bundle_routes_both_directions` |
| A12 | сквозная идемпотентность к 1C (`X-Idempotency-Key` + `comment`) | `tests/test_onec_idempotency_header.py` | `OneCClient.create_order`, `comment_with_idempotency_key` |
| A13 | 401 → single-flight ротация токена amoCRM (1 refresh за эпоху, ≤1 retry) | `tests/test_amocrm_token_rotation.py` | `AmoClient.refresh_token`, `AmoClient._request`, `test_concurrent_token_refreshes_are_single_flight` |
| A14 | TokenBucketRateLimiter: blocking pacing по RPS/BURST, оба плеча | `tests/test_resilience.py` | `TokenBucketRateLimiter.acquire`, `test_acquire_*`, `test_token_bucket_*` |
| A15 | `classify_status` + breaker bypass для 429/401/403 | `tests/test_resilience.py`, `tests/test_onec_throttling.py` | `classify_status`, `test_classify_status_*`, `test_circuit_breaker_is_not_tripped_by_429/401/403` |
| A16 | `parse_retry_after` / `retry_after_floor`, включая HTTP-date | `tests/test_onec_throttling.py`, `tests/test_resilience.py` | `parse_retry_after`, `retry_after_floor`, `test_parse_retry_after_http_date`, `test_retry_after_floor_*` |

## 8. Решение по замоканию сети (strict_offline)

- `OneCClient` принимает `transport`-функцию; в тестах — recorder, реальные сокеты не открываются.
- `AmoClient` принимает `transport`-функцию (три стиля вызова), поэтому всё
  исходящее плечо 1C → amoCRM тестируется без сети.
- `AmoClient` — единственный владелец заголовка `Authorization`: токен
  подставляется динамически на каждый запрос, а при 401 ротация выполняется
  single-flight (`threading.Lock`, без event loop), поэтому N параллельных
  воркеров вызывают ровно один refresh и повторяют запрос не более раза.
- `send_to_dlq` принимает bound-транспорт, вызываемый с dict-payload.
- Autouse-фикстура `strict_offline` блокирует `socket.socket.connect` и падает
  с `RuntimeError`, если код попытается выйти в сеть.
- `time.sleep` инжектируется как `sleep_fn`, поэтому backoff не тормозит тесты.