# DEPLOYMENT_GUIDE — amocrm-1c-resilient-gateway

## 1. Требования

- Python 3.10+
- Зависимости: `pydantic>=2`, `fastapi`, `httpx`, `pytest` (см. `pyproject.toml`)
- Постоянный том для файла SQLite (WAL создаёт `-wal` и `-shm` рядом с БД)
- Исходящий доступ к 1C OData и amoCRM API (прод) и к n8n DLQ Webhook (fallback)

## 2. Установка

```
python -m pip install -e .
# или
pip install "pydantic>=2" "fastapi>=0.110" "httpx>=0.27" "pytest>=8.0"
```

## 3. Секрет HMAC-SHA256

Секрет **никогда** не хранится в коде или `config/gateway.json`.
Источник — переменная окружения `GATEWAY_HMAC_SECRET` (или внешний секрет-стор,
который её экспортирует). При отсутствии используется
`change-me-in-production` (только для локальной разработки).

```
export GATEWAY_HMAC_SECRET="$(cat /run/secrets/amocrm_hmac)"
```

## 4. Конфигурация

Все 13 параметров находятся в `config/gateway.json`.

### 4.1. Egress pacing (env-only)

Темп исходящих запросов задаётся четырьмя плоскими scalar'ами — **только через
окружение**, секций в `config/gateway.json` для них нет:

| Переменная | По умолчанию | Смысл |
|---|---|---|
| `GATEWAY_ONEC_RATE_LIMIT_RPS` | `2.0` | 1C pacer: запросов в секунду |
| `GATEWAY_ONEC_RATE_LIMIT_BURST` | `5` | 1C pacer: ёмкость burst (токенов) |
| `GATEWAY_AMOCRM_RATE_LIMIT_RPS` | `2.0` | amoCRM pacer: запросов в секунду |
| `GATEWAY_AMOCRM_RATE_LIMIT_BURST` | `5` | amoCRM pacer: ёмкость burst (токенов) |

**Бюджет vs pacer.** `RateLimiter` — это **бюджет**: при превышении он
**отклоняет** вызов (`AmoRateLimitError`, `location=egress.amocrm.rate_limit`),
и событие уходит в ретраи. `TokenBucketRateLimiter` — это **pacer**: он
**блокирует** (спит) до появления токена, поэтому запрос не теряется. Бюджет
100 rps (`GATEWAY_AMOCRM_REQUESTS_PER_SECOND`) остаётся защитой от
злоупотребления, а pacer 2 rps обеспечивает плавный темп к зависимости.

### circuit_breaker
| Параметр | Значение по умолчанию | Описание |
|---|---|---|
| `failure_threshold` | 5 | число подряд неудач до перехода в `open` |
| `cooldown_seconds` | 30.0 | время `open` до `half-open` |
| `half_open_probe_interval` | 5.0 | минимальный интервал между probe-попытками |

### replay_guard
| Параметр | Значение | Описание |
|---|---|---|
| `timestamp_window_seconds` | 300 | допустимое расхождение timestamp |
| `nonce_journal_size` | 10000 | размер скользящего nonce-журнала |

### backoff
| Параметр | Значение | Описание |
|---|---|---|
| `base_seconds` | 0.5 | базовая задержка |
| `multiplier` | 2.0 | множитель (exponential) |
| `max_seconds` | 30.0 | потолок задержки |
| `jitter_ratio` | 1.0 | доля jitter (full jitter при 1.0) |
| `max_attempts` | 5 | максимум попыток доставки |

### rate_limit
| Параметр | Значение | Описание |
|---|---|---|
| `algorithm` | token_bucket | `token_bucket` или `sliding_window` |
| `requests_per_window` | 100 | запросов на IP за окно |
| `window_seconds` | 60 | длина окна |

Переопределение через окружение: `GATEWAY_<SECTION>_<PARAM>`, например
`GATEWAY_BACKOFF_MAX_ATTEMPTS=8`.

## 5. SQLite на постоянном томе

- Файл БД: `./data/gateway.db` (монтируется в контейнер как `/app/data/gateway.db`
  через том `./data:/app/data`; настраивается `GATEWAY_DB_PATH`).
- `PRAGMA journal_mode=WAL`, `synchronous=NORMAL`, `busy_timeout=5000`.
- Для 50 параллельных воркеров используются транзакции `BEGIN IMMEDIATE`.
- Резервная копия — только через online-backup, не копированием файла:

```
sqlite3 gateway.db ".backup 'backup/gateway-$(date +%Y%m%d).db'"
```

`-wal`/`-shm` нельзя удалять при работающем процессе.

Триаж очереди Outbox (SQL-рецепты для `PENDING`/`IN_FLIGHT`/`FAILED`,
локализация 429/401, сверка с DLQ) — `docs/MAINTENANCE.md`, §3.

## 6. Запуск (systemd + uvicorn)

```
[Unit]
Description=amocrm-1c-resilient-gateway
After=network-online.target

[Service]
WorkingDirectory=/opt/amocrm-1c-gateway
Environment=GATEWAY_HMAC_SECRET=...
ExecStart=/opt/amocrm-1c-gateway/.venv/bin/uvicorn gateway.app:app --host 0.0.0.0 --port 8000 --workers 1
Restart=always

[Install]
WantedBy=multi-user.target
```

`--workers 1` обязателен: конкурентность обеспечивается `BEGIN IMMEDIATE`
и фоном-доставкой, а не несколькими процессами.

Проверка живости: `GET /health` → `{"status": "ok", "service": "amocrm-1c-gateway"}`.

### Docker Compose

`docker-compose.yml` собирает образ и запускает два сервиса на одном томе
`./data` (SQLite WAL): `gateway` публикуется на `127.0.0.1:8000`, а
`egress-worker` (`python -m scripts.egress_worker`) фоново доставляет
`VALIDATED`/`FAILED`-события в 1C OData. Перед первым запуском создайте `.env`
из шаблона (файл `.env` не хранится в репозитории):

```
cp .env.example .env   # задайте GATEWAY_HMAC_SECRET и адреса 1C / n8n
docker compose up -d --build
```

## 7. Мониторинг n8n Dead Letter Queue

1. События со статусом `FAILED` отправляются в n8n DLQ с payload:
   `event_id`, `idempotency_key`, `payload`, `error` (5 полей), `attempts`, `failed_at`.
2. Процедура повторной отправки:
   - устранить причину в 1C;
   - вызвать `run_delivery_pass(...)` повторно (или `resend_failed(conn, event_id)`);
   - `FAILED → VALIDATED` выполняется **без повторной валидации**.
3. Отслеживать метрику `count(status='FAILED')` и глубину DLQ в n8n.

> **Ротация токена amoCRM.** `AmoClient` ротирует токен single-flight только в
> памяти процесса (эпоха `_token_version`). Ручное обновление
> `GATEWAY_AMOCRM_ACCESS_TOKEN` в `.env` **не подхватывается** живым
> контейнером: выполните
> `docker compose up -d --force-recreate egress-worker`.

## 8. Чек-лист релиза

- [ ] `pytest -q` → `300 passed (100% pass, offline execution)`, `network_mode=strict_offline`.
- [ ] регенерация `OPENAPI_SPEC.json`: `python scripts/generate_openapi.py`.
- [ ] `GATEWAY_HMAC_SECRET` задан в окружении.
- [ ] том с `gateway.db` смонтирован и доступен на запись.
- [ ] n8n DLQ Webhook доступен; URL прописан в транспорте деплоя.
- [ ] `docs/MAINTENANCE.md` присутствует в поставке (эксплуатационный runbook).