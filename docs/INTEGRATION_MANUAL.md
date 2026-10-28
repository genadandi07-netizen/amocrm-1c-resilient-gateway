# INTEGRATION_MANUAL — amocrm-1c-resilient-gateway

Практическое руководство по подключению **двустороннего** шлюза
**AmoCRM ↔ 1C** в продакшене. Документ описывает контуры развёртывания
(Docker), входное плечо amoCRM → 1C (Ingress, §2), исходящее плечо 1C OData
(Egress, §3), обратное плечо 1C → amoCRM (§7), регламент аварийного
восстановления через SQLite-очередь и n8n Dead Letter Queue (§4), а также
операционный Runbook: логи, диагностика БД и типовые сбои (§8).

Сверено с исходным кодом ревизии: `gateway/config.py`, `gateway/app.py`,
`gateway/ingress_guard.py`, `gateway/egress_dispatcher.py`,
`gateway/amocrm_client.py`, `gateway/state_store.py`, `gateway/resilience.py`,
`gateway/contracts/*`, `scripts/egress_worker.py`, `config/gateway.json`.

**Связанные runbooks:** [`docs/MAINTENANCE.md`](MAINTENANCE.md) — SLA-эксплуатация
(первые 60 секунд, SQL-рецепты Outbox, миграции); `DEPLOYMENT_GUIDE.md` —
установка и релизный чек-лист.

---

## 0. Справочник контуров

| Параметр | Значение |
|---|---|
| Публичные endpoint'ы приёма | `POST /webhook/amocrm` (amoCRM → 1C), `POST /webhook/onec` (1C → amoCRM) |
| Healthcheck | `GET /health` → `{"status": "ok", "service": "amocrm-1c-gateway"}` |
| Публикация Docker | `127.0.0.1:8000` (только loopback хоста, наружу порт не публикуется) |
| Хранилище | SQLite WAL, таблицы `events` (автомат) и `deliveries` (Transactional Outbox), том `./data` |
| Целевые сущности 1C | `Document_ЗаказПокупателя`, `Catalog_Контрагенты` |
| Целевой endpoint amoCRM | `PATCH /api/v4/leads/<id>` (статус сделки) |
| Fallback | n8n Dead Letter Queue Webhook |
| Аутентификация входа | amoCRM: HMAC-SHA256 (`X-Signature`); 1C: гибрид `X-Api-Token`/`Bearer` **или** HMAC; общий replay-guard (300 с) |
| Троттлинг egress | бюджет: client-side 100 rps к amoCRM (`GATEWAY_AMOCRM_REQUESTS_PER_SECOND`) + блокирующий pacer на обоих плечах (`GATEWAY_ONEC_RATE_LIMIT_RPS/BURST`, `GATEWAY_AMOCRM_RATE_LIMIT_RPS/BURST`, по умолчанию 2 rps / burst 5) |

---

## 1. Быстрый старт (Production Docker Setup)

### 1.1. Требования

- Docker Engine 24+ и Docker Compose v2 (`docker compose`).
- Постоянный каталог `./data` на хосте (SQLite WAL создаёт `gateway.db`,
  `gateway.db-wal`, `gateway.db-shm`).
- Исходящий сетевой доступ из контейнера к 1C OData и к n8n DLQ.
- Внешний TLS-терминатор (nginx/traefik) перед шлюзом: контейнер публикуется
  только на loopback `127.0.0.1:8000`. Готовый production-блок nginx — §6.

### 1.2. Копирование шаблона окружения

```bash
cd /opt/amocrm-1c-gateway
cp .env.example .env
$EDITOR .env
```

`.env` в git не хранится (см. `.gitignore`). Минимальный продакшен-набор:

```dotenv
# --- HTTP (ТОЛЬКО для ручного запуска вне Docker, см. примечание ниже) ---
GATEWAY_HOST=0.0.0.0
GATEWAY_PORT=8000

# --- Безопасность входа ---
# Секрет HMAC-SHA256, общий с релеем amoCRM (>= 32 случайных байт).
# Сгенерировать: openssl rand -hex 32
GATEWAY_HMAC_SECRET=<openssl rand -hex 32>

# --- 1C OData (Egress) ---
# ВНИМАНИЕ: корневой адрес публикации БЕЗ сегмента /odata/standard.odata.
# Шлюз сам добавляет /odata/standard.odata/<Entity> (см. gateway/egress_dispatcher.py).
GATEWAY_ONEC_BASE_URL=http://1c-server:8314/base
# Штатные учётные данные воркера доставки (HTTP Basic, см. §3.4).
GATEWAY_ONEC_USER=odata_gateway
GATEWAY_ONEC_PASSWORD=<1c-user-password>
# Таймаут одного HTTP-запроса к 1C, с (воркер доставки; тюнинг — §3.4).
GATEWAY_ONEC_TIMEOUT_SECONDS=15

# --- n8n Dead Letter Queue ---
GATEWAY_N8N_DLQ_URL=http://n8n:5678/webhook/dlq-fallback

# --- SQLite / доставка ---
GATEWAY_DB_PATH=/app/data/gateway.db
GATEWAY_BUSY_TIMEOUT_MS=5000
GATEWAY_DB_CONNECT_TIMEOUT_SECONDS=5.0
GATEWAY_DISPATCH_BATCH_LIMIT=50
# Пауза между проходами очереди egress-worker, с.
GATEWAY_POLL_INTERVAL=2

# --- Egress pacing (env-only, см. §7.4) ---
# Блокирующий token-bucket pacer на каждом плече (не бюджет): спит до токена.
GATEWAY_ONEC_RATE_LIMIT_RPS=2.0
GATEWAY_ONEC_RATE_LIMIT_BURST=5
GATEWAY_AMOCRM_RATE_LIMIT_RPS=2.0
GATEWAY_AMOCRM_RATE_LIMIT_BURST=5
```

Назначение ключей:

| Переменная | Обязательна | Назначение |
|---|---|---|
| `GATEWAY_HMAC_SECRET` | да | ключ HMAC-SHA256 для `X-Signature` |
| `GATEWAY_ONEC_BASE_URL` | да | корень публикации 1C (например `http://1c-server:8314/base`) |
| `GATEWAY_ONEC_USER` | да (в проде) | штатный логин воркера доставки для HTTP Basic (см. §3.4) |
| `GATEWAY_ONEC_PASSWORD` | да (в проде) | штатный пароль воркера доставки для HTTP Basic (см. §3.4) |
| `GATEWAY_ONEC_TIMEOUT_SECONDS` | нет | таймаут одного запроса к 1C, с (по умолчанию `15`) |
| `GATEWAY_POLL_INTERVAL` | нет | интервал опроса очереди `egress-worker`, с (по умолчанию `2`) |
| `GATEWAY_N8N_DLQ_URL` | нет | резервный вебхук n8n; пусто — DLQ отключён |
| `GATEWAY_DB_PATH` | нет | файл SQLite (по умолчанию `./gateway.db`) |
| `GATEWAY_BUSY_TIMEOUT_MS` | нет | ожидание блокировки SQLite, мс (по умолчанию `5000`) |
| `GATEWAY_DB_CONNECT_TIMEOUT_SECONDS` | нет | timeout соединения с SQLite (по умолчанию `5.0`) |
| `GATEWAY_DISPATCH_BATCH_LIMIT` | нет | размер батча доставки (по умолчанию `50`) |
| `GATEWAY_ONEC_RATE_LIMIT_RPS` | нет | pacer 1C, запросов в секунду (по умолчанию `2.0`, §7.4) |
| `GATEWAY_ONEC_RATE_LIMIT_BURST` | нет | pacer 1C, burst (по умолчанию `5`, §7.4) |
| `GATEWAY_AMOCRM_RATE_LIMIT_RPS` | нет | pacer amoCRM, запросов в секунду (по умолчанию `2.0`, §7.4) |
| `GATEWAY_AMOCRM_RATE_LIMIT_BURST` | нет | pacer amoCRM, burst (по умолчанию `5`, §7.4) |

> **`GATEWAY_HOST` / `GATEWAY_PORT` — только для ручного запуска.**
> В Docker-контуре адрес прослушивания **зафиксирован**: контейнер стартует с
> `CMD ["uvicorn", "gateway.app:app", "--host", "0.0.0.0", "--port", "8000"]`,
> а на хост публикуется loopback-проброс `127.0.0.1:8000` (см. `docker-compose.yml`).
> Значения этих переменных из `.env` применяются **только при ручном локальном
> запуске Uvicorn вне контейнера**:
>
> ```bash
> uvicorn gateway.app:app --host "${GATEWAY_HOST:-0.0.0.0}" --port "${GATEWAY_PORT:-8000}"
> ```
>
> При `docker compose up -d --build` эти две переменные на адрес/порт контейнера
> не влияют — менять их в `.env` бессмысленно.
>
> **`GATEWAY_ONEC_USER` / `GATEWAY_ONEC_PASSWORD` — штатные учётные данные
> воркера доставки.** Ядро `gateway/config.py` (класс `Settings`) их не читает,
> потому что сам шлюз в 1C не ходит: транспорты `gateway/app.py` по умолчанию
> заглушены (`_no_transport` / `_no_dlq`). Эти ключи читает и использует
> **`scripts/egress_worker.py`** (сервис `egress-worker`): из них собирается
> заголовок `Authorization: Basic ...` для каждого запроса к OData (§3.4).
> Без них воркер пишет в лог предупреждение и шлёт запросы в 1C **без
> авторизации** → HTTP 401 и уход событий в `FAILED`. Поэтому в проде обе
> переменные обязательны.
>
> **Корень публикации 1C.** `GATEWAY_ONEC_BASE_URL` — корень **без** сегмента
> `/odata/standard.odata`: воркер сам дописывает `/odata/standard.odata/<Entity>`.
> В `.env.example` уже указано корректное значение `http://1c-server:8314/base`;
> если подставить `.../base/odata/standard.odata`, получится дублирующийся путь
> `/odata/standard.odata/odata/standard.odata/...` (§3.2).

### 1.3. Подъём стека

```bash
docker compose up -d --build
```

Команда собирает образ и запускает **два сервиса** (в один контейнер — ничего
не «сворачивается»):

| Сервис | Команда | Назначение | Порты |
|---|---|---|---|
| `gateway` | `uvicorn gateway.app:app --host 0.0.0.0 --port 8000` | приём/валидация вебхуков, `GET /health` | `127.0.0.1:8000->8000/tcp` (только loopback хоста) |
| `egress-worker` | `python -m scripts.egress_worker` | фоновая доставка `VALIDATED`/`FAILED` в 1C OData | портов не публикует |

```bash
docker compose ps
```

Реальный вывод — оба контейнера в статусе `Up (healthy)`:

```
NAME                           IMAGE                        COMMAND                  SERVICE         CREATED          STATUS                    PORTS
amocrm-1c-resilient-gateway-egress-worker-1   amocrm-1c-resilient-gateway-egress-worker   "python /app/scripts…"   egress-worker   2 hours ago      Up 2 hours (healthy)      8000/tcp
amocrm-1c-resilient-gateway-gateway-1         amocrm-1c-resilient-gateway-gateway         "python /app/scripts…"   gateway         2 hours ago      Up 2 hours (healthy)      127.0.0.1:8000->8000/tcp
```

`STATUS ... (healthy)` означает, что healthcheck-пробы прошли:

- у **`gateway`** — HTTP-проба образа `curl -f http://localhost:8000/health`
  (интервал 30 с, таймаут 5 с, 3 ретрая);
- у **`egress-worker`** — проба общего SQLite из `docker-compose.yml`
  (`sqlite3.connect($GATEWAY_DB_PATH); SELECT 1`), т.к. HTTP-порта у воркера нет.

Логи обоих сервисов (`-f` — в режиме follow, выход `Ctrl+C` не останавливает
контейнеры):

```bash
docker compose logs -f gateway
docker compose logs -f egress-worker
```

Ожидаемая строка запуска в логах `gateway`:

```
Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
```

### 1.4. Проверка статуса

```bash
curl -f http://127.0.0.1:8000/health
```

Ожидаемый ответ (HTTP 200):

```json
{"status":"ok","service":"amocrm-1c-gateway"}
```

Тот же вызов выполняет Docker `HEALTHCHECK` (интервал 30 с, таймаут 5 с,
3 ретрая). Если `curl -f` завершился ошибкой — смотрите
`docker compose logs gateway`.

### 1.5. Проверка записи в хранилище

```bash
docker compose exec gateway python -c \
  "import sqlite3,os;c=sqlite3.connect(os.environ.get('GATEWAY_DB_PATH','/app/data/gateway.db'));print(c.execute('select count(*) from events').fetchone())"
```

---

## 2. Интеграция с amoCRM (Ingress)

### 2.1. Алгоритм настройки вебхука в amoCRM

1. Войдите в amoCRM под администратором аккаунта.
2. Откройте **amoCRM → АмоМаркет / Настройки → Интеграции → Webhooks**.
3. Нажмите **«Добавить вебхук»** (или «Создать вебхук»).
4. В поле **URL** укажите адрес шлюза через публичный домен:
   `https://your-domain.com/webhook/amocrm`.
5. Отметьте отслеживаемые события:
   - **«Сделка создана»**;
   - **«Смена статуса сделки»**.
6. Сохраните вебхук. amoCRM начнёт POST-запросы с
   `Content-Type: application/json`.

> amoCRM **не отправляет** заголовки `X-Signature`, `X-Timestamp`, `X-Nonce`
> и не формирует тело в контракте `WebhookPayload`. Поэтому перед шлюзом
> обязателен релей-адаптер (§2.5), который преобразует нативный payload
> amoCRM, добавляет заголовки и подпись. Прямой URL amoCRM → шлюз без релея
> будет отклонён (403 «Missing required header: X-Signature»).

### 2.2. Обязательные заголовки запроса

| Заголовок | Обязателен | Формат | Проверка в ядре |
|---|---|---|---|
| `Content-Type` | да | `application/json` (charset допускается) | иначе 403 `Unsupported Content-Type` |
| `X-Signature` | да | hex HMAC-SHA256, допускается префикс `sha256=` | иначе 403; неверная — 401 |
| `X-Timestamp` | да | ISO-8601 с зоной или epoch-секунды | вне окна 300 с — 403 |
| `X-Nonce` | да | непустая строка, уникальная | повтор в журнале — 403 |

Порядок проверок в `authenticate_request()` фиксирован:
**rate limit (429) → header guard (403) → HMAC (401) → timestamp/replay (403)**.
Дублирующиеся критичные заголовки отклоняются до проверки подписи (403).

### 2.3. Защита от Replay-атак (окно 300 с)

Источник параметров — секция `replay_guard` в `config/gateway.json`
(переопределяется через env):

| Параметр | Значение | Env-override | Смысл |
|---|---|---|---|
| `timestamp_window_seconds` | **300** | `GATEWAY_REPLAY_GUARD_TIMESTAMP_WINDOW_SECONDS` | максимальное расхождение `X-Timestamp` с серверным временем |
| `nonce_journal_size` | 10000 | `GATEWAY_REPLAY_GUARD_NONCE_JOURNAL_SIZE` | размер скользящего nonce-журнала |

Правила:
- `X-Timestamp` вне окна ±300 с → HTTP 403,
  `message: "Timestamp outside the replay window"`, `location: "replay"`.
- Повторно использованный `X-Nonce` (пока он в журнале) → HTTP 403,
  `message: "Nonce has already been used (replay)"`.
- Nonce генерируется на каждый запрос (UUID4). Журнал ограничен
  `nonce_journal_size` записями (FIFO-вытеснение).
- Дополнительно: лимит частоты `rate_limit` = `token_bucket`, 100 запросов
  на IP за 60 с (`GATEWAY_RATE_LIMIT_*`), превышение → HTTP 429.

Подпись считается по **сырому телу запроса** (до парсинга JSON). Любая
пересериализация тела после подписи ломает проверку.

```python
import hashlib, hmac

secret = "<GATEWAY_HMAC_SECRET>"
raw_body = b'{"idempotency_key":"amo-123:2026-10-16T10:00:00+00:00", ...}'
signature = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
# header: X-Signature: <signature>
```

### 2.4. Контракт тела (`WebhookPayload`)

Тело — JSON-объект, строго без лишних полей (`extra=forbid`):

```json
{
  "idempotency_key": "amo-123:2026-10-16T10:00:00+00:00",
  "event_type": "order",
  "occurred_at": "2026-10-16T10:00:00+00:00",
  "nonce": "6f1c0e2a-8f52-4a7c-9e2b-2a3c44d1a9a1",
  "data": { }
}
```

| Поле | Тип | Ограничение |
|---|---|---|
| `idempotency_key` | string | непустое, уникальное; повторный ключ → 200 `status: duplicate` |
| `event_type` | string | одно из `lead`, `order`, `counterparty` (иначе 422) |
| `occurred_at` | datetime | время события в источнике |
| `nonce` | string | непустое |
| `data` | object | модель события по `event_type` (см. §2.6) |

Маршрутизация `event_type` → схема (`gateway/contract_validator.py`):

| `event_type` | Схема `data` | Поведение Egress |
|---|---|---|
| `lead` | `AmoLeadCreateSchema` | сохраняется, 1C **не вызывается** (событие сразу `DELIVERED`) |
| `order` | `OneCOrderCreateSchema` | POST в `Document_ЗаказПокупателя` |
| `counterparty` | `OneCCounterpartySchema` | POST в `Catalog_Контрагенты` |

### 2.5. Релей amoCRM → шлюз (n8n)

Рекомендуемая схема: `amoCRM → n8n Webhook → Code (адаптер) → HTTP Request → шлюз`.

**Code node (JavaScript, n8n):**

```javascript
const crypto = require('crypto');
const secret = $env.GATEWAY_HMAC_SECRET;          // тот же, что в .env шлюза
const body = $input.first().json.body;             // нативный payload amoCRM

// 1. Трансляция сделки amoCRM в контракт шлюза.
const lead = body.leads && body.leads.add
  ? body.leads.add[0]
  : (body.leads && body.leads.update ? body.leads.update[0] : body);
const leadId = lead.id;
const occurredAt = new Date((lead.updated_at || lead.created_at || Date.now() / 1000) * 1000).toISOString();

const envelope = {
  idempotency_key: `amo-lead-${leadId}:${occurredAt}`,
  event_type: 'order',
  occurred_at: occurredAt,
  nonce: crypto.randomUUID(),
  data: {
    number: `SO-${leadId}`,
    date: occurredAt,
    counterparty_ref: `cp-${leadId}`,
    items: [{ sku: 'SERVICE', quantity: 1, price: Number(lead.price || 0) }],
    total: Number(lead.price || 0)                  // бюджет сделки → сумма документа
  }
};

// 2. Подпись сырого тела (порядок ключей важен: подписываем итоговую строку).
const raw = JSON.stringify(envelope);
const signature = crypto.createHmac('sha256', secret).update(raw).digest('hex');

return [{
  json: envelope,
  headers: {
    'Content-Type': 'application/json',
    'X-Signature': signature,
    'X-Timestamp': new Date().toISOString(),
    'X-Nonce': envelope.nonce
  },
  rawBody: raw
}];
```

**HTTP Request node:** `POST https://your-domain.com/webhook/amocrm`,
`Body Content-Type: application/json`, тело = `rawBody` из Code node,
заголовки — из `headers`. Отправляйте именно байты `rawBody`, иначе подпись
не сойдётся.

### 2.6. Маппинг полей amoCRM → контракты шлюза

| Источник amoCRM | Контракт шлюза | Примечание |
|---|---|---|
| `lead.id` | `order.number` = `SO-<id>` | соглашение о номере |
| `lead.updated_at` / `created_at` | `order.date`, `occurred_at` | ISO-8601 |
| `lead.price` (бюджет) | `order.total` | бюджет → сумма документа |
| `lead.id` | `order.counterparty_ref` | ссылка на контрагента |
| товарные позиции (если есть) | `order.items[].sku/quantity/price` | минимум 1 позиция |
| `lead.status_id` | `lead.status_id` (`event_type: lead`) | журналируется, в 1C не уходит |
| `lead.name` | `lead.name` | |
| `lead.tags` | `lead.tags` | список строк |
| поле «ИНН» контрагента | `counterparty.inn` | ровно 10 или 12 цифр |
| компания/название | `counterparty.name`, `legal_name` | |
| телефон / e-mail / адрес | `counterparty.phone/email/address` | опционально |

Схемы `data` (все `extra=forbid`):

```jsonc
// event_type = "order"  -> OneCOrderCreateSchema
{"number":"SO-123","date":"2026-10-16T10:00:00+00:00",
 "counterparty_ref":"cp-123","items":[{"sku":"SKU-1","quantity":2,"price":5.0}],
 "total":10.0}

// event_type = "counterparty" -> OneCCounterpartySchema
{"inn":"1234567890","name":"Romashka","legal_name":"ООО Ромашка",
 "phone":"+7-000-000-00-00","email":"ops@example.com","address":"Moscow"}

// event_type = "lead" -> AmoLeadCreateSchema
{"amo_lead_id":123,"name":"Сделка","budget":10.0,"status_id":142,
 "created_at":"2026-10-16T10:00:00+00:00","tags":["vip"]}
```

#### Ошибки валидации 422

Причин отказа схемы больше, чем принято перечислять «по памяти» — ниже
полный список того, что чаще всего прилетает как **422** (все модели
объявлены с `extra="forbid"`, поэтому лишнее поле гасит запрос целиком):

| # | Причина | Где проверяется | Пример «плохого» значения |
|---|---|---|---|
| 1 | лишнее (необъявленное) поле | любой уровень: envelope и `data` | `"qty": 1` в `items[0]` |
| 2 | **отрицательный `total < 0`** | `order.total` (`ge=0`) | `"total": -100` |
| 3 | **пустой `number`** | `order.number` (`min_length=1`) | `"number": ""` |
| 4 | **пустой `counterparty_ref`** | `order.counterparty_ref` (`min_length=1`) | `"counterparty_ref": ""` |
| 5 | **пустой `name`** | `counterparty.name`, `lead.name` (`min_length=1`) | `"name": ""` |
| 6 | **невалидный формат даты** | `occurred_at`, `order.date`, `lead.created_at` (`datetime`) | `"date": "16.10.2026"` |
| 7 | пустой `idempotency_key` / `nonce` / `event_type` | envelope (`min_length=1`) | `"nonce": ""` |
| 8 | неизвестный `event_type` | маршрутизация `lead`/`order`/`counterparty` | `"event_type": "payment"` |
| 9 | `items` пуст (нужна ≥ 1 позиция) | `order.items` | `"items": []` |
| 10 | `quantity <= 0`, `price < 0`, пустой `sku` | `order.items[]` | `"quantity": 0` |
| 11 | `budget < 0` | `lead.budget` (`ge=0`) | `"budget": -5` |
| 12 | `inn` не ровно 10 или 12 цифр | `counterparty.inn` | `"inn": "12345"` |

Помимо схемы, **422** возвращается и за синтаксис тела: пустое тело, не-UTF-8,
невалидный JSON, JSON-массив вместо объекта (см. §2.8, `location: body`).

### 2.7. Smoke-тест и дедупликация (два изолированных блока)

Проверки **нельзя смешивать**: у них разные входные данные и разный ожидаемый
ответ. Ключи дедупликации, nonce и метки времени генерируются по-разному, поэтому
ниже два независимых блока — запускайте каждый из чистого шелла.

#### 2.7.1. Smoke-тест (первичный приём) — ожидаем `200 {"status":"validated", ...}`

Скрипт каждый раз генерирует **свежий `idempotency_key`, свежий `NONCE` и свежую
`TIMESTAMP`**, поэтому повторный запуск никогда не «упирается» в replay-защиту:

```bash
SECRET="<GATEWAY_HMAC_SECRET>"
TS="$(date -u +%Y-%m-%dT%H:%M:%S+00:00)"
NONCE="$(cat /proc/sys/kernel/random/uuid)"
BODY='{"idempotency_key":"smoke-'"$NONCE"'","event_type":"order","occurred_at":"'"$TS"'","nonce":"'"$NONCE"'","data":{"number":"SO-SMOKE","date":"'"$TS"'","counterparty_ref":"cp-1","items":[{"sku":"SKU-1","quantity":1,"price":1.0}],"total":1.0}}'

SIG="$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$SECRET" -hex | awk '{print $NF}')"

curl -sS -o - -w '\nHTTP %{http_code}\n' \
  -X POST https://your-domain.com/webhook/amocrm \
  -H "Content-Type: application/json" \
  -H "X-Signature: $SIG" \
  -H "X-Timestamp: $TS" \
  -H "X-Nonce: $NONCE" \
  --data-raw "$BODY"
```

Ожидаемо:

```
{"status":"validated","event_id":<N>}
HTTP 200
```

`event_id` монотонно растёт — события легли в `events` со статусом `VALIDATED`
(доставку выполнит `egress-worker`, §3.4).

#### 2.7.2. Тест дедупликации — ожидаем `200 {"status":"duplicate", ...}`

Здесь **тот же `idempotency_key`** (зашит в скрипт литерально), но **свежие
`NONCE` и `TIMESTAMP`**:

```bash
SECRET="<GATEWAY_HMAC_SECRET>"
KEY="smoke-dedup-2026-10-16T10:00:00+00:00"           # фиксированный = повторный ключ
TS="$(date -u +%Y-%m-%dT%H:%M:%S+00:00)"               # СВЕЖАЯ метка времени
NONCE="$(cat /proc/sys/kernel/random/uuid)"            # СВЕЖИЙ nonce
BODY='{"idempotency_key":"'"$KEY"'","event_type":"order","occurred_at":"'"$TS"'","nonce":"'"$NONCE"'","data":{"number":"SO-DEDUP","date":"'"$TS"'","counterparty_ref":"cp-1","items":[{"sku":"SKU-1","quantity":1,"price":1.0}],"total":1.0}}'

SIG="$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$SECRET" -hex | awk '{print $NF}')"

curl -sS -o - -w '\nHTTP %{http_code}\n' \
  -X POST https://your-domain.com/webhook/amocrm \
  -H "Content-Type: application/json" \
  -H "X-Signature: $SIG" \
  -H "X-Timestamp: $TS" \
  -H "X-Nonce: $NONCE" \
  --data-raw "$BODY"
```

Ожидаемо — **при любом числе повторных запусков**:

```
{"status":"duplicate","event_id":<N>}
HTTP 200
```

**Механика.** Шлюз отсекает дубли по `idempotency_key` **только при валидном
новом nonce**: порядок проверок в `process_webhook()` жёсткий —
`rate limit (429) → заголовки (403) → HMAC (401) → timestamp/nonce (403) →
валидация схемы (422) → INSERT`. Ключ дедупликации проверяется последним, на
шаге `INSERT ... ON CONFLICT(idempotency_key) DO NOTHING`
(`gateway/state_store.py`), то есть:

- **тот же ключ + свежий nonce + валидная подпись** → 200 `status: duplicate`,
  `event_id` указывает на уже сохранённое событие, в базе ничего не меняется;
- **тот же ключ + тот же nonce** → 403 `Nonce has already been used (replay)`,
  до сравнения ключа дело вообще не доходит;
- **тот же ключ + просроченный timestamp** → 403 `Timestamp outside the replay
  window` — и снова дедупликация не срабатывает;
- **неверная подпись** → 401, независимо от ключа.

Поэтому именно повторный прогон **второго** блока (новые nonce/timestamp, старый
ключ) и есть штатный способ убедиться, что дедупликация работает. Простой
повтор **первого** блока даст снова `validated` — ключ там новый каждый раз.

### 2.8. Коды ответов и формат ошибок

| Код | Причина |
|---|---|
| 200 | событие принято (`validated`) или дубликат (`duplicate`) |
| 401 | неверная подпись HMAC-SHA256 |
| 403 | replay / отсутствие или дубль критичного заголовка / неверный `Content-Type` |
| 422 | тело не прошло парсинг/валидацию (`WebhookPayload` или модель `data`) |
| 429 | превышен rate limit по IP |

Формат ошибки (`ErrorShape`, всегда 5 полей):

```json
{
  "type": "security|schema|semantic|dependency",
  "message": "human readable",
  "severity": "critical|warning",
  "location": "headers|body|signature|replay|rate_limit|egress.onec|egress.amocrm|egress.amocrm.rate_limit",
  "how_to_fix": "concrete corrective action"
}
```

### 2.9. Операционное замечание

`POST /webhook/amocrm` **только принимает и сохраняет** событие: ответ 200
означает `status: VALIDATED`, а не доставку в 1C. Доставка выполняется
egress-воркером (§3.4), поэтому в проде воркер обязателен; без него события
остаются в статусе `VALIDATED`.

---

## 3. Интеграция с 1C (Egress OData)

### 3.1. Публикация стандартного интерфейса OData в 1C

1. Откройте базу 1C и выполните публикацию стандартного интерфейса OData:
   - **в конфигураторе** — раздел **«Администрирование» → «Публикация OData-сервиса»**;
   - **в клиент-серверной базе** — через панель администрирования сервера 1C
     (`rac` / веб-панель) либо штатную обработку публикации.
2. Включите **«Стандартный интерфейс»** (Standard OData). Шлюз работает с
   версией **v2** — путь публикации `/odata/standard.odata`.
3. В списке публикуемых объектов отметьте:
   - `Document_ЗаказПокупателя` (Документ «Заказ покупателя»);
   - `Catalog_Контрагенты` (Справочник «Контрагенты»).
4. Включите аутентификацию по HTTP (**Basic**). Создайте пользователя 1C
   `odata_gateway` с правами на чтение и запись этих объектов; его логин и
   пароль пойдут в `GATEWAY_ONEC_USER` / `GATEWAY_ONEC_PASSWORD`.
5. Проверьте публикацию. `1c-server` — это имя из Docker DNS-сети Compose,
   оно резолвится **только внутри контейнеров**, поэтому `curl` запускаем
   через `docker compose exec` в контейнере `gateway` (переменные
   `$GATEWAY_ONEC_*` там уже подставлены из `.env`):

```bash
docker compose exec gateway sh -c \
  'curl -sS -f -u "$GATEWAY_ONEC_USER:$GATEWAY_ONEC_PASSWORD" \
        -H "Accept: application/json" \
        "$GATEWAY_ONEC_BASE_URL/odata/standard.odata/\$metadata"' | head
```

Эквивалент с буквальными значениями (если переменные не заданы в `.env`):

```bash
docker compose exec gateway curl -sS -f \
  -u 'odata_gateway:<1c-user-password>' \
  -H 'Accept: application/json' \
  'http://1c-server:8314/base/odata/standard.odata/$metadata' | head
```

Успешный ответ — XML/JSON-описание метаданных (`edmx:Edmx`). Если проверяете
шлюз, запущенный **вне Docker** (ручной `uvicorn` на хосте) и `1c-server`
доступен с хоста напрямую — достаточно обычного `curl` без префикса
`docker compose exec`.

### 3.2. Целевые адреса и сущности

Шлюз формирует URL как `${GATEWAY_ONEC_BASE_URL} + <PATH>`
(`gateway/egress_dispatcher.py`, `ORDER_PATH` / `COUNTERPARTY_PATH`):

| Сущность | Метод | URL |
|---|---|---|
| Заказ покупателя | POST | `${GATEWAY_ONEC_BASE_URL}/odata/standard.odata/Document_ЗаказПокупателя` |
| Контрагенты | POST | `${GATEWAY_ONEC_BASE_URL}/odata/standard.odata/Catalog_Контрагенты` |

Пример при `GATEWAY_ONEC_BASE_URL=http://1c-server:8314/base`:

```
http://1c-server:8314/base/odata/standard.odata/Document_ЗаказПокупателя
http://1c-server:8314/base/odata/standard.odata/Catalog_Контрагенты
```

> `GATEWAY_ONEC_BASE_URL` — **корень публикации** (`http://1c-server:8314/base`).
> Не включайте `/odata/standard.odata` — иначе путь продублируется.
> Кириллица в имени сущности отправляется как есть; HTTP-клиент воркера
> кодирует её в URL автоматически.

Тело запроса = `model_dump(mode="json")` соответствующей схемы
(`OneCOrderCreateSchema` / `OneCCounterpartySchema`), заголовки —
`Content-Type: application/json`, `Accept: application/json`.

### 3.3. Маппинг кастомных полей

| Поле amoCRM | Поле контракта | Поле 1C OData | Трансляция |
|---|---|---|---|
| ИНН контрагента | `counterparty.inn` | `Catalog_Контрагенты.inn` | строка, ровно 10 (ЮЛ) или 12 (ИП) цифр |
| Название/компания | `counterparty.name` | `Catalog_Контрагенты.name` | |
| Полное юр. имя | `counterparty.legal_name` | `Catalog_Контрагенты.legal_name` | опционально |
| Телефон / e-mail / адрес | `counterparty.phone/email/address` | одноимённые поля | опционально |
| **Бюджет сделки** (`lead.price`) | `order.total` | `Document_ЗаказПокупателя.total` | **сумма документа** = бюджет |
| Номер сделки | `order.number` | `Document_ЗаказПокупателя.number` | соглашение `SO-<lead.id>` |
| Дата изменения сделки | `order.date` | `Document_ЗаказПокупателя.date` | ISO-8601 |
| Контрагент сделки | `order.counterparty_ref` | `Document_ЗаказПокупателя.counterparty_ref` | ссылка на контрагента |
| Товарные позиции | `order.items[]` | строки документа | при отсутствии — 1 служебная позиция на всю сумму |
| **Статус сделки** (`lead.status_id`) | `lead.status_id` (`event_type: lead`) | — | журналируется в SQLite; текущий контракт заказа не содержит поля статуса |

> Трансляция статуса сделки в статус документа 1C текущим контрактом
> `OneCOrderCreateSchema` не предусмотрена: событие `lead` фиксируется в
> журнале, а статус документа выставляется правилами самой 1C (или потребует
> расширения схемы — см. `agent.md`, раздел «Примеры задач»).

Пример тела, уходящего в 1C:

```json
{
  "number": "SO-123",
  "date": "2026-10-16T10:00:00+00:00",
  "counterparty_ref": "cp-123",
  "items": [{"sku": "SERVICE", "quantity": 1, "price": 15000.0}],
  "total": 15000.0
}
```

### 3.4. Egress-воркер доставки

`dispatch_event()` и `run_delivery_pass()` — библиотечные функции: FastAPI-приложение
их само не вызывает, а транспорты по умолчанию заглушены (`gateway/app.py`,
`_no_transport` / `_no_dlq`).

**Воркер уже входит в состав проекта — писать его отдельно не нужно.**
Реализация лежит в **`scripts/egress_worker.py`**, и `docker-compose.yml`
оркестрирует её автоматически сервисом **`egress-worker`**: тот же образ, та же
команда `python -m scripts.egress_worker`, общий том `./data` (SQLite WAL).
`docker compose up -d --build` поднимает и шлюз, и воркер — никаких
дополнительных действий по развёртыванию не требуется.

Цикл воркера (`main()` в `scripts/egress_worker.py`):

1. раз в `GATEWAY_POLL_INTERVAL` секунд (по умолчанию **2 с**) читает батч
   `VALIDATED`/`FAILED`-событий размером до `GATEWAY_DISPATCH_BATCH_LIMIT`
   (50);
2. каждое событие доставляет через `dispatch_event()` — те же ретраи,
   exponential backoff и circuit breaker, что описаны в §4.1;
3. `FAILED`-события переотправляются **без повторной валидации**
   (`resend_failed()`), финальные отказы уходят в n8n DLQ (§4.3);
4. HTTP Basic (`GATEWAY_ONEC_USER` / `GATEWAY_ONEC_PASSWORD`) подставляется в
   каждый запрос к OData;
5. процесс однопоточный и изолирует ошибки по событию: исключение на одном
   ряде (битый payload, `SQLITE_BUSY`) логируется и не останавливает батч;
6. `SIGTERM`/`SIGINT` воркер перехватывает, дозавершает текущий проход и
   закрывает httpx-пулы и SQLite (поэтому в compose задан
   `stop_grace_period: 45s`).

Переменные окружения воркера:

| Переменная | Дефолт | Назначение |
|---|---|---|
| `GATEWAY_POLL_INTERVAL` | `2` | пауза между проходами очереди, с (алиасы: `POLL_INTERVAL`, `GATEWAY_WORKER_INTERVAL_SECONDS`) |
| `GATEWAY_ONEC_TIMEOUT_SECONDS` | `15` | таймаут одного HTTP-запроса к 1C, с |
| `GATEWAY_ONEC_USER` / `GATEWAY_ONEC_PASSWORD` | — | HTTP Basic для OData; не заданы → запросы уходят **без** авторизации (предупреждение в логе, 401 от 1C, события в `FAILED`) |
| `GATEWAY_ONEC_BASE_URL` | `https://1c.example` | корень публикации OData (§3.2) |
| `GATEWAY_N8N_DLQ_URL` | `""` | пусто → DLQ выключен, события остаются в `FAILED` |
| `GATEWAY_DISPATCH_BATCH_LIMIT` | `50` | размер батча за один проход |
| `GATEWAY_LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` |

**Таймауты и тюнинг под медленные OData-базы:**

| Параметр | Значение по умолчанию | Где читается |
|---|---|---|
| `GATEWAY_ONEC_TIMEOUT_SECONDS` | **15 с** | `scripts/egress_worker.py` → `httpx.Client(timeout=...)` |
| `half_open_probe_interval` | **5 с** | `config/gateway.json`, override `GATEWAY_CIRCUIT_BREAKER_HALF_OPEN_PROBE_INTERVAL` |

Правила:

- **симптом «база медленная»**: в логе воркера `ReadTimeout` /
  `1C OData returned HTTP ...`, при этом база жива и отвечает вручную (§3.6).
  Лечится увеличением `GATEWAY_ONEC_TIMEOUT_SECONDS` (30–60 с), а не уменьшением
  интервала опроса: каждая неудача идёт в счётчик circuit breaker и в backoff
  (до 5 попыток, пауза до 30 с), то есть «медленный, но живой» запрос стоит
  до `5 × timeout` времени воркера на событие и зря уводит его в `FAILED`;
- **`timeout` (15 с) больше `half_open_probe_interval` (5 с) — это нормально.**
  В `half-open` цепь пропускает ровно одну probe-попытку за интервал, а пока
  она в полёте, воркер занят и новых probe не порождает: накопление параллельных
  probe невозможно, т.к. circuit breaker живёт в одном потоке одного процесса.
  Фактический ритм probe = `max(half_open_probe_interval, длительность запроса)`;
- если `GATEWAY_ONEC_TIMEOUT_SECONDS` поднимается до 30–60 с, пропорционально
  поднимите `GATEWAY_CIRCUIT_BREAKER_COOLDOWN_SECONDS` (30 → 60–90) и
  `half_open_probe_interval` (5 → не меньше медианной длительности запроса),
  иначе восстановление цепи начнёт определяться таймаутом, а не регламентом;
- `GATEWAY_ONEC_TIMEOUT_SECONDS` не должен превышать `stop_grace_period: 45s`
  из `docker-compose.yml`, иначе при остановке стека in-flight запрос будет
  убит до graceful-завершения.

Требования к эксплуатации:
- запускайте **один процесс** шлюза (`uvicorn ... --workers 1`) и **один
  экземпляр** `egress-worker`: replay- и rate-limit-кэши живут в памяти процесса
  шлюза, а circuit breaker — в процессе воркера (у каждого воркера своя копия
  цепи);
- батч ограничен `GATEWAY_DISPATCH_BATCH_LIMIT` (по умолчанию 50).

> Канонический источник по поведению воркера — сам файл
> [`scripts/egress_worker.py`](../scripts/egress_worker.py). Приводить его
> копию в руководстве незачем: любое расхождение снижает ценность документа.

### 3.5. Таймауты блокировки SQLite (`GATEWAY_BUSY_TIMEOUT_MS`)

`GATEWAY_BUSY_TIMEOUT_MS` управляет не таймаутом 1C, а ожиданием блокировки
SQLite: при открытии БД выполняется `PRAGMA busy_timeout=<мс>`
(`gateway/state_store.py`). Запись защищена транзакциями `BEGIN IMMEDIATE`,
поэтому вторая параллельная запись ожидает освобождения блокировки не более
`GATEWAY_BUSY_TIMEOUT_MS`, после чего получает `SQLITE_BUSY`
(`database is locked`).

Практика:

| Симптом | Действие |
|---|---|
| периодические `database is locked` при пиковой нагрузке | поднять `GATEWAY_BUSY_TIMEOUT_MS` до 10000–15000 |
| задержка ответа `/webhook/amocrm` из-за ожидания блокировки | уменьшить `GATEWAY_DISPATCH_BATCH_LIMIT`, следить за длительностью транзакций |
| БД лежит на сетевом диске | недопустимо: SQLite WAL требует локального диска |

`GATEWAY_DB_CONNECT_TIMEOUT_SECONDS` (по умолчанию `5.0`) — timeout
установления соединения с файлом БД. `-wal`/`-shm` нельзя удалять при
работающем процессе; резервное копирование — только online-backup
(`sqlite3 gateway.db ".backup 'backup/gateway-YYYYMMDD.db'"`).

### 3.6. Чек-лист подключения внешних хостов

Сценарий: 1C опубликована **не** в compose-сети (не как `1c-server`), а на
внешнем IP / корпоративном хосте (`1c-corp.example.local`, `203.0.113.10`),
n8n — на своём узле, а шлюз остаётся в Docker. Пройдите пункты по порядку:

- [ ] **1. Адрес в `.env`.** Пропишите внешний адрес публикации:
      `GATEWAY_ONEC_BASE_URL=http://1c-corp.example.local:8314/base`
      (корень **без** `/odata/standard.odata` — §3.2), для n8n —
      `GATEWAY_N8N_DLQ_URL=https://n8n.example.local/webhook/dlq-fallback`.
      Убедитесь, что значение видит именно контейнер:

      ```bash
      docker compose exec gateway sh -c \
        'echo "onec=$GATEWAY_ONEC_BASE_URL dlq=$GATEWAY_N8N_DLQ_URL"'
      ```

- [ ] **2. DNS / hosts.** Контейнеры используют DNS-резолвер Docker-демона;
      запись из `/etc/hosts` хоста **не наследуется**. Варианты:
      - имя резолвится корпоративным DNS — ничего делать не нужно;
      - имя резолвится только через `/etc/hosts` хоста — перенесите его в
        `extra_hosts` соответствующего сервиса в `docker-compose.yml` либо
        укажите в `GATEWAY_ONEC_BASE_URL` IP-адрес/публичный домен;
      - проверка: `docker compose exec gateway getent hosts 1c-corp.example.local`
        (пусто — имени в контейнере нет).

- [ ] **3. Доступность OData и DLQ изнутри контейнера** (а не «с браузера»):

      ```bash
      docker compose exec gateway sh -c \
        'curl -sS -o /dev/null -w "%{http_code}\n" -m 10 \
              -u "$GATEWAY_ONEC_USER:$GATEWAY_ONEC_PASSWORD" \
              "$GATEWAY_ONEC_BASE_URL/odata/standard.odata/\$metadata"'
      ```

      Ожидаемо `200`; `000` или таймаут → сеть/файрвол (п. 6); `401` → п. 4.
      Доступность n8n DLQ проверяется так же, как в §4.3 (URL берите оттуда
      литерально — переменная `$GATEWAY_N8N_DLQ_URL` тоже существует в
      контейнере): `docker compose exec gateway curl -sS -o /dev/null -w "%{http_code}\n" -m 10 http://n8n:5678/webhook/dlq-fallback`.

- [ ] **4. Basic-авторизация.** `401` — неверные `GATEWAY_ONEC_USER` /
      `GATEWAY_ONEC_PASSWORD`, либо на публикации отключён Basic. `403`/`404` —
      пользователь есть, но нет прав на `Document_ЗаказПокупателя` /
      `Catalog_Контрагенты` (§3.1, п. 3–4).

- [ ] **5. TLS-сертификаты.** Если 1C/n8n отдают корпоративный или
      самоподписанный сертификат, контейнер откажет с
      `SSL: CERTIFICATE_VERIFY_FAILED`. Решение: положить CA-файл в контейнер
      (например, смонтировать рядом с `./data`) и задать в `.env`
      `SSL_CERT_FILE=/etc/ssl/certs/corp-ca.pem` — переменная читается обоими
      сервисами (`gateway` и `egress-worker`). Альтернатива — терминировать TLS
      на корпоративном прокси перед 1C и оставить внутренний участок HTTP.
      Проверка путей доверия:
      `docker compose exec gateway python -c "import ssl; print(ssl.get_default_verify_paths())"`.

- [ ] **6. Корпоративный файрвол и таймауты.** Пропустите трафик из подсети
      контейнеров на порты 1C (8314/443) и n8n (5678/443) и измерьте
      задержку:

      ```bash
      docker compose exec gateway sh -c \
        'time curl -sS -o /dev/null -m 15 http://1c-corp.example.local:8314/'
      ```

      Если по линии уходит > 5–10 с — поднимите `GATEWAY_ONEC_TIMEOUT_SECONDS`
      (правила тюнинга — §3.4): дефолтных 15 с может не хватить на медленный
      OData за файрволом, и живая база начнёт уходить в `FAILED`.

- [ ] **7. Smoke после правок.** Изменения `.env` в работающий контейнер не
      подмешиваются — пересоздайте его:

      ```bash
      docker compose up -d --force-recreate
      docker compose ps
      ```

      и прогоните проверки §2.7.1 (первичный приём) и §2.7.2 (дедупликация).

---

## 4. Регламент аварийного восстановления (DLQ & n8n Fallback)

### 4.1. Поведение Circuit Breaker при недоступности 1C

Реализация — `CircuitBreaker` в `gateway/egress_dispatcher.py`, три состояния
`closed / open / half-open`, потокобезопасен.

| Параметр | По умолчанию | Env-override | Смысл |
|---|---|---|---|
| `failure_threshold` | 5 | `GATEWAY_CIRCUIT_BREAKER_FAILURE_THRESHOLD` | подряд неудач до `open` |
| `cooldown_seconds` | 30.0 | `GATEWAY_CIRCUIT_BREAKER_COOLDOWN_SECONDS` | время `open` до `half-open` |
| `half_open_probe_interval` | 5.0 | `GATEWAY_CIRCUIT_BREAKER_HALF_OPEN_PROBE_INTERVAL` | интервал probe-попыток |

Сценарий при падении 1C:
1. `closed`: доставка идёт; каждая ошибка 1C увеличивает счётчик.
2. При достижении `failure_threshold` подряд неудач цепь **размыкается**
   (`open`). Рекомендуемый регламент — порог **3**:
   `GATEWAY_CIRCUIT_BREAKER_FAILURE_THRESHOLD=3`.
3. В `open` вызовы не выполняются; `dispatch_event()` прерывает цикл ретраев,
   переводит событие `DISPATCHED → FAILED` и отправляет его в n8n DLQ.
   Если попыток не было, ошибка формируется как
   `message: "Circuit breaker is open"`, `location: "egress.onec"`.
4. Через `cooldown_seconds` цепь переходит в `half-open`: пропускается ровно
   одна probe-попытка за `half_open_probe_interval`. Успех → `closed`
   (счётчик сброшен); неудача → снова `open`.
5. **Bypass для 429/401/403.** `classify_status()` относит 429 к классу
   `retry`, а 401/403 — к `auth`; эти классы **не** вызывают
   `breaker.record_failure()`. Троттлинг и проблемы авторизации не должны
   размыкать цепь, поэтому breaker реагирует только на `fail` (5xx) и
   транспортные ошибки. Классы описаны в `gateway/resilience.py`.

**RFC 9110 `Retry-After` — пол, не потолок.** Если ответ содержит заголовок
`Retry-After` (формат `delay-seconds` или `HTTP-date`), эффективная пауза
равна `max(jitter_backoff, retry_after)` (`retry_after_floor()`). Запрос
сервера о длинной паузе соблюдается всегда, даже если локальный jitter короче;
`Retry-After` никогда не сокращает задержку.

Ретраи внутри доставки: exponential backoff с full jitter
(`base_seconds=0.5`, `multiplier=2.0`, `max_seconds=30.0`, `jitter_ratio=1.0`,
`max_attempts=5`).

### 4.2. Ротация и хранение необработанных событий (`events`, статус `FAILED`)

Конечный автомат: `RECEIVED → VALIDATED → DISPATCHED → DELIVERED / FAILED`,
повторный проход `FAILED → VALIDATED` (без повторной валидации).

Схема таблицы `events` (`gateway/state_store.py`):

| Колонка | Тип | Назначение |
|---|---|---|
| `id` | INTEGER PK | номер события (возвращается в ответе API) |
| `idempotency_key` | TEXT UNIQUE | дедупликация входящих вебхуков |
| `payload` | TEXT | envelope `{event_type, data}` |
| `status` | TEXT CHECK | `RECEIVED/VALIDATED/DISPATCHED/DELIVERED/FAILED` |
| `attempt_count` | INTEGER | число попыток **текущего цикла** доставки (см. примечание) |
| `last_error` | TEXT | текст последней ошибки 1C |
| `created_at` / `updated_at` | TEXT | ISO-8601 UTC |

Индекс: `idx_events_status` по `status`.

> **`attempt_count` — счётчик текущего цикла, а не суммарный за жизнь события.**
> `record_attempt()` делает `UPDATE ... SET attempt_count=?`, то есть значение
> **перезаписывается**, а не накапливается: `dispatch_event()` начинает с `0`
> и записывает `1..max_attempts` (5) внутри одного прохода. Когда событие
> после `FAILED` уходит в новый цикл (`FAILED → VALIDATED → DISPATCHED`),
> счётчик стартует заново. Поэтому `MAX(attempt_count)` в запросах ниже — это
> максимум по *текущим* циклам, а не «сколько раз событие вообще доставлялось».
> Число же повторных циклов отражает `updated_at` (и сам факт перехода
> `DISPATCHED → FAILED`).

Диагностика и выгрузка «застрявших» событий:

```bash
# сводка по статусам
sqlite3 data/gateway.db \
  "SELECT status, COUNT(*), MAX(attempt_count) FROM events GROUP BY status;"

# очередь аварийного восстановления
sqlite3 -header -column data/gateway.db \
  "SELECT id, idempotency_key, attempt_count, last_error, updated_at
     FROM events WHERE status='FAILED' ORDER BY updated_at;"

# одно событие целиком
sqlite3 data/gateway.db "SELECT payload FROM events WHERE id=<N>;"
```

Ротация и хранение:
- `FAILED` — **не удалять** и не редактировать вручную: это очередь
  восстановления;
- `DELIVERED` — архив/удаление по регламенту, например старше 30 дней:

```bash
sqlite3 data/gateway.db \
  "DELETE FROM events WHERE status='DELIVERED' AND substr(updated_at,1,10) < date('now','-30 days');"
```

- перед ротацией — онлайн-бэкап; `VACUUM` только на остановленном сервисе;
- хранить `gateway.db` и WAL-файлы на локальном постоянном томе.

### 4.3. Подключение резервного вебхука n8n (`GATEWAY_N8N_DLQ_URL`)

`send_to_dlq()` отправляет в `GATEWAY_N8N_DLQ_URL` ровно один JSON с шестью
ключами:

```json
{
  "event_id": 42,
  "idempotency_key": "amo-lead-123:2026-10-16T10:00:00+00:00",
  "payload": {"event_type": "order", "data": {"number": "SO-123"}},
  "error": {
    "type": "dependency",
    "message": "1C OData returned HTTP 500",
    "severity": "critical",
    "location": "egress.onec",
    "how_to_fix": "Check 1C OData availability and payload correctness, then retry."
  },
  "attempts": 5,
  "failed_at": "2026-10-16T10:00:05+00:00"
}
```

Ошибка доставки самого DLQ не роняет шлюз: `send_to_dlq()` возвращает
`false`, событие остаётся в `FAILED` и будет доставлено следующим проходом.

**Настройка n8n (workflow fallback):**

1. Node **Webhook** — метод `POST`, path `dlq-fallback`
   (итоговый URL: `http://n8n:5678/webhook/dlq-fallback`).
2. Node **Switch/IF** — маршрутизация по `error.severity` / `error.type`
   (`critical` → немедленный алерт, `warning` → дневной дайджест).
3. Форматирование сообщения (Code node):

```javascript
const d = $json;
return [{ json: {
  text:
    `🚨 ${d.error.severity.toUpperCase()} 1C gateway\n` +
    `event_id: ${d.event_id}\n` +
    `type: ${d.error.type} @ ${d.error.location}\n` +
    `message: ${d.error.message}\n` +
    `attempts: ${d.attempts}\n` +
    `fix: ${d.error.how_to_fix}\n` +
    `failed_at: ${d.failed_at}`
}}];
```

4. Node **Telegram → Send Message** (`chatId` — ID дежурного чата/группы) и
   параллельно **Email → Send** (SMTP-креды n8n) для почтового алерта.
5. Проверка канала. `n8n` резолвится по Docker DNS **только внутри compose-сети**,
   поэтому `curl` выполняется из контейнера `gateway`:

```bash
# Адрес = значение GATEWAY_N8N_DLQ_URL из .env (здесь — значение по умолчанию
# из .env.example). JSON — в одну строку, в одинарных кавычках:
docker compose exec gateway curl -sS -X POST \
  http://n8n:5678/webhook/dlq-fallback \
  -H "Content-Type: application/json" \
  -d '{"event_id":0,"idempotency_key":"smoke","payload":{},"error":{"type":"dependency","message":"smoke test","severity":"critical","location":"egress.onec","how_to_fix":"none"},"attempts":0,"failed_at":"2026-10-16T00:00:00+00:00"}'
```

Если используете нестандартный URL, возьмите его из переменной контейнера:

```bash
docker compose exec gateway sh -c 'curl -sS -o /dev/null -w "%{http_code}\n" -X POST "$GATEWAY_N8N_DLQ_URL" -H "Content-Type: application/json" -d "{\"event_id\":0}"'
```

Если шлюз запущен **вне Docker** и `n8n` доступна с хоста, достаточно
`curl -sS -X POST "$GATEWAY_N8N_DLQ_URL" ...` с обычным подстановлением
переменной окружения.

Ожидаемо: n8n отвечает `200`, в Telegram/на почту приходит сообщение.

### 4.4. Runbook восстановления после аварии 1C

1. Убедиться, что 1C OData снова доступен **из контейнера шлюза**
   (на внешнем IP/корпоративном хосте — см. чек-лист §3.6):

   ```bash
   docker compose exec gateway sh -c \
     'curl -sS -f -m 10 -u "$GATEWAY_ONEC_USER:$GATEWAY_ONEC_PASSWORD" \
           "$GATEWAY_ONEC_BASE_URL/odata/standard.odata/\$metadata"' >/dev/null \
     && echo "1C OData OK" || echo "1C OData недоступна"
   ```

2. Остановить рост очереди: воркер сам переводит события в `FAILED`, ручных
   действий не требуется (circuit breaker защищает 1C от шквала запросов).
3. Проверить список:

   ```bash
   sqlite3 data/gateway.db "SELECT id, last_error FROM events WHERE status='FAILED';"
   ```
4. Запустить повторную доставку **без повторной валидации** — воркер делает
   это автоматически: `dispatch_event()` для `FAILED` вызывает
   `resend_failed()` (`FAILED → VALIDATED → DISPATCHED`). Разовый ручной
   прогон — из Python:
   `run_delivery_pass(conn, settings, client, dlq_transport, time.sleep)`.
5. Подтвердить исход: события переходят в `DELIVERED`; в 1C появляются
   документы/контрагенты.
6. Если 1C недоступен длительно — разобрать n8n-алерты, зафиксировать
   инцидент, при необходимости увеличить `cooldown_seconds`.

### 4.5. Мониторинг (минимальный набор)

```bash
# глубина очереди FAILED (алерт при росте)
sqlite3 data/gateway.db "SELECT COUNT(*) FROM events WHERE status='FAILED';"

# события, «зависшие» в промежуточных статусах дольше 15 минут
# ВАЖНО: updated_at хранится как "2026-10-19T08:00:00+00:00" (ISO-8601, разделитель
# "T"), а datetime('now','-15 minutes') возвращает "2026-10-19 07:45:00" (пробел).
# Прямое строковое сравнение ("updated_at < datetime(...)") для событий текущего
# дня всегда ложно: 'T' (0x54) > ' ' (0x20). Поэтому ISO-строку прогоняем через
# datetime() — SQLite приведёт обе стороны к одному типу.
sqlite3 data/gateway.db \
  "SELECT id, status, updated_at, last_error
     FROM events
    WHERE status IN ('VALIDATED', 'DISPATCHED')
      AND datetime(updated_at) < datetime('now', '-15 minutes');"

# healthcheck
curl -f http://127.0.0.1:8000/health
```

---

## 5. Приложение: переменные окружения `GATEWAY_*`

| Переменная | По умолчанию | Где читается |
|---|---|---|
| `GATEWAY_HMAC_SECRET` | `change-me-in-production` | `config.py` |
| `GATEWAY_ONEC_API_TOKEN` | `""` (только HMAC) | `config.py`; проверяется в `ingress_guard.py` на `POST /webhook/onec` |
| `GATEWAY_ONEC_BASE_URL` | `https://1c.example` | `config.py` |
| `GATEWAY_AMOCRM_BASE_URL` | `https://amocrm.example` | `config.py`; база для `AmoClient` (`/api/v4/leads/<id>`) |
| `GATEWAY_AMOCRM_ACCESS_TOKEN` | `""` | `config.py`; `Authorization: Bearer` к amoCRM |
| `GATEWAY_AMOCRM_REQUESTS_PER_SECOND` | `100` | `config.py`; token bucket client-side в `amocrm_client.py` |
| `GATEWAY_N8N_DLQ_URL` | `""` (DLQ выключен) | `config.py` |
| `GATEWAY_DB_PATH` | `<repo>/gateway.db` | `config.py` |
| `GATEWAY_HOST` / `GATEWAY_PORT` | `0.0.0.0` / `8000` | `config.py` — **в Docker игнорируются** (контейнер всегда `0.0.0.0:8000`, на хост публикуется `127.0.0.1:8000`); применяются только при ручном запуске Uvicorn вне контейнера (§1.2) |
| `GATEWAY_BUSY_TIMEOUT_MS` | `5000` | `config.py` → `state_store.connect` |
| `GATEWAY_DB_CONNECT_TIMEOUT_SECONDS` | `5.0` | `config.py` |
| `GATEWAY_DISPATCH_BATCH_LIMIT` | `50` | `config.py` |
| `GATEWAY_ONEC_RATE_LIMIT_RPS` | `2.0` | `config.py`; blocking pacer 1C (`scripts/egress_worker.py`) |
| `GATEWAY_ONEC_RATE_LIMIT_BURST` | `5` | `config.py`; burst pacer 1C |
| `GATEWAY_AMOCRM_RATE_LIMIT_RPS` | `2.0` | `config.py`; blocking pacer amoCRM (`AmoClient(pacer=...)`) |
| `GATEWAY_AMOCRM_RATE_LIMIT_BURST` | `5` | `config.py`; burst pacer amoCRM |
| `GATEWAY_CIRCUIT_BREAKER_FAILURE_THRESHOLD` | `5` | `config.py` |
| `GATEWAY_CIRCUIT_BREAKER_COOLDOWN_SECONDS` | `30.0` | `config.py` |
| `GATEWAY_CIRCUIT_BREAKER_HALF_OPEN_PROBE_INTERVAL` | `5.0` | `config.py` |
| `GATEWAY_REPLAY_GUARD_TIMESTAMP_WINDOW_SECONDS` | `300` | `config.py` |
| `GATEWAY_REPLAY_GUARD_NONCE_JOURNAL_SIZE` | `10000` | `config.py` |
| `GATEWAY_BACKOFF_BASE_SECONDS` | `0.5` | `config.py` |
| `GATEWAY_BACKOFF_MULTIPLIER` | `2.0` | `config.py` |
| `GATEWAY_BACKOFF_MAX_SECONDS` | `30.0` | `config.py` |
| `GATEWAY_BACKOFF_JITTER_RATIO` | `1.0` | `config.py` |
| `GATEWAY_BACKOFF_MAX_ATTEMPTS` | `5` | `config.py` |
| `GATEWAY_RATE_LIMIT_ALGORITHM` | `token_bucket` | `config.py` |
| `GATEWAY_RATE_LIMIT_REQUESTS_PER_WINDOW` | `100` | `config.py` |
| `GATEWAY_RATE_LIMIT_WINDOW_SECONDS` | `60` | `config.py` |
| `GATEWAY_ONEC_USER` / `GATEWAY_ONEC_PASSWORD` | — | штатные учётные данные воркера доставки (HTTP Basic), `scripts/egress_worker.py` (§3.4); ядро шлюза их не читает |
| `GATEWAY_ONEC_TIMEOUT_SECONDS` | `15` | egress-воркер (§3.4), httpx-клиент |
| `GATEWAY_POLL_INTERVAL` | `2` | egress-воркер (§3.4), пауза между проходами очереди |
| `GATEWAY_WORKER_INTERVAL_SECONDS` | — | алиас `GATEWAY_POLL_INTERVAL` (legacy); приоритет ниже, чем у `GATEWAY_POLL_INTERVAL` |
| `GATEWAY_LOG_LEVEL` | `INFO` | egress-воркер (§3.4), уровень логирования |
| `GATEWAY_DATA_DIR` | `/app/data` | `scripts/docker_entrypoint.py`: каталог данных, готовится для непривилегированного пользователя до старта сервиса |
| `GATEWAY_APP_USER` | `appuser` | `scripts/docker_entrypoint.py`: пользователь, под которым контейнер работает после снижения привилегий |
| `SSL_CERT_FILE` | — | корпоративный CA для 1C/n8n (смонтировать файл и указать путь, §3.6, п. 5) |

Общий формат override для секций `config/gateway.json`:
`GATEWAY_<SECTION>_<PARAM>` (например
`GATEWAY_BACKOFF_MAX_ATTEMPTS=8`, `GATEWAY_RATE_LIMIT_ALGORITHM=sliding_window`).

---

## 6. Минимальный реверс-прокси (Nginx)

Контейнер `gateway` публикуется только на loopback
(`127.0.0.1:8000`, §1.1), поэтому перед ним обязателен TLS-терминатор.
Ниже — минимальный production-блок, который закрывает весь контур: 443/SSL на
внешнем IP, проброс заголовков и **байт-в-байтная** передача тела (иначе
ломается HMAC-подпись, §2.3).

```nginx
# /etc/nginx/conf.d/gateway.conf
# Терминал TLS для amocrm-1c-resilient-gateway.
# Тело запроса проксируется без изменений: подпись X-Signature считается по
# сырому телу, любая пересериализация или URL-re-encoding его убьёт (401).

server {
    listen 80;
    server_name your-domain.com;
    return 301 https://$host$request_uri;   # только redirect на 443
}

server {
    listen 443 ssl;
    server_name your-domain.com;
    # http2 on;                              # nginx >= 1.25, опционально

    ssl_certificate     /etc/letsencrypt/live/your-domain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/your-domain.com/privkey.pem;
    ssl_protocols       TLSv1.2 TLSv1.3;
    ssl_session_cache   shared:SSL:10m;

    access_log  /var/log/nginx/gateway.access.log;
    error_log   /var/log/nginx/gateway.error.log;

    # Вебхуки amoCRM небольшие; лимит защищает от случайного огромного POST.
    client_max_body_size 1m;

    location / {
        # Без URI после хоста: proxy_pass в таком виде отдаёт исходный путь
        # и query string клиента без нормализации и перекодировки.
        proxy_pass http://127.0.0.1:8000;

        proxy_http_version 1.1;
        proxy_set_header Connection "";     # keepalive до шлюза

        # Проброс сетевого контекста на шлюз.
        proxy_set_header Host              $host;
        proxy_set_header X-Real-IP         $remote_addr;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Сырое тело — байт-в-байт, без URL-re-encoding и пересериализации:
        # именно эти байты подписал отправитель, HMAC считает по ним шлюз.
        proxy_pass_request_body  on;        # значение по умолчанию, фиксируем
        proxy_set_body           off;       # тело не модифицируем никогда

        proxy_read_timeout   30s;
        proxy_send_timeout   30s;
        proxy_buffering      off;           # ответ отдаётся как пришёл
    }
}
```

Проверка конфигурации и подключение:

```bash
sudo nginx -t
sudo systemctl reload nginx
```

Смоук через прокси (ожидаем `HTTP 200`, §2.7.1):

```bash
curl -sS -o - -w '\nHTTP %{http_code}\n' https://your-domain.com/health
# -> {"status":"ok","service":"amocrm-1c-gateway"} / HTTP 200
```

Ответ `200` на подписанный вебхук (§2.7.1), посланный на
`https://your-domain.com/webhook/amocrm`, и доказывает, что прокси не искажает
тело. Если в ответе `401 Invalid signature` — кто-то на пути переписал тело:
проверьте модули `modsecurity`, `lua`, `brotli`/`gzip` на стороне запроса и
`proxy_set_body` (он обязан быть `off`).

### 6.1. Нюансы, которые ломают контур

| Симптом | Причина | Решение |
|---|---|---|
| `401` только на запросах через прокси, напрямую на `127.0.0.1:8000` — `200` | тело изменено/перекодировано на пути | убрать модулы, пишущие в тело; `proxy_set_body off;` |
| `403 Missing required header: X-Signature` | заголовок не доехал (например, имя с подчёркиванием отброшено nginx'ом) | `underscores_in_headers on;` в `http {}`; имена `X-Signature`/`X-Timestamp`/`X-Nonce` валидны и без этого |
| внезапный `429` за все клиенты сразу | rate-limit в шлюзе ключуется по `request.client.host`, а за локальным nginx все клиенты приходят с `127.0.0.1` → общий bucket `100/60 с` (§2.3) | поднять `GATEWAY_RATE_LIMIT_REQUESTS_PER_WINDOW` и/или `GATEWAY_RATE_LIMIT_WINDOW_SECONDS` под суммарный поток, либо ставить nginx на отдельный IP и пробрасывать реальный адрес иначе |
| `502 Bad Gateway` | контейнер `gateway` остановлен/не healthy | `docker compose ps`, `docker compose logs gateway` |
| `504 Gateway Time-out` | воркер/БД заняты дольше `proxy_read_timeout` | поднять `proxy_read_timeout`, проверить `GATEWAY_BUSY_TIMEOUT_MS` (§3.5) |

> Адрес обратной связи (`127.0.0.1:8000`) указывается **на том же хосте**, где
> работает nginx, и совпадает с публикацией из `docker-compose.yml`
> (`ports: ["127.0.0.1:8000:8000"]`). Менять его на имя из Docker DNS не нужно:
> наружу контейнер отдаёт loopback хоста.

---

## 7. Обратное плечо: 1C → amoCRM

Двусторонний контур добавлен в Сессии 1: наряду с `amoCRM → 1C` шлюз принимает
статусные события из 1C и доставляет их в amoCRM. Это плечо полностью
асимметрично по авторизации (гибрид) и по направлению исходящего трафика.

### 7.1. Endpoint и контракт

| Параметр | Значение |
|---|---|
| Метод / путь | `POST /webhook/onec` |
| Контракт `data` | `OneCOrderStatusSchema` (`gateway/contracts/onec_order_status.py`) |
| `event_type` | `onec_order_status` (обязателен; другие → 422 `Unsupported event_type`) |
| Outbox-назначение | `deliveries.destination = 'amocrm'` (создаётся атомарно с событием) |
| Исходящий вызов | `PATCH /api/v4/leads/<order_id>` в amoCRM |

Тело — тот же конверт `WebhookPayload` (см. §2.4), но `data` строго
`OneCOrderStatusSchema` (все модели `extra=forbid`):

```json
{
  "idempotency_key": "1c-order-42:paid:2026-10-16T10:00:00+00:00",
  "event_type": "onec_order_status",
  "occurred_at": "2026-10-16T10:00:00+00:00",
  "nonce": "8d2b3c44-1f52-4a7c-9e2b-2a3c44d1a9a1",
  "data": {
    "order_id": "LEAD-42",
    "status": "paid",
    "payment_status": "paid",
    "updated_at": "2026-10-12T00:00:00+00:00",
    "comment": "оплачено"
  }
}
```

Поля `OneCOrderStatusSchema`:

| Поле | Тип | Обязательно | Примечание |
|---|---|---|---|
| `order_id` | string (`min_length=1`) | да | естественный ключ; `order_id` = id сделки amoCRM (в примерах `LEAD-42`) |
| `status` | string (`min_length=1`) | да | статус документа 1C; маппится в стадию amoCRM (`map_status_to_amo`) |
| `payment_status` | string / null | нет | `paid` / `completed` → стадия `PAID` (перекрывает `status`) |
| `updated_at` | datetime | да | момент статуса в 1C |
| `comment` | string / null | нет | попадает в `comment` при обновлении сделки |

### 7.2. Гибридная авторизация 1C

В отличие от amoCRM-плеча (только HMAC), вход 1C поддерживает два пути:

| Заголовок | Обязателен | Смысл |
|---|---|---|
| `X-Api-Token` | один из двух | API-token из `GATEWAY_ONEC_API_TOKEN` |
| `Authorization: Bearer <token>` | один из двух | эквивалент `X-Api-Token` |
| `X-Signature` | один из двух | если присутствует — выбирается **HMAC-путь** (проверяется `GATEWAY_HMAC_SECRET`) |
| `X-Timestamp` | да | окно replay 300 с |
| `X-Nonce` | да | уникален на запрос |
| `Content-Type` | да | `application/json` |

Правило выбора (`gateway/app.py`, `gateway/pipeline.py`): наличие заголовка
`X-Signature` переключает запрос на HMAC-проверку; иначе используется токен.
Порядок проверок общий для обоих плеч:
**rate limit (429) → headers (403) → auth (401) → replay (403) → схема (422) → INSERT**.
Сравнение токена — константное (`hmac.compare_digest`). Если
`GATEWAY_ONEC_API_TOKEN` пуст, любой запрос без `X-Signature` получит
`401 Invalid 1C API token`.

### 7.3. Маршрутизация 1C → amoCRM

`delivery_destinations("onec_order_status")` в
`gateway/contract_validator.py` возвращает `("amocrm",)`, поэтому событие
создаётся сразу с delivery `amocrm`. Воркер
(`scripts/egress_worker.py`) собирает bundle `SimpleNamespace(one_c=...,
amocrm=...)`, а `egress_dispatcher.dispatch_event()` ветвится по
`delivery.destination`:

```
destination == "1c"      → OneCClient.create_order / create_counterparty  → 1C OData
destination == "amocrm"  → AmoClient.update_order_status                  → PATCH /api/v4/leads/<id>
```

Маппинг статуса 1C → стадия amoCRM (`gateway/amocrm_client.py`,
`map_status_to_amo`):

| `status` 1C | Стадия amoCRM |
|---|---|
| `new`, `created` | `NEW` |
| `in_progress`, `processing` | `IN_PROGRESS` |
| `paid` | `PAID` |
| `ready` | `READY` |
| `shipped` | `SHIPPED` |
| `cancelled`, `canceled` | `CANCELLED` |
| `closed` | `CLOSED` |
| неизвестное значение | передаётся как есть |

`payment_status` в `("paid", "completed")` принудительно даёт стадию `PAID`
независимо от `status`. Идемпотентность в amoCRM передаётся заголовком
`X-Idempotency-Key` (та же `idempotency_key` события), а `payment_status`
добавляется в `comment` как `payment:<значение>`.

### 7.4. Троттлинг и circuit breaker исходящего плеча amoCRM

- **Client-side лимит 100 rps.** `AmoClient` использует токен-бакет
  (`RateLimiter`, окно 1 с, `requests_per_window = GATEWAY_AMOCRM_REQUESTS_PER_SECOND`).
  Превышение не идёт в сеть: выбрасывается `AmoRateLimitError`
  (`severity: warning`, `location: egress.amocrm.rate_limit`) → событие уходит
  в ретраи, как и обычная ошибка. Причина настройки лимита — публичный
  лимит amoCRM; поднимайте значение только осознанно.
- **Два лимитера amoCRM (dual-limiter).** Бюджет `RateLimiter` (**отклоняет**
  вызов до сети) сосуществует с блокирующим pacer'ом `TokenBucketRateLimiter`
  (**спит** до появления токена, запрос не теряется). Параметры pacer'а —
  `GATEWAY_AMOCRM_RATE_LIMIT_RPS` / `GATEWAY_AMOCRM_RATE_LIMIT_BURST`
  (по умолчанию `2.0` / `5`). Бюджет — защита от злоупотребления, pacer —
  плавный темп к зависимости.
- **Single-flight ротация токена.** `AmoClient` — единственный владелец
  заголовка `Authorization: Bearer`. На HTTP 401 он под `threading.Lock`
  увеличивает эпоху `_token_version` и вызывает `token_refresher` **не более
  одного раза за эпоху** (rotate-at-most-once); N параллельных воркеров
  переиспользуют уже ротированный токен, а запрос повторяется **ровно один
  раз** (≤1 retry). Поэтому единичная 401 на событие — штатное поведение, а не
  инцидент.
- **Отдельный circuit breaker.** В воркере два независимых `CircuitBreaker`:
  `breakers["1c"]` и `breakers["amocrm"]`. Падение 1C не «глушит» amoCRM-плечо
  и наоборот; параметры (`failure_threshold`, `cooldown_seconds`,
  `half_open_probe_interval`) общие — §4.1.
- **Ретраи** — те же: exponential backoff с full jitter, `max_attempts=5`,
  после исчерпания событие `FAILED`, delivery `FAILED`, отправка в n8n DLQ (§4.3).

### 7.5. Smoke-тест обратного плеча (токен)

```bash
TOKEN="<GATEWAY_ONEC_API_TOKEN>"
TS="$(date -u +%Y-%m-%dT%H:%M:%S+00:00)"
NONCE="$(cat /proc/sys/kernel/random/uuid)"
BODY='{"idempotency_key":"onec-smoke-'"$NONCE"'","event_type":"onec_order_status","occurred_at":"'"$TS"'","nonce":"'"$NONCE"'","data":{"order_id":"LEAD-42","status":"paid","payment_status":"paid","updated_at":"'"$TS"'","comment":"smoke"}}'

curl -sS -o - -w '\nHTTP %{http_code}\n' \
  -X POST https://your-domain.com/webhook/onec \
  -H "Content-Type: application/json" \
  -H "X-Api-Token: $TOKEN" \
  -H "X-Timestamp: $TS" \
  -H "X-Nonce: $NONCE" \
  --data-raw "$BODY"
```

Ожидаемо: `HTTP 200` и `{"status":"validated","event_id":<N>,"destinations":["amocrm"]}`.
Если вместо этого `401 Invalid 1C API token` — проверьте `GATEWAY_ONEC_API_TOKEN`
и точное значение заголовка; `422` — схему `data`; `403` — `X-Timestamp`/`X-Nonce`.

Коды ответа `POST /webhook/onec`: `200` (accepted / duplicate), `401`
(неверный токен или HMAC), `403` (replay / заголовки / Content-Type), `422`
(ошибка схемы), `429` (rate limit). Формат ошибок — тот же `ErrorShape` (§2.8).

---

## 8. Эксплуатация, логирование и траблшутинг (Runbook)

Этот раздел рассчитан на дежурного инженера: за 5 минут локализовать сбой,
снять логи и разобрать застрявшие транзакции. Все команды выполняются из
корня репозитория, где лежит `docker-compose.yml`.

### 8.1. Карта логов и мониторинг

| Источник | Команда | Что смотреть |
|---|---|---|
| HTTP-шлюз (`gateway`) | `docker compose logs -f gateway` | старт Uvicorn, ошибки приёма, трассировки FastAPI |
| Воркер доставки (`egress-worker`) | `docker compose logs -f egress-worker` | `-> DELIVERED`, `-> FAILED`, circuit breaker, DLQ |
| Оба сервиса сразу | `docker compose logs -f gateway egress-worker` | коррелированные события (по `id`/`key`) |
| Последние N строк | `docker compose logs --tail=200 egress-worker` | разбор без follow |
| С метками времени | `docker compose logs -t egress-worker` | сопоставление с `updated_at` в БД |
| 429 / pacing | `docker compose logs egress-worker \| grep -E "429\|pacer\|Retry-After"` | пауза pacer'а, `Retry-After`, класс `retry` (breaker намеренно не размыкается) |
| 401 amoCRM (первый) | `docker compose logs egress-worker \| grep -E "401\|token"` | single-flight ротация + ровно один ретрай; повторный 401 — токен отозван |

Формат строки лога воркера:

```
2026-10-19T08:00:00+00:00 INFO [egress_worker] event id=42 key=amo-123 -> DELIVERED
```

Уровень задаётся `GATEWAY_LOG_LEVEL` (`DEBUG` / `INFO` / `WARNING`). Подробные
строки о повторах (`-> FAILED again, reason unchanged`) идут на `DEBUG`, чтобы
не спамить WARNING при каждой итерации ожидания.

**Единый формат ошибки.** Все ошибки конвейера строятся фабрикой `make_error`
в `gateway/errors.py` и имеют ровно 5 полей (`ErrorShape`):

| Поле | Тип | Значение |
|---|---|---|
| `type` | string | `security` / `schema` / `semantic` / `dependency` |
| `message` | string | человекочитаемое описание |
| `severity` | string | `critical` / `warning` |
| `location` | string | `headers`, `body`, `signature`, `replay`, `rate_limit`, `egress.onec`, `egress.amocrm`, `egress.amocrm.rate_limit` |
| `how_to_fix` | string | конкретное действие по устранению |

Тот же конверт уходит в DLQ-полезную нагрузку в поле `error` (§4.3). Внешние
системы иногда называют поля того же конверта `code` / `message` / `details` /
`timestamp` — **канонический контракт шлюза — именно `ErrorShape`**
(`type`/`message`/`severity`/`location`/`how_to_fix`), он проверяется тестом
`test_error_shape_five_fields_everywhere` и менять его поля нельзя.

### 8.2. Диагностика очереди и базы (SQLite WAL)

Файл БД — `./data/gateway.db` (внутри контейнера `/app/data/gateway.db`,
`GATEWAY_DB_PATH`). Рядом SQLite держит `gateway.db-wal` и `gateway.db-shm`:
их **нельзя** удалять при работающих сервисах. Обе таблицы живут в одной БД:

- `events` — журнал событий и конечный автомат
  `RECEIVED → VALIDATED → DISPATCHED → DELIVERED | FAILED`;
- `deliveries` — Transactional Outbox, по строке на пару `(event, destination)`.

**Жизненный цикл доставки** (`deliveries.status`):

| Статус | Когда | Что делать оператору |
|---|---|---|
| `PENDING` | строка создана атомарно с событием, ещё не взята воркером | норма; ждать прохода воркера |
| `IN_FLIGHT` | воркер начал транспортный вызов | «завис» надолго → проверить логи воркера/таймауты |
| `DELIVERED` | транспорт подтвердил приём | можно архивировать по регламенту |
| `FAILED` | исчерпаны 5 попыток или circuit breaker open | разобрать `last_error`, после восстановления зависимостей — повторный проход |

Запросы для отладки (выполняются на хосте; клиент `sqlite3`):

```sql
-- Застрявшие и ошибочные доставки (FAILED или уже имевшие попытки)
SELECT id, event_id, destination, status, attempt_count, last_error, updated_at
FROM deliveries
WHERE status = 'FAILED' OR attempt_count > 0;
```

```sql
-- Проверка дубликатов по ключу идемпотентности (подставьте реальный ключ)
SELECT * FROM events WHERE idempotency_key = '...';
```

```sql
-- Сводка по направлениям и статусам outbox
SELECT destination, status, COUNT(*) AS n, MAX(attempt_count) AS max_attempts
FROM deliveries GROUP BY destination, status ORDER BY destination, status;

-- Буфер повторов: всё, что ещё не доставлено
SELECT event_id, destination, status, attempt_count, last_error, updated_at
FROM deliveries WHERE status IN ('PENDING', 'IN_FLIGHT') ORDER BY updated_at;
```

```sql
-- Событие целиком (payload = конверт {event_type, data})
SELECT id, status, attempt_count, last_error, updated_at, payload
FROM events WHERE id = <N>;
```

```sql
-- События, «зависшие» в промежуточном статусе дольше 15 минут
-- (updated_at — ISO-8601 с 'T', поэтому обязательно datetime())
SELECT id, status, updated_at, last_error FROM events
WHERE status IN ('VALIDATED', 'DISPATCHED')
  AND datetime(updated_at) < datetime('now', '-15 minutes');
```

Повторный проход по `FAILED` воркер делает сам (`resend_failed()`:
`FAILED → VALIDATED`); ручной прогон из Python —
`run_delivery_pass(conn, settings, client, dlq_transport, time.sleep)`. Прямое
редактирование строк `deliveries`/`events` не рекомендуется: это ломает
автомат и идемпотентность.

### 8.3. Типовые ошибки и сценарии решения (Failure Scenarios)

| Симптом / код | Причина | Где смотреть | Что делать |
|---|---|---|---|
| **422 Unprocessable Entity** | payload не прошёл схему `WebhookPayload`/`data` (лишнее поле, неверный тип, пустой `order_id`, отсутствует `updated_at`) | `gateway/contract_validator.py` → `parse_webhook_body`/`validate_webhook`; ответ содержит `location` (например `payload.updated_at`) | исправить payload отправителя по схеме §7.1 / §2.4; поля `type="schema"`, `how_to_fix` подсказывают причину |
| **429 Too Many Requests** | превышен входной лимит: `rate_limit` = token bucket, 100 req / 60 с на IP (`config/gateway.json`) | `gateway/ingress_guard.py` → `RateLimiter`, `authenticate_request()` | повторить позже с backoff; для легитимного всплеска поднять `GATEWAY_RATE_LIMIT_REQUESTS_PER_WINDOW` / `GATEWAY_RATE_LIMIT_WINDOW_SECONDS` (за reverse-proxy помнить про общий IP, §6.1) |
| **Circuit Breaker OPEN** | недоступность зависимости: 5 подряд неудач (`failure_threshold`) к 1C **или** amoCRM | `gateway/egress_dispatcher.py` → `CircuitBreaker`; лог воркера `Circuit breaker is open` | зависимость недоступна — дождаться `cooldown_seconds` (30 с) → `half-open` → одна probe-попытка; сброс — перезапуск воркера (`docker compose restart egress-worker`), но сначала проверить доступность 1C/amoCRM (§3.6) |
| **Событие ушло в DLQ** | после `max_attempts=5` или open breaker событие отправлено в n8n (`GATEWAY_N8N_DLQ_URL`) | `send_to_dlq()` в `egress_dispatcher.py`; полезная нагрузка с `error`, `attempts`, `failed_at` | разобрать алерт n8n (§4.3); если DLQ пуст — событие осталось `FAILED` в SQLite для повторного прохода |
| **401 от amoCRM при доставке** | `GATEWAY_AMOCRM_ACCESS_TOKEN` пуст/невалиден или отозван | `gateway/amocrm_client.py` → `_check`; `last_error` в `deliveries` | обновить токен в `.env`, `docker compose up -d --force-recreate egress-worker`; событие станет доступно для повтора |
| **429 от 1C/amoCRM при egress** | превышен темп зависимости; `classify_status()` → `retry`, пауза = `max(jitter_backoff, Retry-After)` | логи воркера (`pacer`, `Retry-After`); `deliveries.last_error` содержит `429` | штатный троттлинг: при устойчивых 429 снизить `GATEWAY_ONEC_RATE_LIMIT_RPS` / `GATEWAY_AMOCRM_RATE_LIMIT_RPS`; breaker намеренно **не** размыкается (§4.1) |
| **401 amoCRM: одна ошибка, затем успех** | single-flight ротация токена: `_token_version` + ровно один ретрай (`rotate-at-most-once`) | `gateway/amocrm_client.py` → `_request`, `refresh_token` | действий не требуется; если 401 повторяется **после** ретрая — токен отозван, обновить `.env` и `docker compose up -d --force-recreate egress-worker` (§7.4) |
| **401 `Invalid 1C API token`** | на `POST /webhook/onec` нет `X-Signature`, а `X-Api-Token`/`Bearer` неверны | `gateway/ingress_guard.py` → `verify_api_token` | сверить `GATEWAY_ONEC_API_TOKEN`; помните про приоритет `X-Signature` (если он есть — HMAC-путь) |
| `database is locked` | конкуренция записи SQLite, превышен `GATEWAY_BUSY_TIMEOUT_MS` | логи шлюза/воркера; `gateway/state_store.py` | поднять `GATEWAY_BUSY_TIMEOUT_MS` (10000–15000), уменьшить `GATEWAY_DISPATCH_BATCH_LIMIT`, не держать БД на сетевом диске (§3.5) |
| Событие «зависло» в `VALIDATED`/`DISPATCHED` | воркер остановлен или не читает очередь | запрос «зависших» выше; `docker compose ps` | поднять/перезапустить `egress-worker`; проверить общий том `./data` |

### 8.4. Быстрые команды (шпаргалка)

```bash
# состояние стека и здоровье
docker compose ps
curl -f http://127.0.0.1:8000/health

# логи обоих сервисов
docker compose logs -f gateway egress-worker

# ошибки доставки в БД
sqlite3 -header -column data/gateway.db \
  "SELECT id, event_id, destination, status, attempt_count, last_error, updated_at FROM deliveries WHERE status='FAILED' OR attempt_count>0;"

# дубликат по ключу
sqlite3 -header -column data/gateway.db \
  "SELECT * FROM events WHERE idempotency_key='...';"

# сброс circuit breaker только через рестарт воркера
docker compose restart egress-worker
```

---

## Кросс-ссылки

- [`docs/MAINTENANCE.md`](MAINTENANCE.md) — SLA-runbook: первые 60 секунд,
  SQL-триаж Outbox (§3), schema evolution (§4), миграция DTO (§5).
- `DEPLOYMENT_GUIDE.md` — установка, конфигурация, релизный чек-лист.
- `ARCHITECTURE_REPORT.md` — инвариант четырёх компонентов и трассировка A1–A16.

