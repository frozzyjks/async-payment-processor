# Async Payment Processor

Микросервис асинхронной обработки платежей. Принимает запрос на оплату, сразу
отвечает `202 Accepted`, обрабатывает платёж через эмулятор платёжного шлюза
и уведомляет клиента о результате через webhook.

Ключевые свойства:

- **Transactional Outbox** — платёж и событие `payment.created` пишутся в одной транзакции PostgreSQL;
  отдельный процесс публикует события в RabbitMQ с publisher confirms;
- **идемпотентность API** — `Idempotency-Key` + fingerprint запроса + уникальный индекс в БД;
- **идемпотентный consumer** — повторная доставка не приводит к повторному списанию;
- **retry с экспоненциальной задержкой и DLQ** — 3 попытки обработки, затем `payments.new.dlq`;
- **надёжные webhooks** — отдельная таблица доставок, ограниченные повторы с backoff, стабильный `event_id`.

---

## Содержание

1. [Архитектура](#архитектура)
2. [Технологии](#технологии)
3. [Структура проекта](#структура-проекта)
4. [Запуск](#запуск)
5. [API](#api)
6. [Outbox Pattern](#outbox-pattern)
7. [RabbitMQ: очереди, retry и DLQ](#rabbitmq-очереди-retry-и-dlq)
8. [Идемпотентность](#идемпотентность)
9. [Доставка webhook](#доставка-webhook)
10. [Отказоустойчивость](#отказоустойчивость)
11. [Тесты и проверки](#тесты-и-проверки)
12. [Известные ограничения](#известные-ограничения)

---

## Архитектура

```
             POST /api/v1/payments
 client ───────────────────────────► api (FastAPI)
                                      │  одна транзакция:
                                      │  INSERT payments (pending) + INSERT outbox
                                      ▼
                                  PostgreSQL ◄──────────────────────────────┐
                                      │                                     │
                 FOR UPDATE SKIP LOCKED + lease                             │
                                      ▼                                     │
                              outbox-publisher ──publisher confirms──►  RabbitMQ
                                                                   exchange "payments"
                                                                            │ payments.new
                                                                            ▼
                                 consumer (FastStream) ◄──────────── queue payments.new
                                   │  SELECT ... FOR UPDATE payment
                                   │  эмулятор шлюза (2–5 c, 90% / 10%)
                                   │  одна транзакция: status + processed_at + webhook_deliveries
                                   ▼
                          webhook-dispatcher (фоновая задача consumer)
                                   │  POST webhook_url, retry с backoff
                                   ▼
                               client webhook
```

Сервисы Docker Compose:

| Сервис | Назначение |
|---|---|
| `postgres` | PostgreSQL 17 — единственный источник истины для всех процессов |
| `rabbitmq` | RabbitMQ 4.1 c Management UI |
| `migrate` | одноразовый запуск `alembic upgrade head` (единственное место запуска миграций) |
| `api` | FastAPI (uvicorn) |
| `outbox-publisher` | публикация событий из таблицы `outbox` в RabbitMQ |
| `consumer` | обработка `payments.new` + фоновая доставка webhooks |

`api`, `outbox-publisher` и `consumer` стартуют только после того, как `postgres`
и `rabbitmq` прошли healthcheck, а `migrate` завершился успешно
(`service_completed_successfully`). Поэтому миграции никогда не выполняются конкурентно.

## Технологии

Python 3.12, FastAPI, Pydantic v2 (+ pydantic-settings), SQLAlchemy 2 (async) + asyncpg,
PostgreSQL 17, Alembic, RabbitMQ 4.1, FastStream (aio-pika), httpx, Docker / Docker Compose,
pytest + pytest-asyncio + respx, ruff, mypy (strict). Зависимости управляются через
[uv](https://docs.astral.sh/uv/): `pyproject.toml` + `uv.lock`.

## Структура проекта

```
app/
  config.py               настройки из переменных окружения
  logging_config.py       JSON-логирование
  polling.py              общий фоновой цикл (outbox publisher, webhook dispatcher)
  api/                    FastAPI: main (фабрика), routes, schemas, security (X-API-Key), errors
  domain/                 enums, деньги, fingerprint, backoff, ошибки, интерфейс и эмулятор шлюза
  db/                     модели SQLAlchemy, сессии, репозитории
  services/               payments (создание + идемпотентность), processing (проведение платежа)
  messaging/              топология RabbitMQ, брокер и publisher с confirms, схема сообщения
  outbox/                 OutboxPublisher + точка входа `python -m app.outbox`
  consumer/               handler (ack/retry/DLQ) + точка входа `python -m app.consumer`
  webhooks/               payload, sender (одна HTTP-попытка), dispatcher (повторы)
migrations/               Alembic (async env, начальная миграция)
tests/                    unit + интеграционные тесты (PostgreSQL и RabbitMQ)
Dockerfile                multi-stage: runtime и test
docker-compose.yml
```

## Запуск

### Требования

- Docker 24+ и Docker Compose v2;
- свободные порты на `127.0.0.1`: `8000`, `5432`, `5672`, `15672` (меняются в `.env`);
- для локального запуска тестов без Docker: Python 3.12 и [uv](https://docs.astral.sh/uv/).

### Настройка `.env`

```bash
cp .env.example .env
# задайте свои значения API_KEY, POSTGRES_PASSWORD, RABBITMQ_PASSWORD, например:
#   API_KEY=$(openssl rand -hex 32)
```

Основные переменные:

| Переменная | Описание |
|---|---|
| `API_KEY` | статический ключ для заголовка `X-API-Key` (минимум 16 символов) |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | учётные данные БД (используются и контейнером, и приложением) |
| `RABBITMQ_USER` / `RABBITMQ_PASSWORD` / `RABBITMQ_VHOST` | учётные данные RabbitMQ |
| `POSTGRES_HOST` / `POSTGRES_PORT`, `RABBITMQ_HOST` / `RABBITMQ_PORT` | адреса для запуска **с хоста** (тесты); внутри Compose переопределяются на `postgres:5432` / `rabbitmq:5672` |
| `*_PUBLISHED_PORT` | порты, публикуемые на хост |
| `GATEWAY_*` | параметры эмулятора шлюза; `GATEWAY_TRANSIENT_ERROR_RATE>0` включает временные ошибки (демонстрация retry/DLQ) |
| `PROCESSING_MAX_ATTEMPTS`, `PROCESSING_RETRY_BASE_DELAY_MS` | попытки обработки и базовая задержка |
| `WEBHOOK_MAX_ATTEMPTS`, `WEBHOOK_RETRY_*`, `WEBHOOK_TIMEOUT_SECONDS` | политика доставки webhooks |
| `OUTBOX_*` | размер пачки, интервал опроса, lease и backoff outbox |

Полный список с значениями по умолчанию — в `.env.example` и `app/config.py`.

> Если порт 5432 на хосте занят локальным PostgreSQL, задайте, например,
> `POSTGRES_PUBLISHED_PORT=55432` и `POSTGRES_PORT=55432`.

### Старт

```bash
docker compose up --build
# или в фоне:
docker compose up --build -d
docker compose ps
```

Миграции применяются автоматически сервисом `migrate`. Вручную:

```bash
docker compose run --rm migrate                       # alembic upgrade head
docker compose run --rm migrate alembic current
docker compose run --rm migrate alembic downgrade -1
```

Адреса:

- API: <http://localhost:8000>
- Swagger UI: <http://localhost:8000/docs> (кнопка **Authorize** → `X-API-Key`), ReDoc: <http://localhost:8000/redoc>
- RabbitMQ Management UI: <http://localhost:15672> (логин/пароль — `RABBITMQ_USER` / `RABBITMQ_PASSWORD`)

Остановка: `docker compose down` (данные сохраняются в volumes), полная очистка: `docker compose down -v`.

Масштабирование: `docker compose up -d --scale consumer=3 --scale outbox-publisher=2` —
несколько экземпляров безопасно работают одновременно.

## API

Все эндпоинты (кроме `/docs`, `/redoc`, `/openapi.json`) требуют заголовок `X-API-Key`.
Проверка выполняется middleware **до** маршрутизации и валидации, поэтому даже запрос на
несуществующий путь или с некорректным JSON без ключа получит `401`. Ключ сравнивается
через `secrets.compare_digest` и нигде не логируется.

Единый формат ошибок:

```json
{"error": {"code": "validation_error", "message": "Request validation failed", "details": [...]}}
```

| HTTP | `code` | Когда |
|---|---|---|
| 401 | `unauthorized` | нет `X-API-Key` |
| 403 | `forbidden` | неверный `X-API-Key` |
| 404 | `payment_not_found` / `not_found` | платёж / маршрут не найден |
| 409 | `idempotency_conflict` | ключ уже использован с другим телом запроса |
| 422 | `validation_error` | некорректные данные, заголовок или `payment_id` |
| 503 | `service_unavailable` | PostgreSQL недоступен — повторите запрос с тем же ключом |
| 500 | `internal_error` | непредвиденная ошибка (детали только в логах) |

### Создание платежа — `POST /api/v1/payments`

Заголовки: `X-API-Key`, `Idempotency-Key` (обязательный, 1–255 печатных ASCII-символов без пробелов).

| Поле | Тип | Правила |
|---|---|---|
| `amount` | string / number | > 0, не более 2 знаков после запятой, до 18 цифр. Рекомендуется передавать строкой |
| `currency` | string | `RUB`, `USD`, `EUR` |
| `description` | string | 1–1000 символов |
| `metadata` | object | произвольный JSON-объект, по умолчанию `{}` |
| `webhook_url` | string | `http(s)` URL, до 2048 символов |

```bash
export API_KEY=...   # значение из .env

curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: order-42-attempt-1" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42},
        "webhook_url": "https://webhook.site/your-uuid"
      }'
```

Ответ `202 Accepted` (заголовок `Idempotent-Replayed: false`, при повторе — `true`):

```json
{
  "payment_id": "d54a7197-4719-4962-b998-e618aae94975",
  "status": "pending",
  "created_at": "2026-10-15T07:32:11.206249Z"
}
```

Ошибка валидации `422`:

```json
{
  "error": {
    "code": "validation_error",
    "message": "Request validation failed",
    "details": [
      {"loc": ["body", "amount"], "message": "Input should be greater than 0", "type": "greater_than"},
      {"loc": ["body", "currency"], "message": "Input should be 'RUB', 'USD' or 'EUR'", "type": "enum"}
    ]
  }
}
```

Конфликт идемпотентности `409`:

```json
{"error": {"code": "idempotency_conflict", "message": "Idempotency-Key was already used with a different request body", "details": null}}
```

### Получение платежа — `GET /api/v1/payments/{payment_id}`

```bash
curl http://localhost:8000/api/v1/payments/d54a7197-4719-4962-b998-e618aae94975 \
  -H "X-API-Key: $API_KEY"
```

```json
{
  "payment_id": "d54a7197-4719-4962-b998-e618aae94975",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Order #42",
  "metadata": {"order_id": 42},
  "status": "succeeded",
  "webhook_url": "https://webhook.site/your-uuid",
  "created_at": "2026-10-15T07:32:11.206249Z",
  "processed_at": "2026-10-15T07:32:15.919291Z"
}
```

Несуществующий платёж — `404`:

```json
{"error": {"code": "payment_not_found", "message": "Payment not found", "details": null}}
```

### `GET /health`

Проверка доступности API и PostgreSQL (тоже требует `X-API-Key`; используется healthcheck контейнера).

### Webhook для локальной проверки

Для локальной проверки без внешних сервисов можно поднять простой приёмник в сети Compose
(отвечает `200` на `/ok` и `503` на `/fail`, печатает полученные уведомления):

```bash
docker run -d --rm --network async-payment-processor_default --name webhook-receiver \
  async-payment-processor:latest python -u -c '
import http.server
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        code = 503 if self.path.startswith("/fail") else 200
        print(code, self.path, body.decode(), flush=True)
        self.send_response(code); self.end_headers()
http.server.HTTPServer(("0.0.0.0", 8080), H).serve_forever()'

# в запросе укажите "webhook_url": "http://webhook-receiver:8080/ok"
docker logs -f webhook-receiver
```

## Outbox Pattern

Таблица `outbox`: `id`, `event_type`, `aggregate_id` (FK на `payments.id`), `payload` (JSONB),
`created_at`, `published_at`, `attempts`, `next_attempt_at`, `last_error`.

1. **Запись.** `POST /payments` в одной транзакции вставляет платёж (`pending`) и событие
   `payment.created`. Если упадёт любая из вставок, откатятся обе (проверяется тестом).
   API никогда не публикует в RabbitMQ напрямую.
2. **Захват.** `outbox-publisher` раз в `OUTBOX_POLL_INTERVAL_SECONDS` (или сразу, если пачка
   была полной) выполняет в короткой транзакции:
   ```sql
   UPDATE outbox SET attempts = attempts + 1, next_attempt_at = now() + :lease
   WHERE id IN (SELECT id FROM outbox
                WHERE published_at IS NULL AND (next_attempt_at IS NULL OR next_attempt_at <= now())
                ORDER BY created_at LIMIT :batch
                FOR UPDATE SKIP LOCKED)
   RETURNING *;
   ```
   `SKIP LOCKED` не даёт двум экземплярам publisher захватить одну запись, а **lease**
   (`next_attempt_at` в будущем) исключает вечную блокировку: если publisher умер, запись
   снова станет доступна по истечении `OUTBOX_LEASE_SECONDS`. Блокировки строк не держатся
   во время сетевого вызова.
3. **Публикация.** Сообщение `persistent`, `mandatory`, `message_id = outbox.id`; publisher ждёт
   **publisher confirm**. Nack брокера, возврат неотмаршрутизированного сообщения, таймаут или
   обрыв соединения — это ошибка.
4. **Фиксация.** `published_at` выставляется только после подтверждения брокера.
   При ошибке — `next_attempt_at = now() + backoff(attempts)` (1, 2, 4… до 60 c) и `last_error`;
   остаток пачки сразу освобождается, чтобы не ждать ту же недоступность брокера по каждому событию.
   При остановке (SIGTERM) захваченные, но не обработанные записи также освобождаются.
   Событие публикуется до успеха: потерять его значит потерять платёж.

**Гарантия — at-least-once, не exactly-once.** Если RabbitMQ принял сообщение, а publisher упал
до записи `published_at`, после истечения lease событие будет опубликовано повторно.
Дубликат безопасен благодаря идемпотентному consumer.

## RabbitMQ: очереди, retry и DLQ

| Объект | Тип | Назначение |
|---|---|---|
| exchange `payments` | direct, durable | основной; routing key `payments.new` |
| queue `payments.new` | quorum, durable | основная очередь; DLX → `payments.dlx`, `x-delivery-limit=5`, at-least-once dead-lettering |
| exchange `payments.retry` | direct, durable | отложенные повторы |
| queue `payments.new.retry.2000ms`, `payments.new.retry.4000ms` | quorum, `x-message-ttl` | по истечении TTL сообщение возвращается в `payments` / `payments.new` |
| exchange `payments.dlx` | direct, durable | dead-letter exchange |
| queue `payments.new.dlq` | quorum, durable | окончательно не обработанные сообщения |

Все сообщения persistent, consumer работает в режиме ручного подтверждения (`AckPolicy.MANUAL`),
`prefetch_count = CONSUMER_PREFETCH_COUNT`.

Решения consumer по каждому сообщению (ровно одно действие на путь):

| Ситуация | Действие |
|---|---|
| платёж обработан или уже в финальном статусе | `ack` |
| некорректный payload / платёж не найден (постоянная ошибка) | `reject(requeue=False)` → сразу в DLQ |
| временная ошибка (шлюз, БД, сеть), попытка < 3 | копия в `payments.new.retry.<delay>ms` с заголовком `x-processing-attempt = n+1` (с confirm), затем `ack` |
| временная ошибка, попытка = 3 | `reject(requeue=False)` → DLQ через DLX (в `x-death` — причина и очередь) |
| не удалось опубликовать копию для retry | `nack(requeue=True)` |

Задержки: `PROCESSING_RETRY_BASE_DELAY_MS * 2^(n-1)` → 2 c, 4 c. Для каждой задержки своя очередь
с фиксированным TTL, поэтому нет head-of-line blocking; задержка входит в имя очереди, и смена
настроек не ломает объявление существующих очередей.

### Четыре механизма повторов и их зоны ответственности

| Механизм | Где | Что решает | Ограничение |
|---|---|---|---|
| Повтор публикации Outbox | `outbox-publisher` | брокер недоступен / нет confirm | бессрочно, backoff до 60 c |
| Повтор обработки платежа | consumer → retry-очереди | временная ошибка шлюза / БД | 3 попытки, затем DLQ |
| Повторная доставка RabbitMQ | брокер | consumer упал до `ack`, `nack(requeue)` | `x-delivery-limit=5`, затем DLQ (защита от crash-loop) |
| Повтор webhook | webhook-dispatcher | получатель недоступен / 5xx / 429 | `WEBHOOK_MAX_ATTEMPTS=3`, backoff 2, 4… c |

Механизмы независимы: ошибка webhook никогда не возвращает сообщение в очередь и не вызывает
повторную обработку платежа; повтор обработки не трогает outbox.

Что делать с DLQ: сообщения видны в Management UI (Queues → `payments.new.dlq` → Get messages).
Платёж из DLQ остаётся `pending` — после устранения причины сообщение можно вернуть в exchange
`payments` с routing key `payments.new` (Management UI → Publish message или shovel); повторная
обработка безопасна.

## Идемпотентность

### API

1. Вычисляется **fingerprint** — SHA-256 от канонического JSON (ключи отсортированы) из значимых полей:
   - `amount`, нормализованный к 2 знакам (`100`, `100.0` и `"100.00"` эквивалентны);
   - `currency`;
   - `description`;
   - `metadata` (порядок ключей не важен);
   - `webhook_url` в нормализованном валидатором виде.
2. `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING RETURNING *` вместе с вставкой в outbox
   в одной транзакции.
3. Если строка вставлена — `202`, `Idempotent-Replayed: false`.
4. Если ключ уже существует — читается сохранённый платёж: fingerprint совпал → `202` с тем же
   платежом (текущий статус), `Idempotent-Replayed: true`; не совпал → `409`.

Конкурентные запросы с одним ключом: окончательную защиту даёт уникальный индекс
`uq_payments_idempotency_key`. PostgreSQL заставляет «проигравшую» транзакцию дождаться
победителя, после чего `ON CONFLICT DO NOTHING` возвращает пустой результат — без
IntegrityError и без дублей (10 параллельных запросов → 1 платёж, 1 событие; проверено тестом).

Если API упал после коммита, но до ответа, клиент повторяет запрос с тем же ключом и получает
уже созданный платёж.

### Consumer

Источник истины — БД, а не сообщение. В одной транзакции:

1. `SELECT ... FOR UPDATE` строки платежа (`lock_timeout` ограничивает ожидание);
2. если статус финальный — `ack` без побочных эффектов;
3. иначе вызов шлюза, `status` + `processed_at` и запись в `webhook_deliveries` — один коммит.

Дубликат, обрабатываемый параллельно другим экземпляром consumer, ждёт блокировку и затем
видит финальный статус. Проверено тестами: 3 параллельные обработки → 1 вызов шлюза.

## Доставка webhook

- Таблица `webhook_deliveries` (`id` = `event_id`, `payment_id` UNIQUE, `url`, `payload`, `status`
  `pending/delivered/failed`, `attempts`, `next_attempt_at`, `last_error`, `last_response_status`,
  `delivered_at`). Запись создаётся в той же транзакции, что и финальный статус платежа:
  уведомление не теряется и не может быть создано дважды.
- Тело уведомления формируется один раз и не меняется между попытками:

  ```json
  {
    "event_id": "c10c33a8-8b7d-4f84-ab82-008b7dc33f75",
    "event_type": "payment.processed",
    "payment_id": "d54a7197-4719-4962-b998-e618aae94975",
    "status": "succeeded",
    "amount": "101.50",
    "currency": "RUB",
    "processed_at": "2026-10-15T07:32:15.919291Z"
  }
  ```

  Также передаётся заголовок `X-Webhook-Event-Id` — получатель дедуплицирует по нему.
- Доставляет фоновый `webhook-dispatcher` внутри процесса consumer. Он «будится» сразу после
  обработки платежа и дополнительно опрашивает таблицу. Захват — тот же `FOR UPDATE SKIP LOCKED` + lease.
- Одна попытка — один POST с таймаутом `WEBHOOK_TIMEOUT_SECONDS`, редиректы не выполняются.
  - `2xx` → `delivered`;
  - `5xx`, `408`, `425`, `429`, таймаут, сетевая ошибка → повтор через `2 * 2^(n-1)` c (до 300 c);
  - прочие `4xx` / `3xx` → `failed` сразу;
  - после `WEBHOOK_MAX_ATTEMPTS` попыток → `failed` (запись остаётся для разбора).
- Повторы не выполняются внутри обработки сообщения и не блокируют обработку других платежей.

## Отказоустойчивость

| Сценарий | Поведение |
|---|---|
| API упал до коммита | транзакция откатывается: нет ни платежа, ни события; клиент повторяет запрос |
| API упал после коммита, до ответа | повтор с тем же `Idempotency-Key` вернёт созданный платёж |
| RabbitMQ недоступен | API продолжает принимать платежи (202); publisher повторяет с backoff; после восстановления события публикуются автоматически (проверено на стенде) |
| Publisher упал после публикации, до `published_at` | lease истекает, событие публикуется повторно; consumer отбрасывает дубликат |
| Consumer упал во время обработки | транзакция откатывается, сообщение без `ack` возвращается брокером и обрабатывается заново (проверено `docker kill` на стенде) |
| Сообщение пришло повторно / статус уже финальный | `ack` без вызова шлюза и без нового webhook |
| Webhook endpoint недоступен | ограниченные повторы с backoff, платёж не переобрабатывается |
| PostgreSQL недоступен | API → `503`; publisher и dispatcher логируют ошибку и повторяют с backoff; consumer → retry-очереди → DLQ; после восстановления всё продолжает работать без перезапуска (проверено на стенде) |
| Несколько экземпляров publisher / consumer | `SKIP LOCKED` + lease для outbox и webhooks, `FOR UPDATE` для платежа |
| Некорректное сообщение | сразу в DLQ без повторов |
| Исчерпан лимит попыток | DLQ; платёж остаётся `pending` до ручного разбора |
| Остановка (SIGTERM) | consumer дожидается текущих сообщений (graceful timeout), фоновые задачи освобождают захваченные записи |

## Тесты и проверки

Тесты работают с реальным PostgreSQL (отдельная БД `payments_test` пересоздаётся и мигрируется
Alembic в каждом запуске) и реальным RabbitMQ (отдельный vhost `payments-test` создаётся через
Management API). Внешний шлюз и HTTP-получатели webhooks замоканы. Работающий стенд тестам не мешает.

### В Docker (рекомендуется)

```bash
docker compose up -d postgres rabbitmq
docker compose --profile test run --rm --build tests
```

### Локально

```bash
uv sync                                  # создаёт .venv с dev-зависимостями
docker compose up -d postgres rabbitmq   # PostgreSQL и RabbitMQ доступны на 127.0.0.1
uv run pytest -v                         # все тесты
uv run pytest -m "not integration"       # без RabbitMQ
uv run ruff check . && uv run ruff format --check .
uv run mypy app
uv run alembic upgrade head && uv run alembic check   # миграции соответствуют моделям
```

Покрытые сценарии: создание платежа; валидация суммы, валюты, URL, ключа; отсутствующий
и неверный API key (включая несуществующие маршруты и битый JSON); получение существующего
и несуществующего платежа; повтор с тем же / другим телом; конкурентные запросы с одним ключом;
атомарность Payment + Outbox; публикация outbox, ошибки публикации, backoff, lease,
конкурентные publisher'ы; успешная и неуспешная обработка; повторная и конкурентная доставка;
retry и DLQ (unit и на реальном RabbitMQ); отправка webhook и повторы; ошибка webhook без
повторной обработки платежа; полный путь outbox → RabbitMQ → consumer → webhook; недоступность БД → 503.

### Демонстрация retry/DLQ на стенде

```bash
docker compose stop consumer
docker compose run -d --rm --name consumer-always-fails \
  -e GATEWAY_TRANSIENT_ERROR_RATE=1 \
  -e GATEWAY_MIN_DELAY_SECONDS=0.1 -e GATEWAY_MAX_DELAY_SECONDS=0.2 consumer
# создайте платёж; через ~7 c (попытки через 0, 2 и 4 c) сообщение окажется в payments.new.dlq
docker logs consumer-always-fails
docker compose exec rabbitmq rabbitmqctl list_queues name messages
docker stop consumer-always-fails && docker compose up -d consumer
```

## Известные ограничения

- **Эмулятор шлюза** не идемпотентен сам по себе: если consumer упадёт после ответа шлюза, но до
  коммита, шлюз будет вызван повторно. Интерфейс `PaymentGateway.charge` принимает `payment_id`,
  который реальный адаптер обязан передавать провайдеру как idempotency key.
- **10% «ошибок» эмулятора** трактуются как отказ шлюза: платёж получает финальный статус
  `failed`. Временные ошибки (`PaymentGatewayError`) по умолчанию выключены и включаются
  `GATEWAY_TRANSIENT_ERROR_RATE`.
- Сообщения из DLQ не переигрываются автоматически, платёж остаётся `pending`; инструмента
  для replay нет — используется Management UI / shovel.
- Блокировка строки платежа держится на время вызова шлюза (2–5 c): число одновременно
  обрабатываемых платежей одним consumer ограничено `CONSUMER_PREFETCH_COUNT` и размером пула БД.
- Недоставленные webhooks (`failed`) остаются в таблице; повторная отправка — вручную
  (`UPDATE webhook_deliveries SET status='pending', attempts=0, next_attempt_at=NULL WHERE ...`).
- Webhook не подписывается (HMAC), а `webhook_url` не проверяется на SSRF (внутренние адреса
  разрешены — это используется для локальной демонстрации).
- Один статический API-ключ без ротации; ключи идемпотентности хранятся бессрочно
  (нет TTL и очистки старых записей `outbox`).
- Swagger UI и OpenAPI-схема открыты без ключа (иначе браузер не сможет загрузить документацию);
  в них явно описана схема безопасности `X-API-Key`.
