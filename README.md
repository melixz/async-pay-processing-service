# Асинхронный сервис процессинга платежей

Микросервис принимает платежи по HTTP, отдаёт `202 Accepted`, обрабатывает их асинхронно
через эмулятор платёжного шлюза и уведомляет мерчанта через webhook.

Стек: FastAPI + Pydantic v2, SQLAlchemy 2.0 (async), PostgreSQL, RabbitMQ (FastStream),
Alembic, Docker Compose.

## Архитектура

### Outbox pattern

`POST /payments` одной транзакцией пишет строку в `payments` и строку в `outbox`. Событие
физически не может потеряться после ответа `202` и не может быть опубликовано для платежа,
чей коммит откатился.

Relay фоновая `asyncio`-задача внутри процесса `api` ([src/outbox.py](src/outbox.py)). Строки
забираются через `FOR UPDATE SKIP LOCKED`, поэтому несколько реплик `api` могут работать
параллельно, не публикуя одну строку дважды. Публикация и проставление `published_at` идут в одной
транзакции: если процесс упадёт между ними, строка останется неопубликованной и уйдёт в брокер
на следующем тике. Доставка получается **at-least-once** дубли гасит идемпотентность consumer'а.

Relay живёт в `api`, а не в отдельном контейнере, чтобы состав compose остался ровно таким, как
в ТЗ: `postgres`, `rabbitmq`, `api`, `consumer`.

### Идемпотентность

Три независимых уровня:

| Уровень | Механизм                                                                                           |
|---|----------------------------------------------------------------------------------------------------|
| HTTP | `UNIQUE(idempotency_key)`; повтор возвращает существующий платёж                                   |
| HTTP | `request_fingerprint` sha256 тела запроса; тот же ключ с другим телом → `409 Conflict`             |
| Consumer | шлюз вызывается только пока `status = pending`, запись идёт через `UPDATE … WHERE status = 'pending'` |

Условный `UPDATE` это и есть защита от дублей брокера: параллельный дубль обновит 0 строк,
после чего consumer перечитает итог, записанный победителем гонки.

### Retry и DLQ

Ретраится **доставка webhook**. Бизнес-отказ шлюза (10%) это не ошибка обработки, а терминальный
статус `failed`, о котором мерчант штатно узнаёт из webhook.

Задержка растёт экспоненциально: попытка 1 → 2 с, попытка 2 → 4 с, попытка 3 → `payments.dlq`.
Consumer не спит и не занимает prefetch: он публикует сообщение в очередь-задержку с
`x-message-ttl`, откуда оно само dead-letter'ится обратно в `payments.new`.

На каждую задержку заведена **отдельная очередь**. RabbitMQ вытесняет по TTL только из головы
очереди, поэтому в одной очереди с per-message TTL сообщение на 4 с блокировало бы стоящее за ним
сообщение на 2 с.

Страховка: у `payments.new` прописан `x-dead-letter-exchange`. Если упадёт сам republish
(например, брокер недоступен), сообщение отклонится через `AckPolicy.REJECT_ON_ERROR` и уедет
в `payments.dlq` средствами самого RabbitMQ.

## Запуск

```bash
cp .env.example .env      # при желании поменяйте API_KEY
docker compose up --build
```

Поднимутся `postgres`, `rabbitmq`, `api` (сам применяет `alembic upgrade head`) и `consumer`.
`consumer` ждёт healthcheck `api`, чтобы не стартовать до создания таблиц.

- API + Swagger: http://localhost:8000/docs
- RabbitMQ Management: http://localhost:15672 (`guest` / `guest`)

## Примеры

Все эндпоинты требуют `X-API-Key`.

### Создание платежа

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: local-dev-secret-key' \
  -H 'Idempotency-Key: order-42' \
  -H 'Content-Type: application/json' \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Подписка Pro, 1 мес",
        "metadata": {"order_id": 42, "user_id": "u-777"},
        "webhook_url": "https://webhook.site/<ваш-uuid>"
      }'
```

```
HTTP/1.1 202 Accepted

{
  "payment_id": "0f0a1a2e-6d7b-4c1f-9a3d-8e2b5c4f1d90",
  "status": "pending",
  "created_at": "2026-07-15T16:45:52.889394Z"
}
```

Повтор того же запроса с тем же `Idempotency-Key` вернёт тот же `payment_id`.
Тот же ключ с другим телом вернёт `409 Conflict`.

### Получение платежа

```bash
curl -s http://localhost:8000/api/v1/payments/0f0a1a2e-6d7b-4c1f-9a3d-8e2b5c4f1d90 \
  -H 'X-API-Key: local-dev-secret-key'
```

```json
{
  "id": "0f0a1a2e-6d7b-4c1f-9a3d-8e2b5c4f1d90",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Подписка Pro, 1 мес",
  "metadata": {"order_id": 42, "user_id": "u-777"},
  "status": "succeeded",
  "idempotency_key": "order-42",
  "webhook_url": "https://webhook.site/<ваш-uuid>",
  "created_at": "2026-07-15T16:45:52.889394Z",
  "processed_at": "2026-07-15T16:45:56.402117Z"
}
```

### Webhook

Через 2–5 секунд на `webhook_url` уходит `POST` с заголовком `X-Correlation-ID`:

```json
{
  "event": "payment.processed",
  "payment_id": "0f0a1a2e-6d7b-4c1f-9a3d-8e2b5c4f1d90",
  "status": "succeeded",
  "amount": "1500.00",
  "currency": "RUB",
  "metadata": {"order_id": 42, "user_id": "u-777"},
  "processed_at": "2026-07-15T16:45:56.402117Z"
}
```

Ответ не 2xx (либо таймаут) запускает ретраи. Путь до DLQ проверяется заведомо битым
`webhook_url`:

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: local-dev-secret-key' \
  -H 'Idempotency-Key: order-dlq-1' \
  -H 'Content-Type: application/json' \
  -d '{"amount": "10.00", "currency": "USD", "webhook_url": "http://127.0.0.1:9/hook"}'
```

Примерно через 6 секунд (2 с + 4 с) сообщение окажется в `payments.dlq` видно в
RabbitMQ Management или в логах consumer'а.

## Конфигурация

Переменные окружения ([src/config.py](src/config.py)). Обязательны `API_KEY`, `DATABASE_URL`,
`RABBITMQ_URL`.

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `API_KEY` | — | значение заголовка `X-API-Key` |
| `DATABASE_URL` | — | DSN PostgreSQL (`postgresql+asyncpg://…`) |
| `RABBITMQ_URL` | — | DSN RabbitMQ (`amqp://…`) |
| `RETRY_MAX_ATTEMPTS` | `3` | попыток до DLQ |
| `RETRY_BASE_DELAY_SECONDS` | `2` | база экспоненты; очереди задержек строятся из неё |
| `OUTBOX_POLL_INTERVAL_SECONDS` | `1.0` | пауза relay, когда outbox пуст |
| `OUTBOX_BATCH_SIZE` | `100` | размер батча relay |
| `WEBHOOK_TIMEOUT_SECONDS` | `10.0` | таймаут доставки webhook |
| `GATEWAY_MIN_LATENCY_SECONDS` | `2.0` | нижняя граница эмуляции шлюза |
| `GATEWAY_MAX_LATENCY_SECONDS` | `5.0` | верхняя граница эмуляции шлюза |
| `GATEWAY_SUCCESS_RATE` | `0.9` | доля успешных платежей |
| `LOG_LEVEL` | `INFO` | уровень логирования |

## Разработка

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv/Scripts/python.exe -r requirements.txt pytest
docker compose up -d postgres rabbitmq

.venv/Scripts/python.exe -m pytest tests -q
.venv/Scripts/alembic.exe upgrade head
```

Тесты покрывают маршрутизацию ретраев (`retry.2s → retry.4s → dlq`) и отпечаток тела запроса
две ветки, от которых зависят гарантии доставки и идемпотентность.
