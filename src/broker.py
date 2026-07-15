"""Топология RabbitMQ: основная очередь, очереди задержек и DLQ."""

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from src.config import settings

__all__ = [
    "DLQ_QUEUE",
    "DLX_EXCHANGE",
    "NEW_PAYMENTS_QUEUE",
    "NEW_PAYMENTS_ROUTING_KEY",
    "PAYMENTS_EXCHANGE",
    "RETRY_DELAYS_SECONDS",
    "RETRY_QUEUES",
    "build_broker",
    "declare_topology",
    "next_route",
]

NEW_PAYMENTS_ROUTING_KEY = "payments.new"

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
DLX_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

DLQ_QUEUE = RabbitQueue("payments.dlq", durable=True, routing_key="payments.dlq")

NEW_PAYMENTS_QUEUE = RabbitQueue(
    NEW_PAYMENTS_ROUTING_KEY,
    durable=True,
    routing_key=NEW_PAYMENTS_ROUTING_KEY,
    # Страховка на случай падения самого republish: тогда сообщение уедет в DLQ
    # средствами RabbitMQ через AckPolicy.REJECT_ON_ERROR.
    arguments={
        "x-dead-letter-exchange": DLX_EXCHANGE.name,
        "x-dead-letter-routing-key": DLQ_QUEUE.name,
    },
)

# Попытка 1 упала — ждём 2 с, попытка 2 — 4 с, попытка 3 — DLQ.
RETRY_DELAYS_SECONDS: tuple[int, ...] = tuple(
    settings.retry_base_delay_seconds * 2**i for i in range(settings.retry_max_attempts - 1)
)


def _retry_routing_key(delay: int) -> str:
    return f"payments.retry.{delay}s"


# Отдельная очередь на каждую задержку, а не одна с per-message TTL: RabbitMQ
# вытесняет по TTL только из головы очереди, поэтому сообщение на 4 с блокировало
# бы стоящее за ним сообщение на 2 с.
RETRY_QUEUES: tuple[RabbitQueue, ...] = tuple(
    RabbitQueue(
        _retry_routing_key(delay),
        durable=True,
        routing_key=_retry_routing_key(delay),
        arguments={
            "x-message-ttl": delay * 1000,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": NEW_PAYMENTS_ROUTING_KEY,
        },
    )
    for delay in RETRY_DELAYS_SECONDS
)


def next_route(attempt: int) -> tuple[str, int]:
    """Решить, куда уходит сообщение после провала попытки attempt.

    Args:
        attempt: Номер только что провалившейся попытки, начиная с 1.

    Returns:
        Routing key в payments.dlx и номер попытки для перепубликованного
        сообщения. Начиная с retry_max_attempts маршрут ведёт в DLQ.
    """
    if attempt >= settings.retry_max_attempts:
        return DLQ_QUEUE.name, attempt
    return _retry_routing_key(RETRY_DELAYS_SECONDS[attempt - 1]), attempt + 1


def build_broker() -> RabbitBroker:
    """Создать брокер, подключённый к настроенному RabbitMQ."""
    return RabbitBroker(settings.rabbitmq_url, graceful_timeout=30.0)


async def declare_topology(broker: RabbitBroker) -> None:
    """Объявить все обменники, очереди и привязки.

    Вызывается и в api, и в consumer, чтобы ни один порядок старта не терял
    сообщения: api не должен публиковать в обменник без привязанной очереди, а
    очереди задержек должны иметь куда dead-letter'иться. Объявления идемпотентны,
    пока все процессы берут определения из этого модуля.
    """
    await broker.declare_exchange(PAYMENTS_EXCHANGE)
    await broker.declare_exchange(DLX_EXCHANGE)

    main_queue = await broker.declare_queue(NEW_PAYMENTS_QUEUE)
    await main_queue.bind(PAYMENTS_EXCHANGE.name, routing_key=NEW_PAYMENTS_ROUTING_KEY)

    for queue in (DLQ_QUEUE, *RETRY_QUEUES):
        declared = await broker.declare_queue(queue)
        await declared.bind(DLX_EXCHANGE.name, routing_key=queue.routing())
