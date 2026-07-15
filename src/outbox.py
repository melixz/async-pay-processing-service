"""Outbox relay: переносит неопубликованные строки outbox в RabbitMQ."""

import asyncio
import logging
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker
from sqlalchemy import select

from src.broker import PAYMENTS_EXCHANGE
from src.config import settings
from src.database import session_factory
from src.models import OutboxMessage

__all__ = ["publish_batch", "run_relay"]

logger = logging.getLogger(__name__)


async def publish_batch(broker: RabbitBroker) -> int:
    """Опубликовать один батч событий и пометить их отправленными.

    Строки забираются через FOR UPDATE SKIP LOCKED, поэтому несколько реплик api
    могут работать параллельно, не публикуя одну строку дважды. Публикация и
    простановка published_at идут в одной транзакции: если процесс упадёт между
    ними, строка останется неопубликованной и уйдёт повторно. Отсюда доставка
    at-least-once, дубли гасит consumer.

    Args:
        broker: Подключённый брокер.

    Returns:
        Число опубликованных в этом батче событий.

    Raises:
        Exception: Любая ошибка брокера или БД пробрасывается вызывающему.
    """
    async with session_factory() as session, session.begin():
        rows = (
            await session.scalars(
                select(OutboxMessage)
                .where(OutboxMessage.published_at.is_(None))
                .order_by(OutboxMessage.created_at)
                .limit(settings.outbox_batch_size)
                .with_for_update(skip_locked=True)
            )
        ).all()

        for row in rows:
            await broker.publish(
                row.payload,
                exchange=PAYMENTS_EXCHANGE,
                routing_key=row.routing_key,
                message_id=str(row.id),
                correlation_id=str(row.payload["correlation_id"]),
                headers={"x-attempt": 1, "x-event-type": row.event_type},
                persist=True,
            )
            row.published_at = datetime.now(UTC)

    if rows:
        logger.info("outbox batch published", extra={"published": len(rows)})
    return len(rows)


async def run_relay(broker: RabbitBroker) -> None:
    """Гонять outbox бесконечно; спать только когда публиковать нечего.

    Упавший батч логируется и повторяется на следующем тике, а не роняет задачу:
    relay — фоновый демон процесса api.
    """
    while True:
        try:
            published = await publish_batch(broker)
        except Exception as exc:
            logger.error(
                "outbox batch failed, retrying next tick",
                extra={"error_type": type(exc).__name__, "error_message": str(exc)},
            )
            published = 0

        if published < settings.outbox_batch_size:
            await asyncio.sleep(settings.outbox_poll_interval_seconds)
