"""Создание платежа: одна транзакция пишет платёж и его outbox-событие."""

import hashlib
import logging
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.broker import NEW_PAYMENTS_ROUTING_KEY
from src.models import OutboxMessage, Payment
from src.schemas import PaymentCreate, PaymentCreatedEvent

__all__ = ["PAYMENT_CREATED_EVENT", "IdempotencyConflictError", "create_payment"]

logger = logging.getLogger(__name__)

PAYMENT_CREATED_EVENT = "payment.created"


class IdempotencyConflictError(Exception):
    """Idempotency-Key повторён с другим телом запроса."""

    def __init__(self, idempotency_key: str) -> None:
        super().__init__(f"Idempotency-Key {idempotency_key!r} уже использован с другим телом запроса")
        self.idempotency_key = idempotency_key


def _fingerprint(data: PaymentCreate) -> str:
    return hashlib.sha256(data.model_dump_json().encode()).hexdigest()


async def create_payment(session: AsyncSession, data: PaymentCreate, idempotency_key: str) -> Payment:
    """Сохранить платёж в статусе pending и поставить в очередь событие payment.created.

    Строки payments и outbox коммитятся вместе, поэтому событие не может
    потеряться после принятия платежа и не может быть опубликовано для платежа,
    чья транзакция откатилась.

    Args:
        session: Сессия, владеющая транзакцией; коммитится внутри функции.
        data: Провалидированное тело запроса.
        idempotency_key: Значение заголовка Idempotency-Key.

    Returns:
        Созданный платёж либо существующий, если idempotency_key повторяет
        идентичный запрос.

    Raises:
        IdempotencyConflictError: Ключ переиспользован с другим телом.
        IntegrityError: Любое другое нарушение ограничений, пробрасывается как есть.
    """
    fingerprint = _fingerprint(data)
    payment_id = uuid4()
    payment = Payment(
        id=payment_id,
        idempotency_key=idempotency_key,
        request_fingerprint=fingerprint,
        amount=data.amount,
        currency=data.currency,
        description=data.description,
        payment_metadata=data.metadata,
        webhook_url=str(data.webhook_url),
    )
    event = PaymentCreatedEvent(payment_id=payment_id, correlation_id=uuid4())
    session.add_all(
        [
            payment,
            OutboxMessage(
                aggregate_id=payment_id,
                event_type=PAYMENT_CREATED_EVENT,
                routing_key=NEW_PAYMENTS_ROUTING_KEY,
                payload=event.model_dump(mode="json"),
            ),
        ]
    )

    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        existing = await session.scalar(select(Payment).where(Payment.idempotency_key == idempotency_key))
        if existing is None:
            raise
        if existing.request_fingerprint != fingerprint:
            raise IdempotencyConflictError(idempotency_key) from None
        logger.info(
            "idempotent replay, returning existing payment",
            extra={"payment_id": str(existing.id), "idempotency_key": idempotency_key},
        )
        return existing

    logger.info(
        "payment accepted",
        extra={
            "payment_id": str(payment_id),
            "correlation_id": str(event.correlation_id),
            "idempotency_key": idempotency_key,
        },
    )
    return payment
