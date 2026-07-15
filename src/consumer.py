"""Единственный обработчик payments.new: провести, уведомить, повторить или отбросить в DLQ."""

import asyncio
import logging
import random
from datetime import UTC, datetime
from uuid import UUID

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitMessage
from sqlalchemy import update

from src.broker import (
    DLX_EXCHANGE,
    NEW_PAYMENTS_QUEUE,
    PAYMENTS_EXCHANGE,
    build_broker,
    declare_topology,
    next_route,
)
from src.config import settings
from src.database import session_factory
from src.logging_config import setup_logging
from src.models import Payment, PaymentStatus
from src.schemas import PaymentCreatedEvent, WebhookPayload

__all__ = ["app", "broker", "handle_new_payment"]

setup_logging()
logger = logging.getLogger(__name__)

broker = build_broker()
app = FastStream(broker)
http_client = httpx.AsyncClient(timeout=settings.webhook_timeout_seconds)


class PaymentNotFoundError(Exception):
    """Событие ссылается на платёж, которого нет в базе."""

    def __init__(self, payment_id: UUID) -> None:
        super().__init__(f"платежа {payment_id} не существует")
        self.payment_id = payment_id


async def _call_gateway() -> PaymentStatus:
    """Эмулировать внешний шлюз: задержка 2-5 с, 90% успеха."""
    await asyncio.sleep(
        random.uniform(settings.gateway_min_latency_seconds, settings.gateway_max_latency_seconds)  # noqa: S311
    )
    succeeded = random.random() < settings.gateway_success_rate  # noqa: S311
    return PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED


async def _settle(payment_id: UUID) -> Payment:
    """Провести платёж через шлюз и записать результат.

    Идемпотентно: шлюз вызывается только пока платёж в pending, а запись условна
    по этому же статусу, поэтому дубль доставки не перезапишет исход, уже
    зафиксированный другим consumer'ом.

    Args:
        payment_id: Платёж, на который ссылается событие.

    Returns:
        Платёж в финальном статусе.

    Raises:
        PaymentNotFoundError: Такого платежа нет.
    """
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)

        if payment.status is not PaymentStatus.PENDING:
            logger.info(
                "payment already settled, skipping gateway",
                extra={"payment_id": str(payment_id), "status": payment.status.value},
            )
            return payment

        outcome = await _call_gateway()
        await session.execute(
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=outcome, processed_at=datetime.now(UTC))
        )
        await session.commit()
        await session.refresh(payment)

    logger.info(
        "payment settled",
        extra={"payment_id": str(payment_id), "status": payment.status.value},
    )
    return payment


async def _deliver_webhook(payment: Payment, correlation_id: UUID) -> None:
    """Отправить результат на webhook_url мерчанта.

    Raises:
        httpx.HTTPError: Эндпоинт недоступен, ответил не 2xx или истёк таймаут.
            Исключение уводит сообщение в ретрай.
    """
    response = await http_client.post(
        payment.webhook_url,
        json=WebhookPayload.model_validate(payment).model_dump(mode="json"),
        headers={"X-Correlation-ID": str(correlation_id)},
    )
    response.raise_for_status()
    logger.info(
        "webhook delivered",
        extra={
            "payment_id": str(payment.id),
            "correlation_id": str(correlation_id),
            "webhook_url": payment.webhook_url,
            "status_code": response.status_code,
        },
    )


async def _route_failure(event: PaymentCreatedEvent, attempt: int, exc: Exception) -> None:
    """Перепубликовать упавшее сообщение в очередь задержки или в DLQ.

    Raises:
        Exception: Если упала сама перепубликация. Тогда неподтверждённый
            оригинал отклонит AckPolicy.REJECT_ON_ERROR, и сообщение уедет в DLQ
            через x-dead-letter-exchange самой очереди.
    """
    routing_key, next_attempt = next_route(attempt)
    logger.warning(
        "payment processing failed",
        extra={
            "payment_id": str(event.payment_id),
            "correlation_id": str(event.correlation_id),
            "attempt": attempt,
            "routed_to": routing_key,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        },
    )
    await broker.publish(
        event.model_dump(mode="json"),
        exchange=DLX_EXCHANGE,
        routing_key=routing_key,
        correlation_id=str(event.correlation_id),
        headers={"x-attempt": next_attempt, "x-error": f"{type(exc).__name__}: {exc}"[:255]},
        persist=True,
    )


@broker.subscriber(NEW_PAYMENTS_QUEUE, PAYMENTS_EXCHANGE, ack_policy=AckPolicy.REJECT_ON_ERROR)
async def handle_new_payment(event: PaymentCreatedEvent, msg: RabbitMessage) -> None:
    """Провести платёж, затем уведомить мерчанта.

    Ошибки не пробрасываются наверх: они перепубликуются в очередь задержки для
    текущей попытки либо в DLQ, когда retry_max_attempts исчерпан.
    """
    attempt = int(msg.headers.get("x-attempt", 1))
    try:
        payment = await _settle(event.payment_id)
        await _deliver_webhook(payment, event.correlation_id)
    except Exception as exc:
        await _route_failure(event, attempt, exc)


@app.after_startup
async def _declare() -> None:
    await declare_topology(broker)


@app.on_shutdown
async def _close_http_client() -> None:
    await http_client.aclose()
