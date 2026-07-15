"""Две ветки, от которых зависят гарантии: маршрутизация ретраев и идемпотентность."""

from decimal import Decimal

from src.broker import DLQ_QUEUE, RETRY_DELAYS_SECONDS, next_route
from src.models import Currency
from src.schemas import PaymentCreate
from src.service import _fingerprint


def _body(amount: str = "100.00", description: str = "заказ 1") -> PaymentCreate:
    return PaymentCreate(
        amount=Decimal(amount),
        currency=Currency.RUB,
        webhook_url="https://merchant.example/hook",
        description=description,
    )


def test_retry_delays_grow_exponentially() -> None:
    assert RETRY_DELAYS_SECONDS == (2, 4)


def test_failed_attempts_walk_the_retry_queues_then_land_in_the_dlq() -> None:
    assert next_route(1) == ("payments.retry.2s", 2)
    assert next_route(2) == ("payments.retry.4s", 3)
    assert next_route(3) == (DLQ_QUEUE.name, 3)


def test_identical_bodies_share_a_fingerprint() -> None:
    assert _fingerprint(_body()) == _fingerprint(_body())


def test_changed_body_breaks_the_fingerprint() -> None:
    assert _fingerprint(_body()) != _fingerprint(_body(amount="100.01"))
    assert _fingerprint(_body()) != _fingerprint(_body(description="заказ 2"))
