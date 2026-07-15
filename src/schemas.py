"""Контракты Pydantic v2 для HTTP API, брокера и webhook."""

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from src.models import Currency, PaymentStatus

__all__ = [
    "PaymentAccepted",
    "PaymentCreate",
    "PaymentCreatedEvent",
    "PaymentRead",
    "WebhookPayload",
]


class PaymentCreate(BaseModel):
    """Тело POST /api/v1/payments."""

    amount: Decimal = Field(gt=Decimal(0), max_digits=20, decimal_places=2)
    currency: Currency
    webhook_url: HttpUrl
    description: str = Field(default="", max_length=1024)
    metadata: dict[str, Any] = Field(default_factory=dict)


class PaymentAccepted(BaseModel):
    """Ответ POST /api/v1/payments."""

    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentRead(BaseModel):
    """Ответ GET /api/v1/payments/{payment_id}."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="payment_metadata")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None


class PaymentCreatedEvent(BaseModel):
    """Тело сообщения в payments.new."""

    payment_id: UUID
    correlation_id: UUID


class WebhookPayload(BaseModel):
    """Тело, доставляемое на webhook_url мерчанта."""

    model_config = ConfigDict(from_attributes=True)

    event: str = "payment.processed"
    payment_id: UUID = Field(validation_alias="id")
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    metadata: dict[str, Any] = Field(validation_alias="payment_metadata")
    processed_at: datetime | None
