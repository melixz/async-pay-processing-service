"""Эндпоинты платежей."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, status

from src.api.deps import SessionDep
from src.models import Payment
from src.schemas import PaymentAccepted, PaymentCreate, PaymentRead
from src.service import IdempotencyConflictError, create_payment

__all__ = ["router"]

router = APIRouter(prefix="/payments", tags=["payments"])

IdempotencyKey = Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)]


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    summary="Принять платёж в асинхронную обработку",
    responses={409: {"description": "Idempotency-Key повторён с другим телом"}},
)
async def submit_payment(
    data: PaymentCreate,
    session: SessionDep,
    idempotency_key: IdempotencyKey,
) -> PaymentAccepted:
    """Поставить платёж в очередь и сразу ответить; результат придёт по webhook."""
    try:
        payment = await create_payment(session, data, idempotency_key)
    except IdempotencyConflictError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc

    return PaymentAccepted(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


@router.get(
    "/{payment_id}",
    response_model=PaymentRead,
    summary="Получить платёж",
    responses={404: {"description": "Платежа не существует"}},
)
async def read_payment(payment_id: UUID, session: SessionDep) -> Payment:
    """Вернуть полное текущее состояние платежа."""
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Payment {payment_id} not found")
    return payment
