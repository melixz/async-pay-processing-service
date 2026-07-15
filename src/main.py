"""Приложение FastAPI: эндпоинты платежей плюс outbox relay."""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from fastapi import Depends, FastAPI

from src.api.deps import require_api_key
from src.api.payments import router as payments_router
from src.broker import build_broker, declare_topology
from src.logging_config import setup_logging
from src.outbox import run_relay

__all__ = ["app"]

setup_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    """Владеть подключением к брокеру и задачей outbox relay.

    Relay живёт здесь, а не в отдельном контейнере, чтобы состав compose остался
    ровно таким, как в ТЗ: postgres, rabbitmq, api, consumer. Несколько реплик api
    безопасны — строки забираются через FOR UPDATE SKIP LOCKED.
    """
    broker = build_broker()
    await broker.connect()
    await declare_topology(broker)
    relay = asyncio.create_task(run_relay(broker), name="outbox-relay")
    logger.info("api started")
    try:
        yield
    finally:
        relay.cancel()
        with suppress(asyncio.CancelledError):
            await relay
        await broker.stop()
        logger.info("api stopped")


app = FastAPI(
    title="Payment Processing Service",
    version="1.0.0",
    lifespan=lifespan,
)
app.include_router(payments_router, prefix="/api/v1", dependencies=[Depends(require_api_key)])


@app.get("/health", tags=["ops"], summary="Проверка живости")
async def health() -> dict[str, str]:
    """Сообщить, что процесс поднят. Без аутентификации, для healthcheck контейнера."""
    return {"status": "ok"}
