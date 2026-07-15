"""Асинхронный движок и фабрика сессий, общие для api и consumer."""

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.config import settings

__all__ = ["engine", "get_session", "session_factory"]

engine = create_async_engine(settings.database_url, pool_pre_ping=True, pool_size=10, max_overflow=5)

session_factory = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    """Выдать сессию на время одного запроса; закрывается всегда."""
    async with session_factory() as session:
        yield session
