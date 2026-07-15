"""Общие зависимости FastAPI: аутентификация по API-ключу и сессия на запрос."""

import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.database import get_session

__all__ = ["SessionDep", "require_api_key"]

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(api_key: Annotated[str | None, Depends(_api_key_header)]) -> None:
    """Отклонить запрос, если X-API-Key не совпал с настроенным ключом.

    Raises:
        HTTPException: 401, если заголовок отсутствует или не совпадает.
    """
    if api_key is None or not secrets.compare_digest(api_key, settings.api_key):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Missing or invalid X-API-Key")


SessionDep = Annotated[AsyncSession, Depends(get_session)]
