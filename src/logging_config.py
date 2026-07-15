"""Структурное JSON-логирование для всех процессов сервиса."""

import logging

from pythonjsonlogger.json import JsonFormatter

from src.config import settings

__all__ = ["setup_logging"]

_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def setup_logging() -> None:
    """Направить корневой логгер в stdout однострочным JSON. Идемпотентно."""
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter(_FORMAT, rename_fields={"asctime": "timestamp", "levelname": "level"}))

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(settings.log_level)
