"""Конфигурация из переменных окружения."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Настройки сервиса.

    Значения берутся из окружения или локального .env. Отсутствие обязательной
    переменной роняет процесс на импорте с ValidationError.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    api_key: str = Field(min_length=8)
    database_url: str
    rabbitmq_url: str

    outbox_poll_interval_seconds: float = Field(default=1.0, gt=0)
    outbox_batch_size: int = Field(default=100, gt=0)

    retry_max_attempts: int = Field(default=3, ge=1)
    retry_base_delay_seconds: int = Field(default=2, gt=0)

    webhook_timeout_seconds: float = Field(default=10.0, gt=0)

    gateway_min_latency_seconds: float = Field(default=2.0, ge=0)
    gateway_max_latency_seconds: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    log_level: str = "INFO"


settings = Settings()
