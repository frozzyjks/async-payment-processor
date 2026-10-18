from functools import lru_cache
from typing import Self
from urllib.parse import quote

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    api_key: SecretStr = Field(min_length=16)
    log_level: str = "INFO"

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "payments"
    postgres_password: SecretStr
    postgres_db: str = "payments"
    db_pool_size: int = Field(default=10, ge=1)
    db_max_overflow: int = Field(default=5, ge=0)
    db_lock_timeout_ms: int = Field(default=10_000, ge=100)

    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    rabbitmq_user: str = "payments"
    rabbitmq_password: SecretStr
    rabbitmq_vhost: str = "/"
    rabbitmq_publish_timeout_seconds: float = Field(default=10.0, gt=0)

    outbox_batch_size: int = Field(default=50, ge=1)
    outbox_poll_interval_seconds: float = Field(default=1.0, gt=0)
    outbox_lease_seconds: float = Field(default=60.0, gt=0)
    outbox_retry_base_delay_seconds: float = Field(default=1.0, gt=0)
    outbox_retry_max_delay_seconds: float = Field(default=60.0, gt=0)

    consumer_prefetch_count: int = Field(default=10, ge=1)
    consumer_delivery_limit: int = Field(default=5, ge=1)
    processing_max_attempts: int = Field(default=3, ge=1)
    processing_retry_base_delay_ms: int = Field(default=2_000, ge=1)

    gateway_min_delay_seconds: float = Field(default=2.0, ge=0)
    gateway_max_delay_seconds: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)
    gateway_transient_error_rate: float = Field(default=0.0, ge=0, le=1)

    webhook_timeout_seconds: float = Field(default=5.0, gt=0)
    webhook_max_attempts: int = Field(default=3, ge=1)
    webhook_retry_base_delay_seconds: float = Field(default=2.0, gt=0)
    webhook_retry_max_delay_seconds: float = Field(default=300.0, gt=0)
    webhook_batch_size: int = Field(default=20, ge=1)
    webhook_poll_interval_seconds: float = Field(default=1.0, gt=0)
    webhook_lease_seconds: float = Field(default=60.0, gt=0)

    @model_validator(mode="after")
    def _validate_consistency(self) -> Self:
        if self.gateway_min_delay_seconds > self.gateway_max_delay_seconds:
            raise ValueError("GATEWAY_MIN_DELAY_SECONDS must not exceed GATEWAY_MAX_DELAY_SECONDS")
        if self.outbox_lease_seconds <= self.rabbitmq_publish_timeout_seconds:
            raise ValueError("OUTBOX_LEASE_SECONDS must exceed RABBITMQ_PUBLISH_TIMEOUT_SECONDS")
        if self.webhook_lease_seconds <= self.webhook_timeout_seconds:
            raise ValueError("WEBHOOK_LEASE_SECONDS must exceed WEBHOOK_TIMEOUT_SECONDS")
        return self

    @property
    def database_url(self) -> URL:
        return URL.create(
            drivername="postgresql+asyncpg",
            username=self.postgres_user,
            password=self.postgres_password.get_secret_value(),
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
        )

    @property
    def rabbitmq_url(self) -> str:
        user = quote(self.rabbitmq_user, safe="")
        password = quote(self.rabbitmq_password.get_secret_value(), safe="")
        vhost = quote(self.rabbitmq_vhost, safe="")
        return f"amqp://{user}:{password}@{self.rabbitmq_host}:{self.rabbitmq_port}/{vhost}"

    @property
    def processing_retry_delays_ms(self) -> list[int]:
        return [
            self.processing_retry_base_delay_ms * 2**index
            for index in range(self.processing_max_attempts - 1)
        ]


@lru_cache
def get_settings() -> Settings:
    return Settings()
