from __future__ import annotations

from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = Field(alias="DATABASE_URL")
    ibkr_base_url: str = Field(
        default="https://forecasttrader.interactivebrokers.ie", alias="IBKR_BASE_URL"
    )
    ibkr_public_prefix: str = Field(default="/tws.proxy/public", alias="IBKR_PUBLIC_PREFIX")
    ibkr_exchange: str = Field(default="FORECASTX", alias="IBKR_EXCHANGE")
    seed_underlying_conid: int | None = Field(default=None, alias="SEED_UNDERLYING_CONID")
    http_timeout_seconds: float = Field(default=20.0, alias="HTTP_TIMEOUT_SECONDS")
    http_max_retries: int = Field(default=5, alias="HTTP_MAX_RETRIES")
    http_retry_backoff_seconds: float = Field(default=1.0, alias="HTTP_RETRY_BACKOFF_SECONDS")
    http_requests_per_second: float = Field(default=1.0, alias="HTTP_REQUESTS_PER_SECOND")
    history_periods: list[str] = Field(default_factory=lambda: ["1week", "1month"], alias="HISTORY_PERIODS")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    tz: str = Field(default="UTC", alias="TZ")
    sql_directory: Path = Field(default=Path("sql"))

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("history_periods", mode="before")
    @classmethod
    def parse_history_periods(cls, value: object) -> list[str]:
        if value is None:
            return ["1week", "1month"]
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        raise TypeError("HISTORY_PERIODS must be a comma-delimited string or a list")


def load_settings() -> Settings:
    return Settings()
