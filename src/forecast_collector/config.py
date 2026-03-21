from __future__ import annotations

import json
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
    open_interest_batch_size: int = Field(default=100, alias="OPEN_INTEREST_BATCH_SIZE")
    contract_details_workers: int = Field(default=8, alias="CONTRACT_DETAILS_WORKERS")
    history_workers: int = Field(default=8, alias="HISTORY_WORKERS")
    http_timeout_seconds: float = Field(default=20.0, alias="HTTP_TIMEOUT_SECONDS")
    http_max_retries: int = Field(default=5, alias="HTTP_MAX_RETRIES")
    http_retry_backoff_seconds: float = Field(default=1.0, alias="HTTP_RETRY_BACKOFF_SECONDS")
    http_requests_per_second: float = Field(default=8.0, alias="HTTP_REQUESTS_PER_SECOND")
    history_periods_raw: str = Field(default="1week,1month", alias="HISTORY_PERIODS")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    tz: str = Field(default="UTC", alias="TZ")
    sql_directory: Path = Field(default=Path("sql"))

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("history_periods_raw", mode="before")
    @classmethod
    def parse_history_periods(cls, value: object) -> str:
        if value is None:
            return "1week,1month"
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return "1week,1month"
            return stripped
        if isinstance(value, list):
            return json.dumps([str(item).strip() for item in value if str(item).strip()])
        raise TypeError("HISTORY_PERIODS must be a comma-delimited string or a list")

    @property
    def history_periods(self) -> list[str]:
        raw = self.history_periods_raw.strip()
        if raw.startswith("["):
            parsed = json.loads(raw)
            if not isinstance(parsed, list):
                raise TypeError("HISTORY_PERIODS JSON value must decode to a list")
            return [str(item).strip() for item in parsed if str(item).strip()]
        return [item.strip() for item in raw.split(",") if item.strip()]


def load_settings() -> Settings:
    return Settings()
