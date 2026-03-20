from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


JsonDict = dict[str, Any]


@dataclass(frozen=True)
class RequestSpec:
    endpoint_name: str
    path: str
    params: dict[str, Any]
    fallback_paths: tuple[str, ...] = ()


class ApiResponseEnvelope(BaseModel):
    endpoint_name: str
    request_url: str
    query_params: dict[str, Any]
    http_status: int
    response_json: Any
    fetched_at: datetime


class MarketRecord(BaseModel):
    underlying_conid: int
    market_name: str
    symbol: str
    exchange: str
    logo_category: str | None = None
    payout: float | None = None
    exclude_historical_data: bool | None = None
    first_seen_at: datetime
    last_seen_at: datetime


class ContractRecord(BaseModel):
    conid: int
    underlying_conid: int
    side: str | None = None
    strike: float | None = None
    strike_label: str | None = None
    expiration: str | None = None
    question: str | None = None
    conid_yes: int | None = None
    conid_no: int | None = None
    product_conid: int | None = None
    market_name: str | None = None
    symbol: str | None = None
    measured_period: str | None = None
    measured_period_units: str | None = None
    first_seen_at: datetime
    last_seen_at: datetime

    def merge(self, other: "ContractRecord") -> "ContractRecord":
        merged = self.model_dump()
        for key, value in other.model_dump().items():
            if value is not None:
                merged[key] = value
        return ContractRecord(**merged)


class HistoryPoint(BaseModel):
    conid: int
    ts_utc: datetime
    avg: float | None = None
    volume: int | None = None
    chart_step: str | None = None
    source: str | None = None
    period_requested: str
    collected_at: datetime


class OpenInterestSnapshot(BaseModel):
    conid: int
    open_interest: int | None = None
    collected_at: datetime


class ProjectedProbability(BaseModel):
    underlying_conid: int
    strike: float | None = None
    expiry: str | None = None
    probability: float | None = None
    collected_at: datetime


class CollectionSummary(BaseModel):
    run_id: int
    markets_processed: int = 0
    contracts_processed: int = 0
    history_points_inserted: int = 0
    open_interest_points_inserted: int = 0
    probability_points_inserted: int = 0
    message: str | None = None


class ScheduleDefinition(BaseModel):
    name: str
    interval_seconds: int
    command: list[str] = Field(default_factory=list)
