from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
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


class HistoryCollectionMode(StrEnum):
    BACKFILL = "backfill"
    INCREMENTAL = "incremental"


class CategoryRecord(BaseModel):
    category_key: str
    category_name: str
    parent_category_key: str | None = None
    first_seen_at: datetime
    last_seen_at: datetime


class MarketRecord(BaseModel):
    underlying_conid: int
    market_name: str
    symbol: str
    exchange: str
    product_conid: int | None = None
    category_key: str | None = None
    logo_category: str | None = None
    payout: float | None = None
    exclude_historical_data: bool | None = None
    active: bool = True
    first_seen_at: datetime
    last_seen_at: datetime
    last_discovered_at: datetime | None = None
    last_structure_collected_at: datetime | None = None
    last_probabilities_collected_at: datetime | None = None


class ContractRecord(BaseModel):
    conid: int
    underlying_conid: int
    side: str | None = None
    strike: float | None = None
    strike_label: str | None = None
    expiration: str | None = None
    expiry_label: str | None = None
    time_specifier: str | None = None
    question: str | None = None
    conid_yes: int | None = None
    conid_no: int | None = None
    product_conid: int | None = None
    market_name: str | None = None
    symbol: str | None = None
    measured_period: str | None = None
    measured_period_units: str | None = None
    active: bool = True
    first_seen_at: datetime
    last_seen_at: datetime
    last_details_collected_at: datetime | None = None
    last_open_interest_collected_at: datetime | None = None
    last_history_collected_at: datetime | None = None
    last_history_no_data_at: datetime | None = None

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


class OpenInterestBatchResult(BaseModel):
    snapshots: list[OpenInterestSnapshot] = Field(default_factory=list)
    blank_value_count: int = 0
    missing_conids: list[int] = Field(default_factory=list)


class CollectionSummary(BaseModel):
    run_id: int
    categories_processed: int = 0
    markets_processed: int = 0
    contracts_processed: int = 0
    history_points_inserted: int = 0
    open_interest_points_inserted: int = 0
    probability_points_inserted: int = 0
    blank_open_interest_values: int = 0
    missing_open_interest_ids: int = 0
    empty_probability_markets: int = 0
    no_data_history_contracts: int = 0
    errors: list[str] = Field(default_factory=list)
    message: str | None = None


class ScheduleDefinition(BaseModel):
    name: str
    interval_seconds: int
    command: list[str] = Field(default_factory=list)
    description: str | None = None


class HealthReport(BaseModel):
    generated_at: datetime
    total_markets: int
    active_markets: int
    inactive_markets: int
    new_contracts_last_24h: int
    failed_runs_last_24h: int
    failed_runs_by_job: dict[str, int] = Field(default_factory=dict)
    raw_responses_last_24h: dict[str, int] = Field(default_factory=dict)
    empty_probability_markets_last_24h: list[int] = Field(default_factory=list)
    history_no_data_contracts_last_24h: list[int] = Field(default_factory=list)
    raw_api_responses_disk_bytes: int | None = None
