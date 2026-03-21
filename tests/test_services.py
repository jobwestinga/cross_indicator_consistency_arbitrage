from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from forecast_collector.models import ApiResponseEnvelope
from forecast_collector.service_history import HistoryCollectorService
from forecast_collector.service_market import MarketCollectorService


SAMPLES = Path("samples")


def _load_sample(name: str) -> dict | list:
    return json.loads((SAMPLES / name).read_text(encoding="utf-8"))


class FakeHistoryRepository:
    def __init__(self) -> None:
        self.finished: tuple[int, str, str | None, dict | None] | None = None
        self.job_args: dict | None = None
        self.marked_history: list[tuple[int, bool]] = []
        self.recorded_responses: list[ApiResponseEnvelope] = []
        self.inserted_points = 0

    def acquire_advisory_lock(self, lock_name: str) -> bool:
        return True

    def release_advisory_lock(self, lock_name: str) -> None:
        return None

    def start_run(self, job_name: str, job_args: dict | None = None) -> int:
        self.job_args = job_args
        return 17

    def finish_run(
        self,
        run_id: int,
        status: str,
        error_text: str | None = None,
        summary: dict | None = None,
    ) -> None:
        self.finished = (run_id, status, error_text, summary)

    def list_contracts_for_underlying(
        self,
        underlying_conid: int,
        *,
        active_only: bool = True,
    ) -> list[dict]:
        return [{"conid": 101}, {"conid": 202}]

    @contextmanager
    def transaction(self):
        yield

    def record_raw_response(self, run_id: int, response: ApiResponseEnvelope) -> None:
        self.recorded_responses.append(response)

    def insert_history_points(self, points: list) -> int:
        self.inserted_points += len(points)
        return len(points)

    def mark_contract_history_collected(
        self,
        conid: int,
        collected_at: datetime,
        *,
        no_data: bool = False,
    ) -> None:
        self.marked_history.append((conid, no_data))


class FakeHistoryClient:
    def __init__(self) -> None:
        self.calls: list[tuple[int, str]] = []

    def get_history(self, conid: int, period: str) -> ApiResponseEnvelope:
        self.calls.append((conid, period))
        if (conid, period) == (202, "1month"):
            raise RuntimeError("upstream 500")
        return ApiResponseEnvelope(
            endpoint_name="history",
            request_url=f"https://example.test/history/{conid}/{period}",
            query_params={"conid": conid, "period": period},
            http_status=200,
            response_json={
                "time": [1731283200000],
                "avg": [0.42],
                "volume": [10],
                "chart_step": "1d",
                "source": "Last",
            },
            fetched_at=datetime(2026, 3, 21, tzinfo=UTC),
        )


class FakeMarketRepository:
    def __init__(self) -> None:
        self.finished: tuple[int, str, str | None, dict | None] | None = None
        self.upsert_batches: list[list] = []
        self.raw_responses: list[ApiResponseEnvelope] = []

    def acquire_advisory_lock(self, lock_name: str) -> bool:
        return True

    def release_advisory_lock(self, lock_name: str) -> None:
        return None

    def start_run(self, job_name: str, job_args: dict | None = None) -> int:
        return 23

    def finish_run(
        self,
        run_id: int,
        status: str,
        error_text: str | None = None,
        summary: dict | None = None,
    ) -> None:
        self.finished = (run_id, status, error_text, summary)

    @contextmanager
    def transaction(self):
        yield

    def record_raw_response(self, run_id: int, response: ApiResponseEnvelope) -> None:
        self.raw_responses.append(response)

    def upsert_market(self, market) -> None:
        return None

    def deactivate_missing_contracts(self, underlying_conid: int, active_conids: list[int]) -> None:
        return None

    def upsert_contracts(self, contracts: list) -> int:
        self.upsert_batches.append(list(contracts))
        return len(contracts)


class FakeMarketClient:
    def __init__(self) -> None:
        self.settings = SimpleNamespace(contract_details_workers=4)
        self.detail_calls: list[int] = []

    def get_market(self, underlying_conid: int) -> ApiResponseEnvelope:
        return ApiResponseEnvelope(
            endpoint_name="market",
            request_url=f"https://example.test/market/{underlying_conid}",
            query_params={"underlyingConid": underlying_conid},
            http_status=200,
            response_json=_load_sample("rcnet_market_response.json"),
            fetched_at=datetime(2026, 3, 21, tzinfo=UTC),
        )

    def get_contract_details(self, conid: int) -> ApiResponseEnvelope:
        self.detail_calls.append(conid)
        return ApiResponseEnvelope(
            endpoint_name="contract_details",
            request_url=f"https://example.test/contract-details/{conid}",
            query_params={"conid": conid},
            http_status=200,
            response_json=_load_sample("rcnet_contract_details_response.json"),
            fetched_at=datetime(2026, 3, 21, tzinfo=UTC),
        )


def test_history_collection_continues_when_one_request_fails() -> None:
    settings = SimpleNamespace(history_workers=4, history_periods=["1week", "1month"])
    client = FakeHistoryClient()
    repository = FakeHistoryRepository()

    summary = HistoryCollectorService(settings, client, repository).collect(793085688)

    assert summary.history_points_inserted == 3
    assert len(summary.errors) == 1
    assert "202" in summary.errors[0]
    assert "1month" in summary.errors[0]
    assert repository.finished is not None
    assert repository.finished[1] == "partial"


def test_history_collection_honors_smoke_test_limits() -> None:
    settings = SimpleNamespace(history_workers=4, history_periods=["1week", "1month"])
    client = FakeHistoryClient()
    repository = FakeHistoryRepository()

    summary = HistoryCollectorService(settings, client, repository).collect(
        793085688,
        contract_limit=1,
        history_periods=["1week"],
    )

    assert summary.history_points_inserted == 1
    assert summary.errors == []
    assert client.calls == [(101, "1week")]
    assert repository.job_args == {
        "underlying_conid": 793085688,
        "all_discovered": False,
        "mode": "backfill",
        "contract_limit": 1,
        "history_periods": ["1week"],
    }


def test_market_collection_can_limit_detail_enrichment_for_smoke_tests() -> None:
    client = FakeMarketClient()
    repository = FakeMarketRepository()

    summary = MarketCollectorService(client, repository).collect_seed_market(
        831072285,
        contract_details_limit=1,
    )

    assert summary.contracts_processed == 2
    assert summary.errors == []
    assert len(client.detail_calls) == 1
    assert len(repository.upsert_batches) == 2
    assert len(repository.upsert_batches[0]) == 2
    assert len(repository.upsert_batches[1]) == 1
    assert repository.finished is not None
    assert repository.finished[1] == "success"
