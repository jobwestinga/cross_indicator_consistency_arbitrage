from __future__ import annotations

import json
import zipfile
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from forecast_collector.models import ApiResponseEnvelope, HistoryCollectionMode
from forecast_collector.service_export import DatasetExportService
from forecast_collector.service_history import HistoryCollectorService
from forecast_collector.service_market import MarketCollectorService


SAMPLES = Path("samples")


def _load_sample(name: str) -> dict | list:
    return json.loads((SAMPLES / name).read_text(encoding="utf-8"))


class FakeHistoryRepository:
    def __init__(self) -> None:
        self.finished: tuple[int, str, str | None, dict | None] | None = None
        self.job_args: dict | None = None
        self.marked_history: list[tuple[int, bool, str | None]] = []
        self.recorded_responses: list[ApiResponseEnvelope] = []
        self.inserted_points = 0
        self.incremental_calls: list[tuple[list[str], int, int | None]] = []
        self.backfill_calls: list[tuple[list[str], int, int | None]] = []

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
        return [
            {"conid": 101, "underlying_conid": underlying_conid},
            {"conid": 202, "underlying_conid": underlying_conid},
        ]

    def list_history_requests_for_incremental(
        self,
        periods: list[str],
        *,
        limit: int,
        underlying_conid: int | None = None,
    ) -> list[dict]:
        self.incremental_calls.append((list(periods), limit, underlying_conid))
        return [
            {
                "conid": 101,
                "underlying_conid": 793085688,
                "period_requested": "1week",
            },
            {
                "conid": 202,
                "underlying_conid": 793085688,
                "period_requested": "1month",
            },
        ]

    def list_history_requests_for_backfill(
        self,
        periods: list[str],
        *,
        limit: int,
        no_data_retry_before: datetime,
        underlying_conid: int | None = None,
    ) -> list[dict]:
        self.backfill_calls.append((list(periods), limit, underlying_conid))
        return []

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
        period_requested: str | None = None,
    ) -> None:
        self.marked_history.append((conid, no_data, period_requested))


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


class FakeExportRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple]] = []
        self.payloads = {
            "market_categories": "category_key,category_name\ncrypto,Crypto\n",
            "markets": "underlying_conid,market_name\n793085688,BTC Price\n",
            "contracts": "underlying_conid,conid\n793085688,101\n",
            "projected_probabilities": "underlying_conid,collected_at\n793085688,2026-03-21T00:00:00Z\n",
            "open_interest_snapshots": "underlying_conid,collected_at\n793085688,2026-03-21T00:00:00Z\n",
            "contract_history": "underlying_conid,ts_utc\n793085688,2026-03-21T00:00:00Z\n",
        }

    def write_query_csv(
        self,
        cursor_label: str,
        query: str,
        output,
        params: tuple | None = None,
        *,
        fetch_size: int = 10_000,
    ) -> int:
        del query, fetch_size
        self.calls.append((cursor_label, tuple(params or ())))
        payload = self.payloads[cursor_label]
        output.write(payload)
        return max(0, len(payload.strip().splitlines()) - 1)


def test_history_collection_continues_when_one_request_fails() -> None:
    settings = SimpleNamespace(
        history_workers=4,
        history_periods=["1week", "1month"],
    )
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
    settings = SimpleNamespace(
        history_workers=4,
        history_periods=["1week", "1month"],
    )
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
        "request_limit": None,
    }


def test_history_collection_uses_bounded_incremental_requests_for_all_markets() -> None:
    settings = SimpleNamespace(
        history_workers=1,
        history_periods=["1week", "1month"],
        history_incremental_request_limit=25,
        history_backfill_request_limit=50,
        history_no_data_retry_hours=24,
    )
    client = FakeHistoryClient()
    repository = FakeHistoryRepository()

    summary = HistoryCollectorService(settings, client, repository).collect(
        all_discovered=True,
        mode=HistoryCollectionMode.INCREMENTAL,
        history_periods=["1week", "1month"],
    )

    assert summary.contracts_processed == 2
    assert summary.markets_processed == 1
    assert summary.history_points_inserted == 1
    assert len(summary.errors) == 1
    assert repository.incremental_calls == [(["1week", "1month"], 25, None)]
    assert repository.backfill_calls == []
    assert client.calls == [(101, "1week"), (202, "1month")]
    assert repository.job_args == {
        "underlying_conid": None,
        "all_discovered": True,
        "mode": "incremental",
        "contract_limit": None,
        "history_periods": ["1week", "1month"],
        "request_limit": None,
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


def test_dataset_export_creates_shareable_zip_bundle(tmp_path: Path) -> None:
    repository = FakeExportRepository()
    since = datetime(2026, 3, 1, tzinfo=UTC)

    summary = DatasetExportService(repository).export(
        tmp_path,
        dataset_name="analysis_snapshot",
        underlying_conid=793085688,
        since=since,
    )

    bundle_path = tmp_path / "analysis_snapshot.zip"

    assert bundle_path.exists()
    assert summary.bundle_path == str(bundle_path)
    assert [item.name for item in summary.files] == [
        "market_categories.csv",
        "markets.csv",
        "contracts.csv",
        "projected_probabilities.csv",
        "open_interest_snapshots.csv",
        "contract_history.csv",
    ]

    with zipfile.ZipFile(bundle_path) as archive:
        assert set(archive.namelist()) == {
            "market_categories.csv",
            "markets.csv",
            "contracts.csv",
            "projected_probabilities.csv",
            "open_interest_snapshots.csv",
            "contract_history.csv",
            "manifest.json",
        }
        manifest = json.loads(archive.read("manifest.json"))

    assert manifest["underlying_conid"] == 793085688
    assert manifest["since"] == since.isoformat()
    assert repository.calls[0][0] == "market_categories"
    assert repository.calls[-1][0] == "contract_history"
