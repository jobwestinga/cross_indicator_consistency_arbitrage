from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

from forecast_collector.models import ApiResponseEnvelope, ContractRecord, MarketRecord
from forecast_collector.repository import CollectorRepository


TEST_DATABASE_URL = "TEST_DATABASE_URL"


@pytest.mark.skipif(TEST_DATABASE_URL not in __import__("os").environ, reason="TEST_DATABASE_URL not set")
def test_repository_upserts_market_and_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    database_url = __import__("os").environ[TEST_DATABASE_URL]
    sql_directory = Path("sql")
    now = datetime(2026, 3, 20, tzinfo=UTC)

    with CollectorRepository(database_url) as repository:
        repository.run_migrations(sql_directory)
        run_id = repository.start_run("test")
        repository.record_raw_response(
            run_id,
            ApiResponseEnvelope(
                endpoint_name="market",
                request_url="https://example.test/market",
                query_params={"underlyingConid": 766914406},
                http_status=200,
                response_json={"ok": True},
                fetched_at=now,
            ),
        )
        repository.upsert_market(
            MarketRecord(
                underlying_conid=766914406,
                market_name="Test Market",
                symbol="FF",
                exchange="FORECASTX",
                logo_category="rates",
                payout=1.0,
                exclude_historical_data=False,
                first_seen_at=now,
                last_seen_at=now,
            )
        )
        repository.upsert_contract(
            ContractRecord(
                conid=767285167,
                underlying_conid=766914406,
                side="Y",
                strike=4.5,
                strike_label="Yes",
                expiration="2025-03-19T19:00:00Z",
                question="Question",
                conid_yes=767285167,
                conid_no=767285168,
                product_conid=777000001,
                market_name="Test Market",
                symbol="FF",
                measured_period="March 2025",
                measured_period_units="month",
                first_seen_at=now,
                last_seen_at=now,
            )
        )
        repository.finish_run(run_id, "success")
        rows = repository.list_contracts_for_underlying(766914406)

    assert len(rows) >= 1
    assert rows[0]["conid"] == 767285167
