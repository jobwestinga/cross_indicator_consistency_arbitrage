from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from forecast_collector.parsers import (
    parse_contract_details_response,
    parse_history_response,
    parse_market_response,
    parse_open_interest_response,
    parse_projected_probabilities_response,
)


SAMPLES = Path("samples")


def _load_sample(name: str) -> dict | list:
    return json.loads((SAMPLES / name).read_text(encoding="utf-8"))


def test_parse_market_response_extracts_market_and_contracts() -> None:
    payload = _load_sample("market_response.json")
    collected_at = datetime(2026, 3, 20, tzinfo=UTC)
    market, contracts = parse_market_response(payload, collected_at)

    assert market.underlying_conid == 766914406
    assert market.symbol == "FF"
    assert market.market_name.startswith("Will the Fed")
    assert len(contracts) == 2
    assert {contract.side for contract in contracts} == {"Y", "N"}


def test_parse_market_response_uses_explicit_fallback_when_root_key_missing() -> None:
    payload = {
        "market_name": "Fallback Example",
        "symbol": "FALL",
        "exchange": "FORECASTX",
        "contracts": [
            {
                "conid": 1,
                "side": "Y",
                "strike": 1.0,
                "strike_label": "Yes",
                "expiration": "2027-01-13",
            }
        ],
    }
    collected_at = datetime(2026, 3, 20, tzinfo=UTC)
    market, contracts = parse_market_response(
        payload, collected_at, fallback_underlying_conid=999999
    )

    assert market.underlying_conid == 999999
    assert contracts[0].underlying_conid == 999999


def test_parse_contract_details_response_extracts_question_and_links() -> None:
    payload = _load_sample("contract_details_response.json")
    collected_at = datetime(2026, 3, 20, tzinfo=UTC)
    contract = parse_contract_details_response(
        payload,
        collected_at,
        fallback_underlying_conid=766914406,
        requested_conid=767285167,
    )

    assert contract.conid == 767285167
    assert contract.question is not None
    assert contract.conid_yes == 767285167
    assert contract.conid_no == 767285168
    assert contract.measured_period == "March 2025"


def test_parse_contract_details_response_prefers_requested_conid_when_root_key_missing() -> None:
    payload = {
        "question": "Will something exceed a threshold?",
        "conid_yes": 1001,
        "conid_no": 1002,
        "side": "N",
        "strike": 3.5,
        "symbol": "TEST",
        "market_name": "Test Market",
        "underlying_conid": 999,
    }
    collected_at = datetime(2026, 3, 20, tzinfo=UTC)
    contract = parse_contract_details_response(
        payload,
        collected_at,
        fallback_underlying_conid=999,
        requested_conid=1002,
    )

    assert contract.conid == 1002
    assert contract.side == "N"
    assert contract.conid_yes == 1001
    assert contract.conid_no == 1002


def test_parse_history_response_creates_points() -> None:
    payload = _load_sample("history_response.json")
    collected_at = datetime(2026, 3, 20, tzinfo=UTC)
    points = parse_history_response(payload, conid=767285167, period_requested="1week", collected_at=collected_at)

    assert len(points) == 3
    assert points[0].conid == 767285167
    assert points[0].period_requested == "1week"
    assert points[0].avg == 0.42


def test_parse_history_response_handles_missing_volume_array() -> None:
    payload = {
        "time": [1731283200000, 1731369600000],
        "avg": [0.42, 0.45],
        "chart_step": "1d",
        "source": "Last",
    }
    collected_at = datetime(2026, 3, 20, tzinfo=UTC)
    points = parse_history_response(
        payload,
        conid=767285167,
        period_requested="1week",
        collected_at=collected_at,
    )

    assert len(points) == 2
    assert points[0].volume is None


def test_parse_history_response_handles_nested_data_objects() -> None:
    payload = {
        "data": [
            {"time": 1731283200000, "avg": 0.42, "volume": 120},
            {"time": 1731369600000, "avg": 0.45, "volume": 180},
        ],
        "chart_step": "1d",
        "source": "Last",
    }
    collected_at = datetime(2026, 3, 20, tzinfo=UTC)
    points = parse_history_response(
        payload,
        conid=767285167,
        period_requested="1week",
        collected_at=collected_at,
    )

    assert len(points) == 2
    assert points[1].volume == 180


def test_parse_open_interest_response_creates_snapshot() -> None:
    payload = _load_sample("open_interest_response.json")
    collected_at = datetime(2026, 3, 20, tzinfo=UTC)
    snapshot = parse_open_interest_response(payload, collected_at)

    assert snapshot.conid == 767285167
    assert snapshot.open_interest == 107000


def test_parse_open_interest_response_uses_requested_conid_for_scalar_map() -> None:
    payload = {"107000": 42}
    collected_at = datetime(2026, 3, 20, tzinfo=UTC)
    snapshot = parse_open_interest_response(payload, collected_at, requested_conid=767285167)

    assert snapshot.conid == 107000
    assert snapshot.open_interest == 42


def test_parse_open_interest_response_uses_requested_conid_when_payload_has_no_id() -> None:
    payload = {"open_interest": 107000}
    collected_at = datetime(2026, 3, 20, tzinfo=UTC)
    snapshot = parse_open_interest_response(
        payload,
        collected_at,
        requested_conid=767285167,
    )

    assert snapshot.conid == 767285167
    assert snapshot.open_interest == 107000


def test_parse_open_interest_response_handles_nested_results() -> None:
    payload = {"results": [{"id": "767285167", "open_interest": "107000"}]}
    collected_at = datetime(2026, 3, 20, tzinfo=UTC)
    snapshot = parse_open_interest_response(
        payload,
        collected_at,
        requested_conid=767285167,
    )

    assert snapshot.conid == 767285167
    assert snapshot.open_interest == 107000


def test_parse_projected_probabilities_response_creates_rows() -> None:
    payload = _load_sample("projected_probabilities_response.json")
    collected_at = datetime(2026, 3, 20, tzinfo=UTC)
    rows = parse_projected_probabilities_response(payload, 766914406, collected_at)

    assert len(rows) == 2
    assert rows[0].underlying_conid == 766914406
    assert rows[0].strike == 4.5
