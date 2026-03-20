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


def test_parse_contract_details_response_extracts_question_and_links() -> None:
    payload = _load_sample("contract_details_response.json")
    collected_at = datetime(2026, 3, 20, tzinfo=UTC)
    contract = parse_contract_details_response(payload, collected_at, fallback_underlying_conid=766914406)

    assert contract.conid == 767285167
    assert contract.question is not None
    assert contract.conid_yes == 767285167
    assert contract.conid_no == 767285168
    assert contract.measured_period == "March 2025"


def test_parse_history_response_creates_points() -> None:
    payload = _load_sample("history_response.json")
    collected_at = datetime(2026, 3, 20, tzinfo=UTC)
    points = parse_history_response(payload, conid=767285167, period_requested="1week", collected_at=collected_at)

    assert len(points) == 3
    assert points[0].conid == 767285167
    assert points[0].period_requested == "1week"
    assert points[0].avg == 0.42


def test_parse_open_interest_response_creates_snapshot() -> None:
    payload = _load_sample("open_interest_response.json")
    collected_at = datetime(2026, 3, 20, tzinfo=UTC)
    snapshot = parse_open_interest_response(payload, collected_at)

    assert snapshot.conid == 767285167
    assert snapshot.open_interest == 107000


def test_parse_projected_probabilities_response_creates_rows() -> None:
    payload = _load_sample("projected_probabilities_response.json")
    collected_at = datetime(2026, 3, 20, tzinfo=UTC)
    rows = parse_projected_probabilities_response(payload, 766914406, collected_at)

    assert len(rows) == 2
    assert rows[0].underlying_conid == 766914406
    assert rows[0].strike == 4.5
