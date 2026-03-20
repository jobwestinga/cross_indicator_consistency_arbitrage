from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Iterable

from .models import ContractRecord, HistoryPoint, MarketRecord, OpenInterestSnapshot, ProjectedProbability


def _first(payload: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return default


def _as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _as_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
    return bool(value)


def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        if value > 1_000_000_000_000:
            value = value / 1000.0
        return datetime.fromtimestamp(value, tz=UTC)
    if isinstance(value, str):
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(normalized)
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
        except ValueError:
            if normalized.isdigit():
                return _coerce_datetime(int(normalized))
    raise ValueError(f"Unsupported datetime value: {value!r}")


def _ensure_sequence(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("contracts", "items", "data", "results", "projected_probabilities"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def parse_market_response(
    payload: dict[str, Any],
    collected_at: datetime,
    fallback_underlying_conid: int | None = None,
) -> tuple[MarketRecord, list[ContractRecord]]:
    contract_items = _ensure_sequence(payload)
    underlying_conid = _as_int(
        _first(payload, "underlying_conid", "underlyingConid", "und_conid")
    )
    if underlying_conid is None:
        underlying_conid = fallback_underlying_conid
    if underlying_conid is None:
        for item in contract_items:
            inferred = _as_int(_first(item, "underlying_conid", "underlyingConid", "und_conid"))
            if inferred is not None:
                underlying_conid = inferred
                break
    if underlying_conid is None:
        raise ValueError("Market response missing underlying conid")

    market = MarketRecord(
        underlying_conid=underlying_conid,
        market_name=str(_first(payload, "market_name", "marketName", default="")),
        symbol=str(_first(payload, "symbol", default="")),
        exchange=str(_first(payload, "exchange", default="")),
        logo_category=_first(payload, "logo_category", "logoCategory"),
        payout=_as_float(_first(payload, "payout")),
        exclude_historical_data=_as_bool(
            _first(payload, "exclude_historical_data", "excludeHistoricalData")
        ),
        first_seen_at=collected_at,
        last_seen_at=collected_at,
    )

    contracts: list[ContractRecord] = []
    for item in contract_items:
        conid = _as_int(_first(item, "conid", "id"))
        if conid is None:
            continue
        contracts.append(
            ContractRecord(
                conid=conid,
                underlying_conid=_as_int(
                    _first(item, "underlying_conid", "underlyingConid", default=underlying_conid)
                )
                or underlying_conid,
                side=_first(item, "side"),
                strike=_as_float(_first(item, "strike")),
                strike_label=_first(item, "strike_label", "strikeLabel"),
                expiration=_first(item, "expiration"),
                question=None,
                conid_yes=None,
                conid_no=None,
                product_conid=_as_int(_first(item, "product_conid", "productConid")),
                market_name=market.market_name,
                symbol=market.symbol,
                measured_period=_first(item, "measured_period", "measuredPeriod"),
                measured_period_units=_first(item, "measured_period_units", "measuredPeriodUnits"),
                first_seen_at=collected_at,
                last_seen_at=collected_at,
            )
        )
    return market, contracts


def parse_contract_details_response(
    payload: dict[str, Any],
    collected_at: datetime,
    fallback_underlying_conid: int | None = None,
    requested_conid: int | None = None,
) -> ContractRecord:
    conid = _as_int(_first(payload, "conid", "contract_id", "id"))
    if conid is None:
        conid = requested_conid
    if conid is None:
        conid = _as_int(_first(payload, "conid_yes", "conidYes", "conid_no", "conidNo"))
    underlying_conid = _as_int(
        _first(payload, "underlying_conid", "underlyingConid", "und_conid", default=fallback_underlying_conid)
    )
    if conid is None or underlying_conid is None:
        raise ValueError("Contract details response missing conid or underlying conid")

    return ContractRecord(
        conid=conid,
        underlying_conid=underlying_conid,
        side=_first(payload, "side"),
        strike=_as_float(_first(payload, "strike")),
        strike_label=_first(payload, "strike_label", "strikeLabel"),
        expiration=_first(payload, "expiration"),
        question=_first(payload, "question"),
        conid_yes=_as_int(_first(payload, "conid_yes", "conidYes")),
        conid_no=_as_int(_first(payload, "conid_no", "conidNo")),
        product_conid=_as_int(_first(payload, "product_conid", "productConid")),
        market_name=_first(payload, "market_name", "marketName"),
        symbol=_first(payload, "symbol"),
        measured_period=_first(payload, "measured_period", "measuredPeriod"),
        measured_period_units=_first(payload, "measured_period_units", "measuredPeriodUnits"),
        first_seen_at=collected_at,
        last_seen_at=collected_at,
    )


def parse_history_response(
    payload: dict[str, Any], conid: int, period_requested: str, collected_at: datetime
) -> list[HistoryPoint]:
    times: Iterable[Any] = payload.get("time", [])
    avgs: Iterable[Any] = payload.get("avg", [])
    volumes: Iterable[Any] = payload.get("volume", [])
    chart_step = _first(payload, "chart_step", "chartStep")
    source = _first(payload, "source", default="Last")

    points: list[HistoryPoint] = []
    for ts_value, avg_value, volume_value in zip(times, avgs, volumes):
        points.append(
            HistoryPoint(
                conid=conid,
                ts_utc=_coerce_datetime(ts_value),
                avg=_as_float(avg_value),
                volume=_as_int(volume_value),
                chart_step=str(chart_step) if chart_step is not None else None,
                source=str(source) if source is not None else None,
                period_requested=period_requested,
                collected_at=collected_at,
            )
        )
    return points


def parse_open_interest_response(
    payload: Any,
    collected_at: datetime,
    requested_conid: int | None = None,
) -> OpenInterestSnapshot:
    if isinstance(payload, list):
        if not payload:
            raise ValueError("Open interest response is empty")
        if len(payload) == 1:
            return parse_open_interest_response(
                payload[0],
                collected_at=collected_at,
                requested_conid=requested_conid,
            )
        if requested_conid is not None:
            for item in payload:
                if isinstance(item, dict):
                    item_conid = _as_int(_first(item, "conid", "contract_id", "id"))
                    if item_conid == requested_conid:
                        return parse_open_interest_response(
                            item,
                            collected_at=collected_at,
                            requested_conid=requested_conid,
                        )
        raise ValueError("Open interest response list has no matching contract id")

    if isinstance(payload, dict):
        conid = _as_int(_first(payload, "conid", "contract_id", "id"))
        if conid is None:
            conid = requested_conid

        open_interest = _as_int(_first(payload, "open_interest", "openInterest"))
        if open_interest is None and len(payload) == 1:
            only_key, only_value = next(iter(payload.items()))
            if str(only_key).isdigit():
                conid = conid or int(str(only_key))
                open_interest = _as_int(only_value)

        if conid is None:
            raise ValueError("Open interest response missing conid")

        return OpenInterestSnapshot(
            conid=conid,
            open_interest=open_interest,
            collected_at=collected_at,
        )

    if requested_conid is not None:
        return OpenInterestSnapshot(
            conid=requested_conid,
            open_interest=_as_int(payload),
            collected_at=collected_at,
        )

    raise ValueError("Unsupported open interest response shape")


def parse_projected_probabilities_response(
    payload: Any, underlying_conid: int, collected_at: datetime
) -> list[ProjectedProbability]:
    probabilities: list[ProjectedProbability] = []
    for item in _ensure_sequence(payload):
        probabilities.append(
            ProjectedProbability(
                underlying_conid=underlying_conid,
                strike=_as_float(_first(item, "strike")),
                expiry=_first(item, "expiry", "expiration"),
                probability=_as_float(_first(item, "probability")),
                collected_at=collected_at,
            )
        )
    return probabilities
