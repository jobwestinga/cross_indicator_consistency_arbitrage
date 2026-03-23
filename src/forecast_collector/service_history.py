from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta

from .config import Settings
from .http_client import ForecastTraderClient
from .models import CollectionSummary, HistoryCollectionMode
from .parsers import parse_history_response
from .repository import CollectorRepository
from .service_helpers import limit_items


class HistoryCollectorService:
    def __init__(
        self,
        settings: Settings,
        client: ForecastTraderClient,
        repository: CollectorRepository,
    ) -> None:
        self.settings = settings
        self.client = client
        self.repository = repository

    def collect(
        self,
        underlying_conid: int | None = None,
        *,
        all_discovered: bool = False,
        mode: HistoryCollectionMode = HistoryCollectionMode.BACKFILL,
        contract_limit: int | None = None,
        history_periods: list[str] | None = None,
        request_limit: int | None = None,
    ) -> CollectionSummary:
        lock_name = "collect-history"
        if not self.repository.acquire_advisory_lock(lock_name):
            return CollectionSummary(
                run_id=0,
                message=f"Skipped collect-history ({mode.value}); another run holds the advisory lock.",
            )

        run_id = 0
        try:
            periods = history_periods or self.settings.history_periods
            job_args = {
                "underlying_conid": underlying_conid,
                "all_discovered": all_discovered,
                "mode": mode.value,
                "contract_limit": contract_limit,
                "history_periods": periods,
                "request_limit": request_limit,
            }
            run_id = self.repository.start_run("collect-history", job_args)
            summary = CollectionSummary(run_id=run_id)

            history_requests = self._select_history_requests(
                periods=periods,
                all_discovered=all_discovered,
                underlying_conid=underlying_conid,
                mode=mode,
                contract_limit=contract_limit,
                request_limit=request_limit,
            )

            if all_discovered:
                summary.contracts_processed = len({request["conid"] for request in history_requests})
                summary.markets_processed = len(
                    {request["underlying_conid"] for request in history_requests}
                )
                self._collect_requests(run_id, history_requests, summary)
            else:
                self._collect_requests(run_id, history_requests, summary)

            summary.message = (
                f"Collected {summary.history_points_inserted} history points "
                f"for {summary.contracts_processed} contracts in {mode.value} mode"
            )
            self.repository.finish_run(
                run_id,
                "partial" if summary.errors else "success",
                error_text="; ".join(summary.errors) if summary.errors else None,
                summary=summary.model_dump(),
            )
            return summary
        except Exception as exc:
            if run_id:
                self.repository.finish_run(run_id, "failed", str(exc))
            raise
        finally:
            self.repository.release_advisory_lock(lock_name)

    def _select_history_requests(
        self,
        *,
        periods: list[str],
        all_discovered: bool,
        underlying_conid: int | None,
        mode: HistoryCollectionMode,
        contract_limit: int | None,
        request_limit: int | None,
    ) -> list[dict]:
        if all_discovered:
            if mode == HistoryCollectionMode.INCREMENTAL:
                return self.repository.list_history_requests_for_incremental(
                    periods,
                    limit=request_limit or self.settings.history_incremental_request_limit,
                )
            return self.repository.list_history_requests_for_backfill(
                periods,
                limit=request_limit or self.settings.history_backfill_request_limit,
                no_data_retry_before=datetime.now(tz=UTC)
                - timedelta(hours=self.settings.history_no_data_retry_hours),
            )

        if underlying_conid is None:
            return []

        contracts = limit_items(
            self.repository.list_contracts_for_underlying(underlying_conid),
            contract_limit,
        )
        return [
            {
                "conid": int(contract["conid"]),
                "underlying_conid": int(contract["underlying_conid"]),
                "period_requested": period,
            }
            for contract in contracts
            for period in periods
        ]

    def _collect_requests(
        self,
        run_id: int,
        history_requests: list[dict],
        summary: CollectionSummary,
    ) -> None:
        if not history_requests:
            return

        if not summary.contracts_processed:
            summary.contracts_processed = len({request["conid"] for request in history_requests})
        if not summary.markets_processed:
            summary.markets_processed = len(
                {request["underlying_conid"] for request in history_requests}
            )

        requests = [
            (
                int(request["conid"]),
                int(request["underlying_conid"]),
                str(request["period_requested"]),
            )
            for request in history_requests
        ]
        workers = min(
            max(1, self.settings.history_workers),
            max(1, len(requests)),
        )
        request_errors: list[str] = []

        if workers == 1:
            responses = []
            for conid, market_underlying_conid, period in requests:
                try:
                    responses.append(
                        (
                            conid,
                            market_underlying_conid,
                            period,
                            self.client.get_history(conid, period),
                        )
                    )
                except Exception as exc:
                    request_errors.append(
                        f"{market_underlying_conid}: history[{conid}][{period}]: {exc}"
                    )
        else:
            responses = []
            with ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_request = {
                    executor.submit(self.client.get_history, conid, period): (
                        conid,
                        market_underlying_conid,
                        period,
                    )
                    for conid, market_underlying_conid, period in requests
                }
                for future in as_completed(future_to_request):
                    conid, market_underlying_conid, period = future_to_request[future]
                    try:
                        responses.append(
                            (conid, market_underlying_conid, period, future.result())
                        )
                    except Exception as exc:
                        request_errors.append(
                            f"{market_underlying_conid}: history[{conid}][{period}]: {exc}"
                        )

        if requests and not responses:
            raise RuntimeError(
                "All history requests failed "
                f"for {len(requests)} contract-period fetches"
            )

        no_data_conids: set[int] = set()
        for conid, _market_underlying_conid, period, response in responses:
            points = parse_history_response(
                response.response_json,
                conid=conid,
                period_requested=period,
                collected_at=response.fetched_at,
            )
            no_data = bool(response.response_json.get("no_data")) and not points
            with self.repository.transaction():
                self.repository.record_raw_response(run_id, response)
                summary.history_points_inserted += self.repository.insert_history_points(points)
                self.repository.mark_contract_history_collected(
                    conid,
                    response.fetched_at,
                    no_data=no_data,
                    period_requested=period,
                )
            if no_data:
                no_data_conids.add(conid)

        summary.no_data_history_contracts += len(no_data_conids)
        summary.errors.extend(request_errors)
