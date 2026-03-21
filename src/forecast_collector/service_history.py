from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

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
            }
            run_id = self.repository.start_run("collect-history", job_args)
            summary = CollectionSummary(run_id=run_id)

            target_underlyings = (
                [int(market["underlying_conid"]) for market in self.repository.list_active_markets()]
                if all_discovered
                else [int(underlying_conid)] if underlying_conid is not None else []
            )

            for market_underlying_conid in target_underlyings:
                try:
                    contracts = limit_items(
                        self.repository.list_contracts_for_underlying(market_underlying_conid),
                        contract_limit,
                    )
                    summary.contracts_processed += len(contracts)
                    requests = [
                        (int(contract["conid"]), period)
                        for contract in contracts
                        for period in periods
                    ]
                    workers = min(
                        max(1, self.settings.history_workers),
                        max(1, len(requests)),
                    )
                    request_errors: list[str] = []

                    if workers == 1:
                        responses = []
                        for conid, period in requests:
                            try:
                                responses.append(
                                    (conid, period, self.client.get_history(conid, period))
                                )
                            except Exception as exc:
                                request_errors.append(f"history[{conid}][{period}]: {exc}")
                    else:
                        responses = []
                        with ThreadPoolExecutor(max_workers=workers) as executor:
                            future_to_request = {
                                executor.submit(self.client.get_history, conid, period): (conid, period)
                                for conid, period in requests
                            }
                            for future in as_completed(future_to_request):
                                conid, period = future_to_request[future]
                                try:
                                    responses.append((conid, period, future.result()))
                                except Exception as exc:
                                    request_errors.append(f"history[{conid}][{period}]: {exc}")

                    if requests and not responses:
                        raise RuntimeError(
                            "All history requests failed "
                            f"for {len(requests)} contract-period fetches"
                        )

                    for conid, period, response in responses:
                        points = parse_history_response(
                            response.response_json,
                            conid=conid,
                            period_requested=period,
                            collected_at=response.fetched_at,
                        )
                        no_data = bool(response.response_json.get("no_data")) and not points
                        with self.repository.transaction():
                            self.repository.record_raw_response(run_id, response)
                            summary.history_points_inserted += self.repository.insert_history_points(
                                points
                            )
                            self.repository.mark_contract_history_collected(
                                conid,
                                response.fetched_at,
                                no_data=no_data,
                            )
                        if no_data:
                            summary.no_data_history_contracts += 1
                    summary.markets_processed += 1
                    summary.errors.extend(
                        f"{market_underlying_conid}: {error}" for error in request_errors
                    )
                except Exception as exc:
                    summary.errors.append(f"{market_underlying_conid}: {exc}")
                    if not all_discovered:
                        raise

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
