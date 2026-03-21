from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

from .http_client import ForecastTraderClient
from .models import CollectionSummary
from .parsers import parse_contract_details_response, parse_market_response
from .repository import CollectorRepository


class MarketCollectorService:
    def __init__(self, client: ForecastTraderClient, repository: CollectorRepository) -> None:
        self.client = client
        self.repository = repository

    def collect_seed_market(
        self,
        underlying_conid: int,
        *,
        contract_details_limit: int | None = None,
    ) -> CollectionSummary:
        return self.collect_markets(
            [underlying_conid],
            job_name="collect-seed-market",
            continue_on_error=False,
            contract_details_limit=contract_details_limit,
        )

    def collect_all_discovered(self) -> CollectionSummary:
        markets = self.repository.list_active_markets()
        return self.collect_markets(
            [int(market["underlying_conid"]) for market in markets],
            job_name="collect-market-structures",
            continue_on_error=True,
        )

    def collect_markets(
        self,
        underlying_conids: list[int],
        *,
        job_name: str,
        continue_on_error: bool,
        contract_details_limit: int | None = None,
    ) -> CollectionSummary:
        lock_name = "collect-market-structures"
        if not self.repository.acquire_advisory_lock(lock_name):
            return CollectionSummary(
                run_id=0,
                message=f"Skipped {job_name}; another structure collection run holds the advisory lock.",
            )

        run_id = 0
        try:
            run_id = self.repository.start_run(
                job_name,
                {
                    "underlying_conids": underlying_conids,
                    "contract_details_limit": contract_details_limit,
                },
            )
            summary = CollectionSummary(run_id=run_id)

            for underlying_conid in underlying_conids:
                try:
                    (
                        markets_processed,
                        contracts_processed,
                        market_errors,
                    ) = self._collect_single_market(
                        run_id,
                        underlying_conid,
                        contract_details_limit=contract_details_limit,
                    )
                    summary.markets_processed += markets_processed
                    summary.contracts_processed += contracts_processed
                    summary.errors.extend(
                        f"{underlying_conid}: {error}" for error in market_errors
                    )
                except Exception as exc:
                    summary.errors.append(f"{underlying_conid}: {exc}")
                    if not continue_on_error:
                        raise

            if summary.errors:
                summary.message = (
                    f"Collected {summary.contracts_processed} contracts across "
                    f"{summary.markets_processed} markets with {len(summary.errors)} errors"
                )
                self.repository.finish_run(
                    run_id,
                    "partial",
                    error_text="; ".join(summary.errors),
                    summary=summary.model_dump(),
                )
            else:
                summary.message = (
                    f"Collected {summary.contracts_processed} contracts across "
                    f"{summary.markets_processed} markets"
                )
                self.repository.finish_run(run_id, "success", summary=summary.model_dump())
            return summary
        except Exception as exc:
            if run_id:
                self.repository.finish_run(run_id, "failed", str(exc))
            raise
        finally:
            self.repository.release_advisory_lock(lock_name)

    def _collect_single_market(
        self,
        run_id: int,
        underlying_conid: int,
        *,
        contract_details_limit: int | None = None,
    ) -> tuple[int, int, list[str]]:
        market_response = self.client.get_market(underlying_conid)
        market, contracts = parse_market_response(
            market_response.response_json,
            collected_at=market_response.fetched_at,
            fallback_underlying_conid=underlying_conid,
        )

        stubs_by_conid = {contract.conid: contract for contract in contracts}
        contracts_to_enrich = list(stubs_by_conid.items())
        if contract_details_limit is not None:
            contracts_to_enrich = contracts_to_enrich[:contract_details_limit]
        details_responses = []
        enriched_contracts = []
        detail_errors: list[str] = []
        workers = min(
            max(1, self.client.settings.contract_details_workers),
            max(1, len(contracts_to_enrich)),
        )

        if workers == 1:
            for conid, stub in contracts_to_enrich:
                try:
                    details_response = self.client.get_contract_details(conid)
                    details_responses.append(details_response)
                    enriched = parse_contract_details_response(
                        details_response.response_json,
                        collected_at=details_response.fetched_at,
                        fallback_underlying_conid=market.underlying_conid,
                        requested_conid=conid,
                    )
                    enriched_contracts.append(stub.merge(enriched))
                except Exception as exc:
                    detail_errors.append(f"contract_details[{conid}]: {exc}")
        else:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_conid = {
                    executor.submit(self.client.get_contract_details, conid): (conid, stub)
                    for conid, stub in contracts_to_enrich
                }
                for future in as_completed(future_to_conid):
                    conid, stub = future_to_conid[future]
                    try:
                        details_response = future.result()
                    except Exception as exc:
                        detail_errors.append(f"contract_details[{conid}]: {exc}")
                        continue
                    details_responses.append(details_response)
                    enriched = parse_contract_details_response(
                        details_response.response_json,
                        collected_at=details_response.fetched_at,
                        fallback_underlying_conid=market.underlying_conid,
                        requested_conid=conid,
                    )
                    enriched_contracts.append(stub.merge(enriched))

        with self.repository.transaction():
            self.repository.record_raw_response(run_id, market_response)
            self.repository.upsert_market(
                market.model_copy(
                    update={
                        "active": True,
                        "last_structure_collected_at": market_response.fetched_at,
                    }
                )
            )
            self.repository.deactivate_missing_contracts(
                market.underlying_conid,
                list(stubs_by_conid.keys()),
            )
            self.repository.upsert_contracts(list(stubs_by_conid.values()))
            for details_response in details_responses:
                self.repository.record_raw_response(run_id, details_response)
            self.repository.upsert_contracts(enriched_contracts)

        return 1, len(stubs_by_conid), detail_errors
