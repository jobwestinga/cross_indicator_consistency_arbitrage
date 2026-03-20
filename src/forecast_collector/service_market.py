from __future__ import annotations

from .http_client import ForecastTraderClient
from .models import CollectionSummary
from .parsers import parse_contract_details_response, parse_market_response
from .repository import CollectorRepository


class MarketCollectorService:
    def __init__(self, client: ForecastTraderClient, repository: CollectorRepository) -> None:
        self.client = client
        self.repository = repository

    def collect_seed_market(self, underlying_conid: int) -> CollectionSummary:
        return self.collect_markets(
            [underlying_conid],
            job_name="collect-seed-market",
            continue_on_error=False,
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
    ) -> CollectionSummary:
        lock_name = "collect-market-structures"
        if not self.repository.acquire_advisory_lock(lock_name):
            return CollectionSummary(
                run_id=0,
                message=f"Skipped {job_name}; another structure collection run holds the advisory lock.",
            )

        run_id = 0
        try:
            run_id = self.repository.start_run(job_name, {"underlying_conids": underlying_conids})
            summary = CollectionSummary(run_id=run_id)

            for underlying_conid in underlying_conids:
                try:
                    markets_processed, contracts_processed = self._collect_single_market(
                        run_id,
                        underlying_conid,
                    )
                    summary.markets_processed += markets_processed
                    summary.contracts_processed += contracts_processed
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

    def _collect_single_market(self, run_id: int, underlying_conid: int) -> tuple[int, int]:
        market_response = self.client.get_market(underlying_conid)
        market, contracts = parse_market_response(
            market_response.response_json,
            collected_at=market_response.fetched_at,
            fallback_underlying_conid=underlying_conid,
        )

        stubs_by_conid = {contract.conid: contract for contract in contracts}
        details_responses = []
        enriched_contracts = []
        for conid, stub in stubs_by_conid.items():
            details_response = self.client.get_contract_details(conid)
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

        return 1, len(stubs_by_conid)
