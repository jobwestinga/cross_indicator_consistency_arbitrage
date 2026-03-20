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
        run_id = self.repository.start_run("collect-seed-market")
        summary = CollectionSummary(run_id=run_id)

        try:
            market_response = self.client.get_market(underlying_conid)
            self.repository.record_raw_response(run_id, market_response)

            market, contracts = parse_market_response(
                market_response.response_json,
                collected_at=market_response.fetched_at,
                fallback_underlying_conid=underlying_conid,
            )
            self.repository.upsert_market(market)
            summary.markets_processed = 1

            stubs_by_conid = {}
            for contract in contracts:
                self.repository.upsert_contract(contract)
                stubs_by_conid[contract.conid] = contract

            for conid, stub in stubs_by_conid.items():
                details_response = self.client.get_contract_details(conid)
                self.repository.record_raw_response(run_id, details_response)
                enriched = parse_contract_details_response(
                    details_response.response_json,
                    collected_at=details_response.fetched_at,
                    fallback_underlying_conid=market.underlying_conid,
                    requested_conid=conid,
                )
                self.repository.upsert_contract(stub.merge(enriched))

            summary.contracts_processed = len(stubs_by_conid)
            summary.message = f"Collected {summary.contracts_processed} contracts"
            self.repository.finish_run(run_id, "success")
            return summary
        except Exception as exc:
            self.repository.finish_run(run_id, "failed", str(exc))
            raise
