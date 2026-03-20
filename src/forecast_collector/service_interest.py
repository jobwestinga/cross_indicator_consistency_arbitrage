from __future__ import annotations

from .http_client import ForecastTraderClient
from .models import CollectionSummary
from .parsers import parse_open_interest_response
from .repository import CollectorRepository


class OpenInterestCollectorService:
    def __init__(self, client: ForecastTraderClient, repository: CollectorRepository) -> None:
        self.client = client
        self.repository = repository

    def collect(self, underlying_conid: int) -> CollectionSummary:
        run_id = self.repository.start_run("collect-open-interest")
        summary = CollectionSummary(run_id=run_id)
        try:
            contracts = self.repository.list_contracts_for_underlying(underlying_conid)
            for contract in contracts:
                response = self.client.get_open_interest(int(contract["conid"]))
                self.repository.record_raw_response(run_id, response)
                snapshot = parse_open_interest_response(
                    response.response_json,
                    collected_at=response.fetched_at,
                    requested_conid=int(contract["conid"]),
                )
                self.repository.insert_open_interest_snapshot(snapshot)
                summary.open_interest_points_inserted += 1
            summary.contracts_processed = len(contracts)
            self.repository.finish_run(run_id, "success")
            return summary
        except Exception as exc:
            self.repository.finish_run(run_id, "failed", str(exc))
            raise
