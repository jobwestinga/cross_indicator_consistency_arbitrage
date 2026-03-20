from __future__ import annotations

from .config import Settings
from .http_client import ForecastTraderClient
from .models import CollectionSummary
from .parsers import parse_history_response
from .repository import CollectorRepository


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

    def collect(self, underlying_conid: int) -> CollectionSummary:
        run_id = self.repository.start_run("collect-history")
        summary = CollectionSummary(run_id=run_id)
        try:
            contracts = self.repository.list_contracts_for_underlying(underlying_conid)
            for contract in contracts:
                for period in self.settings.history_periods:
                    response = self.client.get_history(int(contract["conid"]), period)
                    self.repository.record_raw_response(run_id, response)
                    points = parse_history_response(
                        response.response_json,
                        conid=int(contract["conid"]),
                        period_requested=period,
                        collected_at=response.fetched_at,
                    )
                    summary.history_points_inserted += self.repository.insert_history_points(points)
            summary.contracts_processed = len(contracts)
            self.repository.finish_run(run_id, "success")
            return summary
        except Exception as exc:
            self.repository.finish_run(run_id, "failed", str(exc))
            raise
