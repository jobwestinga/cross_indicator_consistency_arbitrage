from __future__ import annotations

from .http_client import ForecastTraderClient
from .models import CollectionSummary
from .parsers import parse_projected_probabilities_response
from .repository import CollectorRepository


class ProjectedProbabilityCollectorService:
    def __init__(self, client: ForecastTraderClient, repository: CollectorRepository) -> None:
        self.client = client
        self.repository = repository

    def collect(self, underlying_conid: int) -> CollectionSummary:
        run_id = self.repository.start_run("collect-probabilities")
        summary = CollectionSummary(run_id=run_id)
        try:
            response = self.client.get_projected_probabilities(underlying_conid)
            self.repository.record_raw_response(run_id, response)
            probabilities = parse_projected_probabilities_response(
                response.response_json,
                underlying_conid=underlying_conid,
                collected_at=response.fetched_at,
            )
            summary.probability_points_inserted = self.repository.insert_projected_probabilities(
                probabilities
            )
            self.repository.finish_run(run_id, "success")
            return summary
        except Exception as exc:
            self.repository.finish_run(run_id, "failed", str(exc))
            raise
