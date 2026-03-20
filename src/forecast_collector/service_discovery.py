from __future__ import annotations

from .http_client import ForecastTraderClient
from .models import CollectionSummary
from .parsers import parse_category_tree_response
from .repository import CollectorRepository


class MarketDiscoveryService:
    def __init__(self, client: ForecastTraderClient, repository: CollectorRepository) -> None:
        self.client = client
        self.repository = repository

    def discover(self) -> CollectionSummary:
        lock_name = "discover-markets"
        if not self.repository.acquire_advisory_lock(lock_name):
            return CollectionSummary(
                run_id=0,
                message="Skipped discover-markets; another run holds the advisory lock.",
            )

        run_id = 0
        try:
            run_id = self.repository.start_run("discover-markets", {})
            summary = CollectionSummary(run_id=run_id)
            response = self.client.get_category_tree()
            categories, markets = parse_category_tree_response(
                response.response_json,
                collected_at=response.fetched_at,
            )
            with self.repository.transaction():
                self.repository.record_raw_response(run_id, response)
                self.repository.upsert_categories(categories)
                self.repository.mark_missing_markets_inactive(
                    [market.underlying_conid for market in markets],
                    response.fetched_at,
                )
                self.repository.upsert_markets(markets)
            summary.categories_processed = len(categories)
            summary.markets_processed = len(markets)
            summary.message = (
                f"Discovered {summary.markets_processed} markets across "
                f"{summary.categories_processed} categories"
            )
            self.repository.finish_run(run_id, "success", summary=summary.model_dump())
            return summary
        except Exception as exc:
            if run_id:
                self.repository.finish_run(run_id, "failed", str(exc))
            raise
        finally:
            self.repository.release_advisory_lock(lock_name)
