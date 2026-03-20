from __future__ import annotations

from .http_client import ForecastTraderClient
from .models import CollectionSummary
from .parsers import parse_projected_probabilities_response
from .repository import CollectorRepository


class ProjectedProbabilityCollectorService:
    def __init__(self, client: ForecastTraderClient, repository: CollectorRepository) -> None:
        self.client = client
        self.repository = repository

    def collect(
        self,
        underlying_conid: int | None = None,
        *,
        all_discovered: bool = False,
    ) -> CollectionSummary:
        lock_name = "collect-probabilities"
        if not self.repository.acquire_advisory_lock(lock_name):
            return CollectionSummary(
                run_id=0,
                message="Skipped collect-probabilities; another run holds the advisory lock.",
            )

        run_id = 0
        try:
            job_args = {"underlying_conid": underlying_conid, "all_discovered": all_discovered}
            run_id = self.repository.start_run("collect-probabilities", job_args)
            summary = CollectionSummary(run_id=run_id)

            target_underlyings = (
                [int(market["underlying_conid"]) for market in self.repository.list_active_markets()]
                if all_discovered
                else [int(underlying_conid)] if underlying_conid is not None else []
            )

            for market_underlying_conid in target_underlyings:
                try:
                    response = self.client.get_projected_probabilities(market_underlying_conid)
                    probabilities = parse_projected_probabilities_response(
                        response.response_json,
                        underlying_conid=market_underlying_conid,
                        collected_at=response.fetched_at,
                    )
                    with self.repository.transaction():
                        self.repository.record_raw_response(run_id, response)
                        summary.probability_points_inserted += (
                            self.repository.insert_projected_probabilities(probabilities)
                        )
                        self.repository.mark_market_probabilities_collected(
                            market_underlying_conid,
                            response.fetched_at,
                        )
                    summary.markets_processed += 1
                    if not probabilities:
                        summary.empty_probability_markets += 1
                except Exception as exc:
                    summary.errors.append(f"{market_underlying_conid}: {exc}")
                    if not all_discovered:
                        raise

            summary.message = (
                f"Collected {summary.probability_points_inserted} projected probabilities "
                f"for {summary.markets_processed} markets"
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
