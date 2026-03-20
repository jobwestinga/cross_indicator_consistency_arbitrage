from __future__ import annotations

from .config import Settings
from .http_client import ForecastTraderClient
from .models import CollectionSummary
from .parsers import parse_open_interest_batch_response
from .repository import CollectorRepository


class OpenInterestCollectorService:
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
    ) -> CollectionSummary:
        lock_name = "collect-open-interest"
        if not self.repository.acquire_advisory_lock(lock_name):
            return CollectionSummary(
                run_id=0,
                message="Skipped collect-open-interest; another run holds the advisory lock.",
            )

        run_id = 0
        try:
            job_args = {"underlying_conid": underlying_conid, "all_discovered": all_discovered}
            run_id = self.repository.start_run("collect-open-interest", job_args)
            summary = CollectionSummary(run_id=run_id)

            target_underlyings = (
                [int(market["underlying_conid"]) for market in self.repository.list_active_markets()]
                if all_discovered
                else [int(underlying_conid)] if underlying_conid is not None else []
            )

            for market_underlying_conid in target_underlyings:
                try:
                    contracts = self.repository.list_contracts_for_underlying(market_underlying_conid)
                    summary.contracts_processed += len(contracts)
                    conids = [int(contract["conid"]) for contract in contracts]
                    for index in range(0, len(conids), self.settings.open_interest_batch_size):
                        batch = conids[index : index + self.settings.open_interest_batch_size]
                        response = self.client.get_open_interest_batch(batch)
                        parsed = parse_open_interest_batch_response(
                            response.response_json,
                            collected_at=response.fetched_at,
                            requested_conids=batch,
                        )
                        with self.repository.transaction():
                            self.repository.record_raw_response(run_id, response)
                            summary.open_interest_points_inserted += (
                                self.repository.insert_open_interest_snapshots(parsed.snapshots)
                            )
                        summary.blank_open_interest_values += parsed.blank_value_count
                        summary.missing_open_interest_ids += len(parsed.missing_conids)
                    summary.markets_processed += 1
                except Exception as exc:
                    summary.errors.append(f"{market_underlying_conid}: {exc}")
                    if not all_discovered:
                        raise

            summary.message = (
                f"Collected {summary.open_interest_points_inserted} open-interest snapshots "
                f"for {summary.contracts_processed} contracts"
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
