from __future__ import annotations

import json
from typing import Any

import typer

from .config import Settings, load_settings
from .http_client import ForecastTraderClient
from .logging import configure_logging
from .models import HistoryCollectionMode
from .repository import CollectorRepository
from .service_discovery import MarketDiscoveryService
from .service_health import HealthReporterService
from .service_history import HistoryCollectorService
from .service_interest import OpenInterestCollectorService
from .service_market import MarketCollectorService
from .service_probabilities import ProjectedProbabilityCollectorService


app = typer.Typer(add_completion=False)


def _resolve_underlying_conid(settings: Settings, supplied: int | None) -> int:
    underlying_conid = supplied if supplied is not None else settings.seed_underlying_conid
    if underlying_conid is None:
        raise typer.BadParameter(
            "Provide --underlying-conid or set SEED_UNDERLYING_CONID in the environment."
        )
    return underlying_conid


def _print_summary(summary: Any) -> None:
    payload = summary.model_dump() if hasattr(summary, "model_dump") else summary
    typer.echo(json.dumps(payload, indent=2, sort_keys=True, default=str))


@app.command("migrate")
def migrate() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    with CollectorRepository(settings.database_url) as repository:
        repository.run_migrations(settings.sql_directory)
    typer.echo("Migrations applied.")


@app.command("discover-markets")
def discover_markets() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    with CollectorRepository(settings.database_url) as repository, ForecastTraderClient(
        settings
    ) as client:
        service = MarketDiscoveryService(client, repository)
        summary = service.discover()
    _print_summary(summary)


@app.command("collect-seed-market")
def collect_seed_market(
    underlying_conid: int | None = typer.Option(None, "--underlying-conid"),
) -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    resolved_underlying_conid = _resolve_underlying_conid(settings, underlying_conid)
    with CollectorRepository(settings.database_url) as repository, ForecastTraderClient(
        settings
    ) as client:
        service = MarketCollectorService(client, repository)
        summary = service.collect_seed_market(resolved_underlying_conid)
    _print_summary(summary)


@app.command("collect-market-structures")
def collect_market_structures(
    all_discovered: bool = typer.Option(
        False,
        "--all-discovered",
        help="Collect structure for every active discovered market.",
    ),
    underlying_conid: int | None = typer.Option(
        None,
        "--underlying-conid",
        help="Fallback single-market mode when --all-discovered is not set.",
    ),
) -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    target_underlyings = (
        None if all_discovered else [_resolve_underlying_conid(settings, underlying_conid)]
    )
    with CollectorRepository(settings.database_url) as repository, ForecastTraderClient(
        settings
    ) as client:
        service = MarketCollectorService(client, repository)
        if all_discovered:
            summary = service.collect_all_discovered()
        else:
            summary = service.collect_markets(
                target_underlyings or [],
                job_name="collect-market-structures",
                continue_on_error=False,
            )
    _print_summary(summary)


@app.command("collect-history")
def collect_history(
    underlying_conid: int | None = typer.Option(None, "--underlying-conid"),
    all_discovered: bool = typer.Option(False, "--all-discovered"),
    mode: HistoryCollectionMode = typer.Option(
        HistoryCollectionMode.BACKFILL,
        "--mode",
        case_sensitive=False,
    ),
) -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    resolved_underlying_conid = None if all_discovered else _resolve_underlying_conid(
        settings, underlying_conid
    )
    with CollectorRepository(settings.database_url) as repository, ForecastTraderClient(
        settings
    ) as client:
        service = HistoryCollectorService(settings, client, repository)
        summary = service.collect(
            resolved_underlying_conid,
            all_discovered=all_discovered,
            mode=mode,
        )
    _print_summary(summary)


@app.command("collect-open-interest")
def collect_open_interest(
    underlying_conid: int | None = typer.Option(None, "--underlying-conid"),
    all_discovered: bool = typer.Option(False, "--all-discovered"),
) -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    resolved_underlying_conid = None if all_discovered else _resolve_underlying_conid(
        settings, underlying_conid
    )
    with CollectorRepository(settings.database_url) as repository, ForecastTraderClient(
        settings
    ) as client:
        service = OpenInterestCollectorService(settings, client, repository)
        summary = service.collect(
            resolved_underlying_conid,
            all_discovered=all_discovered,
        )
    _print_summary(summary)


@app.command("collect-probabilities")
def collect_probabilities(
    underlying_conid: int | None = typer.Option(None, "--underlying-conid"),
    all_discovered: bool = typer.Option(False, "--all-discovered"),
) -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    resolved_underlying_conid = None if all_discovered else _resolve_underlying_conid(
        settings, underlying_conid
    )
    with CollectorRepository(settings.database_url) as repository, ForecastTraderClient(
        settings
    ) as client:
        service = ProjectedProbabilityCollectorService(client, repository)
        summary = service.collect(
            resolved_underlying_conid,
            all_discovered=all_discovered,
        )
    _print_summary(summary)


@app.command("report-health")
def report_health() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    with CollectorRepository(settings.database_url) as repository:
        service = HealthReporterService(repository)
        summary = service.report()
    _print_summary(summary)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
