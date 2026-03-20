from __future__ import annotations

import json

import typer

from .config import Settings, load_settings
from .http_client import ForecastTraderClient
from .logging import configure_logging
from .repository import CollectorRepository
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


def _print_summary(summary: object) -> None:
    typer.echo(json.dumps(summary.model_dump(), indent=2, sort_keys=True, default=str))


@app.command("migrate")
def migrate() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    with CollectorRepository(settings.database_url) as repository:
        repository.run_migrations(settings.sql_directory)
    typer.echo("Migrations applied.")


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


@app.command("collect-history")
def collect_history(
    underlying_conid: int | None = typer.Option(None, "--underlying-conid"),
) -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    resolved_underlying_conid = _resolve_underlying_conid(settings, underlying_conid)
    with CollectorRepository(settings.database_url) as repository, ForecastTraderClient(
        settings
    ) as client:
        service = HistoryCollectorService(settings, client, repository)
        summary = service.collect(resolved_underlying_conid)
    _print_summary(summary)


@app.command("collect-open-interest")
def collect_open_interest(
    underlying_conid: int | None = typer.Option(None, "--underlying-conid"),
) -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    resolved_underlying_conid = _resolve_underlying_conid(settings, underlying_conid)
    with CollectorRepository(settings.database_url) as repository, ForecastTraderClient(
        settings
    ) as client:
        service = OpenInterestCollectorService(client, repository)
        summary = service.collect(resolved_underlying_conid)
    _print_summary(summary)


@app.command("collect-probabilities")
def collect_probabilities(
    underlying_conid: int | None = typer.Option(None, "--underlying-conid"),
) -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    resolved_underlying_conid = _resolve_underlying_conid(settings, underlying_conid)
    with CollectorRepository(settings.database_url) as repository, ForecastTraderClient(
        settings
    ) as client:
        service = ProjectedProbabilityCollectorService(client, repository)
        summary = service.collect(resolved_underlying_conid)
    _print_summary(summary)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
