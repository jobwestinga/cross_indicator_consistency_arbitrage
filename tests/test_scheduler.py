from __future__ import annotations

from forecast_collector.scheduler import build_schedule, render_service, render_timer


def test_scheduler_builds_all_expected_jobs() -> None:
    names = [definition.name for definition in build_schedule()]

    assert names == [
        "forecast-discover",
        "forecast-structure",
        "forecast-open-interest",
        "forecast-probabilities",
        "forecast-history-incremental",
        "forecast-history-backfill",
    ]


def test_scheduler_renders_systemd_units() -> None:
    definition = build_schedule()[0]

    service = render_service(definition, "/srv/cross_indicator_consistency_arbitrage")
    timer = render_timer(definition)

    assert "WorkingDirectory=/srv/cross_indicator_consistency_arbitrage" in service
    assert "ExecStart=docker compose run --rm collector discover-markets" in service
    assert "OnUnitActiveSec=3600" in timer
