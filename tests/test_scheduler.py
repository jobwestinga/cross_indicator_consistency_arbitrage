from __future__ import annotations

from forecast_collector.scheduler import build_schedule, render_service, render_timer


def test_scheduler_builds_all_expected_jobs() -> None:
    schedule = build_schedule()
    names = [definition.name for definition in schedule]

    assert names == [
        "forecast-discover",
        "forecast-structure",
        "forecast-open-interest",
        "forecast-probabilities",
        "forecast-history-incremental",
        "forecast-history-backfill",
    ]

    history_incremental = next(
        definition for definition in schedule if definition.name == "forecast-history-incremental"
    )
    history_backfill = next(
        definition for definition in schedule if definition.name == "forecast-history-backfill"
    )

    assert history_incremental.interval_seconds == 15 * 60
    assert history_incremental.command[-2:] == ["--request-limit", "500"]
    assert history_backfill.interval_seconds == 60 * 60
    assert history_backfill.command[-2:] == ["--request-limit", "1000"]


def test_scheduler_renders_systemd_units() -> None:
    definition = build_schedule()[0]

    service = render_service(definition, "/srv/cross_indicator_consistency_arbitrage")
    timer = render_timer(definition)

    assert "WorkingDirectory=/srv/cross_indicator_consistency_arbitrage" in service
    assert "ExecStart=docker compose run --rm collector discover-markets" in service
    assert "OnActiveSec=60" in timer
    assert "OnUnitActiveSec=3600" in timer
