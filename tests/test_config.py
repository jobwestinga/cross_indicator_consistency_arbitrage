from __future__ import annotations

from forecast_collector.config import Settings


def test_settings_accepts_comma_delimited_history_periods() -> None:
    settings = Settings(
        DATABASE_URL="postgresql://forecast:forecast@localhost:5432/forecast",
        HISTORY_PERIODS="1week,1month",
    )

    assert settings.history_periods == ["1week", "1month"]


def test_settings_accepts_json_array_history_periods() -> None:
    settings = Settings(
        DATABASE_URL="postgresql://forecast:forecast@localhost:5432/forecast",
        HISTORY_PERIODS='["1week","1month"]',
    )

    assert settings.history_periods == ["1week", "1month"]
