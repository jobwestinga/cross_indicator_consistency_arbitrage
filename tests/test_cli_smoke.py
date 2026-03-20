from __future__ import annotations

import pytest

typer = pytest.importorskip("typer")
pytest.importorskip("pydantic_settings")
pytest.importorskip("psycopg")

from forecast_collector.cli import app


def test_cli_registers_commands() -> None:
    commands = {
        command.name or command.callback.__name__.replace("_", "-")
        for command in app.registered_commands
    }
    assert "migrate" in commands
    assert "discover-markets" in commands
    assert "collect-seed-market" in commands
    assert "collect-market-structures" in commands
    assert "collect-history" in commands
    assert "collect-open-interest" in commands
    assert "collect-probabilities" in commands
    assert "report-health" in commands
