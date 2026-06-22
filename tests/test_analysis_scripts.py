"""Tests for the analysis/ scripts.

Two layers (critique C7, options b + a):
  - smoke: each script runs to exit 0 on a tiny synthetic bundle / sqlite.
  - unit : the FRED value parser (ground-truth correctness) is checked directly.

A small synthetic bundle is built in a tmp dir so these tests do not depend on
the large real export. matplotlib/pandas/yaml are required; tests skip if absent.
"""

from __future__ import annotations

import csv
import importlib.util
import io
import sqlite3
import subprocess
import sys
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytest.importorskip("pandas")
pytest.importorskip("yaml")

REPO_ROOT = Path(__file__).resolve().parent.parent
ANALYSIS = REPO_ROOT / "analysis"


# --------------------------------------------------------------------------- #
# fixtures: tiny synthetic bundle + fred sqlite
# --------------------------------------------------------------------------- #
def _csv_bytes(header: list[str], rows: list[list]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue().encode()


def _make_bundle(path: Path, n_hours: int = 60) -> None:
    """Two markets (Core CPI, Unemployment), each a 3-strike survival ladder
    whose implied median oscillates over time, so both the median and prob
    signals are well defined and the z-scores vary."""
    import math

    start = datetime(2026, 4, 10, 12, tzinfo=timezone.utc)
    hist_header = ["underlying_conid", "market_name", "market_symbol", "conid",
                   "side", "strike", "strike_label", "expiration", "expiry_label",
                   "question", "period_requested", "ts_utc", "avg", "volume",
                   "chart_step", "source", "collected_at"]
    hist_rows = []
    prob_rows = []
    # (conid, name, strikes, center, amplitude, period, phase)
    markets = [(727520252, "US Core CPI", [2.5, 3.0, 3.5], 3.0, 0.4, 11, 0.0),
               (573031117, "US Unemployment Rate", [4.0, 4.5, 5.0], 4.5, 0.4, 9, 1.6)]
    conid = 9000
    for ucid, name, strikes, center, amp, period, phase in markets:
        for si, strike in enumerate(strikes):
            cid = conid = conid + 1
            for i in range(n_hours):
                ts = (start + timedelta(hours=i)).isoformat()
                level = center + amp * math.sin(i / period + phase)
                # survival P(X>strike): decreasing in strike, crossing 0.5 at level
                price = min(0.99, max(0.01, round(0.5 + 0.6 * (level - strike), 4)))
                hist_rows.append([ucid, name, "SYM", cid, "Y", strike, f"Above {strike}",
                                  "2026-12-31", "Dec", "?", "1week", ts, price, 10,
                                  "1h", "test", ts])
                if si == 1:
                    prob_rows.append([ucid, name, cid, price, ts])

    manifest = ('{"generated_at":"2026-04-10T12:00:00+00:00","underlying_conid":null,'
                '"since":null,"files":[]}')
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("market_categories.csv",
                   _csv_bytes(["category_key", "category_name"], [["us", "United States"]]))
        z.writestr("markets.csv", _csv_bytes(
            ["underlying_conid", "market_name", "category_key", "active"],
            [[m[0], m[1], "us", True] for m in markets]))
        # one contracts row per (market, strike)
        contract_rows = [[ucid * 10 + si, ucid, "Y", strike]
                         for ucid, _n, strikes, *_ in markets
                         for si, strike in enumerate(strikes)]
        z.writestr("contracts.csv", _csv_bytes(
            ["conid", "underlying_conid", "side", "strike"], contract_rows))
        z.writestr("projected_probabilities.csv", _csv_bytes(
            ["underlying_conid", "conid", "probability", "collected_at"],
            [[r[0], r[2], r[3], r[4]] for r in prob_rows]))
        z.writestr("contract_history.csv", _csv_bytes(hist_header, hist_rows))
        z.writestr("manifest.json", manifest)


def _make_fred(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE macro_observations (series_id TEXT, label TEXT, mapping TEXT,"
            " obs_date TEXT, value REAL, realtime_start TEXT, realtime_end TEXT, fetched_at TEXT)")
        conn.executemany(
            "INSERT INTO macro_observations VALUES (?,?,?,?,?,?,?,?)",
            [("CPILFESL", "Core CPI", "Phillips", "2026-05-01", 336.1, None, None, "x"),
             ("UNRATE", "Unemployment", "Phillips", "2026-05-01", 4.3, None, None, "x")])


def _run(script: str, *args: str, cwd: Path = ANALYSIS) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, script, *args], cwd=cwd,
                          capture_output=True, text=True, check=False)


# --------------------------------------------------------------------------- #
# unit: FRED value parser
# --------------------------------------------------------------------------- #
def _load_collect_fred():
    spec = importlib.util.spec_from_file_location("collect_fred", ANALYSIS / "collect_fred.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_fred_parse_value_handles_missing_and_numbers():
    cf = _load_collect_fred()
    assert cf.parse_value(".") is None
    assert cf.parse_value("") is None
    assert cf.parse_value(None) is None
    assert cf.parse_value("4.3") == pytest.approx(4.3)
    assert cf.parse_value("0") == 0.0


# --------------------------------------------------------------------------- #
# smoke: scripts run on synthetic data
# --------------------------------------------------------------------------- #
def test_collect_fred_dry_run():
    r = _run("collect_fred.py", "--dry-run")
    assert r.returncode == 0, r.stderr
    assert "UNRATE" in r.stdout


def test_explore_dataset_runs_on_synthetic(tmp_path: Path):
    pytest.importorskip("matplotlib")
    bundle = tmp_path / "forecast_analysis_dataset_20260410T000000Z.zip"
    _make_bundle(bundle)
    r = _run("explore_dataset.py", "--zip", str(bundle), "--no-figures")
    assert r.returncode == 0, r.stderr
    assert "Summary" in r.stdout


def test_check_readiness_runs_on_synthetic(tmp_path: Path):
    bundle = tmp_path / "forecast_analysis_dataset_20260410T000000Z.zip"
    _make_bundle(bundle)
    fred = tmp_path / "fred.sqlite"
    _make_fred(fred)
    r = _run("check_readiness.py", "--zip", str(bundle), "--fred", str(fred))
    # exit 1 is allowed (synthetic data is stale -> FAIL); we assert it ran, not crashed
    assert "Data-readiness check" in r.stdout
    assert r.returncode in (0, 1), r.stderr


def test_run_consistency_phillips_on_synthetic(tmp_path: Path):
    bundle = tmp_path / "forecast_analysis_dataset_20260410T000000Z.zip"
    _make_bundle(bundle)
    r = _run("run_consistency.py", "--rule", "phillips", "--zip", str(bundle))
    assert r.returncode == 0, r.stderr
    assert "phillips consistency" in r.stdout
    assert (ANALYSIS / "consistency" / "phillips_consistency.csv").exists()


def test_validate_consistency_runs_on_synthetic(tmp_path: Path):
    # 60 hourly points; small z-window + horizons so the test data suffices.
    bundle = tmp_path / "forecast_analysis_dataset_20260410T000000Z.zip"
    _make_bundle(bundle)
    r = _run("validate_consistency.py", "--rule", "phillips", "--zip", str(bundle),
             "--z-window", "6", "--horizons", "1", "4", "--threshold", "0.5", "--min-gap", "4")
    assert r.returncode == 0, r.stderr
    assert "validation" in r.stdout
    assert "OVERALL" in r.stdout


def test_backtest_runs_on_synthetic(tmp_path: Path):
    bundle = tmp_path / "forecast_analysis_dataset_20260410T000000Z.zip"
    _make_bundle(bundle)
    r = _run("backtest.py", "--rule", "phillips", "--zip", str(bundle),
             "--z-window", "6", "--threshold", "0.5", "--max-hold", "8", "--cost", "0.01")
    assert r.returncode == 0, r.stderr
    assert "backtest" in r.stdout
