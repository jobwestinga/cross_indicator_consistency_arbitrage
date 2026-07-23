"""Tests for the analysis/ scripts.

Three layers:
  - unit : rule engine (scorer/flags), signal internals (ladder median,
           front-expiry filter, causal z-score, loader dedupe), FRED parser.
  - smoke: each script runs to exit 0 on a tiny synthetic bundle / sqlite.
  - pipeline: run_all.py end-to-end on the synthetic bundle.

The synthetic bundle mimics the real export closely enough to exercise the
methodology fixes: int-YYYYMMDD expirations with a mid-window roll, both YES
and NO sides, per-(conid,ts) duplication across period_requested values.
"""

from __future__ import annotations

import csv
import importlib.util
import io
import json
import math
import os
import sqlite3
import subprocess
import sys
import zipfile
from datetime import datetime, timedelta, UTC
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
np = pytest.importorskip("numpy")
pytest.importorskip("yaml")

REPO_ROOT = Path(__file__).resolve().parent.parent
ANALYSIS = REPO_ROOT / "analysis"
sys.path.insert(0, str(ANALYSIS))

import rules  # noqa: E402  (analysis/rules.py)
import signals as sig  # noqa: E402  (analysis/signals.py)

N_HOURS = 240          # long enough for run_all's default z-window/horizons
ROLL_HOURS = 120       # first expiry's front window ends here (expiry - roll_days)
START = datetime(2026, 4, 1, 12, tzinfo=UTC)


# --------------------------------------------------------------------------- #
# fixtures: synthetic bundle + fred sqlite
# --------------------------------------------------------------------------- #
def _csv_bytes(header: list[str], rows: list[list]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue().encode()


def _yyyymmdd(dt: datetime) -> int:
    return int(dt.strftime("%Y%m%d"))


def _make_bundle(path: Path, n_hours: int = N_HOURS) -> None:
    """Two markets (Core CPI, Unemployment), each a 3-strike survival ladder
    whose implied median oscillates over time. Two expirations per market with
    a mid-window front-expiry roll; YES and NO sides; every row duplicated in
    a second period_requested with a coarser chart_step (loader must dedupe)."""
    exp1 = START + timedelta(hours=ROLL_HOURS + 48)   # front until exp1 - roll_days(2d)
    exp2 = START + timedelta(days=40)
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
        for exp in (exp1, exp2):
            for si, strike in enumerate(strikes):
                conid += 1
                cid = conid
                for i in range(n_hours):
                    ts = (START + timedelta(hours=i)).isoformat()
                    level = center + amp * math.sin(i / period + phase)
                    # survival P(X>strike): decreasing in strike, crossing 0.5 at level
                    price = min(0.99, max(0.01, round(0.5 + 0.6 * (level - strike), 4)))
                    for side, px in (("Y", price), ("N", round(1 - price, 4))):
                        base = [ucid, name, "SYM", cid if side == "Y" else cid + 5000,
                                side, strike, f"Above {strike}", _yyyymmdd(exp),
                                exp.strftime("%B %Y"), "?", "1week", ts, px, 10,
                                1800, "test", ts]
                        hist_rows.append(base)
                        # duplicate row under the coarser period: slightly off
                        # price -> the dedupe preference is observable
                        dup = list(base)
                        dup[10] = "1month"
                        dup[12] = round(min(0.999, px + 0.005), 4)
                        dup[14] = 3600
                        hist_rows.append(dup)
                    if si == 1 and exp is exp1:
                        prob_rows.append([ucid, name, "SYM", "us", strike,
                                          _yyyymmdd(exp), price, ts])

    manifest = ('{"generated_at":"2026-04-01T12:00:00+00:00","underlying_conid":null,'
                '"since":null,"files":[]}')
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("market_categories.csv",
                   _csv_bytes(["category_key", "category_name"], [["us", "United States"]]))
        z.writestr("markets.csv", _csv_bytes(
            ["underlying_conid", "market_name", "category_key", "active"],
            [[m[0], m[1], "us", True] for m in markets]))
        contract_rows = [[ucid * 10 + si, ucid, "Y", strike]
                         for ucid, _n, strikes, *_ in markets
                         for si, strike in enumerate(strikes)]
        z.writestr("contracts.csv", _csv_bytes(
            ["conid", "underlying_conid", "side", "strike"], contract_rows))
        # header mirrors the real export (no conid column)
        z.writestr("projected_probabilities.csv", _csv_bytes(
            ["underlying_conid", "market_name", "market_symbol", "category_key",
             "strike", "expiry", "probability", "collected_at"], prob_rows))
        z.writestr("contract_history.csv", _csv_bytes(hist_header, hist_rows))
        z.writestr("manifest.json", manifest)


@pytest.fixture(scope="module")
def bundle(tmp_path_factory) -> Path:
    p = tmp_path_factory.mktemp("bundle") / "forecast_analysis_dataset_20260401T000000Z.zip"
    _make_bundle(p)
    return p


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


@pytest.fixture(scope="module")
def out_dir(tmp_path_factory) -> Path:
    """Isolated output root: scripts honor ANALYSIS_OUT_DIR so test runs never
    clobber real results in analysis/{consistency,validation,backtest,...}."""
    return tmp_path_factory.mktemp("out")


def _run(script: str, *args: str, out: Path | None = None) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    if out is not None:
        env["ANALYSIS_OUT_DIR"] = str(out)
    return subprocess.run([sys.executable, str(ANALYSIS / script), *args],
                          cwd=ANALYSIS, capture_output=True, text=True, check=False,
                          env=env)


# --------------------------------------------------------------------------- #
# unit: rule engine
# --------------------------------------------------------------------------- #
def _z(vals: dict[str, list[float]]) -> dict[str, pd.Series]:
    idx = pd.date_range("2026-01-01", periods=3, freq="1h", tz="UTC")
    return {k: pd.Series(v, index=idx) for k, v in vals.items()}


def test_score_product_sign():
    z = _z({"a": [1.0, -1.0, 2.0], "b": [1.0, 1.0, -1.0]})
    rule = {"logic": {"score": {"type": "product", "terms": ["a", "b"], "sign": 1}}}
    assert rules.score_from_logic(rule, z).tolist() == [1.0, -1.0, -2.0]
    rule["logic"]["score"]["sign"] = -1
    assert rules.score_from_logic(rule, z).tolist() == [-1.0, 1.0, 2.0]


def test_score_linear_weights():
    z = _z({"u": [2.0, 0.0, 1.0], "r": [1.0, 1.0, -1.0]})
    rule = {"logic": {"score": {"type": "linear", "weights": {"u": 1, "r": -1}}}}
    assert rules.score_from_logic(rule, z).tolist() == [1.0, -1.0, 2.0]


def test_flag_metric_value_vs_abs():
    idx = pd.date_range("2026-01-01", periods=3, freq="1h", tz="UTC")
    score = pd.Series([2.0, -2.0, 0.5], index=idx)
    rule_v = {"logic": {"flag": {"metric": "value", "threshold": 1.0}}}
    rule_a = {"logic": {"flag": {"metric": "abs", "threshold": 1.0}}}
    assert rules.flag_series(rule_v, score).tolist() == [True, False, False]
    assert rules.flag_series(rule_a, score).tolist() == [True, True, False]


def test_mappings_yaml_parses_and_all_implemented_rules_wellformed():
    cfg = sig.load_mappings()
    impl = rules.implemented_rules(cfg)
    assert {"phillips", "sahm", "taylor", "okun", "beveridge", "uip",
            "claims_labor", "payrolls_labor", "core_headline"} <= set(impl)
    z = _z({r: [0.5, -1.0, 2.0] for r in
            {role for k in impl for role in cfg["rules"][k]["indicators"]}})
    for key in impl:
        rule = rules.get_rule(cfg, key)
        score = rules.score_from_logic(rule, z)      # must not raise
        rules.flag_series(rule, score)               # must not raise


# --------------------------------------------------------------------------- #
# unit: signal internals
# --------------------------------------------------------------------------- #
def test_ladder_median_interpolation():
    med = sig._ladder_median(np.array([2.0, 3.0, 4.0]), np.array([0.9, 0.5, 0.1]))
    assert med == pytest.approx(3.0)
    med = sig._ladder_median(np.array([2.0, 3.0]), np.array([0.8, 0.6]))
    assert med == pytest.approx(3.0)      # all survival > 0.5 -> top strike
    med = sig._ladder_median(np.array([2.0, 3.0]), np.array([0.4, 0.2]))
    assert med == pytest.approx(2.0)      # all survival < 0.5 -> bottom strike
    assert sig._ladder_median(np.array([5.0]), np.array([0.7])) == 5.0


def test_front_expiry_filter_rolls():
    ts = pd.to_datetime(["2026-04-01", "2026-04-10", "2026-04-20"], utc=True)
    exp1 = pd.Timestamp("2026-04-12", tz="UTC")
    exp2 = pd.Timestamp("2026-05-12", tz="UTC")
    df = pd.DataFrame({
        "ts_utc": list(ts) * 2,
        "expiration": [exp1] * 3 + [exp2] * 3,
        "avg": [0.5] * 6,
    })
    out = sig.front_expiry_filter(df, roll_days=2)
    # Apr 1: front = exp1; Apr 10: exp1 cutoff (Apr 10) passed -> exp2; Apr 20: exp2
    kept = set(zip(out["ts_utc"].dt.day, out["expiration"].dt.month, strict=True))
    assert kept == {(1, 4), (10, 5), (20, 5)}


def test_zscore_rolling_is_causal():
    idx = pd.date_range("2026-01-01", periods=50, freq="1h", tz="UTC")
    rng = np.random.default_rng(1)
    a = pd.Series(rng.normal(size=50), index=idx)
    b = a.copy()
    b.iloc[40:] += 100.0     # perturb the future only
    za = sig.zscore_rolling(a, 10)
    zb = sig.zscore_rolling(b, 10)
    pd.testing.assert_series_equal(za.iloc[:40], zb.iloc[:40])


def test_load_history_dedupes_preferring_finer_step(bundle: Path):
    h = sig.load_history(bundle, use_cache=False)
    assert not h.duplicated(["conid", "ts_utc"]).any()
    # the 1month duplicates were shifted +0.005 with a coarser chart_step; the
    # kept row must be the finer-step original. At i=0 the Core CPI strike-3.0
    # generator price is exactly 0.5 (level==strike): dup would be 0.505.
    row = h[(h.market_name == "US Core CPI") & (h.side == "Y")
            & (h.strike == 3.0) & (h.ts_utc == pd.Timestamp(START))]
    assert set(row["avg"]) == {0.5}
    assert h["expiration"].notna().all()


def test_implied_median_tracks_generator(bundle: Path):
    h = sig.load_history(bundle, use_cache=False)
    m = sig.load_markets(bundle)
    med = sig.implied_median_series(h, m, "US Core CPI").dropna()
    # generator oscillates around 3.0 with amplitude 0.4
    assert 2.4 < med.min() < med.max() < 3.6
    assert len(med) >= N_HOURS - 24


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


def test_explore_dataset_runs_on_synthetic(bundle: Path, out_dir: Path):
    pytest.importorskip("matplotlib")
    r = _run("explore_dataset.py", "--zip", str(bundle), "--no-figures", out=out_dir)
    assert r.returncode == 0, r.stderr
    assert "Summary" in r.stdout


def test_check_readiness_runs_on_synthetic(bundle: Path, tmp_path: Path):
    fred = tmp_path / "fred.sqlite"
    _make_fred(fred)
    r = _run("check_readiness.py", "--zip", str(bundle), "--fred", str(fred))
    # exit 1 is allowed (synthetic data is stale -> FAIL); we assert it ran, not crashed
    assert "Data-readiness check" in r.stdout
    assert r.returncode in (0, 1), r.stderr


def test_run_consistency_phillips_on_synthetic(bundle: Path, out_dir: Path):
    r = _run("run_consistency.py", "--rule", "phillips", "--zip", str(bundle), out=out_dir)
    assert r.returncode == 0, r.stderr
    assert "phillips consistency" in r.stdout
    assert (out_dir / "consistency" / "phillips_consistency.csv").exists()


def test_validate_consistency_runs_on_synthetic(bundle: Path, out_dir: Path):
    r = _run("validate_consistency.py", "--rule", "phillips", "--zip", str(bundle),
             "--z-window", "6", "--horizons", "1", "4", "--threshold", "0.5",
             "--min-gap", "4", "--grid", out=out_dir)
    assert r.returncode == 0, r.stderr
    assert "OVERALL" in r.stdout
    out = json.loads((out_dir / "validation" / "phillips_validation.json").read_text())
    assert out["rule"] == "phillips"
    assert "overall" in out and "verdicts" in out and out["grid"]


def test_backtest_runs_on_synthetic(bundle: Path, out_dir: Path):
    r = _run("backtest.py", "--rule", "phillips", "--zip", str(bundle),
             "--z-window", "6", "--threshold", "0.5", "--max-hold", "8", "--cost", "0.01",
             out=out_dir)
    assert r.returncode == 0, r.stderr
    assert "backtest" in r.stdout
    out = json.loads((out_dir / "backtest" / "phillips_backtest.json").read_text())
    assert out["rule"] == "phillips"


def test_arbitrage_scan_runs_on_synthetic(bundle: Path, out_dir: Path):
    r = _run("arbitrage_scan.py", "--zip", str(bundle), "--min-rows", "10", out=out_dir)
    assert r.returncode == 0, r.stderr
    assert "static-arbitrage scan" in r.stdout
    out = json.loads((out_dir / "arbitrage" / "scan_summary.json").read_text())
    # synthetic ladders are monotone by construction -> (near) zero violations
    assert out["parity"]["n_pairs"] > 0
    assert out["parity"]["frac_abs_gt_2c"] == pytest.approx(0.0, abs=1e-6)


def test_run_all_pipeline_on_synthetic(bundle: Path, out_dir: Path):
    r = _run("run_all.py", "--zip", str(bundle), "--rules", "phillips", "--z-window", "12",
             out=out_dir)
    assert r.returncode == 0, r.stderr
    report = (out_dir / "report" / "REPORT.md").read_text()
    assert "phillips" in report
    assert "Static arbitrage" in report


def test_oos_test_runs_on_synthetic(bundle: Path, out_dir: Path):
    # split mid-window so some events fall OOS
    split = (START + timedelta(hours=N_HOURS // 2)).date().isoformat()
    r = _run("oos_test.py", "--zip", str(bundle), "--rules", "phillips",
             "--split", split, "--z-window", "12", "--horizons", "1", "4",
             out=out_dir)
    assert r.returncode == 0, r.stderr
    out = json.loads((out_dir / "oos" / "summary.json").read_text())
    assert out["results"][0]["rule"] == "phillips"
    assert "n_events_oos" in out["results"][0]


def test_discover_rules_runs_on_synthetic(bundle: Path, out_dir: Path):
    pytest.importorskip("scipy")
    split = (START + timedelta(hours=N_HOURS // 2)).date().isoformat()
    r = _run("discover_rules.py", "--zip", str(bundle), "--split", split,
             "--min-rows", "100", "--min-overlap", "10", out=out_dir)
    assert r.returncode == 0, r.stderr
    out = json.loads((out_dir / "discovery" / "summary.json").read_text())
    assert out["universe"] == 2


def test_fed_path_fails_gracefully_without_fed_markets(bundle: Path, out_dir: Path):
    r = _run("fed_path_check.py", "--zip", str(bundle), out=out_dir)
    assert r.returncode == 1
    assert "required market missing" in (r.stderr + r.stdout)


def test_simulate_forces_exit_before_ref_switch():
    import backtest as bt

    idx = pd.date_range("2026-01-01", periods=12, freq="1h", tz="UTC")
    score = pd.Series([0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0], index=idx, dtype=float)
    ref = pd.Series([1] * 6 + [2] * 6, index=idx, dtype=float)  # switch at bar 6
    px = pd.Series([0.5] * 6 + [0.9] * 6, index=idx)            # jump across switch
    panel = pd.DataFrame({"score": score, "z_a": score, "px_a": px, "pxref_a": ref})
    rule = {"logic": {"score": {"type": "product", "terms": ["a", "a"], "sign": 1},
                      "flag": {"metric": "abs", "threshold": 1.0}}}
    trades = bt.simulate(panel, ["a"], rule, threshold=1.0, exit_band=0.5,
                         max_hold=10, cost=0.0, size="fixed")
    assert len(trades) >= 1
    t = trades.iloc[0]
    # entry at bar 2 (crossing at bar 1); must exit at bar 5, BEFORE the switch
    assert t["reason"] == "ref_roll"
    assert t["exit"] == idx[5]
    # no fictitious PnL from the 0.5 -> 0.9 contract morph
    assert t["gross"] == pytest.approx(0.0)
