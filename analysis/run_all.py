"""Run the whole research pipeline and write one report.

EXPLORATORY / TESTING. One command that answers "where does the thesis stand
today" against the latest bundle:

  1. data readiness check (informational)
  2. static-arbitrage scan (model-free, within-market)
  3. every implemented rule: consistency run -> validation (+ optional grid)
     -> costed backtest
  4. out-of-sample gate (frozen params, post-split events only)
  5. fed_path same-event identity check + pair-mining discovery
  6. a single markdown report assembled from the machine-readable outputs

Each step runs as a subprocess so one thin/broken rule cannot kill the rest;
failures are recorded in the report instead.

Usage:
    python3 analysis/run_all.py                # everything, latest bundle
    python3 analysis/run_all.py --grid         # + robustness grids (slower)
    python3 analysis/run_all.py --rules phillips taylor uip
Output:
    analysis/report/REPORT.md (+ everything the individual scripts write)
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, UTC
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # runnable from anywhere

import rules
import signals as sig

ANALYSIS = Path(__file__).resolve().parent
OUT_BASE = sig.out_base()          # ANALYSIS_OUT_DIR env override (tests) or analysis/
REPORT_DIR = OUT_BASE / "report"
VALIDATION_DIR = OUT_BASE / "validation"
BACKTEST_DIR = OUT_BASE / "backtest"
ARB_DIR = OUT_BASE / "arbitrage"
OOS_DIR = OUT_BASE / "oos"
FED_DIR = OUT_BASE / "fed_path"
DISC_DIR = OUT_BASE / "discovery"
SETTLE_DIR = OUT_BASE / "settlement"
FACTOR_DIR = OUT_BASE / "factor"
REGIONAL_DIR = OUT_BASE / "regional_cpi"
RELEASE_DIR = OUT_BASE / "releases"
DEFAULT_OOS_SPLIT = "2026-05-01"


def _bh_qvalues(pvals: dict[str, float]) -> dict[str, float]:
    """Benjamini-Hochberg adjusted q-values across the tested rules [A8]."""
    items = [(k, p) for k, p in pvals.items() if p == p]
    if not items:
        return {}
    items.sort(key=lambda kv: kv[1])
    m = len(items)
    q: dict[str, float] = {}
    prev = 1.0
    for rank, (k, p) in reversed(list(enumerate(items, start=1))):
        prev = min(prev, p * m / rank)
        q[k] = prev
    return q


def _run(script: str, *args: str) -> tuple[int, str]:
    """Run one analysis script; return (exit code, last stderr/stdout line)."""
    p = subprocess.run([sys.executable, str(ANALYSIS / script), *args],
                       capture_output=True, text=True)
    lines = (p.stderr.strip() or p.stdout.strip()).splitlines()
    return p.returncode, lines[-1].strip() if lines else ""


def _load_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _fmt(x, spec=".3f", na="-") -> str:
    try:
        if x is None or (isinstance(x, float) and x != x):
            return na
        return format(x, spec)
    except (TypeError, ValueError):
        return na


def _rule_spread_proxies() -> dict[str, float]:
    """Per rule: worst leg's p75 |Y+N-1| parity gap (effective-cost proxy)."""
    path = ARB_DIR / "parity_by_market.csv"
    if not path.exists():
        return {}
    import csv as _csv
    with open(path) as f:
        per_market = {row["market_name"]: float(row["p75_abs_gap"])
                      for row in _csv.DictReader(f) if row.get("p75_abs_gap")}
    cfg = sig.load_mappings()
    out = {}
    for key in rules.implemented_rules(cfg):
        gaps = [per_market[spec["market_name"]]
                for spec in cfg["rules"][key]["indicators"].values()
                if spec["market_name"] in per_market]
        if gaps:
            out[key] = max(gaps)
    return out


# --------------------------------------------------------------------------- #
# report assembly
# --------------------------------------------------------------------------- #
def build_report(zip_path: Path, rule_status: dict[str, str], readiness: str,
                 grid: bool) -> str:
    lines = [
        "# Cross-indicator consistency arbitrage — research report",
        "",
        f"- generated: {datetime.now(UTC).isoformat(timespec='seconds')}",
        f"- bundle: `{zip_path.name}`",
        f"- data readiness: {readiness}",
        "",
        "> **Status: EXPLORATORY.** Necessary-condition tests + toy costed",
        "> backtests. Nothing here is a production trading result.",
        "",
        "## 1. Static arbitrage (within single markets, model-free)",
        "",
    ]
    scan = _load_json(ARB_DIR / "scan_summary.json")
    if scan:
        par = scan.get("parity", {})
        lines += [
            f"- markets scanned: {scan['markets_scanned']} · adjacent-strike pairs: "
            f"{scan['n_adjacent_pairs']:,}",
            f"- ladder-inversion rate (within expiry): "
            f"**{_fmt(scan['aggregate_violation_rate'], '.4f')}**",
            f"- persistent inversion runs (>=2 bars): {scan['n_persistent_runs']}"
            f" — with volume on both legs: **{scan['n_persistent_traded_runs']}**",
            f"- YES/NO parity: mean gap {_fmt(par.get('mean_gap'), '+.4f')}, "
            f"|gap|>5c in {_fmt(100 * par.get('frac_abs_gt_5c', float('nan')), '.2f')}% "
            f"of {par.get('n_pairs', 0):,} pairs",
        ]
        if scan.get("suspect_semantics_markets"):
            lines.append(f"- excluded (inverted question semantics): "
                         f"{', '.join(scan['suspect_semantics_markets'])}")
        lines.append("")
        lines.append("Single-bar inversions are trade-average artifacts unless persistent; "
                     "the persistent+traded count above is the credible arbitrage evidence.")
    else:
        lines.append("_scan did not produce output_")
    lines += ["", "## 2. Consistency rules (cross-market economic identities)", ""]

    perm_ps = {}
    for rule_key, status in rule_status.items():
        if status == "ok":
            v = _load_json(VALIDATION_DIR / f"{rule_key}_validation.json")
            perm_ps[rule_key] = ((v or {}).get("permutation") or {}).get(
                "p_value", float("nan")) or float("nan")
    qvals = _bh_qvalues(perm_ps)

    header = ("| rule | status | events | validation verdict | %rev@24h | mean_rev vs matched | "
              "perm p* | BH q | trades | mean net/trade | break-even cost/leg |")
    lines += [header, "|" + "---|" * 11]
    for rule_key, status in rule_status.items():
        v = _load_json(VALIDATION_DIR / f"{rule_key}_validation.json")
        b = _load_json(BACKTEST_DIR / f"{rule_key}_backtest.json")
        if status != "ok":
            lines.append(f"| {rule_key} | FAILED: {status[:60]} | - | - | - | - | - | - | - | - | - |")
            continue
        v24 = (v or {}).get("verdicts", {}).get("24", {})
        mean_rev = _fmt(v24.get("mean_rev"), ".2f")
        matched = _fmt(v24.get("matched_mean"), ".2f")
        perm_p = _fmt(((v or {}).get("permutation") or {}).get("p_value"), ".3f")
        lines.append(
            f"| {rule_key} | ok | {(v or {}).get('n_events', '-')} "
            f"| {(v or {}).get('overall', '-').split(' (')[0]} "
            f"| {_fmt(v24.get('pct_revert'), '.2f')} "
            f"| {mean_rev} vs {matched} "
            f"| {perm_p} "
            f"| {_fmt(qvals.get(rule_key), '.3f')} "
            f"| {(b or {}).get('n_trades', '-')} "
            f"| {_fmt((b or {}).get('mean_net_per_trade'), '+.4f')} "
            f"| {_fmt((b or {}).get('breakeven_cost_per_leg'), '+.4f')} |")
    lines += [
        "",
        "Reading guide: a rule matters only if validation beats the magnitude-matched",
        "control AND the backtest break-even cost/leg exceeds a realistic spread",
        "(ForecastTrader all-in round trip is realistically >= $0.01-0.02/leg).",
        "*perm p = circular-shift permutation test (each leg's own dynamics kept,",
        "cross-leg alignment destroyed); the strictest null available here. BH q =",
        "Benjamini-Hochberg adjusted across the rules tested (A8): the multiple-",
        "testing-honest version of perm p.",
        "",
        "### 2b. Hold-to-settlement construction (no rolls, no marks at exit)",
        "",
    ]
    settle = _load_json(SETTLE_DIR / "summary.json")
    if settle:
        lines += ["| rule | flags | settled | net/trade | win | baseline net | edge |",
                  "|" + "---|" * 7]
        for r in settle.get("results", []):
            if "failed" in r:
                continue
            mn = r.get("mean_net_per_trade")
            base = r.get("baseline_mean_net")
            edge = (mn - base) if mn is not None and base is not None else None
            lines.append(
                f"| {r['rule']} | {r.get('n_flags', '-')} | {r.get('n_settled', '-')} "
                f"| {_fmt(mn, '+.4f')} | {_fmt(r.get('win_rate'), '.2f')} "
                f"| {_fmt(base, '+.4f')} | {_fmt(edge, '+.4f')} |")
        lines += [
            "",
            "Entry at the flag, hold the actual front contract to settlement",
            "(FRED-mapped or price-pinned outcome; one-way entry cost). Sidesteps",
            "the reference-switch problem by construction; capital locked to expiry;",
            "small N until more expiries resolve.",
            "",
        ]
    else:
        lines.append("_no settlement results (run settlement_test.py)_")
    lines += ["",
        "## 3. Out-of-sample gate (frozen parameters, post-split events only)",
        "",
    ]
    oos = _load_json(OOS_DIR / "summary.json")
    if oos:
        spread = _rule_spread_proxies()
        lines += [
            f"Split: **{oos['split']}** — thresholds/z-windows frozen; only events "
            f"starting after the split count; controls drawn from the OOS period.",
            "",
            "| rule | OOS events | OOS verdict | OOS trades | net/trade "
            "| BE cost/leg | proxy spread* | clears? |",
            "|---|---|---|---|---|---|---|---|",
        ]
        for r in oos.get("results", []):
            if "failed" in r:
                lines.append(f"| {r['rule']} | - | FAILED: {r['failed'][:40]} "
                             f"| - | - | - | - | - |")
                continue
            b = r.get("backtest", {})
            be = b.get("breakeven_cost_per_leg")
            sp = spread.get(r["rule"])
            clears = ("YES" if be is not None and sp is not None
                      and be == be and be > sp else
                      "no" if be is not None and sp is not None and be == be
                      else "-")
            lines.append(
                f"| {r['rule']} | {r['n_events_oos']} | {r['overall'].split(' (')[0]} "
                f"| {b.get('n_trades', 0)} "
                f"| {_fmt(b.get('mean_net_per_trade'), '+.4f')} "
                f"| {_fmt(be, '+.4f')} "
                f"| {_fmt(sp, '.4f')} | {clears} |")
        lines += [
            "",
            "This is the honest table; expect it to be weaker than section 2.",
            "",
            "*proxy spread = worst leg's p75 |YES+NO-1| parity gap "
            "(`analysis/arbitrage/parity_by_market.csv`) — the best effective-cost "
            "proxy this dataset allows; real order-book spreads need websocket "
            "capture (F3) and are likely wider.",
        ]
    else:
        lines.append("_no OOS results (run oos_test.py)_")

    lines += ["", "## 4. Same-event identity: Fed Decision vs Fed Funds ladder", ""]
    fed = _load_json(FED_DIR / "summary.json")
    if fed:
        gc, gh = fed.get("gap_cut", {}), fed.get("gap_hike", {})
        bgc, bgh = fed.get("back_gap_cut", {}), fed.get("back_gap_hike", {})
        lines += [
            f"- meetings covered: {fed.get('meetings')} · front grid points: "
            f"{fed.get('n_points'):,} · back: {fed.get('n_points_back', 0):,}",
            f"- no-cut boundary gap (front): mean {_fmt(gc.get('mean'), '+.4f')}, "
            f"mean|gap| {_fmt(gc.get('mean_abs'), '.4f')}, "
            f">5c {_fmt(100 * gc.get('frac_abs_gt_5c', float('nan')), '.1f')}% , "
            f"max {_fmt(gc.get('max_abs'), '.3f')}",
            f"- hike boundary gap (front): mean {_fmt(gh.get('mean'), '+.4f')}, "
            f"mean|gap| {_fmt(gh.get('mean_abs'), '.4f')}, "
            f">5c {_fmt(100 * gh.get('frac_abs_gt_5c', float('nan')), '.1f')}%, "
            f"max {_fmt(gh.get('max_abs'), '.3f')}",
            f"- back meetings: no-cut mean|gap| {_fmt(bgc.get('mean_abs'), '.4f')}, "
            f"hike mean|gap| {_fmt(bgh.get('mean_abs'), '.4f')} "
            f"(thinner prints, wider and staler than front)",
        ]
        cuts = fed.get("cuts_bound") or {}
        if cuts.get("n_bounds"):
            lines.append(
                f"- cuts-count bound (terminal {cuts.get('terminal_meeting')}): "
                f"{cuts['n_bounds']} testable k-levels, worst max gap "
                f"{_fmt(cuts.get('worst_max_gap'), '.3f')}")
        elif cuts:
            lines.append(
                "- cuts-count bound: NOT TESTABLE — 'Number of Fed Rate Cuts' tail "
                "categories are never-traded marks (median category sum "
                f"{_fmt(cuts.get('median_category_sum'), '.2f')} > 1); re-check on "
                "future bundles")
        lines += [
            "",
            "Same event priced in two markets; gaps are staleness-inflated "
            "(sparse prints, 48h carry) — persistent large gaps are the leads.",
        ]
    else:
        lines.append("_no fed_path results (run fed_path_check.py)_")

    lines += ["", "### 4b. Regional CPI aggregation identity (B10)", ""]
    reg = _load_json(REGIONAL_DIR / "summary.json")
    if reg:
        lines.append(
            f"- mean gap {_fmt(reg.get('mean_gap_pp'), '+.2f')}pp (constant ≈ "
            f"weights/base error); demeaned |gap| p95 "
            f"{_fmt(reg.get('demeaned_p95_abs_pp'), '.2f')}pp; persistent "
            f"(24h+) runs: {reg.get('n_persistent_runs', '-')} — "
            f"{'**coherent** (no persistent violations)' if not reg.get('n_persistent_runs') else '**VIOLATIONS — inspect regional_gap.csv**'}")
    else:
        lines.append("_no regional results (run regional_cpi_check.py)_")

    lines += ["", "### 4c. Do inconsistencies close at releases? (D4)", ""]
    rel = _load_json(RELEASE_DIR / "summary.json")
    if rel:
        any_compress = any((r.get("verdict") or "").startswith("RELEASES")
                           for r in rel.get("results", []))
        lines.append(
            f"- pooled post/pre |score| ratio around leg-market release days: "
            f"**{_fmt(rel.get('pooled_post_pre_ratio'), '.3f')}** "
            + ("— some rules compress AT releases (see "
               "`analysis/releases/summary.json`)" if any_compress else
               "— NO rule shows release-specific compression: inconsistencies "
               "decay on their own clock, which argues AGAINST the "
               "data-resolves-mispricing mechanism"))
    else:
        lines.append("_no release-study results (run release_event_study.py)_")

    lines += ["", "## 5. Data-driven rule discovery (pair mining)", ""]
    disc = _load_json(DISC_DIR / "summary.json")
    if disc:
        lines += [
            f"- universe: {disc.get('universe')} markets · pairs tested: "
            f"{disc.get('pairs_tested', disc.get('pairs', 0))} · Bonferroni survivors "
            f"kept for OOS: {disc.get('n_train_pairs', 0)} · OOS survivors: "
            f"**{disc.get('n_survivors', 0)}**",
            "",
            "Zero survivors is the expected result while the venue is thin: strong raw "
            "correlations all sit on ~25-bar overlaps (chance). Candidates, when they "
            "appear, land in `analysis/discovery/candidate_rules.yaml` as hypotheses "
            "for the standard pipeline.",
        ]
    else:
        lines.append("_no discovery results (run discover_rules.py)_")

    lines += ["", "### 5b. Factor residuals (C2)", ""]
    fac = _load_json(FACTOR_DIR / "summary.json")
    if fac:
        lines += [
            f"- universe {fac.get('universe')} markets · first PC explains "
            f"{_fmt(100 * fac.get('explained_variance_share', float('nan')), '.1f')}% "
            f"of train change variance · test-period deviation events: "
            f"{fac.get('n_events')}",
            f"- mean reversion {_fmt(fac.get('mean_reversion'), '+.3f')} vs matched "
            f"control {_fmt(fac.get('matched_control_mean'), '+.3f')} "
            f"(t(diff) {_fmt(fac.get('t_diff_vs_control'), '+.2f')}) — "
            f"**{fac.get('verdict', '-').split(' (')[0]}**",
        ]
    else:
        lines.append("_no factor results (run factor_residuals.py)_")

    lines += [
        "",
        "## 6. Power roadmap (how much more data do we need?)",
        "",
        "| rule | events | events/30d | days to 40 events |",
        "|---|---|---|---|",
    ]
    for rule_key, status in rule_status.items():
        if status != "ok":
            continue
        v = _load_json(VALIDATION_DIR / f"{rule_key}_validation.json")
        if not v:
            continue
        p = v.get("power", {})
        lines.append(f"| {rule_key} | {v.get('n_events')} | {p.get('events_per_30d', '-')} "
                     f"| {p.get('days_to_40_events', '-')} |")
    if grid:
        lines += ["", "## 7. Robustness grids", "",
                  "Per-rule z-window x threshold sweeps: see "
                  "`analysis/validation/<rule>_grid.csv`. A real effect should be "
                  "stable in a neighborhood, not one cell."]
    lines += [
        "",
        "## Caveats (unchanged by any single good-looking number)",
        "",
        "- `avg` bar prices are not executable quotes; no bid/ask in the bundle.",
        "- Costs are a flat proxy; venue fees/spread are guessed, not measured.",
        "- Window is short; event counts are small; thresholds were chosen in-sample.",
        "- Multiple rules and horizons are tested: expect some false positives; "
        "verdicts must survive out-of-sample data before being believed.",
        "",
    ]
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# driver
# --------------------------------------------------------------------------- #
def main() -> None:
    ap = argparse.ArgumentParser(description="Run the full research pipeline.")
    ap.add_argument("--zip", type=Path, default=None)
    ap.add_argument("--rules", nargs="+", default=None,
                    help="subset of rules (default: all implemented)")
    ap.add_argument("--z-window", type=int, default=48)
    ap.add_argument("--grid", action="store_true", help="run robustness grids too")
    ap.add_argument("--oos-split", default=DEFAULT_OOS_SPLIT,
                    help="OOS gate split date (default: %(default)s)")
    ap.add_argument("--permute", type=int, default=0, metavar="N",
                    help="run the circular-shift permutation test with N draws")
    ap.add_argument("--skip-backtest", action="store_true")
    ap.add_argument("--skip-scan", action="store_true")
    ap.add_argument("--skip-oos", action="store_true")
    ap.add_argument("--skip-settlement", action="store_true")
    ap.add_argument("--skip-extras", action="store_true",
                    help="skip fed_path + discovery + factor steps")
    args = ap.parse_args()

    zip_path = args.zip or sig.find_latest_zip()
    zip_arg = ["--zip", str(zip_path)]
    cfg = sig.load_mappings()
    todo = args.rules or rules.implemented_rules(cfg)

    print(f"bundle: {zip_path.name}")
    print("readiness check ...")
    rc, tail = _run("check_readiness.py", *zip_arg)
    readiness = "PASS/WARN" if rc == 0 else f"FAIL — run check_readiness.py ({tail[:60]})"
    print(f"  -> {readiness}")

    if not args.skip_scan:
        print("static-arbitrage scan ...")
        rc, tail = _run("arbitrage_scan.py", *zip_arg)
        print(f"  -> {'ok' if rc == 0 else 'FAILED: ' + tail}")

    rule_status: dict[str, str] = {}
    for rule_key in todo:
        print(f"rule {rule_key}: consistency ...", flush=True)
        rc, tail = _run("run_consistency.py", "--rule", rule_key,
                        "--z-window", str(args.z_window), *zip_arg)
        if rc != 0:
            rule_status[rule_key] = tail or "run_consistency failed"
            print(f"  -> FAILED: {tail}")
            continue
        print(f"  validation{' + grid' if args.grid else ''} ...", flush=True)
        vargs = ["--rule", rule_key, "--z-window", str(args.z_window), *zip_arg]
        if args.grid:
            vargs.append("--grid")
        if args.permute:
            vargs += ["--permute", str(args.permute)]
        rc, tail = _run("validate_consistency.py", *vargs)
        if rc != 0:
            rule_status[rule_key] = tail or "validation failed"
            print(f"  -> FAILED: {tail}")
            continue
        if not args.skip_backtest:
            print("  backtest ...", flush=True)
            rc, tail = _run("backtest.py", "--rule", rule_key,
                            "--z-window", str(args.z_window), *zip_arg)
            if rc != 0:
                rule_status[rule_key] = tail or "backtest failed"
                print(f"  -> FAILED: {tail}")
                continue
        rule_status[rule_key] = "ok"
        print("  -> ok")

    if not args.skip_settlement:
        print("hold-to-settlement test ...", flush=True)
        settle_args = [*zip_arg, "--z-window", str(args.z_window)]
        if args.rules:
            settle_args += ["--rules", *args.rules]
        rc, tail = _run("settlement_test.py", *settle_args)
        print(f"  -> {'ok' if rc == 0 else 'FAILED: ' + tail}")
    if not args.skip_oos:
        print(f"OOS gate (split {args.oos_split}) ...", flush=True)
        oos_args = ["--split", args.oos_split, *zip_arg]
        if args.rules:
            oos_args += ["--rules", *args.rules]
        rc, tail = _run("oos_test.py", *oos_args)
        print(f"  -> {'ok' if rc == 0 else 'FAILED: ' + tail}")
    if not args.skip_extras:
        print("fed_path identity check ...", flush=True)
        rc, tail = _run("fed_path_check.py", *zip_arg)
        print(f"  -> {'ok' if rc == 0 else 'FAILED: ' + tail}")
        print("pair-mining discovery ...", flush=True)
        rc, tail = _run("discover_rules.py", "--split", args.oos_split, *zip_arg)
        print(f"  -> {'ok' if rc == 0 else 'FAILED: ' + tail}")
        print("factor residuals ...", flush=True)
        rc, tail = _run("factor_residuals.py", "--split", args.oos_split, *zip_arg)
        print(f"  -> {'ok' if rc == 0 else 'FAILED: ' + tail}")
        print("regional CPI identity ...", flush=True)
        rc, tail = _run("regional_cpi_check.py", *zip_arg)
        print(f"  -> {'ok' if rc == 0 else 'FAILED: ' + tail}")
        print("release event study ...", flush=True)
        rc, tail = _run("release_event_study.py", *zip_arg)
        print(f"  -> {'ok' if rc == 0 else 'FAILED: ' + tail}")

    REPORT_DIR.mkdir(exist_ok=True)
    report = build_report(zip_path, rule_status, readiness, args.grid)
    out = REPORT_DIR / "REPORT.md"
    out.write_text(report)
    ok = sum(1 for s in rule_status.values() if s == "ok")
    print(f"\nreport -> {out}   ({ok}/{len(rule_status)} rules ran clean)")


if __name__ == "__main__":
    main()
