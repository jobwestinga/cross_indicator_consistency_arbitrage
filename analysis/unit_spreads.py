"""Unit-space spread checks (C4): inflation rules in %-points, not z-scores.

EXPLORATORY / TESTING. The z-space rules flag RELATIVE moves; this states the
same coherence claims in interpretable UNITS using full ladder quantiles:

  - core_headline: implied headline yoy MEDIAN minus implied core yoy median,
    checked against the realized distribution of that wedge (FRED NSA yoy,
    trailing years). Food/energy can push headline around core, but the pair
    should not price a wedge outside its realized historical range.
  - pce_cpi: same for Core PCE minus Core CPI (known methodological wedge,
    historically tight).

Also reports each leg's implied IQR (p75-p25 strike spread) — the market's
own uncertainty in units, context for whether a wedge is meaningful.

Flag = implied wedge outside the realized [p5, p95] band; PERSISTENT flags
(>= persist_bars consecutive hours) are the signal, single bars are noise.

Usage:
    python3 analysis/unit_spreads.py
Output (analysis/unit_spreads/): per-pair CSV + summary.json.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))  # runnable from anywhere

import signals as sig

OUT_DIR = sig.out_base() / "unit_spreads"
PERSIST_BARS = 24
REALIZED_YEARS = 10

# pair -> (market_a, market_b, fred_a, fred_b); wedge = a - b, yoy % units
PAIRS = {
    "core_headline": ("US Consumer Price Index Yearly", "US Core CPI",
                      "CPIAUCNS", "CPILFENS"),
    "pce_cpi": ("US Core PCE Price Index", "US Core CPI",
                "PCEPILFE", "CPILFESL"),
}


def realized_band(fred_a: str, fred_b: str, years: int) -> tuple[float, float, int]:
    a = sig.load_fred_series(fred_a)
    b = sig.load_fred_series(fred_b)
    ya = (a.pct_change(12) * 100).dropna()
    yb = (b.pct_change(12) * 100).dropna()
    wedge = (ya - yb).dropna()
    wedge = wedge[wedge.index >= wedge.index.max() - pd.DateOffset(years=years)]
    return float(wedge.quantile(0.05)), float(wedge.quantile(0.95)), int(len(wedge))


def persistent_runs(mask: pd.Series, min_bars: int) -> int:
    if mask.empty:
        return 0
    grp = (mask != mask.shift()).cumsum()
    runs = mask.groupby(grp).agg(["all", "size"])
    return int(((runs["all"]) & (runs["size"] >= min_bars)).sum())


def check_pair(key: str, hist, markets, zip_path: Path, years: int) -> dict:
    mkt_a, mkt_b, fred_a, fred_b = PAIRS[key]
    med_a = sig.cached_implied_series(zip_path, hist, markets, mkt_a)
    med_b = sig.cached_implied_series(zip_path, hist, markets, mkt_b)
    q25_a = sig.implied_quantile_series(hist, markets, mkt_a, p=0.25)
    q75_a = sig.implied_quantile_series(hist, markets, mkt_a, p=0.75)
    q25_b = sig.implied_quantile_series(hist, markets, mkt_b, p=0.25)
    q75_b = sig.implied_quantile_series(hist, markets, mkt_b, p=0.75)

    panel = sig.align(med_a, med_b, q25_a, q75_a, q25_b, q75_b)
    panel.columns = ["a", "b", "a25", "a75", "b25", "b75"]
    if panel.empty:
        raise ValueError(f"no overlap for {key}")
    panel["wedge"] = panel["a"] - panel["b"]
    lo, hi, n_real = realized_band(fred_a, fred_b, years)
    outside = (panel["wedge"] < lo) | (panel["wedge"] > hi)
    n_persist = persistent_runs(outside, PERSIST_BARS)

    out = {
        "pair": key, "markets": [mkt_a, mkt_b],
        "realized_band_p5_p95": [round(lo, 2), round(hi, 2)],
        "n_realized_months": n_real,
        "n_bars": int(len(panel)),
        "implied_wedge_mean": float(panel["wedge"].mean()),
        "implied_wedge_min": float(panel["wedge"].min()),
        "implied_wedge_max": float(panel["wedge"].max()),
        "frac_outside_band": float(outside.mean()),
        "n_persistent_runs": n_persist,
        "mean_iqr_a": float((panel["a75"] - panel["a25"]).mean()),
        "mean_iqr_b": float((panel["b75"] - panel["b25"]).mean()),
    }
    print(f"  {key:14s} wedge mean {out['implied_wedge_mean']:+.2f}pp "
          f"range [{out['implied_wedge_min']:+.2f}, {out['implied_wedge_max']:+.2f}] "
          f"vs realized [{lo:+.2f}, {hi:+.2f}]  outside: "
          f"{out['frac_outside_band']:.1%}  persistent runs: {n_persist}  "
          f"(IQRs {out['mean_iqr_a']:.2f}/{out['mean_iqr_b']:.2f}pp)")
    OUT_DIR.mkdir(exist_ok=True)
    panel.round(4).to_csv(OUT_DIR / f"{key}_unit_spread.csv")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Unit-space inflation spread checks (C4).")
    ap.add_argument("--zip", type=Path, default=None)
    ap.add_argument("--years", type=int, default=REALIZED_YEARS,
                    help="trailing years for the realized wedge band")
    args = ap.parse_args()

    zip_path = args.zip or sig.find_latest_zip()
    print(f"Loading bundle: {zip_path.name}  (unit-space spreads, realized band "
          f"= trailing {args.years}y [p5, p95])")
    markets = sig.load_markets(zip_path)
    hist = sig.load_history(zip_path)

    results = []
    for key in PAIRS:
        try:
            results.append(check_pair(key, hist, markets, zip_path, args.years))
        except (ValueError, KeyError) as exc:
            print(f"  {key:14s} FAILED: {exc}")
            results.append({"pair": key, "failed": str(exc)})
    print("  Reading: a wedge INSIDE the realized band is coherent pricing; only")
    print("  persistent excursions beyond it are candidate mispricings, and even")
    print("  those need staleness checks (medians ride carried bars).")

    OUT_DIR.mkdir(exist_ok=True)
    (OUT_DIR / "summary.json").write_text(json.dumps(
        {"bundle": zip_path.name, "params": {"years": args.years,
                                             "persist_bars": PERSIST_BARS},
         "results": results}, indent=2, default=float))
    print(f"  wrote {OUT_DIR}/summary.json")


if __name__ == "__main__":
    main()
