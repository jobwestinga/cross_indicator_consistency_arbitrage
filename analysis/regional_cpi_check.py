"""Regional CPI aggregation identity (rule B10).

EXPLORATORY / TESTING. National CPI yoy is (to first order) the expenditure-
weighted average of the four census-region CPI yoy rates — an ACCOUNTING
identity, not a correlation. The venue lists all five markets:

  - national ("US Consumer Price Index Yearly"): strikes in yoy %
  - regions ("Northeastern/Midwestern/Southern/Western US CPI"): strikes in
    NSA INDEX LEVELS (each region has its own base period!)

so regional implied medians are converted to implied yoy using the region's
FRED NSA index 12 months before the front contract's reference month
(CUUR0x00SA0, collected by collect_fred.py). Then:

    national_yoy  ≈  Σ_r w_r · regional_yoy_r

Weights are BLS CPI-U census-region relative-importance shares (approximate,
slow-moving). Persistent |gap| beyond strike granularity (regions have $1
level strikes ≈ 0.3pp of yoy) and staleness is the identity-violation
signal — the strongest evidence class after within-market arbitrage, because
no economic theory is assumed.

Usage:
    python3 analysis/regional_cpi_check.py
Output (analysis/regional_cpi/): gap CSV + plot + summary.json.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))  # runnable from anywhere

import signals as sig

OUT_DIR = sig.out_base() / "regional_cpi"
NATIONAL = "US Consumer Price Index Yearly"
# market -> (BLS CPI-U census-region relative-importance share, approx 2024;
#            FRED NSA index series for the yoy denominator)
REGIONS = {
    "Northeastern US CPI": (0.175, "CUUR0100SA0"),
    "Midwestern US CPI": (0.213, "CUUR0200SA0"),
    "Southern US CPI": (0.375, "CUUR0300SA0"),
    "Western US CPI": (0.237, "CUUR0400SA0"),
}
GAP_NOTABLE = 0.35       # pp of yoy — regional $1 level strikes ~ 0.3pp
PERSIST_BARS = 24        # consecutive hourly bars for a "persistent" run


def ref_month_series(hist: pd.DataFrame, markets: pd.DataFrame,
                     grid: pd.DatetimeIndex, roll_days: int = 2) -> pd.Series:
    """Reference month (Period[M]) of the FRONT contract at each grid bar.

    CPI markets expire on release day for the PREVIOUS month, so the front
    reference month = month(next expiry after t + roll buffer) - 1. All five
    markets share the CPI release schedule; the national market's expiries
    define it.
    """
    conid = sig.resolve_conid(markets, NATIONAL)
    exps = (hist.loc[hist.underlying_conid == conid, "expiration"]
            .dropna().drop_duplicates().sort_values().reset_index(drop=True))
    cut = exps - pd.Timedelta(days=roll_days)
    idx = np.searchsorted(cut.to_numpy(), grid.to_numpy(), side="right")
    idx = np.minimum(idx, len(exps) - 1)
    front = exps.iloc[idx].dt.tz_localize(None)
    return pd.Series((front.dt.to_period("M") - 1).to_numpy(), index=grid)


def regional_yoy(median_level: pd.Series, fred_series: str,
                 ref_months: pd.Series) -> pd.Series:
    """Implied yoy % from an implied index-level median: the denominator is
    the region's realized NSA index 12 months before the reference month."""
    base = sig.load_fred_series(fred_series)
    base.index = base.index.tz_localize(None).to_period("M")
    denom_month = ref_months.reindex(median_level.index) - 12
    denom = pd.Series(base.reindex(denom_month.to_numpy()).to_numpy(),
                      index=median_level.index)
    return (median_level / denom - 1.0) * 100.0


def persistent_runs(mask: pd.Series, min_bars: int) -> int:
    """Number of runs of >= min_bars consecutive True bars."""
    if mask.empty:
        return 0
    grp = (mask != mask.shift()).cumsum()
    runs = mask.groupby(grp).agg(["all", "size"])
    return int(((runs["all"]) & (runs["size"] >= min_bars)).sum())


def main() -> None:
    ap = argparse.ArgumentParser(description="Regional CPI aggregation identity (B10).")
    ap.add_argument("--zip", type=Path, default=None)
    ap.add_argument("--gap-notable", type=float, default=GAP_NOTABLE)
    args = ap.parse_args()

    zip_path = args.zip or sig.find_latest_zip()
    print(f"Loading bundle: {zip_path.name}  (regional CPI aggregation identity)")
    markets = sig.load_markets(zip_path)
    hist = sig.load_history(zip_path)

    series: dict[str, pd.Series] = {}
    missing = []
    for name in [NATIONAL, *REGIONS]:
        try:
            series[name] = sig.cached_implied_series(zip_path, hist, markets, name)
        except (KeyError, ValueError) as exc:
            missing.append(f"{name} ({exc})")
    if NATIONAL not in series or len(series) < 3:
        raise SystemExit(f"required CPI markets missing/empty: {missing}")

    panel = sig.align(*series.values())
    panel.columns = list(series.keys())
    if panel.empty:
        raise SystemExit("no overlapping observations across the CPI markets")

    # regions price index LEVELS -> convert to implied yoy with FRED bases
    ref_m = ref_month_series(hist, markets, panel.index)
    have = []
    for name, (_w, fred_id) in REGIONS.items():
        if name not in panel.columns:
            continue
        try:
            panel[f"yoy_{name}"] = regional_yoy(panel[name], fred_id, ref_m)
            have.append(name)
        except ValueError as exc:        # FRED series missing locally
            missing.append(f"{name} yoy base ({exc})")
    if len(have) < 3:
        raise SystemExit(f"too few regional yoy conversions: {missing}")

    w = np.array([REGIONS[r][0] for r in have])
    w = w / w.sum()                      # renormalize over available regions
    panel["aggregate"] = panel[[f"yoy_{r}" for r in have]].to_numpy() @ w
    panel["gap"] = panel[NATIONAL] - panel["aggregate"]

    g = panel["gap"].dropna()
    # a constant offset is indistinguishable from weights/base error at this
    # resolution; the DEMEANED gap is what can indicate time-varying
    # mispricing between the national and regional markets.
    gd = g - g.mean()
    notable = gd.abs() > args.gap_notable
    n_persist = persistent_runs(notable, PERSIST_BARS)
    stats = {
        "n_bars": int(len(g)),
        "regions_used": have,
        "weights": {r: round(float(x), 3) for r, x in zip(have, w, strict=True)},
        "mean_gap_pp": float(g.mean()),
        "mean_abs_gap_pp": float(g.abs().mean()),
        "demeaned_p75_abs_pp": float(gd.abs().quantile(0.75)),
        "demeaned_p95_abs_pp": float(gd.abs().quantile(0.95)),
        "demeaned_max_abs_pp": float(gd.abs().max()),
        "frac_demeaned_gt_notable": float(notable.mean()),
        "n_persistent_runs": n_persist,
    }

    print("\n=== regional CPI aggregation identity (EXPLORATORY) ===")
    print(f"  regions: {', '.join(have)}  (weights renormalized: "
          f"{[round(float(x), 3) for x in w]})")
    print(f"  bars: {stats['n_bars']:,}   mean gap {stats['mean_gap_pp']:+.3f}pp "
          f"(constant offset ~ weights/base error)   mean|gap| "
          f"{stats['mean_abs_gap_pp']:.3f}pp")
    print(f"  demeaned |gap| p75 {stats['demeaned_p75_abs_pp']:.3f}  "
          f"p95 {stats['demeaned_p95_abs_pp']:.3f}  "
          f"max {stats['demeaned_max_abs_pp']:.3f}pp")
    print(f"  demeaned |gap| > {args.gap_notable}pp: "
          f"{stats['frac_demeaned_gt_notable']:.1%} of bars; "
          f"persistent (>= {PERSIST_BARS}h) runs: {n_persist}")
    if missing:
        print(f"  skipped markets: {missing}")
    print("  CAVEATS: implied MEDIANS on carried bars (not quotes); regional strikes")
    print("  are coarser than national; weights approximate; regional/national")
    print("  reference months assumed aligned. Persistent multi-day |gap| well above")
    print("  strike granularity is the signal, single bars are noise.")

    OUT_DIR.mkdir(exist_ok=True)
    panel.round(4).to_csv(OUT_DIR / "regional_gap.csv")
    (OUT_DIR / "summary.json").write_text(json.dumps(
        {"bundle": zip_path.name, "params": {"gap_notable": args.gap_notable,
                                             "persist_bars": PERSIST_BARS},
         **stats}, indent=2, default=float))

    try:
        import matplotlib.pyplot as plt
        fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True,
                                 height_ratios=[2, 1])
        axes[0].plot(panel.index, panel[NATIONAL], label="national", linewidth=1.6)
        axes[0].plot(panel.index, panel["aggregate"], label="weighted regions",
                     linewidth=1.6, linestyle="--")
        axes[0].set_ylabel("implied median CPI yoy (%)")
        axes[0].legend()
        axes[0].grid(alpha=0.3)
        axes[0].set_title("Regional CPI aggregation identity (EXPLORATORY)")
        axes[1].plot(panel.index, panel["gap"], color="purple", linewidth=1)
        axes[1].axhline(0, color="black", linewidth=0.6)
        axes[1].axhline(args.gap_notable, color="red", linewidth=0.6, linestyle=":")
        axes[1].axhline(-args.gap_notable, color="red", linewidth=0.6, linestyle=":")
        axes[1].set_ylabel("gap (pp)")
        axes[1].grid(alpha=0.3)
        fig.autofmt_xdate()
        fig.tight_layout()
        fig.savefig(OUT_DIR / "regional_gap.png", dpi=140)
        plt.close(fig)
        print(f"  plot -> {OUT_DIR / 'regional_gap.png'}")
    except Exception:  # noqa: BLE001
        pass
    print(f"  wrote {OUT_DIR}/regional_gap.csv + summary.json")


if __name__ == "__main__":
    main()
