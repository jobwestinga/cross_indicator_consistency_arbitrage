"""Scan for WITHIN-market static-arbitrage violations in the bundle.

EXPLORATORY / TESTING. The consistency rules test *cross*-market coherence via
economic identities; this script asks the more primitive question first: does
the venue even price *single* markets coherently? Two model-free checks:

  1. Ladder monotonicity: for one market, one expiration, one timestamp, the
     YES survival curve must satisfy P(X > K1) >= P(X > K2) for K1 < K2.
     An "inversion" (higher strike priced above lower strike) is a static
     arbitrage: buy the cheap low-strike YES, sell the rich high-strike YES.
  2. YES/NO parity: for the same contract strike/expiry/timestamp,
     P(YES) + P(NO) should equal 1 (minus fees). Gaps are two-sided lock-ins.

IMPORTANT caveats baked into the output:
  - `avg` is a bar AVERAGE of trades, not a simultaneous quote pair. A
    violation inside one bar may never have been executable. Persistence
    (same strike-pair inverted over >= `persist_bars` consecutive bars) is
    reported separately and is the credible subset.
  - Markets whose question semantics invert the ladder (violation rate > 50%)
    are reported but excluded from the aggregate.

Usage:
    python3 analysis/arbitrage_scan.py
    python3 analysis/arbitrage_scan.py --market "US Core CPI" --min-size 0.02
Output (analysis/arbitrage/):
    per-market CSV, persistent-violations CSV, JSON summary.
"""

from __future__ import annotations
from typing import Any

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))  # runnable from anywhere

import signals as sig

OUT_DIR = sig.out_base() / "arbitrage"
MIN_ROWS_DEFAULT = 500        # skip markets with fewer YES history rows
PERSIST_GAP = pd.Timedelta("2h")   # max spacing for "consecutive" bars


# --------------------------------------------------------------------------- #
# ladder monotonicity
# --------------------------------------------------------------------------- #
def scan_market_ladder(sub: pd.DataFrame, min_size: float) -> tuple[dict, pd.DataFrame]:
    """Violations of survival-ladder monotonicity within (expiration, ts).

    Returns (market stats, per-violation rows for persistence analysis).
    """
    pairs = viols = 0
    sizes: list[float] = []
    v_rows = []
    for (exp, ts), g in sub.groupby(["expiration", "ts_utc"], sort=False):
        if len(g) < 2:
            continue
        g = g.sort_values("strike")
        strikes = g["strike"].to_numpy()
        avg = g["avg"].to_numpy()
        vol = g["volume"].to_numpy()
        d = np.diff(avg)                      # survival: should be <= 0
        pairs += len(d)
        hit = np.flatnonzero(d > min_size)
        viols += len(hit)
        for k in hit:
            sizes.append(float(d[k]))
            v_rows.append({"expiration": exp, "ts_utc": ts,
                           "k_low": strikes[k], "k_high": strikes[k + 1],
                           "size": float(d[k]),
                           "both_traded": bool(vol[k] > 0 and vol[k + 1] > 0)})
    stats = {
        "n_pairs": pairs,
        "n_violations": viols,
        "violation_rate": viols / pairs if pairs else np.nan,
        "mean_size": float(np.mean(sizes)) if sizes else 0.0,
        "max_size": float(np.max(sizes)) if sizes else 0.0,
    }
    return stats, pd.DataFrame(v_rows)


def persistent_violations(v: pd.DataFrame) -> pd.DataFrame:
    """Group violations by (expiration, strike pair); keep runs of >= 2
    near-consecutive bars. These are the credible (not bar-artifact) cases."""
    if v.empty:
        return v
    runs = []
    for (exp, k1, k2), g in v.groupby(["expiration", "k_low", "k_high"]):
        g = g.sort_values("ts_utc")
        ts = g["ts_utc"].to_numpy()
        run_start = 0
        for i in range(1, len(g) + 1):
            if i == len(g) or (ts[i] - ts[i - 1]) > PERSIST_GAP.to_timedelta64():
                if i - run_start >= 2:
                    seg = g.iloc[run_start:i]
                    runs.append({
                        "expiration": exp, "k_low": k1, "k_high": k2,
                        "start": seg["ts_utc"].iloc[0], "end": seg["ts_utc"].iloc[-1],
                        "n_bars": len(seg),
                        "mean_size": float(seg["size"].mean()),
                        "max_size": float(seg["size"].max()),
                        "any_both_traded": bool(seg["both_traded"].any()),
                    })
                run_start = i
    return pd.DataFrame(runs)


# --------------------------------------------------------------------------- #
# YES/NO parity (also doubles as an effective-spread proxy)
# --------------------------------------------------------------------------- #
def scan_parity(history: pd.DataFrame, gap_warn: float) -> tuple[dict, pd.DataFrame]:
    """Global parity stats + a per-market table.

    Y+N-1 on matched bars is also the best spread proxy this dataset allows:
    both legs print at traded prices, so a persistent positive gap ~ the
    combined cost of crossing both books (upper bound on one round-trip leg,
    modulo asynchronous bars). The per-market p75(|gap|) is joined against
    rule break-evens in the report.
    """
    pv = history.pivot_table(index=["underlying_conid", "expiration", "strike", "ts_utc"],
                             columns="side", values="avg", aggfunc="first")
    if not {"Y", "N"} <= set(pv.columns):
        return {"n_pairs": 0}, pd.DataFrame()
    pv = pv.dropna(subset=["Y", "N"])
    gap = pv["Y"] + pv["N"] - 1.0
    stats = {
        "n_pairs": int(len(pv)),
        "mean_gap": float(gap.mean()),
        "frac_abs_gt_2c": float((gap.abs() > 0.02).mean()),
        "frac_abs_gt_5c": float((gap.abs() > 0.05).mean()),
        "frac_abs_gt_warn": float((gap.abs() > gap_warn).mean()),
        "max_abs_gap": float(gap.abs().max()),
    }
    names = (history.drop_duplicates("underlying_conid")
             .set_index("underlying_conid")["market_name"])
    g = gap.reset_index()
    g.columns = [*g.columns[:-1], "gap"]
    per = (g.groupby("underlying_conid")["gap"]
           .agg(n_pairs="size", mean_gap="mean",
                mean_abs_gap=lambda x: x.abs().mean(),
                p75_abs_gap=lambda x: x.abs().quantile(0.75),
                p95_abs_gap=lambda x: x.abs().quantile(0.95))
           .round(4).reset_index())
    per.insert(1, "market_name", per["underlying_conid"].map(names))
    return stats, per.sort_values("n_pairs", ascending=False)


# --------------------------------------------------------------------------- #
# driver
# --------------------------------------------------------------------------- #
def main() -> None:
    ap = argparse.ArgumentParser(description="Static-arbitrage scan (exploratory).")
    ap.add_argument("--zip", type=Path, default=None)
    ap.add_argument("--market", default=None, help="scan one market_name only")
    ap.add_argument("--min-rows", type=int, default=MIN_ROWS_DEFAULT)
    ap.add_argument("--min-size", type=float, default=0.001,
                    help="ignore inversions smaller than this ($)")
    ap.add_argument("--gap-warn", type=float, default=0.05,
                    help="parity |gap| reported as notable above this")
    args = ap.parse_args()

    zip_path = args.zip or sig.find_latest_zip()
    print(f"Loading bundle: {zip_path.name}")
    history = sig.load_history(zip_path)

    y = history[(history.side == "Y") & history.strike.notna()]
    counts = y.groupby("market_name").size()
    names = [args.market] if args.market else sorted(counts[counts >= args.min_rows].index)

    OUT_DIR.mkdir(exist_ok=True)
    per_market = []
    persist_all = []
    for name in names:
        sub = y[y.market_name == name]
        if sub.empty or sub["strike"].nunique() < 2:
            continue
        stats, v = scan_market_ladder(sub, args.min_size)
        runs = persistent_violations(v)
        stats.update({
            "market_name": name,
            "n_rows": int(len(sub)),
            "n_persistent_runs": int(len(runs)),
            "n_persistent_traded": int(runs["any_both_traded"].sum()) if not runs.empty else 0,
            "suspect_semantics": bool(stats["violation_rate"] > 0.5)
                                 if stats["n_pairs"] else False,
        })
        per_market.append(stats)
        if not runs.empty:
            runs.insert(0, "market_name", name)
            persist_all.append(runs)

    pm = pd.DataFrame(per_market).sort_values("violation_rate", ascending=False)
    parity, parity_by_market = scan_parity(history, args.gap_warn)

    # aggregate over trustworthy-semantics markets only
    ok = pm[~pm.suspect_semantics] if not pm.empty else pm
    agg_rate = ok.n_violations.sum() / ok.n_pairs.sum() if len(ok) and ok.n_pairs.sum() else np.nan
    n_persist = int(ok.n_persistent_runs.sum()) if len(ok) else 0
    n_persist_traded = int(ok.n_persistent_traded.sum()) if len(ok) else 0

    print(f"\n=== static-arbitrage scan (EXPLORATORY) ===")
    print(f"  markets scanned: {len(pm)}  (>= {args.min_rows} YES rows, >= 2 strikes)")
    print(f"  ladder inversions (within expiry): rate={agg_rate:.4f} across "
          f"{int(ok.n_pairs.sum()) if len(ok) else 0:,} adjacent-strike pairs")
    print(f"  persistent runs (>=2 consecutive bars): {n_persist}  "
          f"of which with volume on both legs: {n_persist_traded}")
    if (pm.suspect_semantics if not pm.empty else pd.Series(dtype=bool)).any():
        bad = pm[pm.suspect_semantics].market_name.tolist()
        print(f"  excluded (rate>50% -> inverted question semantics, check manually): {bad}")
    print("\n  top markets by inversion rate (trusted semantics):")
    show_cols = ["market_name", "n_pairs", "n_violations", "violation_rate",
                 "mean_size", "max_size", "n_persistent_runs", "n_persistent_traded"]
    if len(ok):
        print(ok.head(12)[show_cols].to_string(index=False))
    if parity.get("n_pairs"):
        print(f"\n  YES/NO parity: n={parity['n_pairs']:,}  mean gap={parity['mean_gap']:+.4f}  "
              f"|gap|>2c: {parity['frac_abs_gt_2c']:.3%}  >5c: {parity['frac_abs_gt_5c']:.3%}  "
              f"max: {parity['max_abs_gap']:.3f}")
    print("\n  CAVEAT: `avg` bars are trade averages, not simultaneous quotes. Single-bar")
    print("  inversions may not have been executable; persistent+traded runs are the")
    print("  credible subset. No fees/spread modeled here.")

    pm.to_csv(OUT_DIR / "ladder_violations_by_market.csv", index=False)
    if not parity_by_market.empty:
        parity_by_market.to_csv(OUT_DIR / "parity_by_market.csv", index=False)
    if persist_all:
        pd.concat(persist_all, ignore_index=True).sort_values(
            "max_size", ascending=False).to_csv(
            OUT_DIR / "persistent_violations.csv", index=False)
    summary = {
        "bundle": zip_path.name,
        "params": {"min_rows": args.min_rows, "min_size": args.min_size},
        "markets_scanned": int(len(pm)),
        "aggregate_violation_rate": None if np.isnan(agg_rate) else float(agg_rate),
        "n_adjacent_pairs": int(ok.n_pairs.sum()) if len(ok) else 0,
        "n_persistent_runs": n_persist,
        "n_persistent_traded_runs": n_persist_traded,
        "suspect_semantics_markets": pm[pm.suspect_semantics].market_name.tolist()
                                     if not pm.empty else [],
        "parity": parity,
    }
    (OUT_DIR / "scan_summary.json").write_text(json.dumps(summary, indent=2, default=str))
    print(f"\n  wrote {OUT_DIR}/ladder_violations_by_market.csv, "
          f"persistent_violations.csv, scan_summary.json")


if __name__ == "__main__":
    main()
