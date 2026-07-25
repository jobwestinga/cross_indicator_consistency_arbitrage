"""Factor residuals (C2): flag markets that deviate from the common macro factor.

EXPLORATORY / TESTING. The hand-written rules test PAIRWISE identities; this
asks the panel: is there a common macro factor across the implied-median
series, and do individual markets that deviate from their factor-implied move
revert? A cross-sectional "own rule" generator.

Method (causal, hard split):
  1. universe = markets with enough ladder history (discover_rules criteria)
  2. 6h changes per market; missing bars = 0 change (no print = no move on
     this venue; documented approximation)
  3. TRAIN (< --split): standardize per market on train stats; first
     principal component (SVD) = the macro factor; loadings frozen
  4. residual_it = standardized change - loading_i * factor_t, evaluated on
     ALL bars with train-frozen loadings; trailing z of the residual (causal)
  5. TEST (>= --split) only: |z| >= threshold crossings, non-overlapping;
     outcome = signed forward reversion of the market's own series over
     --horizon bars vs a magnitude-matched control from the test period

Zero robust deviations is the expected result while the venue is thin (the
pair-mining scan found no stable co-movement at all); this instrument exists
so the question is re-asked mechanically as data accrues.

Usage:
    python3 analysis/factor_residuals.py
    python3 analysis/factor_residuals.py --split 2026-07-07 --threshold 2.5
Output (analysis/factor/): loadings CSV + summary.json.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))  # runnable from anywhere

import discover_rules as dr
import signals as sig

OUT_DIR = sig.out_base() / "factor"
DEFAULT_SPLIT = "2026-05-01"
FREQ = "6h"


def change_panel(series: dict[str, pd.Series], freq: str = FREQ) -> pd.DataFrame:
    df = pd.concat(series, axis=1, sort=True).resample(freq).last().ffill(limit=8)
    return df.diff().iloc[1:]


def fit_factor(train: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    """First principal component on train changes. Returns (loadings, mu, sd)
    with mu/sd the train standardization frozen for the test period."""
    mu = train.mean()
    sd = train.std(ddof=0).replace(0.0, np.nan)
    x = ((train - mu) / sd).fillna(0.0).to_numpy()
    _, _, vt = np.linalg.svd(x, full_matrices=False)
    w = vt[0]
    if w.sum() < 0:                      # sign convention: factor ~ "macro up"
        w = -w
    return pd.Series(w, index=train.columns, name="loading"), mu, sd


def residual_zscores(changes: pd.DataFrame, loadings: pd.Series, mu: pd.Series,
                     sd: pd.Series, z_window: int) -> tuple[pd.DataFrame, pd.Series]:
    z = ((changes - mu) / sd).fillna(0.0)
    factor = z.to_numpy() @ loadings.to_numpy() / max((loadings ** 2).sum(), 1e-12)
    factor = pd.Series(factor, index=z.index, name="factor")
    resid = z.sub(pd.DataFrame(np.outer(factor, loadings),
                               index=z.index, columns=z.columns))
    rz = resid.apply(lambda c: sig.zscore_rolling(c, z_window))
    return rz, factor


def test_events(rz: pd.DataFrame, zchanges: pd.DataFrame, split: pd.Timestamp,
                threshold: float, horizon: int, min_gap: int, rng
                ) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Pooled flagged events across markets (test period), plus a magnitude-
    matched control: equally-extreme test-period bars that were not events.

    Outcomes are measured on TRAIN-STANDARDIZED changes so markets with big
    natural units (payrolls in jobs) cannot dominate the pooled statistic.
    """
    events, control_pool = [], []
    for mkt in rz.columns:
        z = rz[mkt]
        hot = z.abs() >= threshold
        crossings = hot & ~hot.shift(1, fill_value=False)
        idx = np.flatnonzero(crossings.to_numpy())
        kept: list[int] = []
        for i in idx:
            if z.index[i] < split or i + horizon >= len(z):
                continue
            if kept and i - kept[-1] < min_gap:
                continue
            kept.append(i)
            fwd = zchanges[mkt].iloc[i + 1:i + 1 + horizon].sum()
            events.append({"market": mkt, "ts": z.index[i], "z": float(z.iloc[i]),
                           "reversion": float(-np.sign(z.iloc[i]) * fwd)})
        hot_all = np.flatnonzero((z.abs() >= threshold).to_numpy())
        for i in hot_all:
            if z.index[i] >= split and i + horizon < len(z) and i not in kept:
                fwd = zchanges[mkt].iloc[i + 1:i + 1 + horizon].sum()
                control_pool.append({"market": mkt,
                                     "reversion": float(-np.sign(z.iloc[i]) * fwd)})
    ev = pd.DataFrame(events)
    ctl = pd.DataFrame(control_pool)
    if len(ctl) > 500:
        ctl = ctl.iloc[rng.choice(len(ctl), size=500, replace=False)]
    return ev, ctl


def main() -> None:
    ap = argparse.ArgumentParser(description="PCA factor residual scan (exploratory).")
    ap.add_argument("--zip", type=Path, default=None)
    ap.add_argument("--split", default=DEFAULT_SPLIT)
    ap.add_argument("--min-rows", type=int, default=1000)
    ap.add_argument("--z-window", type=int, default=28, help="trailing bars (6h each)")
    ap.add_argument("--threshold", type=float, default=2.0)
    ap.add_argument("--horizon", type=int, default=4, help="forward bars (6h each)")
    ap.add_argument("--min-gap", type=int, default=4)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    zip_path = args.zip or sig.find_latest_zip()
    split = pd.Timestamp(args.split, tz="UTC")
    rng = np.random.default_rng(args.seed)
    print(f"Loading bundle: {zip_path.name}  (factor residuals, split {split.date()})")
    markets = sig.load_markets(zip_path)
    hist = sig.load_history(zip_path)

    series = dr.build_universe(hist, markets, args.min_rows, zip_path=zip_path)
    if len(series) < 5:
        raise SystemExit(f"universe too small for a factor ({len(series)} markets)")
    changes = change_panel(series)
    train = changes[changes.index < split].dropna(how="all")
    if len(train) < 3 * args.z_window:
        raise SystemExit("train window too short for the chosen z-window")

    loadings, mu, sd = fit_factor(train)
    rz, factor = residual_zscores(changes, loadings, mu, sd, args.z_window)
    # share of train variance the factor explains (fit quality context)
    zt = ((train - mu) / sd).fillna(0.0)
    expl = float(np.linalg.norm(zt.to_numpy() @ loadings.to_numpy() /
                                max((loadings ** 2).sum(), 1e-12)) ** 2
                 * (loadings ** 2).sum() / max(np.linalg.norm(zt.to_numpy()) ** 2, 1e-12))

    zchanges = ((changes - mu) / sd).fillna(0.0)
    ev, ctl = test_events(rz, zchanges, split, args.threshold, args.horizon,
                          args.min_gap, rng)

    print(f"universe: {len(series)} markets   factor explains ~{expl:.1%} of "
          f"train change variance")
    print(f"test-period events (|resid z| >= {args.threshold}, non-overlapping): {len(ev)}")
    verdict = "INCONCLUSIVE (too few events)"
    stats: dict = {}
    if len(ev) >= 8:
        mean_rev = float(ev["reversion"].mean())
        ctl_mean = float(ctl["reversion"].mean()) if len(ctl) else float("nan")
        # the claim is "flagged deviations revert MORE than equally-extreme
        # bars", so the test statistic is the DIFFERENCE (Welch two-sample t)
        v1 = ev["reversion"].var(ddof=1) / len(ev)
        v2 = ctl["reversion"].var(ddof=1) / len(ctl) if len(ctl) > 1 else np.nan
        t_diff = ((mean_rev - ctl_mean) / np.sqrt(v1 + v2)
                  if not np.isnan(v2) and v1 + v2 > 0 else float("nan"))
        stats = {"mean_reversion": mean_rev, "matched_control_mean": ctl_mean,
                 "t_diff_vs_control": float(t_diff), "n_control": int(len(ctl))}
        beats = (not np.isnan(t_diff)) and mean_rev > ctl_mean * 1.10 and t_diff > 2
        verdict = ("EDGE-SUGGESTIVE (deviations revert beyond matched control)"
                   if beats else "WEAK (no clear edge over matched control)")
        print(f"  mean reversion {mean_rev:+.3f} (train-standardized change units) "
              f"vs matched control {ctl_mean:+.3f}   t(diff)={t_diff:+.2f}")
    print(f"  VERDICT: {verdict}")
    print("  CAVEATS: missing bars treated as zero change; loadings train-frozen; "
          "thin venue -> factor is weak by construction; reversion is z-space, "
          "not tradeable PnL.")

    OUT_DIR.mkdir(exist_ok=True)
    loadings.round(4).to_csv(OUT_DIR / "loadings.csv")
    if len(ev):
        ev.to_csv(OUT_DIR / "events.csv", index=False)
    (OUT_DIR / "summary.json").write_text(json.dumps({
        "bundle": zip_path.name, "split": str(split.date()),
        "params": {"min_rows": args.min_rows, "z_window": args.z_window,
                   "threshold": args.threshold, "horizon": args.horizon,
                   "min_gap": args.min_gap, "freq": FREQ, "seed": args.seed},
        "universe": int(len(series)),
        "explained_variance_share": expl,
        "n_events": int(len(ev)),
        "verdict": verdict,
        **stats,
    }, indent=2, default=float))
    print(f"  wrote {OUT_DIR}/loadings.csv + summary.json")


if __name__ == "__main__":
    main()
