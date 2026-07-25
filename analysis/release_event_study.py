"""Release-time event study (D4): do inconsistencies close AT macro releases?

EXPLORATORY / TESTING. Mechanism evidence for the whole thesis: if flagged
cross-market inconsistencies are real mispricings that data resolves, |score|
should compress when the relevant macro number is RELEASED, not at random
times. If |score| decays independently of releases, the "inconsistency" is
mostly signal-construction noise reverting on its own clock.

No external release calendar is needed: ForecastTrader contracts EXPIRE on
release day (CPI markets on CPI day, unemployment on jobs day, ...), so each
rule's release times are the union of its leg markets' expiration dates that
fall inside the score window.

Statistic per rule: mean |score| over the 48h AFTER a release divided by the
48h BEFORE it (a ratio < 1 = compression), compared against the same ratio at
N random non-release anchors (bootstrap p-value for "release compression is
bigger than random-time compression").

Usage:
    python3 analysis/release_event_study.py
    python3 analysis/release_event_study.py --rules taylor core_headline
Output (analysis/releases/): per-rule JSON + summary.json.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))  # runnable from anywhere

import rules
import signals as sig

OUT_DIR = sig.out_base() / "releases"
WINDOW_H = 48
N_BOOT = 500


def release_times(hist: pd.DataFrame, markets: pd.DataFrame, rule: dict,
                  lo: pd.Timestamp, hi: pd.Timestamp) -> pd.DatetimeIndex:
    """Union of the rule's leg-market expiration dates inside (lo, hi)."""
    out: set[pd.Timestamp] = set()
    for spec in rule["indicators"].values():
        try:
            conid = sig.resolve_conid(markets, spec["market_name"])
        except KeyError:
            continue
        exps = hist.loc[hist.underlying_conid == conid, "expiration"].dropna()
        out |= {e for e in exps.drop_duplicates()
                if lo + pd.Timedelta(hours=WINDOW_H) < e < hi - pd.Timedelta(hours=WINDOW_H)}
    return pd.DatetimeIndex(sorted(out))


def compression_ratio(abs_score: pd.Series, anchor: pd.Timestamp) -> float:
    """mean|score| after / before the anchor (window WINDOW_H hours)."""
    pre = abs_score.loc[anchor - pd.Timedelta(hours=WINDOW_H):anchor]
    post = abs_score.loc[anchor:anchor + pd.Timedelta(hours=WINDOW_H)]
    if len(pre) < 12 or len(post) < 12 or pre.mean() == 0:
        return np.nan
    return float(post.mean() / pre.mean())


def study_rule(rule_key: str, zip_path: Path, z_window: int, rng,
               history=None, markets=None) -> dict:
    panel, _roles, rule = rules.build_rule_panel(
        rule_key, zip_path, z_window, history=history, markets=markets)
    abs_score = panel["score"].abs()
    hist = history if history is not None else sig.load_history(zip_path)
    mkts = markets if markets is not None else sig.load_markets(zip_path)

    rels = release_times(hist, mkts, rule, panel.index.min(), panel.index.max())
    ratios = np.array([compression_ratio(abs_score, r) for r in rels])
    ratios = ratios[~np.isnan(ratios)]
    out: dict = {"rule": rule_key, "n_releases": int(len(ratios))}
    if len(ratios) < 4:
        out["verdict"] = "INCONCLUSIVE (too few releases in window)"
        print(f"  {rule_key:16s} releases={len(ratios):>3}  {out['verdict']}")
        return out

    rel_mean = float(np.median(ratios))   # median: ratios explode when pre~0
    # bootstrap null: same-count random anchors inside the valid range
    valid = panel.index[(panel.index > panel.index.min() + pd.Timedelta(hours=WINDOW_H))
                        & (panel.index < panel.index.max() - pd.Timedelta(hours=WINDOW_H))]
    null_means = []
    for _ in range(N_BOOT):
        anchors = valid[rng.choice(len(valid), size=len(ratios), replace=False)]
        rs = np.array([compression_ratio(abs_score, a) for a in anchors])
        rs = rs[~np.isnan(rs)]
        if len(rs):
            null_means.append(np.median(rs))
    null_arr = np.array(null_means)
    p = float((null_arr <= rel_mean).mean()) if len(null_arr) else float("nan")

    out.update({
        "median_post_pre_ratio": rel_mean,
        "null_median_ratio": float(np.median(null_arr)) if len(null_arr) else float("nan"),
        "p_value_compression": p,
        "verdict": ("RELEASES COMPRESS (score closes at releases)"
                    if p < 0.05 and rel_mean < 1 else
                    "no release-specific compression"),
    })
    print(f"  {rule_key:16s} releases={len(ratios):>3}  post/pre="
          f"{rel_mean:.3f}  null={out['null_median_ratio']:.3f}  p={p:.3f}  "
          f"-> {out['verdict']}")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Release-time event study (D4).")
    ap.add_argument("--zip", type=Path, default=None)
    ap.add_argument("--rules", nargs="+", default=None)
    ap.add_argument("--z-window", type=int, default=48)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    zip_path = args.zip or sig.find_latest_zip()
    cfg = sig.load_mappings()
    todo = args.rules or rules.implemented_rules(cfg)
    rng = np.random.default_rng(args.seed)
    print(f"Loading bundle: {zip_path.name}  (release event study, "
          f"window ±{WINDOW_H}h, {N_BOOT} bootstrap draws)")
    markets = sig.load_markets(zip_path)
    history = sig.load_history(zip_path)

    results = []
    for rule_key in todo:
        try:
            results.append(study_rule(rule_key, zip_path, args.z_window, rng,
                                      history=history, markets=markets))
        except (rules.RuleError, ValueError, KeyError) as exc:
            print(f"  {rule_key:16s} FAILED: {exc}")
            results.append({"rule": rule_key, "failed": str(exc)})

    ok = [r for r in results if "median_post_pre_ratio" in r]
    pooled = (float(np.median([r["median_post_pre_ratio"] for r in ok]))
              if ok else float("nan"))
    print(f"\n  pooled post/pre ratio across rules: {pooled:.3f} "
          f"(<1 = |score| compresses after releases)")
    print("  CAVEAT: releases coincide with expiry rolls; part of any compression is")
    print("  mechanical (front-expiry switch resets the ladder). Read jointly with")
    print("  the ref-roll-aware backtest, not alone.")

    OUT_DIR.mkdir(exist_ok=True)
    (OUT_DIR / "summary.json").write_text(json.dumps(
        {"bundle": zip_path.name,
         "params": {"z_window": args.z_window, "window_h": WINDOW_H,
                    "n_boot": N_BOOT, "seed": args.seed},
         "pooled_post_pre_ratio": pooled,
         "results": results}, indent=2, default=float))
    print(f"  wrote {OUT_DIR}/summary.json")


if __name__ == "__main__":
    main()
