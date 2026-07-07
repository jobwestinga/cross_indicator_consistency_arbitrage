"""Out-of-sample gate: evaluate frozen rules on post-split data only.

EXPLORATORY / TESTING, but this is the honest test. All thresholds, z-windows
and rule definitions were chosen while looking at data up to the split date
(the legacy rules pre-date May 2026; the rules added 2026-07-07 use untuned
conventional defaults). Here they are evaluated ONLY on events that start at
or after the split:

  - events: crossings of the rule's frozen flag metric, entry >= split
  - controls: magnitude-matched + random baselines drawn from the OOS
    period only
  - backtest: same frozen parameters, trades entered >= split only

The causal trailing z-score may warm up on pre-split data — that is
legitimate (it only uses information available at each timestamp).

Usage:
    python3 analysis/oos_test.py                       # split 2026-05-01
    python3 analysis/oos_test.py --split 2026-06-01 --rules payrolls_labor
Output:
    analysis/oos/<rule>_oos.json + console table + analysis/oos/summary.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))  # runnable from anywhere

import backtest as bt
import rules
import signals as sig
import validate_consistency as vc

OUT_DIR = sig.out_base() / "oos"
DEFAULT_SPLIT = "2026-05-01"   # legacy rules were designed on data through Apr 30
DEFAULT_HORIZONS = [1, 4, 24, 72]


def oos_one_rule(rule_key: str, zip_path: Path, split: pd.Timestamp, z_window: int,
                 horizons: list[int], cost: float, seed: int,
                 history=None, markets=None) -> dict:
    rng = np.random.default_rng(seed)
    panel, roles, rule = rules.build_rule_panel(
        rule_key, zip_path, z_window, with_prices=True,
        history=history, markets=markets)
    score = panel["score"]
    metric = rules.flag_metric(rule)
    threshold = rules.flag_threshold(rule)
    min_gap = max(horizons)

    entries = vc.find_flag_entries(score, threshold, metric, min_gap=min_gap)
    entries_oos = entries[entries >= split]
    flagged = vc.forward_outcomes(score, entries_oos, horizons)
    rand = vc.random_baseline(score, horizons, vc.RANDOM_BASELINE_K, rng, after=split)
    matched = vc.magnitude_matched_baseline(score, horizons, threshold, metric,
                                            entries, vc.RANDOM_BASELINE_K, rng,
                                            after=split)
    result = vc.summarize(f"{rule_key} [OOS >= {split.date()}]",
                          flagged, rand, matched, horizons, rng)

    # frozen-parameter backtest, OOS trades only
    trades = bt.simulate(panel, roles, rule, threshold, threshold / 2,
                         max_hold=max(horizons), cost=cost, size="fixed")
    t_oos = trades[trades["entry"] >= split] if not trades.empty else trades
    bt_stats: dict = {"n_trades": int(len(t_oos))}
    if not t_oos.empty:
        net = t_oos["net"].to_numpy()
        legs = bt._legs(t_oos)
        bt_stats.update({
            "mean_net_per_trade": float(net.mean()),
            "win_rate": float((net > 0).mean()),
            "t_stat": bt._tstat(net),
            "net_pnl": float(net.sum()),
            "breakeven_cost_per_leg": float(t_oos["gross"].sum() / legs.sum())
                                      if legs.sum() > 0 else float("nan"),
        })
        print(f"  OOS backtest: {len(t_oos)} trades  mean net/trade "
              f"{net.mean():+.4f}  break-even/leg "
              f"{bt_stats['breakeven_cost_per_leg']:+.4f} (cost {cost})")
    else:
        print("  OOS backtest: no trades in the OOS window")

    oos_days = (score.index.max() - max(split, score.index.min())).total_seconds() / 86400
    return {
        "rule": rule_key,
        "split": str(split.date()),
        "oos_days": round(oos_days, 1),
        "params": {"z_window": z_window, "threshold": threshold, "metric": metric,
                   "horizons": horizons, "min_gap": min_gap, "cost": cost,
                   "seed": seed},
        "n_events_total": int(len(entries)),
        "n_events_oos": int(len(entries_oos)),
        "overall": result["overall"],
        "verdicts": {str(h): {k: (list(v) if isinstance(v, tuple) else v)
                              for k, v in d.items()}
                     for h, d in result["verdicts"].items()},
        "backtest": bt_stats,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Out-of-sample gate for frozen rules.")
    ap.add_argument("--zip", type=Path, default=None)
    ap.add_argument("--split", default=DEFAULT_SPLIT,
                    help="OOS start date (default: %(default)s)")
    ap.add_argument("--rules", nargs="+", default=None)
    ap.add_argument("--z-window", type=int, default=48)
    ap.add_argument("--horizons", type=int, nargs="+", default=DEFAULT_HORIZONS)
    ap.add_argument("--cost", type=float, default=0.02)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    zip_path = args.zip or sig.find_latest_zip()
    split = pd.Timestamp(args.split, tz="UTC")
    cfg = sig.load_mappings()
    todo = args.rules or rules.implemented_rules(cfg)

    print(f"OOS gate: bundle {zip_path.name}, split {split.date()} "
          f"(frozen thresholds from mappings.yaml)")
    markets = sig.load_markets(zip_path)
    history = sig.load_history(zip_path)

    OUT_DIR.mkdir(exist_ok=True)
    results = []
    for rule_key in todo:
        try:
            res = oos_one_rule(rule_key, zip_path, split, args.z_window,
                               args.horizons, args.cost, args.seed,
                               history=history, markets=markets)
        except (rules.RuleError, ValueError, KeyError) as exc:
            print(f"\n=== {rule_key} [OOS] ===\n  FAILED: {exc}")
            res = {"rule": rule_key, "failed": str(exc)}
        results.append(res)
        (OUT_DIR / f"{rule_key}_oos.json").write_text(
            json.dumps(res, indent=2, default=float))

    (OUT_DIR / "summary.json").write_text(json.dumps(
        {"split": str(split.date()), "bundle": zip_path.name, "results": results},
        indent=2, default=float))

    print(f"\n{'rule':16} {'ev_oos':>6} {'overall':32} {'trades':>6} "
          f"{'net/trade':>10} {'BE/leg':>8}")
    for r in results:
        if "failed" in r:
            print(f"{r['rule']:16} FAILED: {r['failed'][:50]}")
            continue
        b = r["backtest"]
        print(f"{r['rule']:16} {r['n_events_oos']:>6} {r['overall'][:32]:32} "
              f"{b.get('n_trades', 0):>6} "
              f"{b.get('mean_net_per_trade', float('nan')):>+10.4f} "
              f"{b.get('breakeven_cost_per_leg', float('nan')):>+8.4f}")
    print(f"\nwrote {OUT_DIR}/<rule>_oos.json + summary.json")


if __name__ == "__main__":
    main()
