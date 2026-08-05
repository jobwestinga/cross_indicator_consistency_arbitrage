"""A/B the expiry-tracking mode: front vs active (decision harness).

EXPLORATORY / TESTING. `front_expiry_filter` keeps only the nearest unexpired
expiry and discards 52-96% of prints on this venue (measured 2026-08-03: US
Real GDP 269 rows -> 10, which is why `okun` never produced a score; Fed Funds
keeps 17%). `active_expiry_filter` tracks the most-traded unexpired expiry
instead — causal, still one expiry per bar (A1 holds).

More data is not automatically better here: switching expiry JUMPS the series
(a September-CPI median measures a different reference month than August's),
exactly the hazard A11 found for reference-contract switches. A jumpy score
can manufacture apparent mean reversion. So this script measures BOTH modes
per rule on the same bundle and prints them side by side:

  coverage (score points, window), events, validation verdict, permutation p,
  and the number of expiry switches each leg makes (the jump-risk proxy).

Read it as: `active` is worth adopting for a rule when it adds coverage/events
WITHOUT the verdict improving only in step with the switch count.

Usage:
    python3 analysis/expiry_mode_ab.py
    python3 analysis/expiry_mode_ab.py --rules okun taylor --permute 200
Output (analysis/expiry_ab/): summary.json + console table.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))  # runnable from anywhere

import rules
import signals as sig
import validate_consistency as vc

OUT_DIR = sig.out_base() / "expiry_ab"
MODES = ("front", "active")


def switches_for_rule(hist, markets, rule: dict, mode: str) -> int:
    """Total expiry switches across the rule's legs (jump-risk proxy)."""
    total = 0
    for spec in rule["indicators"].values():
        try:
            te = sig.tracked_expiry_series(hist, markets, spec["market_name"],
                                           expiry_mode=mode).dropna()
        except (ValueError, KeyError):
            continue
        if len(te):
            total += int((te != te.shift()).sum() - 1)
    return total


def evaluate(rule_key: str, zip_path: Path, mode: str, z_window: int,
             horizons: list[int], n_perm: int, seed: int,
             history=None, markets=None) -> dict:
    panel, roles, rule = rules.build_rule_panel(
        rule_key, zip_path, z_window, history=history, markets=markets,
        expiry_mode=mode)
    score = panel["score"]
    metric = rules.flag_metric(rule)
    threshold = rules.flag_threshold(rule)
    min_gap = max(horizons)
    rng = np.random.default_rng(seed)
    res, _flagged, _entries = vc.validate_once(score, metric, threshold, horizons,
                                               min_gap, rng, rule_key=rule_key,
                                               quiet=True)
    out = {
        "mode": mode,
        "n_points": int(len(score)),
        "window_days": round((score.index.max() - score.index.min()).total_seconds()
                             / 86400, 1),
        "n_events": res["n_events"],
        "overall": res["overall"].split(" (")[0],
        "switches": switches_for_rule(history, markets, rule, mode),
    }
    if n_perm:
        perm = vc.permutation_test(panel, rule, roles, z_window, threshold, metric,
                                   horizons, min_gap, n_perm,
                                   np.random.default_rng(seed + 1))
        out["perm_p"] = perm.get("p_value")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="A/B front vs active expiry tracking.")
    ap.add_argument("--zip", type=Path, default=None)
    ap.add_argument("--rules", nargs="+", default=None)
    ap.add_argument("--z-window", type=int, default=48)
    ap.add_argument("--horizons", type=int, nargs="+", default=[1, 4, 24, 72])
    ap.add_argument("--permute", type=int, default=0, metavar="N")
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    zip_path = args.zip or sig.find_latest_zip()
    cfg = sig.load_mappings()
    todo = args.rules or rules.implemented_rules(cfg)
    print(f"Loading bundle: {zip_path.name}  (expiry-mode A/B, z={args.z_window}h)")
    markets = sig.load_markets(zip_path)
    history = sig.load_history(zip_path)

    print(f"\n{'rule':16} {'mode':7} {'points':>7} {'days':>6} {'events':>7} "
          f"{'switch':>6} {'perm p':>7}  verdict")
    results = []
    for rule_key in todo:
        for mode in MODES:
            try:
                r = evaluate(rule_key, zip_path, mode, args.z_window, args.horizons,
                             args.permute, args.seed, history=history, markets=markets)
                r["rule"] = rule_key
                pp = r.get("perm_p")
                print(f"{rule_key:16} {mode:7} {r['n_points']:>7} {r['window_days']:>6.0f} "
                      f"{r['n_events']:>7} {r['switches']:>6} "
                      f"{(f'{pp:.3f}' if pp is not None and pp == pp else '-'):>7}  "
                      f"{r['overall']}")
            except (rules.RuleError, ValueError, KeyError) as exc:
                r = {"rule": rule_key, "mode": mode, "failed": str(exc)}
                print(f"{rule_key:16} {mode:7} FAILED: {str(exc)[:56]}")
            results.append(r)

    gained = [r["rule"] for r in results
              if r.get("mode") == "active" and "failed" not in r
              and any(x.get("rule") == r["rule"] and x.get("mode") == "front"
                      and "failed" in x for x in results)]
    if gained:
        print(f"\n  rules that ONLY run in active mode: {', '.join(gained)}")
    print("\n  Reading: extra coverage is only worth it if the verdict does not")
    print("  improve in lockstep with the switch count — every switch jumps the")
    print("  series (A11 hazard) and can manufacture reversion.")

    OUT_DIR.mkdir(exist_ok=True)
    (OUT_DIR / "summary.json").write_text(json.dumps(
        {"bundle": zip_path.name,
         "params": {"z_window": args.z_window, "horizons": args.horizons,
                    "permute": args.permute, "seed": args.seed},
         "results": results}, indent=2, default=float))
    print(f"  wrote {OUT_DIR}/summary.json")


if __name__ == "__main__":
    main()
