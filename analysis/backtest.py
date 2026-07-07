"""Toy backtest of the consistency-convergence trade.

EXPLORATORY / TESTING. Step 3 of the strategy. The validation (step 2) tests
whether flagged inconsistencies revert; this asks the next question: does
fading the flag, on the ACTUAL tradeable contracts and after costs, make money?

What it does
------------
- Signal: the validated causal inconsistency score (trailing z on the rule's
  signal; median for ladder-rich markets, prob for thin ones).
- Execution: you cannot trade a score, you trade contracts. PnL is computed on
  each leg's front-expiry reference YES contract (the causal `prob` series, in
  $0-1 payout units). On a flag we put on the convergence trade:
    product rules: fade each leg toward its mean  (pos = -sign(z_leg))
    linear rules : short the score coherently     (pos = -sign(score) * weight)
- Realism: enter one bar AFTER the signal (no look-ahead); hold until the score
  reverts below an exit band or a max-hold cap; one position at a time; charge a
  fixed round-trip cost per leg and sweep it to find break-even.
- `--min-volume N` rebuilds all series from bars with volume >= N only:
  sensitivity of everything to the 87%-of-bars-are-marks problem.

What it is NOT
--------------
A production backtest. No order book / market impact / partial fills; cost is a
flat proxy (the bundle has no bid/ask); exit is mark-to-market, not settlement;
small N. Treat results as a sanity check on tradeability, not a P&L promise.

Usage:
    python3 analysis/backtest.py --rule phillips
    python3 analysis/backtest.py --rule taylor --cost 0.01 --size zscaled
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

OUT_DIR = sig.out_base() / "backtest"
DEFAULT_HORIZON_EXIT = 72          # max hold (bars/hours)
COST_GRID = [0.0, 0.005, 0.01, 0.02, 0.03, 0.05]


# --------------------------------------------------------------------------- #
# positions
# --------------------------------------------------------------------------- #
def leg_positions(rule: dict, roles: list[str], z_entry: dict[str, float],
                  score_entry: float, size: str) -> dict[str, float]:
    """Convergence-trade positions per leg at entry.

    product rules: each leg is faded toward its own mean (-sign(z_leg)); when
      the product flags, both legs are extreme in a coherent corner.
    linear rules : short the score. score = sum(w_i * z_i), so the coherent
      trade is pos_i = -sign(score) * w_i, whatever sign each leg's own z has.
    zscaled size multiplies every leg by |score| at entry.
    """
    spec = rule["logic"]["score"]
    scale = abs(score_entry) if size == "zscaled" else 1.0
    if spec["type"] == "product":
        pos = {r: -float(np.sign(z_entry[r])) for r in roles}
    else:
        weights = spec["weights"]
        pos = {r: -float(np.sign(score_entry)) * float(weights.get(r, 0.0)) for r in roles}
    return {r: p * scale for r, p in pos.items()}


# --------------------------------------------------------------------------- #
# simulate
# --------------------------------------------------------------------------- #
def _metric_arr(s: np.ndarray, metric: str) -> np.ndarray:
    return s if metric == "value" else np.abs(s)


def simulate(panel: pd.DataFrame, roles: list[str], rule: dict, threshold: float,
             exit_band: float, max_hold: int, cost: float, size: str) -> pd.DataFrame:
    """One position at a time. Enter the bar AFTER the rule's flag metric
    crosses threshold; exit when it falls below exit_band, after max_hold
    bars, or FORCED the bar before any leg's reference contract switches
    (the stitched px series jumps across a switch — holding through it would
    book a fictitious contract-morph PnL). PnL per leg =
    pos * (px_exit - px_entry) - round-trip cost."""
    metric = rules.flag_metric(rule)
    s = panel["score"].to_numpy()
    m = _metric_arr(s, metric)
    n = len(panel)
    px = {r: panel[f"px_{r}"].to_numpy() for r in roles}
    ref = {r: panel[f"pxref_{r}"].to_numpy() for r in roles}
    zz = {r: panel[f"z_{r}"].to_numpy() for r in roles}
    idx = panel.index

    def ref_switches(at: int, entry: int) -> bool:
        return any(ref[r][at] != ref[r][entry] for r in roles)

    trades = []
    i = 1
    while i < n - 1:
        # flag crossing at i-1 -> entry at i (next bar, no look-ahead)
        crossed = m[i - 1] >= threshold and (m[i - 2] if i >= 2 else 0.0) < threshold
        if not crossed:
            i += 1
            continue
        entry = i
        pos = leg_positions(rule, roles, {r: zz[r][i - 1] for r in roles},
                            s[i - 1], size)

        # hold until exit
        j = entry
        reason = "max_hold"
        while j < n - 1 and (j - entry) < max_hold:
            if ref_switches(j + 1, entry):
                reason = "ref_roll"     # forced: next bar prices a different contract
                break
            j += 1
            if m[j] < exit_band:
                reason = "converged"
                break

        rec = {"entry": idx[entry], "exit": idx[j], "hold": j - entry,
               "score_in": s[entry], "score_out": s[j], "reason": reason}
        gross = 0.0
        legs_cost = 0.0
        for r in roles:
            leg = pos[r] * (px[r][j] - px[r][entry])
            rec[f"pos_{r}"] = pos[r]
            rec[f"pnl_{r}"] = leg
            gross += leg
            legs_cost += abs(pos[r]) * cost  # round-trip cost scales with size
        rec["gross"] = gross
        rec["cost"] = legs_cost
        rec["net"] = gross - legs_cost
        trades.append(rec)
        i = j + 1  # flat again, look for next entry after exit
    return pd.DataFrame(trades)


def random_baseline(panel: pd.DataFrame, roles: list[str], rule: dict, n_trades: int,
                    max_hold: int, cost: float, size: str, rng) -> pd.DataFrame:
    """Same trade construction entered at random times (any score), for
    comparison. Applies the same forced exit before a reference switch."""
    n = len(panel)
    if n - max_hold - 2 <= 1:
        return pd.DataFrame()
    s = panel["score"].to_numpy()
    px = {r: panel[f"px_{r}"].to_numpy() for r in roles}
    ref = {r: panel[f"pxref_{r}"].to_numpy() for r in roles}
    zz = {r: panel[f"z_{r}"].to_numpy() for r in roles}
    idx = panel.index
    starts = rng.choice(np.arange(1, n - max_hold - 1),
                        size=min(n_trades, n - max_hold - 2), replace=False)
    rows = []
    for entry in starts:
        pos = leg_positions(rule, roles, {r: zz[r][entry] for r in roles},
                            s[entry] if s[entry] != 0 else 1e-9, size)
        j = entry
        while j < min(entry + max_hold, n - 1):
            if any(ref[r][j + 1] != ref[r][entry] for r in roles):
                break
            j += 1
        net = sum(pos[r] * (px[r][j] - px[r][entry]) - abs(pos[r]) * cost for r in roles)
        rows.append({"entry": idx[entry], "net": net})
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# report
# --------------------------------------------------------------------------- #
def _tstat(x: np.ndarray) -> float:
    x = x[~np.isnan(x)]
    if len(x) < 2 or x.std(ddof=1) == 0:
        return float("nan")
    return float(x.mean() / (x.std(ddof=1) / np.sqrt(len(x))))


def _legs(trades: pd.DataFrame) -> np.ndarray:
    """Per-trade sum of |position| across legs (cost scales with this)."""
    cols = [c for c in trades.columns if c.startswith("pos_")]
    return trades[cols].abs().sum(axis=1).to_numpy()


def summarize(rule_key, trades: pd.DataFrame, baseline: pd.DataFrame, cost: float) -> dict:
    print(f"\n=== {rule_key} backtest (EXPLORATORY; fade-the-flag, costs included) ===")
    out: dict = {"n_trades": int(len(trades))}
    if trades.empty:
        print("  no trades generated.")
        return out
    net = trades["net"].to_numpy()
    conv = float((trades["reason"] == "converged").mean())
    print(f"  trades: {len(trades)}   avg hold: {trades['hold'].mean():.0f}h   "
          f"converged-exit: {conv*100:.0f}%")
    print(f"  gross PnL: {trades['gross'].sum():+.3f}   total cost: {trades['cost'].sum():.3f}   "
          f"NET PnL: {trades['net'].sum():+.3f}  ($/contract-leg units)")
    print(f"  mean net/trade: {net.mean():+.4f}   win rate: {(net>0).mean()*100:.0f}%   "
          f"t-stat: {_tstat(net):+.2f}")
    base_mean = float("nan")
    if not baseline.empty:
        base_mean = float(baseline["net"].mean())
        print(f"  random-entry baseline mean net/trade: {base_mean:+.4f}  "
              f"(strategy edge: {net.mean()-base_mean:+.4f})")

    print("  cost sensitivity (mean net/trade):")
    per_leg = _legs(trades)
    cost_sens = {}
    for c in COST_GRID:
        adj = trades["gross"].to_numpy() - per_leg * c
        cost_sens[c] = float(adj.mean())
        flag = "  <- current" if abs(c - cost) < 1e-9 else ""
        print(f"    cost={c:.3f}: {adj.mean():+.4f}{flag}")
    # break-even cost (where mean net crosses 0), per round-trip-leg
    be = float("nan")
    if per_leg.sum() > 0:
        be = float(trades["gross"].sum() / per_leg.sum())
        print(f"  break-even round-trip cost/leg: {be:+.4f} "
              f"(current {cost:.3f} -> {'profitable' if be > cost else 'unprofitable'})")
    print("  CAVEATS: cost is a flat proxy (no bid/ask in data); no impact/fills/settlement;")
    print("  mark-to-market exit; small N -> sanity check, not a P&L promise.")

    out.update({
        "avg_hold_h": float(trades["hold"].mean()),
        "converged_frac": conv,
        "gross_pnl": float(trades["gross"].sum()),
        "total_cost": float(trades["cost"].sum()),
        "net_pnl": float(trades["net"].sum()),
        "mean_net_per_trade": float(net.mean()),
        "win_rate": float((net > 0).mean()),
        "t_stat": _tstat(net),
        "baseline_mean_net": base_mean,
        "breakeven_cost_per_leg": be,
        "cost_sensitivity": cost_sens,
    })
    return out


def equity_plot(rule_key, trades: pd.DataFrame) -> None:
    if trades.empty:
        return
    try:
        import matplotlib.pyplot as plt
    except Exception:  # noqa: BLE001
        return
    eq = trades.sort_values("exit")["net"].cumsum()
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(trades.sort_values("exit")["exit"], eq, marker="o", markersize=3)
    ax.axhline(0, color="black", linewidth=0.6)
    ax.set_title(f"{rule_key} backtest equity curve (net, EXPLORATORY)")
    ax.set_ylabel("cumulative net PnL ($/contract-leg)")
    ax.grid(alpha=0.3)
    fig.autofmt_xdate()
    fig.tight_layout()
    OUT_DIR.mkdir(exist_ok=True)
    out = OUT_DIR / f"{rule_key}_equity.png"
    fig.savefig(out, dpi=140)
    plt.close(fig)
    print(f"  equity plot -> {out}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Backtest the consistency-convergence trade.")
    ap.add_argument("--rule", default="phillips")
    ap.add_argument("--zip", type=Path, default=None)
    ap.add_argument("--signal", choices=["median", "prob"], default=None)
    ap.add_argument("--z-window", type=int, default=48)
    ap.add_argument("--threshold", type=float, default=None,
                    help="entry threshold on the rule's flag metric (default from mappings)")
    ap.add_argument("--exit-band", type=float, default=None,
                    help="exit when the metric is below this (default: threshold/2)")
    ap.add_argument("--max-hold", type=int, default=DEFAULT_HORIZON_EXIT)
    ap.add_argument("--cost", type=float, default=0.02, help="round-trip cost per leg ($)")
    ap.add_argument("--size", choices=["fixed", "zscaled"], default="fixed")
    ap.add_argument("--min-volume", type=int, default=0,
                    help="use only bars with volume >= N (marks-sensitivity)")
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    zip_path = args.zip or sig.find_latest_zip()

    try:
        panel, roles, rule = rules.build_rule_panel(
            args.rule, zip_path, args.z_window, kind=args.signal,
            min_volume=args.min_volume, with_prices=True)
    except (rules.RuleError, ValueError, KeyError) as exc:
        raise SystemExit(str(exc)) from exc
    threshold = args.threshold if args.threshold is not None else rules.flag_threshold(rule)
    exit_band = args.exit_band if args.exit_band is not None else threshold / 2

    print(f"Loading bundle: {zip_path.name}  (rule={args.rule}, "
          f"z_window={args.z_window}h, entry>{threshold}, exit<{exit_band}, "
          f"max_hold={args.max_hold}h, cost={args.cost}, size={args.size}, "
          f"min_volume={args.min_volume})")
    trades = simulate(panel, roles, rule, threshold, exit_band,
                      args.max_hold, args.cost, args.size)
    baseline = random_baseline(panel, roles, rule, max(len(trades) * 5, 100),
                               args.max_hold, args.cost, args.size, rng)

    OUT_DIR.mkdir(exist_ok=True)
    if not trades.empty:
        trades.to_csv(OUT_DIR / f"{args.rule}_trades.csv", index=False)
    stats = summarize(args.rule, trades, baseline, args.cost)
    equity_plot(args.rule, trades)

    summary = {
        "rule": args.rule,
        "bundle": zip_path.name,
        "params": {"z_window": args.z_window, "threshold": threshold,
                   "exit_band": exit_band, "max_hold": args.max_hold,
                   "cost": args.cost, "size": args.size,
                   "min_volume": args.min_volume, "seed": args.seed},
        **stats,
    }
    json_path = OUT_DIR / f"{args.rule}_backtest.json"
    json_path.write_text(json.dumps(summary, indent=2, default=float))
    print(f"\n  JSON summary -> {json_path}")
    if not trades.empty:
        print(f"  per-trade CSV -> {OUT_DIR / (args.rule + '_trades.csv')}")


if __name__ == "__main__":
    main()
