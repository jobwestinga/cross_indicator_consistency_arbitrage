"""Toy backtest of the consistency-convergence trade.

EXPLORATORY / TESTING. Step 3 of the strategy. The validation (step 2) showed
flagged inconsistencies revert; this asks the next question: does fading the
flag, on the ACTUAL tradeable contracts and after costs, make money?

What it does
------------
- Signal: the validated causal inconsistency score (trailing z on the rule's
  signal; median for ladder-rich markets, prob for thin ones).
- Execution: you cannot trade a score, you trade contracts. So PnL is computed
  on each market's at-the-money YES contract price (the `prob` series, in $0-1
  payout units). On a flag we FADE each leg toward its mean:
  position = -sign(z_leg). As the inconsistency reverts, faded legs profit.
- Realism: enter one bar AFTER the signal (no look-ahead); hold until the score
  reverts below an exit band or a max-hold cap; one position at a time; charge a
  fixed round-trip cost per leg and sweep it to find break-even.

What it is NOT
--------------
A production backtest. No order book / market impact / partial fills; cost is a
flat proxy (the bundle has no bid/ask); exit is mark-to-market, not settlement;
small N. Treat results as a sanity check on tradeability, not a P&L promise.

Usage:
    python3 analysis/backtest.py --rule phillips
    python3 analysis/backtest.py --rule phillips --cost 0.01 --size zscaled \
        --z-window 48 --max-hold 72
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

import signals as sig
from run_consistency import _score

OUT_DIR = Path(__file__).resolve().parent / "backtest"
DEFAULT_HORIZON_EXIT = 72          # max hold (bars/hours)
COST_GRID = [0.0, 0.005, 0.01, 0.02, 0.03, 0.05]


# --------------------------------------------------------------------------- #
# build score + tradeable prices
# --------------------------------------------------------------------------- #
def build_panel(rule_key: str, zip_path: Path, z_window: int, kind: str) -> tuple[pd.DataFrame, list[str]]:
    cfg = sig.load_mappings()
    if rule_key not in cfg["rules"]:
        raise SystemExit(f"unknown rule {rule_key!r}; have {list(cfg['rules'])}")
    rule = cfg["rules"][rule_key]
    if rule.get("status", "planned") == "planned":
        raise SystemExit(f"rule {rule_key!r} is planned, not implemented")
    defaults = cfg["defaults"]
    band = tuple(defaults["prob_band"])
    freq = defaults["resample_freq"]

    markets = sig.load_markets(zip_path)
    history = sig.load_history(zip_path)
    roles = list(rule["indicators"])

    sigser, pxser = {}, {}
    for role, spec in rule["indicators"].items():
        name = spec["market_name"]
        sigser[role] = sig.implied_series(history, markets, name, kind=kind, band=band, freq=freq)
        # tradeable price = single ATM YES contract (prob signal), always
        pxser[role] = sig.implied_series(history, markets, name, kind="prob", band=band, freq=freq)

    cols = {f"sig_{r}": sigser[r] for r in roles}
    cols.update({f"px_{r}": pxser[r] for r in roles})
    panel = pd.concat(cols, axis=1).dropna()
    panel.columns = list(cols.keys())

    z = {r: sig.zscore_rolling(panel[f"sig_{r}"], z_window) for r in roles}
    for r in roles:
        panel[f"z_{r}"] = z[r]
    panel["score"] = _score(rule_key, z)
    return panel.dropna(subset=["score"]), roles


# --------------------------------------------------------------------------- #
# simulate
# --------------------------------------------------------------------------- #
def simulate(panel: pd.DataFrame, roles: list[str], threshold: float, exit_band: float,
             max_hold: int, cost: float, size: str) -> pd.DataFrame:
    """One position at a time. Enter the bar AFTER |score| crosses threshold;
    fade each leg (pos = -sign(z_leg)); exit when |score| < exit_band or after
    max_hold bars. PnL per leg = pos * (px_exit - px_entry) - round-trip cost."""
    s = panel["score"].to_numpy()
    n = len(panel)
    px = {r: panel[f"px_{r}"].to_numpy() for r in roles}
    zz = {r: panel[f"z_{r}"].to_numpy() for r in roles}
    idx = panel.index

    trades = []
    i = 1
    while i < n - 1:
        # flag crossing at i-1 -> entry at i (next bar, no look-ahead)
        crossed = abs(s[i - 1]) >= threshold and abs(s[i - 2] if i >= 2 else 0) < threshold
        if not crossed:
            i += 1
            continue
        entry = i
        # position per leg: fade the extreme; size fixed or by |z|
        pos = {}
        for r in roles:
            base = -np.sign(zz[r][i - 1]) or 0.0
            pos[r] = base * (abs(zz[r][i - 1]) if size == "zscaled" else 1.0)

        # hold until exit
        j = entry
        reason = "max_hold"
        while j < n - 1 and (j - entry) < max_hold:
            j += 1
            if abs(s[j]) < exit_band:
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


def random_baseline(panel: pd.DataFrame, roles: list[str], n_trades: int, max_hold: int,
                    cost: float, size: str, rng) -> pd.DataFrame:
    """Same fade trade entered at random flat times (any score), for comparison."""
    n = len(panel)
    px = {r: panel[f"px_{r}"].to_numpy() for r in roles}
    zz = {r: panel[f"z_{r}"].to_numpy() for r in roles}
    idx = panel.index
    starts = rng.choice(np.arange(1, n - max_hold - 1), size=min(n_trades, n - max_hold - 2),
                        replace=False)
    rows = []
    for entry in starts:
        j = min(entry + max_hold, n - 1)
        net = 0.0
        for r in roles:
            base = -np.sign(zz[r][entry]) or 0.0
            p = base * (abs(zz[r][entry]) if size == "zscaled" else 1.0)
            net += p * (px[r][j] - px[r][entry]) - abs(p) * cost
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


def summarize(rule_key, trades: pd.DataFrame, baseline: pd.DataFrame, cost: float) -> None:
    print(f"\n=== {rule_key} backtest (EXPLORATORY; fade-the-flag, costs included) ===")
    if trades.empty:
        print("  no trades generated.")
        return
    net = trades["net"].to_numpy()
    print(f"  trades: {len(trades)}   avg hold: {trades['hold'].mean():.0f}h   "
          f"converged-exit: {(trades['reason']=='converged').mean()*100:.0f}%")
    print(f"  gross PnL: {trades['gross'].sum():+.3f}   total cost: {trades['cost'].sum():.3f}   "
          f"NET PnL: {trades['net'].sum():+.3f}  ($/contract-leg units)")
    print(f"  mean net/trade: {net.mean():+.4f}   win rate: {(net>0).mean()*100:.0f}%   "
          f"t-stat: {_tstat(net):+.2f}")
    if not baseline.empty:
        bn = baseline["net"].to_numpy()
        print(f"  random-entry baseline mean net/trade: {bn.mean():+.4f}  "
              f"(strategy edge: {net.mean()-bn.mean():+.4f})")

    print("  cost sensitivity (mean net/trade):")
    for c in COST_GRID:
        adj = trades["gross"].to_numpy() - (trades["cost"].to_numpy() / cost * c if cost > 0
                                            else _legs(trades) * c)
        flag = "  <- current" if abs(c - cost) < 1e-9 else ""
        print(f"    cost={c:.3f}: {adj.mean():+.4f}{flag}")
    # break-even cost (where mean net crosses 0), per round-trip-leg
    per_leg = _legs(trades)
    if per_leg.sum() > 0:
        be = trades["gross"].sum() / per_leg.sum()
        print(f"  break-even round-trip cost/leg: {be:+.4f} "
              f"(current {cost:.3f} -> {'profitable' if be > cost else 'unprofitable'})")
    print("  CAVEATS: cost is a flat proxy (no bid/ask in data); no impact/fills/settlement;")
    print("  mark-to-market exit; small N -> sanity check, not a P&L promise.")


def _legs(trades: pd.DataFrame) -> np.ndarray:
    """Per-trade sum of |position| across legs (cost scales with this)."""
    cols = [c for c in trades.columns if c.startswith("pos_")]
    return trades[cols].abs().sum(axis=1).to_numpy()


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
                    help="entry |score| (default from mappings)")
    ap.add_argument("--exit-band", type=float, default=None,
                    help="exit when |score| below this (default: threshold/2)")
    ap.add_argument("--max-hold", type=int, default=DEFAULT_HORIZON_EXIT)
    ap.add_argument("--cost", type=float, default=0.02, help="round-trip cost per leg ($)")
    ap.add_argument("--size", choices=["fixed", "zscaled"], default="fixed")
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    zip_path = args.zip or sig.find_latest_zip()
    cfg = sig.load_mappings()
    kind = args.signal or cfg["rules"].get(args.rule, {}).get("signal", "median")
    threshold = args.threshold
    if threshold is None:
        threshold = float(cfg["rules"][args.rule]["logic"]["flag_when"].split(">")[1])
    exit_band = args.exit_band if args.exit_band is not None else threshold / 2

    print(f"Loading bundle: {zip_path.name}  (rule={args.rule}, signal={kind}, "
          f"z_window={args.z_window}h, entry>{threshold}, exit<{exit_band}, "
          f"max_hold={args.max_hold}h, cost={args.cost}, size={args.size})")
    panel, roles = build_panel(args.rule, zip_path, args.z_window, kind)
    trades = simulate(panel, roles, threshold, exit_band, args.max_hold, args.cost, args.size)
    baseline = random_baseline(panel, roles, max(len(trades) * 5, 100),
                               args.max_hold, args.cost, args.size, rng)

    OUT_DIR.mkdir(exist_ok=True)
    if not trades.empty:
        trades.to_csv(OUT_DIR / f"{args.rule}_trades.csv", index=False)
    summarize(args.rule, trades, baseline, args.cost)
    equity_plot(args.rule, trades)
    if not trades.empty:
        print(f"\n  per-trade CSV -> {OUT_DIR / (args.rule + '_trades.csv')}")


if __name__ == "__main__":
    main()
