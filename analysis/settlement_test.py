"""Hold-to-settlement test of the consistency trade (no mark-to-market exit).

EXPLORATORY / TESTING. The mark-to-market backtest (backtest.py) showed that
z-space reversion is not captureable: the tradeable series is stitched across
reference-contract switches, and forcing exits before every switch (A11) kills
the PnL. This script tests the alternative trade construction the README TODO
proposes: on a flag, take the front reference contract of each leg and HOLD IT
TO SETTLEMENT. No exits, no rolls, no stitching — the position is a real
contract whose PnL is settlement value minus entry price.

Settlement determination is model-free: a resolved YES contract pins to ~0 or
~1 by expiration in `contract_history` itself. The last print at/near expiry
decides the outcome (must be <= pin_band or >= 1-pin_band, else the contract
is counted "unpinned" and excluded). Contracts expiring after the bundle ends
are "unresolved" and excluded. No FRED mapping is needed, so no vintage
issues (A7) can leak in.

Costs: entry crossing only (`--cost` is per leg, ONE-WAY). Settlement pays
exactly 0/1 with no exit spread — that is the main structural advantage of
this construction.

What it is NOT: an executable strategy proof. Entry fills are still bar
averages (87% of bars are carried marks) and capital is locked until expiry.

Usage:
    python3 analysis/settlement_test.py                    # all implemented rules
    python3 analysis/settlement_test.py --rules taylor payrolls_labor
Output (analysis/settlement/): per-rule trades CSV + JSON + summary.json.
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

OUT_DIR = sig.out_base() / "settlement"
DEFAULT_PIN_BAND = 0.05      # last print must be this close to 0/1 to count as settled
DEFAULT_GRACE = "1D"         # accept prints up to this long after expiration
DEFAULT_MAX_STALE = "5D"     # last print must be within this of expiration


def settlement_value(history: pd.DataFrame, conid: int,
                     pin_band: float = DEFAULT_PIN_BAND,
                     grace: str = DEFAULT_GRACE,
                     max_stale: str = DEFAULT_MAX_STALE) -> tuple[float | None, str]:
    """Outcome (0.0/1.0) of one contract from its own price history.

    Returns (value, status). value is None unless status == "settled".
    Statuses: settled | unresolved (expiry beyond data end) | unpinned (last
    print not near 0/1) | stale (no print near expiry) | no_history.
    """
    rows = history.loc[history.conid == conid, ["ts_utc", "avg", "expiration"]]
    if rows.empty:
        return None, "no_history"
    exp = rows["expiration"].iloc[-1]
    if pd.isna(exp):
        return None, "no_history"
    data_end = history["ts_utc"].max()
    if exp > data_end - pd.Timedelta(grace):
        return None, "unresolved"
    rows = rows[rows["ts_utc"] <= exp + pd.Timedelta(grace)].sort_values("ts_utc")
    if rows.empty:
        return None, "stale"
    last = rows.iloc[-1]
    if exp - last["ts_utc"] > pd.Timedelta(max_stale):
        return None, "stale"
    if last["avg"] <= pin_band:
        return 0.0, "settled"
    if last["avg"] >= 1.0 - pin_band:
        return 1.0, "settled"
    return None, "unpinned"


class FredResolver:
    """Resolve realized outcomes from the FRED sqlite per the indicator's
    `settlement:` spec in mappings.yaml (see the spec comment there).

    Values are the LATEST FRED vintage; for series with material initial-print
    revisions (payrolls) the spec carries a `caution` note and the pin
    cross-check below is the guard: where both FRED and a pinned price decide
    the same contract, disagreement is counted and reported.
    """

    def __init__(self) -> None:
        self._cache: dict[str, pd.Series] = {}

    def _series(self, series_id: str) -> pd.Series | None:
        if series_id not in self._cache:
            try:
                self._cache[series_id] = sig.load_fred_series(series_id)
            except Exception:  # noqa: BLE001 - sqlite missing/series absent -> no FRED
                self._cache[series_id] = pd.Series(dtype=float)
        s = self._cache[series_id]
        return s if len(s) else None

    def realized(self, spec: dict, expiration: pd.Timestamp) -> tuple[float | None, str]:
        """(realized value in strike units, status). status: ok | pending | error."""
        s = self._series(spec["series"])
        if s is None:
            return None, "error"
        kind = spec["kind"]
        scale = float(spec.get("scale", 1))
        if kind in ("level", "yoy_pct", "mom_diff"):
            lag = int(spec.get("ref_lag_months", 1))
            ref = (expiration.tz_localize(None) - pd.DateOffset(months=lag)).to_period("M")
            if kind == "yoy_pct":
                vals = (s.pct_change(12) * 100).round(1)
            elif kind == "mom_diff":
                vals = s.diff()
            else:
                vals = s
            hit = vals[vals.index.tz_localize(None).to_period("M") == ref].dropna()
            if hit.empty:
                return None, "pending"
            return float(hit.iloc[-1]) * scale, "ok"
        if kind == "weekly_level":
            # ICSA obs_date = week-ending Saturday, released the following
            # Thursday; a Thursday expiry settles on the release that morning.
            hit = s[s.index <= expiration - pd.Timedelta(days=4)]
            return (float(hit.iloc[-1]) * scale, "ok") if len(hit) else (None, "pending")
        if kind == "daily_level":
            hit = s[s.index <= expiration]
            return (float(hit.iloc[-1]) * scale, "ok") if len(hit) else (None, "pending")
        if kind == "fedfunds_mid":
            # rate change is effective the day after the meeting; snap the
            # first post-meeting daily prints to the 25bp midpoint grid.
            hit = s[(s.index > expiration) & (s.index <= expiration + pd.Timedelta(days=6))]
            if hit.empty:
                return None, "pending"
            r = float(hit.median())
            return round((r - 0.125) / 0.25) * 0.25 + 0.125, "ok"
        return None, "error"

    def outcome(self, spec: dict | None, strike: float,
                expiration: pd.Timestamp) -> tuple[float | None, str]:
        """YES-contract outcome per the venue convention: 1 if realized > strike."""
        if spec is None or pd.isna(strike) or pd.isna(expiration):
            return None, "unmapped"
        value, status = self.realized(spec, expiration)
        if status != "ok":
            return None, f"fred_{status}"
        return (1.0 if value > strike else 0.0), "settled_fred"


def settlement_with_ladder(history: pd.DataFrame, conid: int,
                           pin_band: float = DEFAULT_PIN_BAND) -> tuple[float | None, str]:
    """settlement_value, falling back to survival-ladder inference.

    Many contracts stop printing mid-range days before resolution (settlement
    happens off-market at the data release). Their outcome is still often
    determined model-free by pinned SIBLINGS in the same (market, expiration)
    YES ladder: a higher strike settled 1 implies X > K' > K -> this one is 1;
    a lower strike settled 0 implies X <= K'' < K -> this one is 0.
    """
    value, status = settlement_value(history, conid, pin_band=pin_band)
    if status != "unpinned":
        return value, status
    own = history.loc[history.conid == conid].iloc[-1]
    if pd.isna(own.get("strike")):
        return None, status
    sibs = history[(history.underlying_conid == own["underlying_conid"])
                   & (history.expiration == own["expiration"])
                   & (history.side == "Y")
                   & (history.strike.notna())
                   & (history.conid != conid)]
    for sib_conid in sibs["conid"].unique():
        sib_val, sib_status = settlement_value(history, int(sib_conid), pin_band=pin_band)
        if sib_status != "settled":
            continue
        sib_strike = float(sibs.loc[sibs.conid == sib_conid, "strike"].iloc[-1])
        if sib_val == 1.0 and sib_strike >= own["strike"]:
            return 1.0, "settled_ladder"
        if sib_val == 0.0 and sib_strike <= own["strike"]:
            return 0.0, "settled_ladder"
    return None, "unpinned"


def resolve_leg(history: pd.DataFrame, conid: int, spec: dict | None,
                resolver: FredResolver, pin_band: float) -> tuple[float | None, str, int]:
    """One contract's settlement: FRED mapping first, price-pin/ladder as
    fallback. Returns (value, status, fred_pin_disagree 0/1) — the third field
    counts cases where FRED and an independent price pin BOTH resolve the
    contract and disagree (mapping/revision guard, reported in the summary)."""
    rows = history.loc[history.conid == conid]
    strike = rows["strike"].iloc[-1] if len(rows) else np.nan
    exp = rows["expiration"].iloc[-1] if len(rows) else pd.NaT
    fred_val, fred_status = resolver.outcome(spec, strike, exp)
    pin_val, pin_status = settlement_with_ladder(history, conid, pin_band=pin_band)
    # PIN OUTRANKS FRED: a pinned venue price is direct settlement evidence,
    # while FRED holds revised values on a possibly different basis (SA vs
    # NSA, later vintages). FRED only fills contracts the venue left mid-range.
    if pin_val is not None:
        disagree = int(fred_status == "settled_fred" and fred_val != pin_val)
        return pin_val, pin_status, disagree
    if fred_status == "settled_fred":
        return fred_val, fred_status, 0
    # neither settled: prefer the more informative reason
    status = pin_status if fred_status in ("unmapped", "fred_error") else fred_status
    return None, status, 0


def build_trades(panel: pd.DataFrame, roles: list[str], rule: dict,
                 history: pd.DataFrame, entries: pd.DatetimeIndex,
                 cost: float, pin_band: float, resolver: FredResolver) -> pd.DataFrame:
    """One hold-to-settlement trade per flag entry (entry = next bar, A4-style
    no look-ahead). Each leg buys/sells its reference contract at the entry
    bar's price and holds to that contract's settlement."""
    pos_map = {ts: i for i, ts in enumerate(panel.index)}
    s = panel["score"].to_numpy()
    zz = {r: panel[f"z_{r}"].to_numpy() for r in roles}
    px = {r: panel[f"px_{r}"].to_numpy() for r in roles}
    ref = {r: panel[f"pxref_{r}"].to_numpy() for r in roles}
    n = len(panel)
    rows = []
    for ts in entries:
        i = pos_map[ts] + 1                      # enter the bar AFTER the flag
        if i >= n:
            continue
        pos = bt.leg_positions(rule, roles, {r: zz[r][i - 1] for r in roles},
                               s[i - 1], size="fixed")
        rec = {"entry": panel.index[i], "score_in": s[i - 1]}
        net = 0.0
        statuses = []
        disagreements = 0
        for r in roles:
            conid = int(ref[r][i])
            entry_px = px[r][i]
            spec = rule["indicators"][r].get("settlement")
            settle, status, disagree = resolve_leg(history, conid, spec,
                                                   resolver, pin_band)
            statuses.append(status)
            disagreements += disagree
            rec[f"conid_{r}"] = conid
            rec[f"entry_px_{r}"] = entry_px
            rec[f"settle_{r}"] = settle
            rec[f"status_{r}"] = status
            if settle is not None and not np.isnan(entry_px):
                net += pos[r] * (settle - entry_px) - abs(pos[r]) * cost
        rec["all_settled"] = all(st.startswith("settled") for st in statuses)
        rec["fred_pin_disagree"] = disagreements
        rec["net"] = net if rec["all_settled"] else np.nan
        rows.append(rec)
    return pd.DataFrame(rows)


def random_baseline(panel: pd.DataFrame, roles: list[str], rule: dict,
                    history: pd.DataFrame, k: int, cost: float,
                    pin_band: float, resolver: FredResolver, rng) -> pd.DataFrame:
    """Same construction from random entry bars: is flag timing worth anything
    beyond generic 'buy the front contract and hold'?"""
    if len(panel) < 3:
        return pd.DataFrame()
    idx = rng.choice(np.arange(1, len(panel) - 1), size=min(k, len(panel) - 2),
                     replace=False)
    entries = panel.index[idx - 1]               # build_trades enters at +1
    return build_trades(panel, roles, rule, history, pd.DatetimeIndex(entries),
                        cost, pin_band, resolver)


def run_rule(rule_key: str, zip_path: Path, z_window: int, cost: float,
             pin_band: float, seed: int, history=None, markets=None) -> dict:
    rng = np.random.default_rng(seed)
    panel, roles, rule = rules.build_rule_panel(
        rule_key, zip_path, z_window, with_prices=True,
        history=history, markets=markets)
    metric = rules.flag_metric(rule)
    threshold = rules.flag_threshold(rule)
    entries = vc.find_flag_entries(panel["score"], threshold, metric, min_gap=72)

    raw_history = history if history is not None else sig.load_history(zip_path)
    resolver = FredResolver()
    trades = build_trades(panel, roles, rule, raw_history, entries, cost,
                          pin_band, resolver)
    base = random_baseline(panel, roles, rule, raw_history,
                           max(len(entries) * 5, 100), cost, pin_band, resolver, rng)

    settled = trades[trades["all_settled"]] if not trades.empty else trades
    base_settled = base[base["all_settled"]] if not base.empty else base
    disagreements = int(trades["fred_pin_disagree"].sum()) if not trades.empty else 0
    out = {
        "rule": rule_key,
        "params": {"z_window": z_window, "threshold": threshold, "metric": metric,
                   "cost_one_way": cost, "pin_band": pin_band, "min_gap": 72,
                   "seed": seed},
        "n_flags": int(len(trades)),
        "n_settled": int(len(settled)),
        "n_unresolved": int((~trades["all_settled"]).sum()) if not trades.empty else 0,
        "fred_pin_disagreements": disagreements,
    }
    print(f"\n=== {rule_key} hold-to-settlement (EXPLORATORY) ===")
    print(f"  flags: {out['n_flags']}   fully settled: {out['n_settled']}   "
          f"excluded (unresolved/unpinned/stale legs): {out['n_unresolved']}")
    if disagreements:
        print(f"  WARNING: FRED settlement disagrees with a pinned price on "
              f"{disagreements} leg(s) - check the mapping/revisions before trusting.")
    if len(settled):
        net = settled["net"].to_numpy()
        out.update({
            "mean_net_per_trade": float(net.mean()),
            "win_rate": float((net > 0).mean()),
            "t_stat": bt._tstat(net),
            "net_pnl": float(net.sum()),
        })
        print(f"  mean net/trade: {net.mean():+.4f}   win rate: "
              f"{(net > 0).mean() * 100:.0f}%   t-stat: {bt._tstat(net):+.2f}   "
              f"(one-way cost {cost}/leg)")
    if len(base_settled):
        bmean = float(base_settled["net"].to_numpy().mean())
        out["baseline_mean_net"] = bmean
        print(f"  random-entry baseline mean net/trade: {bmean:+.4f}  "
              f"(n={len(base_settled)}; edge: "
              f"{out.get('mean_net_per_trade', float('nan')) - bmean:+.4f})")
    if not len(settled):
        print("  no fully-settled flagged trades in this bundle yet "
              "(front contracts expire after the data ends) - rerun on later exports.")
    print("  CAVEATS: entry at bar-average marks; capital locked to expiry; "
          "settlement read from pinned prices.")

    OUT_DIR.mkdir(exist_ok=True)
    if not trades.empty:
        trades.to_csv(OUT_DIR / f"{rule_key}_settlement_trades.csv", index=False)
    (OUT_DIR / f"{rule_key}_settlement.json").write_text(
        json.dumps(out, indent=2, default=float))
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Hold-to-settlement consistency trade test.")
    ap.add_argument("--zip", type=Path, default=None)
    ap.add_argument("--rules", nargs="+", default=None)
    ap.add_argument("--z-window", type=int, default=48)
    ap.add_argument("--cost", type=float, default=0.01,
                    help="ONE-WAY cost per leg at entry (settlement has no exit cost)")
    ap.add_argument("--pin-band", type=float, default=DEFAULT_PIN_BAND)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    zip_path = args.zip or sig.find_latest_zip()
    cfg = sig.load_mappings()
    todo = args.rules or rules.implemented_rules(cfg)
    print(f"Loading bundle: {zip_path.name}  (hold-to-settlement, "
          f"one-way cost {args.cost}/leg, pin band {args.pin_band})")
    markets = sig.load_markets(zip_path)
    history = sig.load_history(zip_path)

    results = []
    for rule_key in todo:
        try:
            results.append(run_rule(rule_key, zip_path, args.z_window, args.cost,
                                    args.pin_band, args.seed,
                                    history=history, markets=markets))
        except (rules.RuleError, ValueError, KeyError) as exc:
            print(f"\n=== {rule_key} hold-to-settlement ===\n  FAILED: {exc}")
            results.append({"rule": rule_key, "failed": str(exc)})

    OUT_DIR.mkdir(exist_ok=True)
    (OUT_DIR / "summary.json").write_text(json.dumps(
        {"bundle": zip_path.name, "results": results}, indent=2, default=float))
    print(f"\nwrote {OUT_DIR}/<rule>_settlement.json + summary.json")


if __name__ == "__main__":
    main()
