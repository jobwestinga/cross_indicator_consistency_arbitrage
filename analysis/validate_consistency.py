"""Validate whether a consistency-flag predicts re-alignment (mean reversion).

EXPLORATORY / TESTING. This answers the gate question for the whole strategy:
when a rule flags an inconsistency, do the markets subsequently CONVERGE back
toward consistency more than baseline? If not, the thesis is dead. If yes, it
sizes the opportunity and justifies a real backtest (step 3).

What this IS:  a necessary-condition test (does the inconsistency revert?) plus
               opportunity sizing (how fast / how reliably), with a baseline to
               control for mechanical mean reversion.
What this is NOT: a proof of profit. No transaction costs, slippage, execution,
               or position sizing are modeled. That is a later backtest.

Key validity choices:
  - the score uses a CAUSAL trailing z-score (signals.zscore_rolling); the
    whole-window z would define 'extreme' with future data -> circular.
  - events are defined by the rule's own flag metric: `value` rules (products)
    only treat score > T as inconsistent; `abs` rules treat both tails.
  - non-overlapping events (--min-gap), block-bootstrap CIs, and a
    magnitude-matched baseline (equally-extreme non-event bars).

Usage:
    python3 analysis/validate_consistency.py --rule phillips
    python3 analysis/validate_consistency.py --rule taylor --grid
Output (analysis/validation/<rule>_*):
    per-event CSV, forward-path plot, JSON summary, console verdict table,
    and with --grid a z-window x threshold robustness table.
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

OUT_DIR = sig.out_base() / "validation"
DEFAULT_HORIZONS = [1, 4, 24, 72]   # hours, on the 1h grid
N_BOOTSTRAP = 2000
RANDOM_BASELINE_K = 500
GRID_Z_WINDOWS = [24, 48, 72, 168]
GRID_THRESHOLDS = [0.75, 1.0, 1.5, 2.0]


# --------------------------------------------------------------------------- #
# events + forward outcomes
# --------------------------------------------------------------------------- #
def _hot(score: pd.Series, threshold: float, metric: str) -> pd.Series:
    return (score >= threshold) if metric == "value" else (score.abs() >= threshold)


def find_flag_entries(score: pd.Series, threshold: float, metric: str = "abs",
                      min_gap: int = 0) -> pd.DatetimeIndex:
    """Timestamps where the rule's flag metric crosses up through threshold.

    `min_gap` (bars) enforces a minimum spacing between accepted events so their
    forward windows do not overlap -> events are ~independent, which is required
    for the bootstrap CIs to be honest. Set min_gap >= max horizon.
    """
    hot = _hot(score, threshold, metric)
    crossings = hot & ~hot.shift(1, fill_value=False)
    idx = np.flatnonzero(crossings.values)
    if min_gap > 0 and len(idx):
        kept = [idx[0]]
        for i in idx[1:]:
            if i - kept[-1] >= min_gap:
                kept.append(i)
        idx = np.array(kept)
    return score.index[idx]


def forward_outcomes(score: pd.Series, entries, horizons: list[int]) -> pd.DataFrame:
    """For each entry, signed reversion at each horizon.

    rev_mag_H = -sign(s0) * (s_{t+H} - s0)   (>0 => moved toward zero)
    reverted_H = rev_mag_H > 0
    """
    s = score
    pos = {ts: i for i, ts in enumerate(s.index)}
    rows = []
    n = len(s)
    for ts in entries:
        i = pos[ts]
        s0 = s.iloc[i]
        rec = {"entry": ts, "s0": s0, "sign": np.sign(s0)}
        for H in horizons:
            j = i + H
            if j < n:
                ds = s.iloc[j] - s0
                rec[f"rev_{H}"] = -np.sign(s0) * ds
            else:
                rec[f"rev_{H}"] = np.nan
        rows.append(rec)
    return pd.DataFrame(rows)


def _block_bootstrap_ci(x: np.ndarray, stat, n_boot: int, rng, block: int = 3
                        ) -> tuple[float, float, float]:
    """Moving-block bootstrap CI: resamples contiguous blocks to preserve any
    residual autocorrelation between (time-ordered) events. With min-gap events
    this is conservative; with overlapping events it stops CIs being too tight."""
    x = x[~np.isnan(x)]
    n = len(x)
    if n == 0:
        return (np.nan, np.nan, np.nan)
    point = stat(x)
    block = max(1, min(block, n))
    n_blocks = int(np.ceil(n / block))
    starts_pool = np.arange(0, n - block + 1) if n - block + 1 > 0 else np.array([0])
    boot = np.empty(n_boot)
    for b in range(n_boot):
        starts = rng.choice(starts_pool, size=n_blocks, replace=True)
        sample = np.concatenate([x[s:s + block] for s in starts])[:n]
        boot[b] = stat(sample)
    return point, float(np.percentile(boot, 2.5)), float(np.percentile(boot, 97.5))


def random_baseline(score: pd.Series, horizons: list[int], k: int, rng,
                    after: pd.Timestamp | None = None) -> pd.DataFrame:
    """Forward reversion from k random entry times (unconditional, any score).
    Shows the series' general mean reversion regardless of being flagged.
    `after` restricts draws to entries at/after that timestamp (OOS runs)."""
    valid = np.arange(len(score) - max(horizons))
    if after is not None:
        valid = valid[score.index[valid] >= after]
    if len(valid) == 0:
        return pd.DataFrame()
    idx = rng.choice(valid, size=min(k, len(valid)), replace=False)
    return forward_outcomes(score, score.index[idx], horizons)


def magnitude_matched_baseline(score: pd.Series, horizons: list[int], threshold: float,
                               metric: str, entries, k: int, rng,
                               after: pd.Timestamp | None = None) -> pd.DataFrame:
    """Forward reversion from non-event bars whose flag metric is also >= threshold.

    This is the key control: flagged events are extreme by construction, so the
    real question is whether ENTRY TIMING adds anything beyond just "the score is
    large". If flagged events revert no more than other equally-extreme bars,
    the signal is only 'extreme magnitude reverts', nothing more.
    `after` restricts the pool to bars at/after that timestamp (OOS runs).
    """
    entry_pos = {ts: i for i, ts in enumerate(score.index)}
    event_idx = {entry_pos[ts] for ts in entries}
    hot = np.flatnonzero(_hot(score, threshold, metric).to_numpy())
    hot = hot[hot < len(score) - max(horizons)]
    if after is not None:
        hot = hot[score.index[hot] >= after]
    # exclude the event-entry bars themselves; keep every other equally-extreme
    # bar (mid-episode bars are fine -> they ARE the right magnitude control).
    pool = [i for i in hot if i not in event_idx]
    if not pool:
        return pd.DataFrame()
    idx = rng.choice(pool, size=min(k, len(pool)), replace=False)
    return forward_outcomes(score, score.index[idx], horizons)


# --------------------------------------------------------------------------- #
# report
# --------------------------------------------------------------------------- #
def summarize(rule_key, flagged: pd.DataFrame, rand_base: pd.DataFrame,
              matched_base: pd.DataFrame, horizons: list[int], rng,
              quiet: bool = False) -> dict:
    def say(*a):
        if not quiet:
            print(*a)

    say(f"\n=== {rule_key} validation (EXPLORATORY; causal trailing z) ===")
    say(f"  flag events (non-overlapping): {len(flagged)}   "
        f"random draws: {len(rand_base)}   magnitude-matched draws: {len(matched_base)}")
    if len(flagged) < 8:
        say("  INCONCLUSIVE: too few events (<8) for a meaningful test.")
    say(f"  {'H(hr)':>6} {'%revert':>9} {'  95% CI(block)':>18} {'mean_rev':>9} "
        f"{'match_mn':>9} {'rand%':>7} {'match%':>7} {'verdict':>9}")

    def stats(df, H):
        if df.empty:
            return float("nan"), float("nan")
        a = df[f"rev_{H}"].to_numpy()
        a = a[~np.isnan(a)]
        if not len(a):
            return float("nan"), float("nan")
        return float((a > 0).mean()), float(a.mean())

    verdicts = {}
    for H in horizons:
        fx = flagged[f"rev_{H}"].to_numpy() if len(flagged) else np.array([np.nan])
        pr, pr_lo, pr_hi = _block_bootstrap_ci(
            fx, lambda a: float((a > 0).mean()), N_BOOTSTRAP, rng)
        mr, mr_lo, mr_hi = _block_bootstrap_ci(fx, np.mean, N_BOOTSTRAP, rng)
        rand_pr, _ = stats(rand_base, H)
        match_pr, match_mean = stats(matched_base, H)

        # the meaningful test: flagged events must revert (CI excludes 50%) AND
        # their reversion MAGNITUDE must clearly exceed equally-extreme bars'
        # (the matched control). %revert alone ~matches the control, so the edge,
        # if any, is in magnitude. Require the flagged mean-rev CI lower bound to
        # beat the matched mean by a clear margin.
        reverts = (not np.isnan(pr_lo)) and (pr_lo > 0.5) and (mr_lo > 0)
        beats_mag = reverts and not np.isnan(match_mean) and mr_lo > match_mean * 1.10
        if beats_mag:
            v = "REVERT+"
        elif reverts:
            v = "revert"          # reverts, but ~like any extreme bar
        elif not np.isnan(pr) and pr > 0.5:
            v = "weak"
        else:
            v = "none"
        verdicts[H] = {"pct_revert": pr, "pct_ci": (pr_lo, pr_hi),
                       "mean_rev": mr, "mean_ci": (mr_lo, mr_hi),
                       "rand_pct": rand_pr, "matched_pct": match_pr,
                       "matched_mean": match_mean, "verdict": v}
        say(f"  {H:>6} {pr:>9.2f} [{pr_lo:>6.2f},{pr_hi:>6.2f}] {mr:>9.2f} "
            f"{match_mean:>9.2f} {rand_pr:>7.2f} {match_pr:>7.2f} {v:>9}")

    # overall verdict hinges on beating the magnitude-matched control
    strong = sum(1 for H in horizons if verdicts[H]["verdict"] == "REVERT+")
    if len(flagged) < 8:
        overall = "INCONCLUSIVE (too few events)"
    elif strong >= max(2, len(horizons) // 2):
        overall = "EDGE-SUGGESTIVE (reverts beyond magnitude-matched control)"
    elif any(not np.isnan(verdicts[H]["pct_revert"]) and verdicts[H]["pct_revert"] > 0.5
             for H in horizons):
        overall = "WEAK (some reversion, not convincingly beyond baseline)"
    else:
        overall = "NO EDGE (flags do not predict convergence)"
    say(f"\n  OVERALL: {overall}")
    say("  How to read: 'rand%' = reversion of ANY random bar (series is mean-reverting,")
    say("  so this is high). 'match%' = reversion of equally-EXTREME non-event bars. The")
    say("  real signal is flagged mean_rev beating match_mn -> entry timing adds info")
    say("  beyond 'the score is large'. REVERT+ = beats matched mean AND CI excludes 50%.")
    say("  CAVEATS: necessary-condition test only, no costs/execution -> NOT proof of profit.")
    return {"verdicts": verdicts, "overall": overall, "n_events": int(len(flagged))}


def power_estimate(n_events: int, window_days: float) -> dict:
    """Crude power roadmap: events accrue ~linearly with data; a two-sided
    binomial test of %revert=0.7 vs 0.5 needs ~40 events for 80% power, and
    a t-test at the observed effect sizes typically needs 30-60. Report the
    days of collection required to reach 40 independent events."""
    if n_events == 0 or window_days <= 0:
        return {"events_per_30d": 0.0, "days_to_40_events": None}
    rate = n_events / window_days
    return {"events_per_30d": round(rate * 30, 1),
            "days_to_40_events": int(np.ceil(max(40 - n_events, 0) / rate)) if rate else None}


def forward_path_plot(rule_key, score: pd.Series, entries, baseline_idx, max_h: int) -> None:
    try:
        import matplotlib.pyplot as plt
    except Exception:  # noqa: BLE001
        return

    def mean_abs_path(times):
        pos = {ts: i for i, ts in enumerate(score.index)}
        mat = []
        for ts in times:
            i = pos[ts]
            if i + max_h < len(score):
                seg = score.iloc[i:i + max_h + 1].abs().to_numpy()
                s0 = seg[0] if seg[0] != 0 else np.nan
                mat.append(seg / s0)  # normalize to entry magnitude
        return np.nanmean(np.array(mat), axis=0) if mat else None

    fp = mean_abs_path(entries)
    bp = mean_abs_path(score.index[baseline_idx])
    if fp is None:
        return
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(range(max_h + 1), fp, label="after flag", linewidth=2, color="crimson")
    if bp is not None:
        ax.plot(range(max_h + 1), bp, label="random baseline", linewidth=1.5,
                color="gray", linestyle="--")
    ax.axhline(1.0, color="black", linewidth=0.6)
    ax.set_title(f"{rule_key}: mean |score| after flag, normalized to entry (EXPLORATORY)")
    ax.set_xlabel("hours after entry")
    ax.set_ylabel("|score| / |score at entry|  (<1 = converged)")
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    OUT_DIR.mkdir(exist_ok=True)
    out = OUT_DIR / f"{rule_key}_forward_path.png"
    fig.savefig(out, dpi=140)
    plt.close(fig)
    print(f"  plot -> {out}")


# --------------------------------------------------------------------------- #
# main validation runs
# --------------------------------------------------------------------------- #
def validate_once(score: pd.Series, metric: str, threshold: float,
                  horizons: list[int], min_gap: int, rng,
                  rule_key: str = "", quiet: bool = False,
                  ) -> tuple[dict, pd.DataFrame, pd.DatetimeIndex]:
    entries = find_flag_entries(score, threshold, metric, min_gap=min_gap)
    flagged = forward_outcomes(score, entries, horizons)
    rand_base = random_baseline(score, horizons, RANDOM_BASELINE_K, rng)
    matched = magnitude_matched_baseline(score, horizons, threshold, metric,
                                         entries, RANDOM_BASELINE_K, rng)
    result = summarize(rule_key, flagged, rand_base, matched, horizons, rng, quiet=quiet)
    return result, flagged, entries


def permutation_test(panel: pd.DataFrame, rule: dict, roles: list[str],
                     z_window: int, threshold: float, metric: str,
                     horizons: list[int], min_gap: int, n_perm: int, rng) -> dict:
    """Circular-shift null [D2]: every role but the first is rotated by a
    random offset, preserving each leg's own dynamics but destroying the
    cross-leg alignment the rule claims to exploit. The statistic is the
    flagged-event mean reversion at the max horizon. Mechanical reversion of
    extreme z-products survives shifting, so it is priced into the null —
    the observed value must beat THAT, which is exactly the question."""
    H = max(horizons)

    def stat_for(series_map: dict[str, pd.Series]) -> tuple[float, int]:
        z = {r: sig.zscore_rolling(series_map[r], z_window) for r in roles}
        score = rules.score_from_logic(rule, z).dropna()
        if len(score) <= H + 10:
            return np.nan, 0
        entries = find_flag_entries(score, threshold, metric, min_gap=min_gap)
        fo = forward_outcomes(score, entries, [H])
        a = fo[f"rev_{H}"].dropna().to_numpy() if len(fo) else np.array([])
        return (float(a.mean()) if len(a) else np.nan), int(len(a))

    obs, n_obs = stat_for({r: panel[r] for r in roles})
    null = []
    n = len(panel)
    for _ in range(n_perm):
        shifted = {roles[0]: panel[roles[0]]}
        for r in roles[1:]:
            k = int(rng.integers(1, n - 1))
            shifted[r] = pd.Series(np.roll(panel[r].to_numpy(), k), index=panel.index)
        s, _cnt = stat_for(shifted)
        if not np.isnan(s):
            null.append(s)
    null_arr = np.array(null)
    p = (float((null_arr >= obs).mean())
         if len(null_arr) and not np.isnan(obs) else np.nan)
    return {"observed_mean_rev": obs, "n_events": n_obs,
            "n_null_draws": int(len(null_arr)),
            "null_mean": float(null_arr.mean()) if len(null_arr) else np.nan,
            "null_p95": float(np.percentile(null_arr, 95)) if len(null_arr) else np.nan,
            "p_value": p, "horizon": H}


def robustness_grid(panel: pd.DataFrame, rule: dict, horizons: list[int],
                    min_gap: int, seed: int) -> pd.DataFrame:
    """Sweep z-window x threshold; the verdict must be stable in a
    neighborhood of the chosen parameters, not one lucky cell [D1].
    Reuses the raw role series in `panel`; only z/score are recomputed."""
    roles = [c for c in panel.columns if not c.startswith(("z_", "px_")) and c != "score"]
    metric = rules.flag_metric(rule)
    rows = []
    for zw in GRID_Z_WINDOWS:
        z = {r: sig.zscore_rolling(panel[r], zw) for r in roles}
        score = rules.score_from_logic(rule, z).dropna()
        if len(score) <= max(horizons) + 10:
            continue
        for thr in GRID_THRESHOLDS:
            rng = np.random.default_rng(seed)  # same seed per cell -> comparable
            res, _, _ = validate_once(score, metric, thr, horizons, min_gap,
                                      rng, quiet=True)
            H = max(horizons)
            v = res["verdicts"][H]
            rows.append({"z_window": zw, "threshold": thr, "n_events": res["n_events"],
                         f"pct_revert_{H}": round(v["pct_revert"], 3),
                         f"mean_rev_{H}": round(v["mean_rev"], 3),
                         f"matched_mean_{H}": round(v["matched_mean"], 3)
                         if not np.isnan(v["matched_mean"]) else np.nan,
                         "overall": res["overall"].split(" ")[0]})
    return pd.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="Validate consistency flags (exploratory).")
    ap.add_argument("--rule", default="phillips")
    ap.add_argument("--zip", type=Path, default=None)
    ap.add_argument("--signal", choices=["median", "prob"], default=None,
                    help="implied signal override (default: rule's 'signal' key, else median)")
    ap.add_argument("--z-window", type=int, default=48, help="trailing z-score window (hours)")
    ap.add_argument("--threshold", type=float, default=None,
                    help="flag threshold (default: from mappings logic.flag)")
    ap.add_argument("--horizons", type=int, nargs="+", default=DEFAULT_HORIZONS)
    ap.add_argument("--min-gap", type=int, default=None,
                    help="min bars between events (default: max horizon -> non-overlapping)")
    ap.add_argument("--grid", action="store_true",
                    help="also sweep z-window x threshold (robustness table)")
    ap.add_argument("--permute", type=int, default=0, metavar="N",
                    help="circular-shift permutation test with N draws (D2)")
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    zip_path = args.zip or sig.find_latest_zip()
    min_gap = args.min_gap if args.min_gap is not None else max(args.horizons)

    try:
        panel, roles, rule = rules.build_rule_panel(
            args.rule, zip_path, args.z_window, kind=args.signal)
    except (rules.RuleError, ValueError, KeyError) as exc:
        raise SystemExit(str(exc)) from exc
    metric = rules.flag_metric(rule)
    threshold = args.threshold if args.threshold is not None else rules.flag_threshold(rule)
    score = panel["score"]

    print(f"Loading bundle: {zip_path.name}  (rule={args.rule}, metric={metric}, "
          f"z_window={args.z_window}h, threshold={threshold}, min_gap={min_gap}h)")
    if len(score) < max(args.horizons) + 10:
        raise SystemExit("not enough causal score points for the requested horizons")

    result, flagged, entries = validate_once(
        score, metric, threshold, args.horizons, min_gap, rng, rule_key=args.rule)

    OUT_DIR.mkdir(exist_ok=True)
    flagged.to_csv(OUT_DIR / f"{args.rule}_events.csv", index=False)

    window_days = (score.index.max() - score.index.min()).total_seconds() / 86400
    power = power_estimate(result["n_events"], window_days)
    print(f"  power roadmap: ~{power['events_per_30d']} events/30d at this rate; "
          f"days of extra data to reach 40 events: {power['days_to_40_events']}")

    grid_df = pd.DataFrame()
    if args.grid:
        print("\n  robustness grid (z_window x threshold, verdict at max horizon):")
        grid_df = robustness_grid(panel, rule, args.horizons, min_gap, args.seed)
        print(grid_df.to_string(index=False))
        grid_df.to_csv(OUT_DIR / f"{args.rule}_grid.csv", index=False)

    perm = {}
    if args.permute:
        perm = permutation_test(panel, rule, roles, args.z_window, threshold,
                                metric, args.horizons, min_gap, args.permute,
                                np.random.default_rng(args.seed + 1))
        print(f"\n  permutation test (circular-shift null, {args.permute} draws, "
              f"H={perm['horizon']}h):")
        print(f"    observed mean_rev {perm['observed_mean_rev']:.3f} "
              f"(n={perm['n_events']})  null mean {perm['null_mean']:.3f}  "
              f"null p95 {perm['null_p95']:.3f}  p={perm['p_value']:.4f}")

    summary = {
        "rule": args.rule,
        "bundle": zip_path.name,
        "params": {"z_window": args.z_window, "threshold": threshold,
                   "metric": metric, "horizons": args.horizons, "min_gap": min_gap,
                   "seed": args.seed},
        "window_days": round(window_days, 1),
        "n_events": result["n_events"],
        "overall": result["overall"],
        "power": power,
        "verdicts": {str(h): {k: (list(v) if isinstance(v, tuple) else v)
                              for k, v in d.items()}
                     for h, d in result["verdicts"].items()},
        "grid": grid_df.to_dict("records") if not grid_df.empty else [],
        "permutation": perm,
    }
    json_path = OUT_DIR / f"{args.rule}_validation.json"
    json_path.write_text(json.dumps(summary, indent=2, default=float))

    base_idx = rng.choice(np.arange(len(score) - max(args.horizons)),
                          size=min(RANDOM_BASELINE_K, len(score) - max(args.horizons)),
                          replace=False)
    forward_path_plot(args.rule, score, entries, base_idx, max(args.horizons))

    print(f"\n  per-event CSV -> {OUT_DIR / (args.rule + '_events.csv')}")
    print(f"  JSON summary  -> {json_path}")
    print(f"  verdict: {result['overall']}")


if __name__ == "__main__":
    main()
