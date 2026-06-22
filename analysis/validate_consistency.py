"""Validate whether a consistency-flag predicts re-alignment (mean reversion).

EXPLORATORY / TESTING. This answers the gate question for the whole strategy:
when a rule flags an inconsistency, do the two markets subsequently CONVERGE
back toward consistency more than baseline? If not, the thesis is dead. If yes,
it sizes the opportunity and justifies a real backtest (step 3).

What this IS:  a necessary-condition test (does the inconsistency revert?) plus
               opportunity sizing (how fast / how reliably), with a baseline to
               control for mechanical mean reversion.
What this is NOT: a proof of profit. No transaction costs, slippage, execution,
               or position sizing are modeled. That is a later backtest.

Key validity choice: the inconsistency score is built from a CAUSAL trailing
z-score (signals.zscore_rolling), NOT the whole-window z-score. The whole-window
version defines 'extreme' using future data, which mechanically guarantees
reversion and would make this validation circular.

Usage:
    python3 analysis/validate_consistency.py --rule phillips
    python3 analysis/validate_consistency.py --rule sahm --z-window 72 \
        --horizons 1 4 24 72 --threshold 1.0
Output (analysis/validation/<rule>_*):
    per-event CSV, forward-path plot, console verdict table.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

import signals as sig
from run_consistency import _score  # reuse the per-rule scorer

OUT_DIR = Path(__file__).resolve().parent / "validation"
DEFAULT_HORIZONS = [1, 4, 24, 72]   # hours, on the 1h grid
N_BOOTSTRAP = 2000
RANDOM_BASELINE_K = 500


# --------------------------------------------------------------------------- #
# build a CAUSAL inconsistency score
# --------------------------------------------------------------------------- #
def build_causal_score(rule_key: str, zip_path: Path, z_window: int,
                       kind: str = "median") -> tuple[pd.Series, pd.DataFrame]:
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
    series = {
        role: sig.implied_series(history, markets, spec["market_name"],
                                 kind=kind, band=band, freq=freq)
        for role, spec in rule["indicators"].items()
    }
    panel = sig.align(*series.values(), freq=freq)
    panel.columns = list(series.keys())
    z = {role: sig.zscore_rolling(panel[role], z_window) for role in series}
    score = _score(rule_key, z).rename("score")
    panel = panel.assign(**{f"z_{r}": z[r] for r in series}, score=score)
    return score.dropna(), panel


# --------------------------------------------------------------------------- #
# events + forward outcomes
# --------------------------------------------------------------------------- #
def find_flag_entries(score: pd.Series, threshold: float, min_gap: int = 0) -> pd.DatetimeIndex:
    """Timestamps where |score| crosses up through threshold (episode starts).

    `min_gap` (bars) enforces a minimum spacing between accepted events so their
    forward windows do not overlap -> events are ~independent, which is required
    for the bootstrap CIs to be honest. Set min_gap >= max horizon.
    """
    hot = score.abs() >= threshold
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


def random_baseline(score: pd.Series, horizons: list[int], k: int, rng) -> pd.DataFrame:
    """Forward reversion from k random entry times (unconditional, any |score|).
    Shows the series' general mean reversion regardless of being flagged."""
    valid = np.arange(len(score) - min(horizons))
    idx = rng.choice(valid, size=min(k, len(valid)), replace=False)
    return forward_outcomes(score, score.index[idx], horizons)


def magnitude_matched_baseline(score: pd.Series, horizons: list[int], threshold: float,
                               entries, k: int, rng) -> pd.DataFrame:
    """Forward reversion from non-event bars whose |score| is also >= threshold.

    This is the key control: flagged events are extreme by construction, so the
    real question is whether ENTRY TIMING adds anything beyond just "the score is
    large". If flagged events revert no more than other equally-extreme bars,
    the signal is only 'extreme magnitude reverts', nothing more.
    """
    entry_pos = {ts: i for i, ts in enumerate(score.index)}
    event_idx = {entry_pos[ts] for ts in entries}
    hot = np.flatnonzero(score.abs().to_numpy() >= threshold)
    hot = hot[hot < len(score) - max(horizons)]
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
              matched_base: pd.DataFrame, horizons: list[int], rng) -> dict:
    print(f"\n=== {rule_key} validation (EXPLORATORY; causal trailing z) ===")
    print(f"  flag events (non-overlapping): {len(flagged)}   "
          f"random draws: {len(rand_base)}   magnitude-matched draws: {len(matched_base)}")
    if len(flagged) < 8:
        print("  INCONCLUSIVE: too few events (<8) for a meaningful test.")
    print(f"  {'H(hr)':>6} {'%revert':>9} {'  95% CI(block)':>18} {'mean_rev':>9} "
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
        fx = flagged[f"rev_{H}"].to_numpy()
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
        reverts = (pr_lo > 0.5) and (mr_lo > 0)
        beats_mag = not np.isnan(match_mean) and mr_lo > match_mean * 1.10
        if reverts and beats_mag:
            v = "REVERT+"
        elif reverts:
            v = "revert"          # reverts, but ~like any extreme bar
        elif pr > 0.5:
            v = "weak"
        else:
            v = "none"
        verdicts[H] = {"pct_revert": pr, "pct_ci": (pr_lo, pr_hi),
                       "mean_rev": mr, "mean_ci": (mr_lo, mr_hi),
                       "rand_pct": rand_pr, "matched_pct": match_pr,
                       "matched_mean": match_mean, "verdict": v}
        print(f"  {H:>6} {pr:>9.2f} [{pr_lo:>6.2f},{pr_hi:>6.2f}] {mr:>9.2f} "
              f"{match_mean:>9.2f} {rand_pr:>7.2f} {match_pr:>7.2f} {v:>9}")

    # overall verdict hinges on beating the magnitude-matched control
    strong = sum(1 for H in horizons if verdicts[H]["verdict"] == "REVERT+")
    reverts_any = sum(1 for H in horizons if verdicts[H]["verdict"] in ("REVERT+", "revert"))
    if len(flagged) < 8:
        overall = "INCONCLUSIVE (too few events)"
    elif strong >= max(2, len(horizons) // 2):
        overall = "EDGE-SUGGESTIVE (reverts beyond magnitude-matched control)"
    elif any(verdicts[H]["pct_revert"] > 0.5 for H in horizons):
        overall = "WEAK (some reversion, not convincingly beyond baseline)"
    else:
        overall = "NO EDGE (flags do not predict convergence)"
    _ = reverts_any  # (kept for readability of the verdict logic above)
    print(f"\n  OVERALL: {overall}")
    print("  How to read: 'rand%' = reversion of ANY random bar (series is mean-reverting,")
    print("  so this is high). 'match%' = reversion of equally-EXTREME non-event bars. The")
    print("  real signal is flagged %revert beating match% -> entry timing adds info beyond")
    print("  'the score is large'. REVERT+ = beats match% AND CI excludes 50%.")
    print("  CAVEATS: necessary-condition test only, no costs/execution -> NOT proof of profit.")
    print("  Events are min-gap separated (non-overlapping) + block-bootstrapped, but the")
    print("  window is still short -> few events; treat as directional, not conclusive.")
    return {"verdicts": verdicts, "overall": overall, "n_events": len(flagged)}


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


def main() -> None:
    ap = argparse.ArgumentParser(description="Validate consistency flags (exploratory).")
    ap.add_argument("--rule", default="phillips")
    ap.add_argument("--zip", type=Path, default=None)
    ap.add_argument("--signal", choices=["median", "prob"], default=None,
                    help="implied signal: median (full-window) or prob (single strike); "
                         "default from rule's 'signal' key, else median")
    ap.add_argument("--z-window", type=int, default=48, help="trailing z-score window (hours)")
    ap.add_argument("--threshold", type=float, default=None,
                    help="|score| flag threshold (default: from mappings logic.flag_when)")
    ap.add_argument("--horizons", type=int, nargs="+", default=DEFAULT_HORIZONS)
    ap.add_argument("--min-gap", type=int, default=None,
                    help="min bars between events (default: max horizon -> non-overlapping)")
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    zip_path = args.zip or sig.find_latest_zip()
    min_gap = args.min_gap if args.min_gap is not None else max(args.horizons)

    cfg = sig.load_mappings()
    kind = args.signal or cfg["rules"].get(args.rule, {}).get("signal", "median")
    threshold = args.threshold
    if threshold is None:
        expr = cfg["rules"][args.rule]["logic"]["flag_when"]
        threshold = float(expr.split(">")[1])

    print(f"Loading bundle: {zip_path.name}  (rule={args.rule}, signal={kind}, "
          f"z_window={args.z_window}h, threshold={threshold}, min_gap={min_gap}h)")
    score, _panel = build_causal_score(args.rule, zip_path, args.z_window, kind=kind)
    if len(score) < max(args.horizons) + 10:
        raise SystemExit("not enough causal score points for the requested horizons")

    entries = find_flag_entries(score, threshold, min_gap=min_gap)
    flagged = forward_outcomes(score, entries, args.horizons)
    rand_base = random_baseline(score, args.horizons, RANDOM_BASELINE_K, rng)
    matched_base = magnitude_matched_baseline(
        score, args.horizons, threshold, entries, RANDOM_BASELINE_K, rng)

    OUT_DIR.mkdir(exist_ok=True)
    flagged.to_csv(OUT_DIR / f"{args.rule}_events.csv", index=False)

    result = summarize(args.rule, flagged, rand_base, matched_base, args.horizons, rng)

    base_idx = rng.choice(np.arange(len(score) - max(args.horizons)),
                          size=min(RANDOM_BASELINE_K, len(score) - max(args.horizons)),
                          replace=False)
    forward_path_plot(args.rule, score, entries, base_idx, max(args.horizons))

    print(f"\n  per-event CSV -> {OUT_DIR / (args.rule + '_events.csv')}")
    print(f"  verdict: {result['overall']}")


if __name__ == "__main__":
    main()
