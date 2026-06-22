"""Run a cross-indicator consistency check on the ForecastTrader bundle.

EXPLORATORY / TESTING. This is the first vertical slice of the strategy
(critique C1, option a): one rule taken end-to-end - load -> build comparable
implied-probability series -> z-score -> compute an inconsistency score ->
flag -> write output + summary. It is deliberately simple and transparent so we
can judge whether the economic identities in strats.txt carry any signal at
all. It is NOT a proven or production trading signal.

Rules are defined in analysis/mappings.yaml. Currently implemented:
  - phillips : unemployment vs core CPI (Phillips curve)
  - sahm     : unemployment vs recession (Sahm rule; low-power, thin data)

Usage:
    python3 analysis/run_consistency.py --rule phillips
    python3 analysis/run_consistency.py --rule sahm --zip path/to/bundle.zip

Output (analysis/consistency/<rule>_*.csv + .png):
    aligned implied-prob series, z-scores, inconsistency score, flags.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

import signals as sig  # local module (analysis/signals.py)

OUT_DIR = Path(__file__).resolve().parent / "consistency"


def _series_for(history, markets, rule, defaults, kind="median"):
    band = tuple(defaults["prob_band"])
    freq = defaults["resample_freq"]
    out = {}
    for role, spec in rule["indicators"].items():
        out[role] = sig.implied_series(
            history, markets, spec["market_name"], kind=kind, band=band, freq=freq
        )
    return out, freq


def _score(rule_key: str, z: dict[str, pd.Series]) -> pd.Series:
    """Evaluate the rule's inconsistency score from z-scored series.

    Kept as an explicit per-rule branch (transparent > clever) rather than
    eval'ing the YAML expression. The YAML `logic.score` documents the intent.
    """
    if rule_key == "phillips":
        return (z["inflation"] * z["labor"]).rename("inconsistency")
    if rule_key == "sahm":
        return (z["unemployment"] - z["recession"]).rename("inconsistency")
    raise NotImplementedError(f"no scorer for rule {rule_key!r}")


def run(rule_key: str, zip_path: Path, kind: str = "median", z_window: int = 48) -> Path:
    cfg = sig.load_mappings()
    defaults = cfg["defaults"]
    if rule_key not in cfg["rules"]:
        raise SystemExit(f"unknown rule {rule_key!r}; have {list(cfg['rules'])}")
    rule = cfg["rules"][rule_key]
    if rule.get("status", "planned") == "planned":
        raise SystemExit(f"rule {rule_key!r} is planned, not implemented yet")

    print(f"Loading bundle: {zip_path.name}  (signal={kind}, z_window={z_window}h causal)")
    markets = sig.load_markets(zip_path)
    history = sig.load_history(zip_path)

    series, freq = _series_for(history, markets, rule, defaults, kind=kind)
    panel = sig.align(*series.values(), freq=freq)
    panel.columns = list(series.keys())  # role names
    if panel.empty:
        raise SystemExit("no overlapping observations after alignment")

    # causal trailing z-score (no look-ahead); drop warmup NaNs
    z = {role: sig.zscore_rolling(panel[role], z_window) for role in series}
    for role in series:
        panel[f"z_{role}"] = z[role]
    panel["inconsistency"] = _score(rule_key, z)
    panel = panel.dropna(subset=["inconsistency"])
    if panel.empty:
        raise SystemExit("no score points after z-score warmup")

    flag_expr = rule["logic"]["flag_when"]  # e.g. "score > 1.0" / "abs(score) > 1.5"
    panel["flag"] = _apply_flag(flag_expr, panel["inconsistency"])

    OUT_DIR.mkdir(exist_ok=True)
    csv_path = OUT_DIR / f"{rule_key}_consistency.csv"
    panel.to_csv(csv_path)

    _summary(rule_key, rule, panel, freq)
    _plot(rule_key, rule, panel, series)
    return csv_path


def _apply_flag(expr: str, score: pd.Series) -> pd.Series:
    """Evaluate the two supported flag forms: 'score > T' and 'abs(score) > T'."""
    thr = float(expr.split(">")[1])
    if expr.startswith("abs(score)"):
        return score.abs() > thr
    return score > thr


def _summary(rule_key, rule, panel, freq) -> None:
    n = len(panel)
    flagged = int(panel["flag"].sum())
    print(f"\n=== {rule_key} consistency (EXPLORATORY) ===")
    print(f"  {rule['description'].strip().splitlines()[0]}")
    print(f"  grid={freq}  aligned points={n:,}  window={panel.index.min()} -> {panel.index.max()}")
    print(f"  flagged inconsistent: {flagged:,} ({100*flagged/max(n,1):.1f}% of time)")
    if flagged:
        worst = panel.loc[panel["flag"]].reindex(
            panel.loc[panel["flag"], "inconsistency"].abs().sort_values(ascending=False).index
        ).head(5)
        print("  top inconsistent timestamps:")
        for ts, row in worst.iterrows():
            print(f"    {ts}  score={row['inconsistency']:+.2f}")
    # FRED ground-truth context (latest realized values)
    print("  realized macro (FRED, latest):")
    for role, spec in rule["indicators"].items():
        try:
            fs = sig.load_fred_series(spec["fred_series"])
            print(f"    {role:12} {spec['fred_series']:9} = {fs.iloc[-1]}  ({fs.index[-1].date()})")
        except Exception as exc:  # noqa: BLE001
            print(f"    {role:12} {spec['fred_series']:9} unavailable: {exc}")


def _plot(rule_key, rule, panel, series) -> None:
    try:
        import matplotlib.pyplot as plt
    except Exception:  # noqa: BLE001
        return
    fig, ax = plt.subplots(figsize=(12, 5))
    for role in series:
        ax.plot(panel.index, panel[role], label=f"{role} (implied P)", linewidth=1.2)
    flagged = panel[panel["flag"]]
    ax.scatter(flagged.index, [0.5] * len(flagged), color="red", s=8,
               label="flagged inconsistent", zorder=5)
    ax.set_title(f"{rule_key} consistency check (EXPLORATORY)")
    ax.set_ylabel("market-implied probability")
    ax.set_ylim(0, 1)
    ax.legend(loc="best", fontsize=8)
    ax.grid(alpha=0.3)
    fig.autofmt_xdate()
    fig.tight_layout()
    out = OUT_DIR / f"{rule_key}_consistency.png"
    fig.savefig(out, dpi=140)
    plt.close(fig)
    print(f"  plot -> {out}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Run a consistency check (exploratory).")
    ap.add_argument("--rule", default="phillips", help="rule key from mappings.yaml")
    ap.add_argument("--zip", type=Path, default=None, help="bundle zip (default: latest)")
    ap.add_argument("--signal", choices=["median", "prob"], default=None,
                    help="implied signal: median (full-window) or prob (single strike); "
                         "default from rule's 'signal' key, else median")
    ap.add_argument("--z-window", type=int, default=48,
                    help="trailing z-score window in hours (causal, no look-ahead)")
    args = ap.parse_args()
    zip_path = args.zip or sig.find_latest_zip()
    cfg = sig.load_mappings()
    kind = args.signal or cfg["rules"].get(args.rule, {}).get("signal", "median")
    out = run(args.rule, zip_path, kind=kind, z_window=args.z_window)
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
