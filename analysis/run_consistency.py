"""Run a cross-indicator consistency check on the ForecastTrader bundle.

EXPLORATORY / TESTING. First look at one rule end-to-end: load -> build
comparable implied series -> causal z-score -> inconsistency score -> flag ->
write CSV + plot + summary. Deliberately simple and transparent so we can judge
whether the economic identities in mappings.yaml carry any signal at all.
It is NOT a proven or production trading signal.

Rules are defined in analysis/mappings.yaml (status != planned is runnable).

Usage:
    python3 analysis/run_consistency.py --rule phillips
    python3 analysis/run_consistency.py --rule taylor --zip path/to/bundle.zip

Output (analysis/consistency/<rule>_*.csv + .png):
    aligned implied series, z-scores, inconsistency score, flags.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # runnable from anywhere

import rules
import signals as sig

OUT_DIR = sig.out_base() / "consistency"


def run(rule_key: str, zip_path: Path, kind: str | None = None, z_window: int = 48,
        history=None, markets=None) -> Path:
    print(f"Loading bundle: {zip_path.name}  (rule={rule_key}, z_window={z_window}h causal)")
    try:
        panel, roles, rule = rules.build_rule_panel(
            rule_key, zip_path, z_window, kind=kind, history=history, markets=markets)
    except (rules.RuleError, ValueError, KeyError) as exc:
        raise SystemExit(str(exc)) from exc

    panel = panel.rename(columns={"score": "inconsistency"})
    panel["flag"] = rules.flag_series(rule, panel["inconsistency"])

    OUT_DIR.mkdir(exist_ok=True)
    csv_path = OUT_DIR / f"{rule_key}_consistency.csv"
    panel.to_csv(csv_path)

    freq = sig.load_mappings()["defaults"]["resample_freq"]
    _summary(rule_key, rule, panel, freq)
    _plot(rule_key, panel, roles)
    return csv_path


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


def _plot(rule_key, panel, roles) -> None:
    try:
        import matplotlib.pyplot as plt
    except Exception:  # noqa: BLE001
        return
    fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True,
                             height_ratios=[2, 1])
    for role in roles:
        # role series may be in prob units or underlying units; normalize view
        s = panel[role]
        rng = s.max() - s.min()
        axes[0].plot(panel.index, (s - s.min()) / rng if rng else s * 0,
                     label=f"{role} (scaled)", linewidth=1.2)
    flagged = panel[panel["flag"]]
    axes[0].scatter(flagged.index, [0.5] * len(flagged), color="red", s=8,
                    label="flagged inconsistent", zorder=5)
    axes[0].set_title(f"{rule_key} consistency check (EXPLORATORY)")
    axes[0].set_ylabel("scaled implied signal")
    axes[0].legend(loc="best", fontsize=8)
    axes[0].grid(alpha=0.3)

    axes[1].plot(panel.index, panel["inconsistency"], color="purple", linewidth=1)
    axes[1].axhline(0, color="black", linewidth=0.6)
    axes[1].set_ylabel("inconsistency score")
    axes[1].grid(alpha=0.3)

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
                    help="implied signal override (default: rule's 'signal' key, else median)")
    ap.add_argument("--z-window", type=int, default=48,
                    help="trailing z-score window in hours (causal, no look-ahead)")
    args = ap.parse_args()
    zip_path = args.zip or sig.find_latest_zip()
    out = run(args.rule, zip_path, kind=args.signal, z_window=args.z_window)
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
