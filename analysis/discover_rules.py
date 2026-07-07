"""Data-driven rule discovery: mine stable co-movement pairs with a hard split.

EXPLORATORY / TESTING. The hand-written rules come from textbook identities;
this script asks the data for candidates we did not think of (goal C1):

  1. universe = every market with enough YES ladder history
  2. implied-median series per market (front expiry, causal — signals.py)
  3. TRAIN (before --split): correlate 6h CHANGES of every pair; keep pairs
     whose correlation is Bonferroni-significant across ALL pairs tested
     (multiple-testing control) and above a |rho| floor
  4. TEST (after --split): survivors must keep the SAME SIGN with p < 0.05
     on unseen data
  5. survivors are printed as ready-to-paste mappings.yaml stubs with
     provenance (a discovered pair is a HYPOTHESIS for the normal
     validation/backtest/OOS pipeline, not a tradeable rule)

Correlating changes (not levels) avoids trending-level spurious correlation;
bars where neither side moved are dropped so carried marks don't fake overlap.

Usage:
    python3 analysis/discover_rules.py
    python3 analysis/discover_rules.py --min-rows 2000 --train-rho 0.6
Output (analysis/discovery/): pairs CSV + JSON + YAML stubs.
"""

from __future__ import annotations

import argparse
import itertools
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats as sps

sys.path.insert(0, str(Path(__file__).resolve().parent))  # runnable from anywhere

import signals as sig

OUT_DIR = sig.out_base() / "discovery"
DEFAULT_SPLIT = "2026-05-01"


def build_universe(hist: pd.DataFrame, markets: pd.DataFrame, min_rows: int
                   ) -> dict[str, pd.Series]:
    y = hist[(hist.side == "Y") & hist.strike.notna()]
    counts = y.groupby("market_name").agg(rows=("avg", "size"),
                                          strikes=("strike", "nunique"))
    names = counts[(counts.rows >= min_rows) & (counts.strikes >= 3)].index
    series: dict[str, pd.Series] = {}
    for name in sorted(names):
        try:
            s = sig.implied_median_series(hist, markets, name)
        except (ValueError, KeyError):
            continue
        if s.notna().sum() >= 200:
            series[name] = s
    return series


def _diff_corr(a: pd.Series, b: pd.Series, min_overlap: int,
               freq: str = "6h") -> tuple[float, int]:
    """Correlation of `freq` changes on bars where at least one side moved.

    6h default: hourly diffs are too asynchronous across markets (median
    |rho| ~ 0.08 on this bundle); 6h balances noise vs sample size."""
    df = (pd.concat([a, b], axis=1, keys=["a", "b"], sort=False)
          .resample(freq).last().dropna().diff().dropna())
    df = df[(df["a"] != 0) | (df["b"] != 0)]
    if len(df) < min_overlap or df["a"].std() == 0 or df["b"].std() == 0:
        return np.nan, len(df)
    return float(df["a"].corr(df["b"])), len(df)


def _corr_pvalue(r: float, n: int) -> float:
    """Two-sided p-value for Pearson r under the t approximation."""
    if np.isnan(r) or n < 4 or abs(r) >= 1:
        return np.nan
    t = r * np.sqrt((n - 2) / (1 - r * r))
    return float(2 * sps.t.sf(abs(t), n - 2))


def mine_pairs(series: dict[str, pd.Series], split: pd.Timestamp,
               train_rho: float, alpha: float, min_overlap: int,
               freq: str = "6h") -> tuple[pd.DataFrame, int]:
    """Returns (screened pairs, number of pairs actually tested).

    Train screen: |rho| >= train_rho floor AND Bonferroni-significant at
    `alpha` across every pair tested (small-overlap junk correlations are the
    dominant failure mode on this venue: thin markets co-print rarely, so
    |rho| ~ 0.5 on n ~ 25 happens by chance across thousands of pairs).
    """
    tested = 0
    cands = []
    names = list(series)
    for a, b in itertools.combinations(names, 2):
        sa, sb = series[a], series[b]
        r_tr, n_tr = _diff_corr(sa[sa.index < split], sb[sb.index < split],
                                min_overlap, freq)
        if np.isnan(r_tr):
            continue
        tested += 1
        if abs(r_tr) < train_rho:
            continue
        cands.append((a, b, r_tr, n_tr))

    rows = []
    for a, b, r_tr, n_tr in cands:
        p_tr = _corr_pvalue(r_tr, n_tr)
        if np.isnan(p_tr) or p_tr * max(tested, 1) > alpha:   # Bonferroni
            continue
        sa, sb = series[a], series[b]
        r_te, n_te = _diff_corr(sa[sa.index >= split], sb[sb.index >= split],
                                max(min_overlap // 3, 30), freq)
        p_te = _corr_pvalue(r_te, n_te)
        survives = (not np.isnan(r_te) and np.sign(r_te) == np.sign(r_tr)
                    and not np.isnan(p_te) and p_te < 0.05)
        rows.append({"market_a": a, "market_b": b,
                     "rho_train": round(r_tr, 3), "n_train": n_tr,
                     "p_train_bonf": round(min(p_tr * tested, 1.0), 4),
                     "rho_test": round(r_te, 3) if not np.isnan(r_te) else np.nan,
                     "n_test": n_te, "survives_oos": survives,
                     # rho>0: move together -> opposite pricing is inconsistent
                     "suggested_sign": -1 if r_tr > 0 else 1})
    df = pd.DataFrame(rows)
    if df.empty:
        return df, tested
    df["strength"] = df[["rho_train", "rho_test"]].abs().min(axis=1)
    return df.sort_values(["survives_oos", "strength"], ascending=False), tested


def _slug(a: str, b: str) -> str:
    short = lambda s: s.lower().replace("us ", "").replace(" ", "_")[:18]
    return f"disc_{short(a)}__{short(b)}"


def yaml_stub(row: pd.Series, bundle: str, split: str) -> str:
    key = _slug(row.market_a, row.market_b)
    return f"""  {key}:
    description: >
      DISCOVERED pair (not an economic identity): changes correlated
      rho_train={row.rho_train} (n={row.n_train}), rho_test={row.rho_test}
      (n={row.n_test}) across the {split} split.
    status: planned   # promote only after validate/backtest/OOS
    discovered_from: {bundle}
    indicators:
      a: {{market_name: "{row.market_a}", fred_series: ""}}
      b: {{market_name: "{row.market_b}", fred_series: ""}}
    logic:
      score: {{type: product, terms: [a, b], sign: {row.suggested_sign}}}
      flag: {{metric: value, threshold: 1.0}}
"""


def main() -> None:
    ap = argparse.ArgumentParser(description="Mine stable co-movement pairs (exploratory).")
    ap.add_argument("--zip", type=Path, default=None)
    ap.add_argument("--split", default=DEFAULT_SPLIT)
    ap.add_argument("--min-rows", type=int, default=1000)
    ap.add_argument("--train-rho", type=float, default=0.3,
                    help="|rho| floor before the Bonferroni significance screen")
    ap.add_argument("--alpha", type=float, default=0.01,
                    help="family-wise significance level (Bonferroni)")
    ap.add_argument("--min-overlap", type=int, default=50)
    ap.add_argument("--freq", default="6h", help="change frequency for correlation")
    ap.add_argument("--top", type=int, default=20)
    args = ap.parse_args()

    zip_path = args.zip or sig.find_latest_zip()
    split = pd.Timestamp(args.split, tz="UTC")
    print(f"Loading bundle: {zip_path.name}  (split {split.date()})")
    markets = sig.load_markets(zip_path)
    hist = sig.load_history(zip_path)

    series = build_universe(hist, markets, args.min_rows)
    print(f"universe: {len(series)} markets with >= {args.min_rows} ladder rows")
    if len(series) < 2:
        raise SystemExit("universe too small")

    pairs, tested = mine_pairs(series, split, args.train_rho, args.alpha,
                               args.min_overlap, args.freq)
    OUT_DIR.mkdir(exist_ok=True)
    if pairs.empty:
        print(f"no pairs pass the train screen ({tested} tested; Bonferroni "
              f"alpha={args.alpha}). Expected while the venue is thin and "
              "markets co-print rarely - rerun as data accrues.")
        (OUT_DIR / "summary.json").write_text(json.dumps(
            {"bundle": zip_path.name, "universe": len(series),
             "pairs_tested": tested, "pairs": 0, "survivors": 0}, indent=2))
        return

    pairs.to_csv(OUT_DIR / "pairs.csv", index=False)
    survivors = pairs[pairs.survives_oos]
    print(f"\ntrain screen: {len(pairs)} of {tested} pairs "
          f"(Bonferroni alpha={args.alpha}, |rho|>={args.train_rho})   "
          f"OOS survivors: {len(survivors)} (same sign, p<0.05)")
    print("\ntop pairs:")
    print(pairs.head(args.top).to_string(index=False))

    stubs = "".join(yaml_stub(r, zip_path.name, str(split.date()))
                    for _, r in survivors.head(args.top).iterrows())
    (OUT_DIR / "candidate_rules.yaml").write_text(
        "# DISCOVERED candidate rules (provenance inside). Paste into\n"
        "# mappings.yaml only to run the standard pipeline on them; they are\n"
        "# correlations, not identities - expect many to be venue artifacts\n"
        "# (same category, shared macro driver, or co-listing effects).\n\n"
        "rules:\n" + stubs)

    (OUT_DIR / "summary.json").write_text(json.dumps({
        "bundle": zip_path.name, "split": str(split.date()),
        "universe": len(series),
        "params": {"min_rows": args.min_rows, "train_rho": args.train_rho,
                   "alpha": args.alpha, "min_overlap": args.min_overlap,
                   "freq": args.freq},
        "pairs_tested": tested,
        "n_train_pairs": int(len(pairs)),
        "n_survivors": int(len(survivors)),
        "top": pairs.head(args.top).to_dict("records"),
    }, indent=2, default=float))
    print(f"\nwrote {OUT_DIR}/pairs.csv, candidate_rules.yaml, summary.json")
    print("NOTE: survivors are hypotheses for the normal pipeline, not rules.")


if __name__ == "__main__":
    main()
