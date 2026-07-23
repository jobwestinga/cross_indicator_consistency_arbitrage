"""Same-event redundancy check: Fed Decision vs Fed Funds ladder (rule B9).

EXPLORATORY / TESTING. The strongest consistency class: two markets price the
SAME event and are linked by identity, not correlation.

  - "Fed Decision" (per meeting): categorical contracts, strike-coded
      1 = raise 50bps+, 2 = raise 25bps, 3 = unchanged,
      4 = lower 25bps,  5 = lower 50bps+
  - "US Fed Funds Target Rate" (per meeting, same expiration date): survival
    ladder S(K) = P(target rate set above K at that meeting).

IB quotes the target rate as the RANGE MIDPOINT (e.g. range 3.50-3.75 ->
3.625), and ladder strikes sit exactly on those midpoints. With the current
midpoint r_mid (snapped from FRED daily DFF) and 25bp moves:

  hold  -> rate = r_mid            (not > r_mid)
  hike  -> rate >= r_mid + 0.25    (> r_mid)
  cut   -> rate <= r_mid - 0.25    (not > r_mid - 0.25)

  S(r_mid - 0.25) should equal 1 - P(cut)     [no-cut boundary]
  S(r_mid)        should equal P(hike)        [hike boundary]

gap_cut(t)  = S(r_mid - 0.25) - (1 - P4 - P5)
gap_hike(t) = S(r_mid) - (P1 + P2)

|gap| beyond fees is a two-market arbitrage on the same event. Also checks the
decision market's own category parity: sum of P1..P5 vs 1.

Usage:
    python3 analysis/fed_path_check.py
Output (analysis/fed_path/): gaps CSV + plot + summary JSON.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))  # runnable from anywhere

import signals as sig

OUT_DIR = sig.out_base() / "fed_path"
DECISION_MARKET = "Fed Decision"
LADDER_MARKET = "US Fed Funds Target Rate"
CUT_STRIKES = {4.0, 5.0}
HIKE_STRIKES = {1.0, 2.0}
FREQ = "1h"
FFILL_LIMIT = 48


def _grid(series_by_key: dict, freq: str = FREQ) -> pd.DataFrame:
    df = pd.DataFrame(series_by_key)
    return df.resample(freq).last().ffill(limit=FFILL_LIMIT)


def decision_probs(hist: pd.DataFrame, markets: pd.DataFrame) -> dict[pd.Timestamp, pd.DataFrame]:
    """Per meeting expiration: 1h grid of P_cut, P_hike, P_hold, category sum."""
    conid = sig.resolve_conid(markets, DECISION_MARKET)
    sub = hist[(hist.underlying_conid == conid) & (hist.side == "Y")]
    out = {}
    for exp, g in sub.groupby("expiration"):
        pv = g.pivot_table(index="ts_utc", columns="strike", values="avg", aggfunc="last")
        pv = _grid({k: pv[k] for k in pv.columns})
        cats = [c for c in pv.columns]
        d = pd.DataFrame(index=pv.index)
        d["p_cut"] = pv[[c for c in cats if c in CUT_STRIKES]].sum(axis=1, min_count=1)
        d["p_hike"] = pv[[c for c in cats if c in HIKE_STRIKES]].sum(axis=1, min_count=1)
        d["p_hold"] = pv[3.0] if 3.0 in cats else np.nan
        d["cat_sum"] = pv.sum(axis=1, min_count=len(cats))  # all categories present
        out[exp] = d
    return out


def ladder_survival(hist: pd.DataFrame, markets: pd.DataFrame) -> dict[pd.Timestamp, pd.DataFrame]:
    """Per meeting expiration: 1h grid of the survival curve (columns=strikes)."""
    conid = sig.resolve_conid(markets, LADDER_MARKET)
    sub = hist[(hist.underlying_conid == conid) & (hist.side == "Y")].dropna(subset=["strike"])
    out = {}
    for exp, g in sub.groupby("expiration"):
        pv = g.pivot_table(index="ts_utc", columns="strike", values="avg", aggfunc="last")
        out[exp] = _grid({k: pv[k] for k in pv.columns})
    return out


def load_dff() -> pd.Series:
    """Daily effective fed funds rate, ffilled onto an hourly grid."""
    dff = sig.load_fred_series("DFF")
    return dff.resample(FREQ).last().ffill()


def front_meeting_gaps(dec: dict, lad: dict, dff: pd.Series) -> pd.DataFrame:
    """Stitch per-meeting gaps into one front-meeting time series."""
    meetings = sorted(set(dec) & set(lad))
    rows = []
    for i, exp in enumerate(meetings):
        d, s = dec[exp], lad[exp]
        idx = d.index.intersection(s.index)
        # front window: from the previous meeting (or start) to this meeting
        lo = meetings[i - 1] if i else idx.min()
        idx = idx[(idx >= lo) & (idx < exp)]
        if idx.empty:
            continue
        r = dff.reindex(idx).ffill()
        strikes = np.array(s.columns, dtype=float)

        def nearest(k: float) -> float | None:
            """Exact-grid match: strikes sit on 25bp-midpoints; tolerate 6bp."""
            if not len(strikes):
                return None
            j = np.abs(strikes - k).argmin()
            return float(strikes[j]) if abs(strikes[j] - k) < 0.06 else None

        for ts in idx:
            r_eff = r.loc[ts]
            if np.isnan(r_eff):
                continue
            # snap effective rate to the midpoint grid (x.125 / x.375 / ...)
            r_mid = round((r_eff - 0.125) / 0.25) * 0.25 + 0.125
            k_cut = nearest(r_mid - 0.25)   # no-cut boundary
            k_hike = nearest(r_mid)         # hike boundary
            rec = {"ts_utc": ts, "meeting": exp, "r_eff": r_eff, "r_mid": r_mid,
                   "p_cut": d.loc[ts, "p_cut"], "p_hike": d.loc[ts, "p_hike"],
                   "cat_sum": d.loc[ts, "cat_sum"]}
            if k_cut is not None:
                s_k = s.loc[ts, k_cut]
                rec["gap_cut"] = (s_k - (1.0 - d.loc[ts, "p_cut"])
                                  if not (np.isnan(s_k) or np.isnan(d.loc[ts, "p_cut"]))
                                  else np.nan)
            if k_hike is not None:
                s_k = s.loc[ts, k_hike]
                rec["gap_hike"] = (s_k - d.loc[ts, "p_hike"]
                                   if not (np.isnan(s_k) or np.isnan(d.loc[ts, "p_hike"]))
                                   else np.nan)
            rows.append(rec)
    return pd.DataFrame(rows).set_index("ts_utc").sort_index() if rows else pd.DataFrame()


def _gap_stats(g: pd.Series) -> dict:
    g = g.dropna()
    if g.empty:
        return {"n": 0}
    return {"n": int(len(g)), "mean": float(g.mean()), "mean_abs": float(g.abs().mean()),
            "frac_abs_gt_2c": float((g.abs() > 0.02).mean()),
            "frac_abs_gt_5c": float((g.abs() > 0.05).mean()),
            "max_abs": float(g.abs().max())}


def main() -> None:
    ap = argparse.ArgumentParser(description="Fed Decision vs Fed Funds ladder identity check.")
    ap.add_argument("--zip", type=Path, default=None)
    args = ap.parse_args()
    zip_path = args.zip or sig.find_latest_zip()

    print(f"Loading bundle: {zip_path.name}")
    markets = sig.load_markets(zip_path)
    hist = sig.load_history(zip_path)

    try:
        dec = decision_probs(hist, markets)
        lad = ladder_survival(hist, markets)
    except KeyError as exc:
        raise SystemExit(f"required market missing from bundle: {exc}") from exc
    try:
        dff = load_dff()
    except ValueError as exc:
        raise SystemExit(f"FRED DFF unavailable ({exc}); run collect_fred.py first") from exc

    gaps = front_meeting_gaps(dec, lad, dff)
    if gaps.empty:
        raise SystemExit("no overlapping decision/ladder meetings found")

    OUT_DIR.mkdir(exist_ok=True)
    gaps.to_csv(OUT_DIR / "front_meeting_gaps.csv")

    cut_stats = _gap_stats(gaps.get("gap_cut", pd.Series(dtype=float)))
    hike_stats = _gap_stats(gaps.get("gap_hike", pd.Series(dtype=float)))
    parity = _gap_stats(gaps["cat_sum"] - 1.0)

    print("\n=== fed_path identity check (EXPLORATORY) ===")
    print(f"  meetings covered: {gaps['meeting'].nunique()}   grid points: {len(gaps):,}")
    print(f"  gap_cut  (ladder no-cut  vs decision): {cut_stats}")
    print(f"  gap_hike (ladder hike    vs decision): {hike_stats}")
    print(f"  decision category-sum minus 1        : {parity}")
    print("  CAVEATS: bar averages carried up to 48h (both markets print sparsely),")
    print("  NOT simultaneous quotes -> gaps are staleness-inflated. r_eff anchor from")
    print("  daily FRED DFF snapped to the midpoint grid; assumes 25bp moves. The")
    print("  category-sum check suffers the same staleness (categories rarely print")
    print("  in the same bar). |gap| large AND persistent = candidate same-event")
    print("  arbitrage; verify against live quotes by hand before believing it.")

    try:
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(12, 5))
        if "gap_cut" in gaps:
            ax.plot(gaps.index, gaps["gap_cut"], label="gap_cut", linewidth=0.9)
        if "gap_hike" in gaps:
            ax.plot(gaps.index, gaps["gap_hike"], label="gap_hike", linewidth=0.9)
        ax.axhline(0, color="black", linewidth=0.6)
        for m in gaps["meeting"].unique():
            ax.axvline(m, color="gray", linewidth=0.5, linestyle=":")
        ax.set_title("Fed Decision vs Fed Funds ladder: front-meeting identity gaps")
        ax.set_ylabel("probability gap ($)")
        ax.legend()
        ax.grid(alpha=0.3)
        fig.autofmt_xdate()
        fig.tight_layout()
        fig.savefig(OUT_DIR / "front_meeting_gaps.png", dpi=140)
        plt.close(fig)
        print(f"  plot -> {OUT_DIR / 'front_meeting_gaps.png'}")
    except Exception:  # noqa: BLE001
        pass

    (OUT_DIR / "summary.json").write_text(json.dumps({
        "bundle": zip_path.name,
        "meetings": int(gaps["meeting"].nunique()),
        "n_points": int(len(gaps)),
        "gap_cut": cut_stats, "gap_hike": hike_stats,
        "category_parity": parity,
    }, indent=2, default=str))
    print(f"  wrote {OUT_DIR}/front_meeting_gaps.csv + summary.json")


if __name__ == "__main__":
    main()
