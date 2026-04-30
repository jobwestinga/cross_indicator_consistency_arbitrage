"""Quick exploratory analysis of the ForecastTrader dataset bundle.

Loads the latest forecast_analysis_dataset_*.zip in the repo root, prints
summary stats, and writes several figures to analysis/figures/.

Usage:
    python3 analysis/explore_dataset.py

Optional:
    python3 analysis/explore_dataset.py --zip path/to/file.zip
"""

from __future__ import annotations

import argparse
import sys
import zipfile
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
FIGURES_DIR = Path(__file__).resolve().parent / "figures"


def find_latest_zip() -> Path:
    candidates = sorted(REPO_ROOT.glob("forecast_analysis_dataset_*.zip"))
    if not candidates:
        sys.exit("No forecast_analysis_dataset_*.zip found in repo root.")
    return candidates[-1]


def load_tables(zip_path: Path) -> dict[str, pd.DataFrame]:
    print(f"Loading {zip_path.name} ...")
    tables: dict[str, pd.DataFrame] = {}
    with zipfile.ZipFile(zip_path) as archive:
        for name in archive.namelist():
            if not name.endswith(".csv"):
                continue
            key = name.removesuffix(".csv")
            with archive.open(name) as f:
                tables[key] = pd.read_csv(f)
            print(f"  {name}: {len(tables[key]):,} rows")
    return tables


def print_summary(tables: dict[str, pd.DataFrame]) -> None:
    markets = tables["markets"]
    contracts = tables["contracts"]
    hist = tables["contract_history"]
    prob = tables["projected_probabilities"]

    print("\n=== Summary ===")
    print(f"Markets total       : {len(markets):,}")
    print(f"Markets active      : {int(markets['active'].sum()):,}")
    print(f"Contracts (traded)  : {len(contracts):,}")
    print(f"History rows        : {len(hist):,}")
    print(f"Probability rows    : {len(prob):,}")
    if not hist.empty:
        hist_ts = pd.to_datetime(hist["ts_utc"])
        print(f"History time range  : {hist_ts.min()}  ->  {hist_ts.max()}")
    if not prob.empty:
        prob_ts = pd.to_datetime(prob["collected_at"])
        print(f"Probability range   : {prob_ts.min()}  ->  {prob_ts.max()}")


def fig_markets_per_category(tables: dict[str, pd.DataFrame]) -> Path:
    markets = tables["markets"]
    cats = tables["market_categories"].set_index("category_key")["category_name"]
    counts = (
        markets.groupby("category_key").size()
        .rename("n_markets").to_frame()
        .join(cats, how="left")
        .sort_values("n_markets", ascending=False)
        .head(15)
    )
    counts["label"] = counts["category_name"].fillna(counts.index.to_series())

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.barh(counts["label"][::-1], counts["n_markets"][::-1], color="steelblue")
    ax.set_xlabel("Markets")
    ax.set_title("Top 15 categories by market count")
    fig.tight_layout()
    out = FIGURES_DIR / "01_markets_per_category.png"
    fig.savefig(out, dpi=140)
    plt.close(fig)
    return out


def fig_probability_distribution(tables: dict[str, pd.DataFrame]) -> Path:
    prob = tables["projected_probabilities"]
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.hist(prob["probability"], bins=50, color="darkorange", edgecolor="black")
    ax.set_xlabel("IBKR projected probability")
    ax.set_ylabel("Count of snapshots")
    ax.set_title(f"Distribution of projected probabilities (n={len(prob):,})")
    fig.tight_layout()
    out = FIGURES_DIR / "02_probability_distribution.png"
    fig.savefig(out, dpi=140)
    plt.close(fig)
    return out


def pick_busy_market(tables: dict[str, pd.DataFrame]) -> int | None:
    hist = tables["contract_history"]
    if hist.empty:
        return None
    counts = hist.groupby("underlying_conid").size().sort_values(ascending=False)
    return int(counts.index[0])


def fig_market_price_paths(tables: dict[str, pd.DataFrame], conid: int) -> Path:
    """For a single market, plot YES probability over time across several strikes.

    Each line shows the market's implied probability that the underlying will
    exceed that strike. Lines are naturally ordered: higher strike => lower
    probability.
    """
    hist = tables["contract_history"]
    markets = tables["markets"].set_index("underlying_conid")
    sub = hist[hist["underlying_conid"] == conid].copy()
    sub = sub.drop_duplicates(subset=["conid", "ts_utc"], keep="first")
    sub["ts_utc"] = pd.to_datetime(sub["ts_utc"])

    yes = sub[sub["side"].isin(["Y", "YES"])].copy()
    if yes.empty:
        yes = sub.copy()

    # Pick up to 6 strikes that have the most observations, sorted by strike.
    contract_counts = yes.groupby("conid").size().sort_values(ascending=False).head(20)
    contract_meta = (
        yes.drop_duplicates("conid").set_index("conid").loc[contract_counts.index]
    )
    contract_meta = contract_meta.dropna(subset=["strike"]).sort_values("strike")
    chosen = contract_meta.head(6)

    fig, ax = plt.subplots(figsize=(11, 6))
    cmap = plt.colormaps.get_cmap("viridis")
    for i, (cid, row) in enumerate(chosen.iterrows()):
        line = yes[yes["conid"] == cid].sort_values("ts_utc")
        if line.empty:
            continue
        label = f"> {row.get('strike_label') or row.get('strike')}"
        color = cmap(i / max(len(chosen) - 1, 1))
        ax.plot(line["ts_utc"], line["avg"], label=label, linewidth=1.4, color=color)

    market_name = markets.loc[conid, "market_name"] if conid in markets.index else str(conid)
    ax.set_title(f"Market-implied probability that {market_name} exceeds each level")
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Implied probability (avg traded price)")
    ax.set_ylim(0, 1)
    ax.grid(alpha=0.3)
    ax.legend(title="Threshold", loc="best", fontsize=9)
    fig.autofmt_xdate()
    fig.tight_layout()
    out = FIGURES_DIR / "03_market_price_paths.png"
    fig.savefig(out, dpi=140)
    plt.close(fig)
    return out


def fig_daily_activity(tables: dict[str, pd.DataFrame]) -> Path:
    """Show how many price observations are recorded per day, all markets."""
    hist = tables["contract_history"].copy()
    hist["ts_utc"] = pd.to_datetime(hist["ts_utc"])
    daily = hist.groupby(hist["ts_utc"].dt.date).size()
    daily.index = pd.to_datetime(daily.index)

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.bar(daily.index, daily.values, color="steelblue", width=1.0)
    ax.set_title("Daily price observations across all markets")
    ax.set_xlabel("Date")
    ax.set_ylabel("Price observations recorded")
    ax.grid(alpha=0.3, axis="y")
    fig.autofmt_xdate()
    fig.tight_layout()
    out = FIGURES_DIR / "04_daily_activity.png"
    fig.savefig(out, dpi=140)
    plt.close(fig)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Explore the forecast dataset.")
    parser.add_argument("--zip", type=Path, default=None, help="Path to dataset zip.")
    args = parser.parse_args()

    zip_path = args.zip or find_latest_zip()
    FIGURES_DIR.mkdir(exist_ok=True)

    tables = load_tables(zip_path)
    print_summary(tables)

    print("\n=== Generating figures ===")
    print(f"  -> {fig_markets_per_category(tables)}")
    print(f"  -> {fig_probability_distribution(tables)}")

    busy_conid = pick_busy_market(tables)
    if busy_conid is not None:
        print(f"  -> {fig_market_price_paths(tables, busy_conid)}")
    print(f"  -> {fig_daily_activity(tables)}")

    print(f"\nFigures written to {FIGURES_DIR}")


if __name__ == "__main__":
    main()
