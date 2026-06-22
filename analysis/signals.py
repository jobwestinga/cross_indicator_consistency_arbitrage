"""Reusable signal extraction for consistency-arbitrage analysis.

EXPLORATORY / TESTING code. The strategy is unproven; this module exists to
turn the raw bundle into comparable market-implied series so the economic
identities in strats.txt can be tested.

Design decisions (see the project critiques discussion):
  - C4: the canonical implied-probability signal is the IBKR `contract_history`
    table (avg traded price). It is continuous and covers every market,
    including Recession. `projected_probabilities` is NOT used here: it has
    gaps (9-day outage Jun 4-12 2026) and is empty for some markets.
  - C6: contracts with a degenerate implied probability (outside the
    `prob_band`, default 0.001-0.999) are dropped before analysis.
  - C3: each market is resampled onto a common time grid (default 1h) with
    forward-fill, so markets on slightly offset clocks join cleanly.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
ANALYSIS_DIR = Path(__file__).resolve().parent
MAPPINGS_PATH = ANALYSIS_DIR / "mappings.yaml"
FRED_DB = ANALYSIS_DIR / "macro" / "fred.sqlite"

DEFAULT_BAND = (0.001, 0.999)
DEFAULT_FREQ = "1h"


def find_latest_zip() -> Path:
    cands = sorted(REPO_ROOT.glob("forecast_analysis_dataset_*.zip"))
    if not cands:
        raise FileNotFoundError("No forecast_analysis_dataset_*.zip in repo root.")
    return cands[-1]


def load_markets(zip_path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path) as z:
        return pd.read_csv(z.open("markets.csv"), usecols=["underlying_conid", "market_name"])


def load_history(zip_path: Path) -> pd.DataFrame:
    """Load contract_history with only the columns the signal needs."""
    with zipfile.ZipFile(zip_path) as z:
        h = pd.read_csv(
            z.open("contract_history.csv"),
            usecols=["underlying_conid", "market_name", "conid", "side",
                     "strike", "ts_utc", "avg"],
        )
    h["ts_utc"] = pd.to_datetime(h["ts_utc"], utc=True)
    return h


def resolve_conid(markets: pd.DataFrame, market_name: str) -> int:
    hit = markets.loc[markets.market_name == market_name, "underlying_conid"]
    if hit.empty:
        raise KeyError(f"market_name not found in bundle: {market_name!r}")
    return int(hit.iloc[0])


def implied_prob_series(
    history: pd.DataFrame,
    markets: pd.DataFrame,
    market_name: str,
    band: tuple[float, float] = DEFAULT_BAND,
    freq: str = DEFAULT_FREQ,
) -> pd.Series:
    """Market-implied probability over time for one market, on a common grid.

    Method: take the YES side, pick the single most-traded strike as the
    reference threshold (stable, liquid), and track P(outcome > strike) = the
    traded price `avg` of that contract through time. Band-filter degenerate
    values, then resample to `freq` with forward-fill.

    Returns a Series indexed by tz-aware timestamp, values in (0, 1).
    """
    conid = resolve_conid(markets, market_name)
    sub = history[(history.underlying_conid == conid) & (history.side == "Y")].copy()
    if sub.empty:
        raise ValueError(f"no YES contract_history for {market_name!r}")

    # reference strike = the contract with the most observations (most liquid)
    ref_conid = sub.groupby("conid").size().idxmax()
    line = sub[sub.conid == ref_conid].sort_values("ts_utc")

    lo, hi = band
    line = line[(line["avg"] >= lo) & (line["avg"] <= hi)]
    if line.empty:
        raise ValueError(f"all {market_name!r} observations outside band {band}")

    s = line.set_index("ts_utc")["avg"]
    s = s[~s.index.duplicated(keep="last")]
    return s.resample(freq).ffill().rename(market_name)


def _ladder_median(strikes: np.ndarray, survival: np.ndarray) -> float:
    """Implied median outcome from a survival ladder: the strike x where
    P(X > x) = 0.5, by linear interpolation. survival = P(X > strike)."""
    order = np.argsort(strikes)
    x = strikes[order]
    s = survival[order]  # ideally decreasing in strike
    if len(x) == 1:
        return float(x[0])
    if s.min() > 0.5:      # median sits above the highest strike we observe
        return float(x[-1])
    if s.max() < 0.5:      # median sits below the lowest strike we observe
        return float(x[0])
    for k in range(len(x) - 1):       # first adjacent pair bracketing 0.5
        s1, s2 = s[k], s[k + 1]
        if (s1 - 0.5) * (s2 - 0.5) <= 0 and s1 != s2:
            return float(x[k] + (0.5 - s1) * (x[k + 1] - x[k]) / (s2 - s1))
    return float(x[np.argmin(np.abs(s - 0.5))])


def implied_median_series(
    history: pd.DataFrame,
    markets: pd.DataFrame,
    market_name: str,
    band: tuple[float, float] = DEFAULT_BAND,
    freq: str = DEFAULT_FREQ,
) -> pd.Series:
    """Market-implied MEDIAN outcome over time, on a common grid.

    Unlike implied_prob_series (one fixed strike), this reads the whole YES
    survival ladder at each timestamp and interpolates the strike where
    P(outcome > strike) = 0.5. As contracts roll their strikes over time the
    median tracks continuously, so coverage is NOT collapsed to one strike's
    lifetime (fixes the single-strike window-collapse limitation).

    Returns a Series in the underlying's units (e.g. % or index level).
    """
    conid = resolve_conid(markets, market_name)
    sub = history[(history.underlying_conid == conid) & (history.side == "Y")].copy()
    if sub.empty:
        raise ValueError(f"no YES contract_history for {market_name!r}")

    lo, hi = band
    sub = sub[(sub["avg"] >= lo) & (sub["avg"] <= hi)]
    sub = sub.dropna(subset=["strike", "avg"])
    if sub.empty:
        raise ValueError(f"all {market_name!r} observations outside band {band}")

    med = sub.groupby("ts_utc").apply(
        lambda g: _ladder_median(g["strike"].to_numpy(), g["avg"].to_numpy()),
        include_groups=False,
    )
    med = med.sort_index()
    med = med[~med.index.duplicated(keep="last")]
    return med.resample(freq).ffill().rename(market_name)


def implied_series(
    history: pd.DataFrame,
    markets: pd.DataFrame,
    market_name: str,
    kind: str = "median",
    band: tuple[float, float] = DEFAULT_BAND,
    freq: str = DEFAULT_FREQ,
) -> pd.Series:
    """Dispatch to the chosen implied-signal extractor.

    kind="median" (default) -> implied_median_series (full-window coverage).
    kind="prob"             -> implied_prob_series (single most-liquid strike).
    """
    if kind == "median":
        return implied_median_series(history, markets, market_name, band, freq)
    if kind == "prob":
        return implied_prob_series(history, markets, market_name, band, freq)
    raise ValueError(f"unknown signal kind {kind!r} (use 'median' or 'prob')")


def align(*series: pd.Series, freq: str = DEFAULT_FREQ) -> pd.DataFrame:
    """Join several implied-prob series on the common grid, drop leading NaNs."""
    df = pd.concat(series, axis=1)
    return df.dropna()


def zscore(s: pd.Series) -> pd.Series:
    """Whole-window z-score. NOTE: uses the full series mean/std -> look-ahead.
    Fine for exploration; use zscore_rolling for any predictive/backtest claim."""
    sd = s.std(ddof=0)
    if sd == 0 or pd.isna(sd):
        return s * 0.0
    return (s - s.mean()) / sd


def zscore_rolling(s: pd.Series, window: int, min_periods: int | None = None) -> pd.Series:
    """Causal (trailing) z-score: at each point uses only the prior `window`
    observations. No look-ahead. Leading warmup points are NaN.

    Required for validation/backtest: the whole-window zscore() defines
    'extreme' using future data, which mechanically guarantees mean reversion.
    """
    min_periods = min_periods or window
    mean = s.rolling(window, min_periods=min_periods).mean()
    sd = s.rolling(window, min_periods=min_periods).std(ddof=0)
    return (s - mean) / sd.replace(0.0, np.nan)


def load_mappings(path: Path = MAPPINGS_PATH) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def load_fred_series(series_id: str, db: Path = FRED_DB) -> pd.Series:
    """Realized macro series from the FRED sqlite, as a tz-aware Series."""
    import sqlite3

    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT obs_date, value FROM macro_observations "
            "WHERE series_id=? AND value IS NOT NULL ORDER BY obs_date",
            (series_id,),
        ).fetchall()
    if not rows:
        raise ValueError(f"no FRED data for {series_id!r}")
    idx = pd.to_datetime([r[0] for r in rows], utc=True)
    return pd.Series([r[1] for r in rows], index=idx, name=series_id)
