"""Reusable signal extraction for consistency-arbitrage analysis.

EXPLORATORY / TESTING code. The strategy is unproven; this module turns the raw
ForecastTrader bundle into comparable market-implied series so the economic
identities in mappings.yaml can be tested.

Data/methodology decisions (see docs/IMPROVEMENTS.md for the audit trail):
  - The canonical implied-probability signal is the IBKR `contract_history`
    table (avg traded price). `projected_probabilities` is NOT used for signal:
    it has gaps (9-day outage Jun 4-12 2026) and is empty for some markets.
  - A1: survival ladders are built PER EXPIRATION and the signal tracks the
    front (nearest unexpired) expiry with a roll buffer. Pooling expiries
    (the original implementation) mixed e.g. April-CPI and June-CPI contracts
    into one fictitious distribution.
  - A2: rows duplicated across `period_requested` values (~43% of the bundle,
    4.9% of them disagreeing) are deduplicated preferring the finer chart_step.
  - A3: forward-fill is bounded (`ffill_limit` bars); a market that stops
    printing goes NaN instead of flatlining forever, so trailing z-scores
    cannot spike on stale segments.
  - A4: the single-strike "prob" signal picks its reference contract causally
    (most observations in a trailing window), never from full-window liquidity.
  - A5: 87% of history bars have volume == 0 (carried marks, not trades).
    Loaders keep `volume` and every series builder accepts `min_volume` so
    mark-sensitivity can be measured.
"""

from __future__ import annotations

import os
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
ANALYSIS_DIR = Path(__file__).resolve().parent


def out_base() -> Path:
    """Root for generated outputs. ANALYSIS_OUT_DIR overrides (tests use this
    so script runs never clobber real results under analysis/)."""
    env = os.environ.get("ANALYSIS_OUT_DIR")
    return Path(env) if env else ANALYSIS_DIR
MAPPINGS_PATH = ANALYSIS_DIR / "mappings.yaml"
FRED_DB = ANALYSIS_DIR / "macro" / "fred.sqlite"
CACHE_DIR = ANALYSIS_DIR / "cache"

DEFAULT_BAND = (0.001, 0.999)
DEFAULT_FREQ = "1h"
DEFAULT_FFILL_LIMIT = 48      # bars on the resampled grid (= 48h at 1h)
DEFAULT_ROLL_DAYS = 2         # stop tracking an expiry this many days before it settles
REF_ACTIVITY_WINDOW = "7D"    # trailing window for causal reference-contract choice

HISTORY_COLS = ["underlying_conid", "market_name", "conid", "side", "strike",
                "expiration", "ts_utc", "avg", "volume", "chart_step"]


# --------------------------------------------------------------------------- #
# bundle loading
# --------------------------------------------------------------------------- #
def find_latest_zip() -> Path:
    cands = sorted(REPO_ROOT.glob("forecast_analysis_dataset_*.zip"))
    if not cands:
        raise FileNotFoundError("No forecast_analysis_dataset_*.zip in repo root.")
    return cands[-1]


def load_markets(zip_path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path) as z:
        return pd.read_csv(z.open("markets.csv"), usecols=["underlying_conid", "market_name"])


def load_history(zip_path: Path, use_cache: bool = True) -> pd.DataFrame:
    """Load contract_history with the columns the signals need, cleaned.

    Cleaning (applies to every downstream script):
      - ts_utc parsed robustly (unparseable rows dropped) [A6]
      - expiration (int YYYYMMDD) parsed to tz-aware timestamps
      - (conid, ts) duplicates across period_requested resolved by preferring
        the finer chart_step, deterministically [A2]

    Results are cached as a pickle keyed by the zip's name+mtime (analysis/
    cache/, gitignored): ~30s of CSV parsing drops to sub-second reloads.
    """
    # only cache repo-root bundles (not tmp test fixtures)
    use_cache = use_cache and zip_path.resolve().parent == REPO_ROOT
    cache = CACHE_DIR / f"{zip_path.stem}-{int(zip_path.stat().st_mtime)}-hist-v2.pkl"
    if use_cache and cache.exists():
        return pd.read_pickle(cache)

    with zipfile.ZipFile(zip_path) as z:
        h = pd.read_csv(z.open("contract_history.csv"), usecols=HISTORY_COLS,
                        dtype={"ts_utc": str})
    h["ts_utc"] = pd.to_datetime(h["ts_utc"], utc=True, errors="coerce")
    h = h.dropna(subset=["ts_utc", "avg"])
    h["expiration"] = pd.to_datetime(h["expiration"].astype(str), format="%Y%m%d",
                                     utc=True, errors="coerce")
    h = (h.sort_values("chart_step")
          .drop_duplicates(["conid", "ts_utc"], keep="first")
          .drop(columns=["chart_step"])
          .sort_values("ts_utc")
          .reset_index(drop=True))

    if use_cache:
        CACHE_DIR.mkdir(exist_ok=True)
        h.to_pickle(cache)
    return h


def resolve_conid(markets: pd.DataFrame, market_name: str) -> int:
    hit = markets.loc[markets.market_name == market_name, "underlying_conid"]
    if hit.empty:
        raise KeyError(f"market_name not found in bundle: {market_name!r}")
    return int(hit.iloc[0])


# --------------------------------------------------------------------------- #
# expiry handling
# --------------------------------------------------------------------------- #
def front_expiry_filter(sub: pd.DataFrame, roll_days: int = DEFAULT_ROLL_DAYS) -> pd.DataFrame:
    """Keep only rows belonging to the front expiry at each timestamp.

    Front = the nearest expiration whose (expiration - roll_days) is still
    ahead of the row's timestamp. The roll buffer avoids the settlement pin
    (prices collapsing to 0/1 right before resolution).
    """
    exps = pd.DatetimeIndex(sub["expiration"].dropna().unique()).sort_values()
    if len(exps) <= 1:
        return sub
    cut_i8 = (exps - pd.Timedelta(days=roll_days)).asi8
    ts_i8 = pd.DatetimeIndex(sub["ts_utc"]).asi8
    idx = np.searchsorted(cut_i8, ts_i8, side="right")  # ts==cutoff -> already rolled
    valid = idx < len(exps)
    front_i8 = exps.asi8[np.minimum(idx, len(exps) - 1)]
    exp_i8 = pd.DatetimeIndex(sub["expiration"]).asi8
    return sub[valid & (exp_i8 == front_i8)]


def _prepare_market(history: pd.DataFrame, markets: pd.DataFrame, market_name: str,
                    band: tuple[float, float], roll_days: int,
                    min_volume: int) -> pd.DataFrame:
    """Common preamble: one market's YES rows, band-filtered, front expiry only."""
    conid = resolve_conid(markets, market_name)
    sub = history[(history.underlying_conid == conid) & (history.side == "Y")]
    if min_volume > 0:
        sub = sub[sub["volume"] >= min_volume]
    lo, hi = band
    sub = sub[(sub["avg"] >= lo) & (sub["avg"] <= hi)]
    if sub.empty:
        raise ValueError(f"no usable YES contract_history for {market_name!r} "
                         f"(band={band}, min_volume={min_volume})")
    sub = front_expiry_filter(sub, roll_days)
    if sub.empty:
        raise ValueError(f"no front-expiry observations for {market_name!r}")
    return sub


def _finalize(values: pd.Series, name: str, freq: str, ffill_limit: int) -> pd.Series:
    """Common postamble: common grid, bounded forward-fill [A3]."""
    values = values.sort_index()
    values = values[~values.index.duplicated(keep="last")]
    return values.resample(freq).last().ffill(limit=ffill_limit).rename(name)


# --------------------------------------------------------------------------- #
# implied series
# --------------------------------------------------------------------------- #
def implied_prob_frame(
    history: pd.DataFrame,
    markets: pd.DataFrame,
    market_name: str,
    band: tuple[float, float] = DEFAULT_BAND,
    freq: str = DEFAULT_FREQ,
    ffill_limit: int = DEFAULT_FFILL_LIMIT,
    roll_days: int = DEFAULT_ROLL_DAYS,
    min_volume: int = 0,
) -> pd.DataFrame:
    """Front-expiry implied probability from one reference contract, plus the
    reference conid per bar.

    The reference contract at each timestamp is the one with the most
    observations in the trailing REF_ACTIVITY_WINDOW — causal (no full-window
    liquidity look-ahead [A4]) and adaptive when activity migrates strikes.

    Columns: `value` (price of the reference contract, (0,1)) and `ref_conid`.
    The stitched value series JUMPS when the reference switches (expiry roll or
    activity migration) — a position cannot be held across a switch, so any
    execution logic must exit before the ref_conid changes.
    """
    sub = _prepare_market(history, markets, market_name, band, roll_days, min_volume)
    px = sub.pivot_table(index="ts_utc", columns="conid", values="avg", aggfunc="last")
    activity = px.notna().rolling(REF_ACTIVITY_WINDOW).sum()
    ref_col = activity.to_numpy().argmax(axis=1)          # ties -> lowest conid
    carried = px.ffill()                                   # per-contract last value
    vals = carried.to_numpy()[np.arange(len(px)), ref_col]
    conids = np.asarray(px.columns)[ref_col].astype(float)
    frame = pd.DataFrame({"value": vals, "ref_conid": conids}, index=px.index)
    frame = frame[~frame.index.duplicated(keep="last")].sort_index()
    out = frame.resample(freq).last().ffill(limit=ffill_limit)
    return out


def implied_prob_series(
    history: pd.DataFrame,
    markets: pd.DataFrame,
    market_name: str,
    band: tuple[float, float] = DEFAULT_BAND,
    freq: str = DEFAULT_FREQ,
    ffill_limit: int = DEFAULT_FFILL_LIMIT,
    roll_days: int = DEFAULT_ROLL_DAYS,
    min_volume: int = 0,
) -> pd.Series:
    """implied_prob_frame's `value` column as a named Series (signal use)."""
    frame = implied_prob_frame(history, markets, market_name, band=band, freq=freq,
                               ffill_limit=ffill_limit, roll_days=roll_days,
                               min_volume=min_volume)
    return frame["value"].rename(market_name)


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
    ffill_limit: int = DEFAULT_FFILL_LIMIT,
    roll_days: int = DEFAULT_ROLL_DAYS,
    min_volume: int = 0,
) -> pd.Series:
    """Market-implied MEDIAN outcome over time, front expiry only [A1].

    Reads the front expiry's YES survival ladder at each timestamp and
    interpolates the strike where P(outcome > strike) = 0.5. Strikes roll as
    expiries roll, so coverage is continuous; values are in the underlying's
    units (e.g. % or index level).
    """
    sub = _prepare_market(history, markets, market_name, band, roll_days, min_volume)
    sub = sub.dropna(subset=["strike"])
    if sub.empty:
        raise ValueError(f"no strike data for {market_name!r}")
    med = sub.groupby("ts_utc").apply(
        lambda g: _ladder_median(g["strike"].to_numpy(), g["avg"].to_numpy()),
        include_groups=False,
    )
    return _finalize(med, market_name, freq, ffill_limit)


def implied_series(
    history: pd.DataFrame,
    markets: pd.DataFrame,
    market_name: str,
    kind: str = "median",
    band: tuple[float, float] = DEFAULT_BAND,
    freq: str = DEFAULT_FREQ,
    ffill_limit: int = DEFAULT_FFILL_LIMIT,
    roll_days: int = DEFAULT_ROLL_DAYS,
    min_volume: int = 0,
) -> pd.Series:
    """Dispatch to the chosen implied-signal extractor.

    kind="median" (default) -> implied_median_series (full survival ladder).
    kind="prob"             -> implied_prob_series (single reference contract).
    """
    if kind not in ("median", "prob"):
        raise ValueError(f"unknown signal kind {kind!r} (use 'median' or 'prob')")
    fn = implied_median_series if kind == "median" else implied_prob_series
    return fn(history, markets, market_name, band=band, freq=freq,
              ffill_limit=ffill_limit, roll_days=roll_days, min_volume=min_volume)


# --------------------------------------------------------------------------- #
# alignment + scoring helpers
# --------------------------------------------------------------------------- #
def align(*series: pd.Series) -> pd.DataFrame:
    """Join several series on their common (already-resampled) grid and keep
    only timestamps where every series has a value."""
    return pd.concat(series, axis=1).dropna()


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


# --------------------------------------------------------------------------- #
# config + ground truth
# --------------------------------------------------------------------------- #
def load_mappings(path: Path = MAPPINGS_PATH) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def load_fred_series(series_id: str, db: Path = FRED_DB) -> pd.Series:
    """Realized macro series from the FRED sqlite, as a tz-aware Series.

    NOTE (A7): the index is the REFERENCE PERIOD (obs_date), not the release
    date. A May CPI value only became public ~mid-June. Fine for context and
    after-the-fact comparison; NOT usable as a causal conditioning variable.
    Use realtime_start (also stored) for vintage-aware work.
    """
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
