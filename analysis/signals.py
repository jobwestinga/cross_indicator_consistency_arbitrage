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
EXPIRY_ACTIVITY_WINDOW = "7D" # trailing window for causal most-active-expiry choice

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
    if len(exps) == 0:
        return sub
    cut_i8 = (exps - pd.Timedelta(days=roll_days)).asi8
    ts_i8 = pd.DatetimeIndex(sub["ts_utc"]).asi8
    idx = np.searchsorted(cut_i8, ts_i8, side="right")  # ts==cutoff -> already rolled
    valid = idx < len(exps)     # past the last cutoff -> everything has rolled
    front_i8 = exps.asi8[np.minimum(idx, len(exps) - 1)]
    exp_i8 = pd.DatetimeIndex(sub["expiration"]).asi8
    return sub[valid & (exp_i8 == front_i8)]


def active_expiry_filter(sub: pd.DataFrame, roll_days: int = DEFAULT_ROLL_DAYS,
                         window: str = EXPIRY_ACTIVITY_WINDOW) -> pd.DataFrame:
    """Keep rows of the MOST ACTIVELY TRADED unexpired expiry at each timestamp.

    Motivation (measured 2026-08-03): `front_expiry_filter` keeps only the
    nearest unexpired expiry, which on this venue discards 52-96% of prints —
    US Real GDP drops 269 rows to 10 (killing `okun`), Fed Funds keeps 17%.
    Traders concentrate in a non-front contract, and a front expiry that goes
    quiet mid-life kills the signal even while a later expiry trades actively.

    This selects, at each timestamp, the eligible expiry (not yet inside its
    `roll_days` buffer) with the most prints in the trailing `window`. It is:
      - CAUSAL: trailing counts only, no future liquidity information [A4];
      - NON-MIXING: exactly one expiry per timestamp, so the A1 expiry-mixing
        bug cannot reappear;
      - ties -> the nearest expiry, so it degrades to front-expiry behaviour
        when activity is uniform.

    CAVEAT: like the reference-contract switch in `implied_prob_frame` [A11],
    switching expiry JUMPS the series — a September-CPI median measures a
    different reference month than August's. Use `tracked_expiry_series` to
    get the tracked expiry per bar and treat switches as roll boundaries.
    """
    exps = pd.DatetimeIndex(sub["expiration"].dropna().unique()).sort_values()
    if len(exps) == 0:
        return sub
    counts = sub.pivot_table(index="ts_utc", columns="expiration",
                             values="avg", aggfunc="size").reindex(columns=exps)
    activity = counts.rolling(window, min_periods=1).sum()
    # eligibility: the expiry's roll cutoff must still be ahead of the bar
    cut = (exps - pd.Timedelta(days=roll_days)).asi8
    ts_i8 = pd.DatetimeIndex(activity.index).asi8
    eligible = ts_i8[:, None] < cut[None, :]
    masked = activity.where(eligible)
    # bars where NO eligible expiry printed inside the window have nothing to
    # track; drop them first (idxmax raises on an all-NA row).
    masked = masked.dropna(how="all")
    if masked.empty:
        return sub.iloc[0:0]
    chosen = masked.idxmax(axis=1)          # ties -> first (nearest) expiry
    pick = sub["ts_utc"].map(chosen)
    return sub[pick.notna() & (sub["expiration"] == pick)]


def tracked_expiry_series(history: pd.DataFrame, markets: pd.DataFrame,
                          market_name: str, expiry_mode: str = "front",
                          band: tuple[float, float] = DEFAULT_BAND,
                          freq: str = DEFAULT_FREQ,
                          ffill_limit: int = DEFAULT_FFILL_LIMIT,
                          roll_days: int = DEFAULT_ROLL_DAYS,
                          min_volume: int = 0) -> pd.Series:
    """Which expiry the signal tracks per bar (roll-boundary diagnostics)."""
    sub = _prepare_market(history, markets, market_name, band, roll_days,
                          min_volume, expiry_mode=expiry_mode)
    exp = sub.set_index("ts_utc")["expiration"]
    exp = exp[~exp.index.duplicated(keep="last")].sort_index()
    return exp.resample(freq).last().ffill(limit=ffill_limit).rename(market_name)


def _prepare_market(history: pd.DataFrame, markets: pd.DataFrame, market_name: str,
                    band: tuple[float, float], roll_days: int,
                    min_volume: int, expiry_mode: str = "front") -> pd.DataFrame:
    """Common preamble: one market's YES rows, band-filtered, one expiry per bar.

    expiry_mode="front"  -> nearest unexpired expiry (default, A1 behaviour)
    expiry_mode="active" -> most-actively-traded unexpired expiry (causal)
    """
    if expiry_mode not in ("front", "active"):
        raise ValueError(f"unknown expiry_mode {expiry_mode!r} (use 'front' or 'active')")
    conid = resolve_conid(markets, market_name)
    sub = history[(history.underlying_conid == conid) & (history.side == "Y")]
    if min_volume > 0:
        sub = sub[sub["volume"] >= min_volume]
    lo, hi = band
    sub = sub[(sub["avg"] >= lo) & (sub["avg"] <= hi)]
    if sub.empty:
        raise ValueError(f"no usable YES contract_history for {market_name!r} "
                         f"(band={band}, min_volume={min_volume})")
    sub = (front_expiry_filter(sub, roll_days) if expiry_mode == "front"
           else active_expiry_filter(sub, roll_days))
    if sub.empty:
        raise ValueError(f"no {expiry_mode}-expiry observations for {market_name!r}")
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
    agg: str = "last",
    expiry_mode: str = "front",
) -> pd.DataFrame:
    """Front-expiry implied probability from one reference contract, plus the
    reference conid per bar.

    The reference contract at each timestamp is the one with the most
    observations in the trailing REF_ACTIVITY_WINDOW — causal (no full-window
    liquidity look-ahead [A4]) and adaptive when activity migrates strikes.

    agg="last" (default): each grid bar carries the reference contract's last
    (possibly mark) price. agg="vwap" [A5]: each grid bar is the reference
    contract's volume-weighted price over its actual prints in that bar;
    zero-volume bars go NaN (then bounded ffill) — 87% of raw bars are
    volume-0 marks, so this is the marks-sensitivity view of the same series.

    Columns: `value` (price of the reference contract, (0,1)) and `ref_conid`.
    The stitched value series JUMPS when the reference switches (expiry roll or
    activity migration) — a position cannot be held across a switch, so any
    execution logic must exit before the ref_conid changes.
    """
    if agg not in ("last", "vwap"):
        raise ValueError(f"unknown agg {agg!r} (use 'last' or 'vwap')")
    sub = _prepare_market(history, markets, market_name, band, roll_days, min_volume,
                          expiry_mode=expiry_mode)
    px = sub.pivot_table(index="ts_utc", columns="conid", values="avg", aggfunc="last")
    activity = px.notna().rolling(REF_ACTIVITY_WINDOW).sum()
    ref_col = activity.to_numpy().argmax(axis=1)          # ties -> lowest conid
    conids = np.asarray(px.columns)[ref_col].astype(float)
    rows = np.arange(len(px))
    if agg == "vwap":
        vol = (sub.pivot_table(index="ts_utc", columns="conid", values="volume",
                               aggfunc="sum").reindex_like(px))
        raw = pd.Series(px.to_numpy()[rows, ref_col], index=px.index)
        w = pd.Series(vol.to_numpy()[rows, ref_col], index=px.index).fillna(0.0)
        num = (raw * w).resample(freq).sum()
        den = w.resample(freq).sum()
        value = num / den.replace(0.0, np.nan)
        ref = (pd.Series(conids, index=px.index)
               [~px.index.duplicated(keep="last")].resample(freq).last())
        return (pd.DataFrame({"value": value, "ref_conid": ref})
                .ffill(limit=ffill_limit))
    carried = px.ffill()                                   # per-contract last value
    vals = carried.to_numpy()[rows, ref_col]
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
    expiry_mode: str = "front",
) -> pd.Series:
    """implied_prob_frame's `value` column as a named Series (signal use)."""
    frame = implied_prob_frame(history, markets, market_name, band=band, freq=freq,
                               ffill_limit=ffill_limit, roll_days=roll_days,
                               min_volume=min_volume, expiry_mode=expiry_mode)
    return frame["value"].rename(market_name)


def _ladder_quantile(strikes: np.ndarray, survival: np.ndarray,
                     level: float = 0.5) -> float:
    """Strike x where the survival curve crosses `level`, by linear
    interpolation. survival = P(X > strike); level=0.5 gives the median,
    level=1-p gives the p-quantile of X [C4]."""
    order = np.argsort(strikes)
    x = strikes[order]
    s = survival[order]  # ideally decreasing in strike
    if len(x) == 1:
        return float(x[0])
    if s.min() > level:    # crossing sits above the highest strike we observe
        return float(x[-1])
    if s.max() < level:    # crossing sits below the lowest strike we observe
        return float(x[0])
    for k in range(len(x) - 1):       # first adjacent pair bracketing `level`
        s1, s2 = s[k], s[k + 1]
        if (s1 - level) * (s2 - level) <= 0 and s1 != s2:
            return float(x[k] + (level - s1) * (x[k + 1] - x[k]) / (s2 - s1))
    return float(x[np.argmin(np.abs(s - level))])


def _ladder_median(strikes: np.ndarray, survival: np.ndarray) -> float:
    """Implied median outcome from a survival ladder (see _ladder_quantile)."""
    return _ladder_quantile(strikes, survival, 0.5)


def implied_quantile_series(
    history: pd.DataFrame,
    markets: pd.DataFrame,
    market_name: str,
    p: float = 0.5,
    band: tuple[float, float] = DEFAULT_BAND,
    freq: str = DEFAULT_FREQ,
    ffill_limit: int = DEFAULT_FFILL_LIMIT,
    roll_days: int = DEFAULT_ROLL_DAYS,
    min_volume: int = 0,
    expiry_mode: str = "front",
) -> pd.Series:
    """Market-implied p-QUANTILE of the outcome over time, front expiry only
    [A1, C4]. p=0.5 is the median; p=0.25/0.75 give the implied IQR.

    Reads the front expiry's YES survival ladder at each timestamp and
    interpolates the strike where P(outcome > strike) = 1 - p. Strikes roll
    as expiries roll, so coverage is continuous; values are in the
    underlying's units (e.g. % or index level).
    """
    sub = _prepare_market(history, markets, market_name, band, roll_days, min_volume,
                          expiry_mode=expiry_mode)
    sub = sub.dropna(subset=["strike"])
    if sub.empty:
        raise ValueError(f"no strike data for {market_name!r}")
    level = 1.0 - p
    q = sub.groupby("ts_utc").apply(
        lambda g: _ladder_quantile(g["strike"].to_numpy(), g["avg"].to_numpy(), level),
        include_groups=False,
    )
    return _finalize(q, market_name, freq, ffill_limit)


def implied_median_series(
    history: pd.DataFrame,
    markets: pd.DataFrame,
    market_name: str,
    band: tuple[float, float] = DEFAULT_BAND,
    freq: str = DEFAULT_FREQ,
    ffill_limit: int = DEFAULT_FFILL_LIMIT,
    roll_days: int = DEFAULT_ROLL_DAYS,
    min_volume: int = 0,
    expiry_mode: str = "front",
) -> pd.Series:
    """Market-implied MEDIAN outcome (implied_quantile_series at p=0.5)."""
    return implied_quantile_series(history, markets, market_name, p=0.5,
                                   band=band, freq=freq, ffill_limit=ffill_limit,
                                   roll_days=roll_days, min_volume=min_volume,
                                   expiry_mode=expiry_mode)


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
    expiry_mode: str = "front",
) -> pd.Series:
    """Dispatch to the chosen implied-signal extractor.

    kind="median" (default) -> implied_median_series (full survival ladder).
    kind="prob"             -> implied_prob_series (single reference contract).
    """
    if kind not in ("median", "prob"):
        raise ValueError(f"unknown signal kind {kind!r} (use 'median' or 'prob')")
    fn = implied_median_series if kind == "median" else implied_prob_series
    return fn(history, markets, market_name, band=band, freq=freq,
              ffill_limit=ffill_limit, roll_days=roll_days, min_volume=min_volume,
              expiry_mode=expiry_mode)


# --------------------------------------------------------------------------- #
# implied-series disk cache [F5]
# --------------------------------------------------------------------------- #
SIGNALS_CACHE_VERSION = "sig-v1"   # bump whenever signal construction changes


def _sig_cache_path(zip_path: Path, market_name: str, what: str, kind: str,
                    band, freq, ffill_limit, roll_days, min_volume,
                    expiry_mode: str = "front") -> Path:
    import hashlib
    key = (f"{market_name}|{what}|{kind}|{band}|{freq}|{ffill_limit}|{roll_days}"
           f"|{min_volume}|{expiry_mode}")
    digest = hashlib.md5(key.encode()).hexdigest()[:12]
    return (CACHE_DIR / f"{zip_path.stem}-{int(zip_path.stat().st_mtime)}"
                        f"-{SIGNALS_CACHE_VERSION}-{digest}.pkl")


def cached_implied_series(
    zip_path: Path | None,
    history: pd.DataFrame,
    markets: pd.DataFrame,
    market_name: str,
    kind: str = "median",
    band: tuple[float, float] = DEFAULT_BAND,
    freq: str = DEFAULT_FREQ,
    ffill_limit: int = DEFAULT_FFILL_LIMIT,
    roll_days: int = DEFAULT_ROLL_DAYS,
    min_volume: int = 0,
    expiry_mode: str = "front",
) -> pd.Series:
    """implied_series with a per-bundle disk cache [F5].

    Every pipeline script recomputes the same implied series from the same
    bundle (run_all spawns one process per step); this memoizes them on disk,
    keyed by bundle name+mtime and every construction parameter. Only
    repo-root bundles are cached (same rule as the history cache); pass
    zip_path=None to bypass. `history` must be the UNFILTERED load_history
    result for that bundle.
    """
    use = zip_path is not None and Path(zip_path).resolve().parent == REPO_ROOT
    cache = None
    if use:
        cache = _sig_cache_path(Path(zip_path), market_name, "series", kind,
                                band, freq, ffill_limit, roll_days, min_volume,
                                expiry_mode)
        if cache.exists():
            return pd.read_pickle(cache)
    s = implied_series(history, markets, market_name, kind=kind, band=band,
                       freq=freq, ffill_limit=ffill_limit, roll_days=roll_days,
                       min_volume=min_volume, expiry_mode=expiry_mode)
    if cache is not None:
        CACHE_DIR.mkdir(exist_ok=True)
        s.to_pickle(cache)
    return s


def cached_implied_prob_frame(
    zip_path: Path | None,
    history: pd.DataFrame,
    markets: pd.DataFrame,
    market_name: str,
    band: tuple[float, float] = DEFAULT_BAND,
    freq: str = DEFAULT_FREQ,
    ffill_limit: int = DEFAULT_FFILL_LIMIT,
    roll_days: int = DEFAULT_ROLL_DAYS,
    min_volume: int = 0,
    agg: str = "last",
    expiry_mode: str = "front",
) -> pd.DataFrame:
    """implied_prob_frame with the same per-bundle disk cache [F5]."""
    use = zip_path is not None and Path(zip_path).resolve().parent == REPO_ROOT
    cache = None
    if use:
        cache = _sig_cache_path(Path(zip_path), market_name, f"frame-{agg}", "prob",
                                band, freq, ffill_limit, roll_days, min_volume,
                                expiry_mode)
        if cache.exists():
            return pd.read_pickle(cache)
    f = implied_prob_frame(history, markets, market_name, band=band, freq=freq,
                           ffill_limit=ffill_limit, roll_days=roll_days,
                           min_volume=min_volume, agg=agg, expiry_mode=expiry_mode)
    if cache is not None:
        CACHE_DIR.mkdir(exist_ok=True)
        f.to_pickle(cache)
    return f


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


def load_fred_series(series_id: str, db: Path = FRED_DB,
                     vintage: str = "latest") -> pd.Series:
    """Realized macro series from the FRED sqlite, as a tz-aware Series.

    vintage="latest" (default): the current revised values.
    vintage="initial": the FIRST-published value where collected (A7,
    ALFRED output_type=4), falling back to the revised value — what a venue
    that settles on the announcement actually pays on.

    NOTE (A7): the index is the REFERENCE PERIOD (obs_date), not the release
    date. A May CPI value only became public ~mid-June. Fine for context and
    after-the-fact comparison; NOT usable as a causal conditioning variable
    (initial_release_date is stored for vintage-aware work).
    """
    import sqlite3

    col = "COALESCE(value_initial, value)" if vintage == "initial" else "value"
    with sqlite3.connect(db) as conn:
        try:
            rows = conn.execute(
                f"SELECT obs_date, {col} FROM macro_observations "
                "WHERE series_id=? AND value IS NOT NULL ORDER BY obs_date",
                (series_id,),
            ).fetchall()
        except sqlite3.OperationalError:   # pre-A7 DB without vintage columns
            rows = conn.execute(
                "SELECT obs_date, value FROM macro_observations "
                "WHERE series_id=? AND value IS NOT NULL ORDER BY obs_date",
                (series_id,),
            ).fetchall()
    if not rows:
        raise ValueError(f"no FRED data for {series_id!r}")
    idx = pd.to_datetime([r[0] for r in rows], utc=True)
    return pd.Series([r[1] for r in rows], index=idx, name=series_id)
