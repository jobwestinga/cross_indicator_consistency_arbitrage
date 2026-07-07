"""Collect macro indicator series from the FRED API into a local SQLite file.

Standalone: no Postgres, no third-party deps (stdlib urllib + sqlite3 only).
Reads FRED_API_KEY from the environment or from a local .env file.

These series are the ground-truth macro outcomes used to backtest the
ForecastTrader consistency-arbitrage signals (see strats.txt). Each FRED id is
mapped to the ForecastTrader market(s) it validates.

Usage:
    python3 analysis/collect_fred.py            # fetch all series -> SQLite
    python3 analysis/collect_fred.py --dry-run  # list series, write nothing
    python3 analysis/collect_fred.py --series UNRATE CPIAUCSL   # subset

Output:
    analysis/macro/fred.sqlite   table: macro_observations
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = REPO_ROOT / ".env"
DB_PATH = Path(__file__).resolve().parent / "macro" / "fred.sqlite"
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

# FRED series id -> (human label, ForecastTrader rule/market it feeds).
SERIES: dict[str, tuple[str, str]] = {
    "CPIAUCSL": ("CPI (headline)", "Taylor/Phillips - US CPI Yearly"),
    "CPILFESL": ("Core CPI", "Phillips - US Core CPI"),
    "PCEPI": ("PCE price index", "Taylor - PCE"),
    "PCEPILFE": ("Core PCE", "Taylor - US Core PCE"),
    "UNRATE": ("Unemployment rate", "Phillips/Okun/Sahm/Beveridge - US Unemployment"),
    "PAYEMS": ("Nonfarm payrolls", "labor - US Payroll Employment"),
    "ICSA": ("Initial jobless claims", "labor - US Initial Jobless Claims"),
    "JTSJOL": ("JOLTS job openings", "Beveridge - JOLTS"),
    "GDPC1": ("Real GDP", "Okun - US Real GDP"),
    "FEDFUNDS": ("Fed funds rate (monthly)", "Taylor/Evans/UIP - US Fed Funds"),
    "DFF": ("Fed funds rate (daily)", "Taylor/Evans - daily policy rate"),
    "DEXJPUS": ("USD/JPY exchange rate", "UIP - US Dollar to Japanese Yen"),
    "USREC": ("US recession indicator (NBER)", "Sahm - US Recession"),
    "LRUNTTTTCAM156S": ("Canada unemployment rate", "Okun (Canada) - Canada Unemployment"),
    "NGDPRSAXDCCAQ": ("Canada real GDP", "Okun (Canada) - Canada Real GDP"),
}


def load_api_key() -> str:
    key = os.environ.get("FRED_API_KEY")
    if not key and ENV_PATH.exists():
        for line in ENV_PATH.read_text().splitlines():
            line = line.strip()
            if line.startswith("FRED_API_KEY="):
                key = line.split("=", 1)[1].strip().strip('"').strip("'")
                break
    if not key:
        sys.exit("FRED_API_KEY not found in environment or .env")
    if len(key) != 32 or not key.isalnum() or not key.islower():
        sys.exit(f"FRED_API_KEY looks malformed (must be 32-char lowercase alnum): {key!r}")
    return key


def fetch_series(series_id: str, key: str) -> list[dict]:
    resp = requests.get(
        FRED_BASE,
        params={"series_id": series_id, "api_key": key, "file_type": "json"},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    if "observations" not in payload:
        raise RuntimeError(f"{series_id}: unexpected response {payload}")
    return payload["observations"]


def parse_value(raw: object) -> float | None:
    """FRED encodes missing observations as '.'; map those (and blanks) to None."""
    if raw in (".", "", None):
        return None
    return float(raw)


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS macro_observations (
            series_id       TEXT NOT NULL,
            label           TEXT,
            mapping         TEXT,
            obs_date        TEXT NOT NULL,
            value           REAL,
            realtime_start  TEXT,
            realtime_end    TEXT,
            fetched_at      TEXT NOT NULL,
            PRIMARY KEY (series_id, obs_date)
        )
        """
    )
    conn.commit()


def store(conn: sqlite3.Connection, series_id: str, observations: list[dict]) -> int:
    label, mapping = SERIES.get(series_id, (series_id, ""))
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for obs in observations:
        value = parse_value(obs.get("value", "."))
        rows.append((
            series_id, label, mapping, obs["date"], value,
            obs.get("realtime_start"), obs.get("realtime_end"), now,
        ))
    conn.executemany(
        """
        INSERT OR REPLACE INTO macro_observations
        (series_id, label, mapping, obs_date, value, realtime_start, realtime_end, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect FRED macro series into SQLite.")
    parser.add_argument("--series", nargs="+", default=None,
                        help="Subset of FRED ids to fetch (default: all mapped).")
    parser.add_argument("--dry-run", action="store_true",
                        help="List series and exit without fetching or writing.")
    args = parser.parse_args()

    ids = args.series or list(SERIES)

    if args.dry_run:
        print(f"Would fetch {len(ids)} series into {DB_PATH}:")
        for sid in ids:
            label, mapping = SERIES.get(sid, (sid, "(unmapped)"))
            print(f"  {sid:10s} {label:28s} -> {mapping}")
        return

    key = load_api_key()
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    ensure_schema(conn)

    total = 0
    for sid in ids:
        try:
            obs = fetch_series(sid, key)
            n = store(conn, sid, obs)
            total += n
            latest = next((o for o in reversed(obs) if o.get("value") not in (".", "")), None)
            tail = f"latest {latest['date']}={latest['value']}" if latest else "no data"
            print(f"  {sid:10s} {n:6d} obs  ({tail})")
        except Exception as exc:  # noqa: BLE001 - report and continue
            print(f"  {sid:10s} ERROR: {exc}")

    conn.close()
    print(f"\nWrote {total:,} observations to {DB_PATH}")


if __name__ == "__main__":
    main()
