"""One-command data-readiness check for the consistency-arbitrage pipeline.

Audits both data sources before any inference/backtest run and prints a
PASS / WARN / FAIL report on three axes: reachable, fresh, expansive.

  - IBKR forecast bundle : latest forecast_analysis_dataset_*.zip (or --zip)
  - FRED macro ground truth: analysis/macro/fred.sqlite (or --fred)

Usage:
    python3 analysis/check_readiness.py
    python3 analysis/check_readiness.py --zip path/to/export.zip
    python3 analysis/check_readiness.py --max-stale-days 3

Exit code is 0 if nothing is worse than WARN, 1 if any FAIL.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import zipfile
from datetime import date, datetime, UTC
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))  # runnable from anywhere

REPO_ROOT = Path(__file__).resolve().parent.parent
FRED_DB = Path(__file__).resolve().parent / "macro" / "fred.sqlite"
TODAY = datetime.now(UTC).date()

# Markets the strategy actually trades on (strats.txt). Used for cadence checks.
KEY_MARKETS = [
    "US Fed Funds Target Rate",
    "US Consumer Price Index Yearly",
    "US Core CPI",
    "US Unemployment Rate",
    "US Dollar to Japanese Yen Exchange Rate",
]
# Daily FRED series that should be near-current; monthly/quarterly lag by design.
FRED_DAILY = {"DFF", "DEXJPUS", "ICSA"}

PASS, WARN, FAIL = "PASS", "WARN", "FAIL"
MARK = {PASS: "[ ok ]", WARN: "[warn]", FAIL: "[FAIL]"}


class Report:
    def __init__(self) -> None:
        self.worst = PASS

    def line(self, status: str, msg: str) -> None:
        order = {PASS: 0, WARN: 1, FAIL: 2}
        if order[status] > order[self.worst]:
            self.worst = status
        print(f"  {MARK[status]} {msg}")

    def section(self, title: str) -> None:
        print(f"\n=== {title} ===")


def find_latest_zip() -> Path | None:
    cands = sorted(REPO_ROOT.glob("forecast_analysis_dataset_*.zip"))
    return cands[-1] if cands else None


def check_fred(rep: Report, db_path: Path, max_daily_stale: int) -> None:
    rep.section("FRED macro ground truth")
    if not db_path.exists():
        rep.line(FAIL, f"not reachable: {db_path} missing (run collect_fred.py)")
        return
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT series_id, COUNT(*), MIN(obs_date), MAX(obs_date), "
        "SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) "
        "FROM macro_observations GROUP BY series_id ORDER BY series_id"
    ).fetchall()
    conn.close()
    if not rows:
        rep.line(FAIL, "table macro_observations empty")
        return
    rep.line(PASS, f"reachable: {len(rows)} series in {db_path.name}")
    for sid, n, first, latest, _nulls in rows:
        stale = (TODAY - date.fromisoformat(latest)).days
        if sid in FRED_DAILY:
            status = PASS if stale <= max_daily_stale + 4 else WARN
            note = "daily"
        else:
            status = PASS  # monthly/quarterly lag is expected, never fail
            note = "monthly/quarterly (lag normal)"
        rep.line(status, f"{sid:9s} n={n:>6} {first}->{latest} stale={stale}d {note}")


def check_ibkr(rep: Report, zip_path: Path | None, max_stale: int) -> None:
    rep.section("IBKR forecast bundle")
    if zip_path is None or not zip_path.exists():
        rep.line(FAIL, "not reachable: no forecast_analysis_dataset_*.zip found")
        return
    rep.line(PASS, f"reachable: {zip_path.name}")

    z = zipfile.ZipFile(zip_path)
    names = {n.rsplit("/", 1)[-1] for n in z.namelist()}
    for needed in ["markets.csv", "projected_probabilities.csv", "contract_history.csv"]:
        if needed not in names:
            rep.line(FAIL, f"missing table {needed}")
            return

    prob = pd.read_csv(z.open("projected_probabilities.csv"),
                       usecols=["collected_at", "underlying_conid"])
    prob["t"] = pd.to_datetime(prob["collected_at"])
    pmin, pmax = prob["t"].min(), prob["t"].max()
    window = (pmax - pmin).days
    stale = (TODAY - pmax.date()).days

    rep.line(PASS, f"prob rows={len(prob):,}  window={pmin.date()}->{pmax.date()} ({window}d)")
    rep.line(FAIL if stale > max_stale + 2 else (WARN if stale > max_stale else PASS),
             f"freshness: latest prob {stale}d old (threshold {max_stale}d)")

    # expansiveness: rough count of resolved macro events (monthly cadence)
    est_events = window // 30
    rep.line(PASS if est_events >= 3 else WARN,
             f"expansive: ~{est_events} monthly-event cycles in window "
             f"({'enough to test' if est_events >= 3 else 'thin - keep collecting'})")

    # cadence: gap between distinct batch times per key market
    mk = pd.read_csv(z.open("markets.csv"), usecols=["underlying_conid", "market_name"])
    ids = mk[mk.market_name.isin(KEY_MARKETS)].set_index("market_name")["underlying_conid"]
    missing_markets = [m for m in KEY_MARKETS if m not in ids.index]
    if missing_markets:
        rep.line(WARN, f"key markets absent: {missing_markets}")
    for name, cid in ids.items():
        times = prob.loc[prob.underlying_conid == cid, "t"].drop_duplicates().sort_values()
        if len(times) < 2:
            rep.line(WARN, f"{name}: <2 snapshots")
            continue
        gap = (times.diff().dt.total_seconds().dropna() / 60).median()
        rep.line(PASS if gap <= 90 else WARN,
                 f"{name}: {len(times):,} batches, median refresh {gap:.0f} min")

    # calendar holes
    days = prob["t"].dt.date
    full = pd.date_range(pmin.date(), pmax.date(), freq="D").date
    missing_days = [str(d) for d in full if d not in set(days)]
    rep.line(PASS if len(missing_days) <= 2 else WARN,
             f"coverage: {len(full) - len(missing_days)}/{len(full)} days have data"
             + (f"; missing {missing_days}" if missing_days else ""))


def check_rule_legs(rep: Report, zip_path: Path | None, max_stale: int) -> None:
    """Per-rule leg freshness: is every market a rule depends on still printing?

    A rule whose leg stops printing silently produces "no events" — which the
    validation/OOS tables would otherwise report as "too few events", i.e.
    indistinguishable from a live rule that just did not fire.

    NOTE on the contract counts: the export includes only history-backed
    contracts, so "0 traded contracts" here does NOT mean the venue delisted
    the market. Verified against the live DB (2026-07-28): US Core PCE had 78
    ACTIVE contracts upstream, every one of them returning `no_data` — listed
    but never traded. Treat this section as "is this leg still printing",
    not as a listing status.
    """
    rep.section("Rule legs (markets each rule depends on)")
    if zip_path is None or not zip_path.exists():
        rep.line(WARN, "no bundle: skipped")
        return
    try:
        import rules
        import signals as sig

        cfg = sig.load_mappings()
        markets = sig.load_markets(zip_path)
        hist = sig.load_history(zip_path)
        with zipfile.ZipFile(zip_path) as z:
            contracts = pd.read_csv(z.open("contracts.csv"),
                                    usecols=["underlying_conid", "active"])
    except Exception as exc:  # noqa: BLE001 - diagnostics must never break the audit
        rep.line(WARN, f"skipped ({type(exc).__name__}: {exc})")
        return

    active_by_conid = contracts.groupby("underlying_conid")["active"].sum()
    data_end = hist["ts_utc"].max()
    seen: dict[str, tuple[int, int]] = {}       # market -> (days stale, active contracts)
    for key in rules.implemented_rules(cfg):
        for spec in cfg["rules"][key]["indicators"].values():
            name = spec["market_name"]
            if name in seen:
                continue
            try:
                conid = sig.resolve_conid(markets, name)
            except KeyError:
                seen[name] = (-1, 0)
                continue
            rows = hist.loc[hist.underlying_conid == conid, "ts_utc"]
            stale = (int((data_end - rows.max()).total_seconds() // 86400)
                     if len(rows) else 9999)
            seen[name] = (stale, int(active_by_conid.get(conid, 0)))

    for name, (stale, n_active) in sorted(seen.items(), key=lambda kv: -kv[1][0]):
        users = [k for k in rules.implemented_rules(cfg)
                 if any(s["market_name"] == name
                        for s in cfg["rules"][k]["indicators"].values())]
        if stale < 0:
            rep.line(WARN, f"{name}: NOT IN BUNDLE (rules: {', '.join(users)})")
            continue
        if stale <= max_stale:
            rep.line(PASS, f"{name}: fresh ({stale}d), {n_active} traded contracts active")
        else:
            state = ("no traded contracts left in the bundle" if n_active == 0
                     else f"{n_active} traded contracts still active")
            rep.line(WARN, f"{name}: LAST PRINT {stale}d before data end — {state}; "
                           f"rules affected: {', '.join(users)}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Data-readiness check before inference.")
    ap.add_argument("--zip", type=Path, default=None, help="IBKR export zip (default: latest).")
    ap.add_argument("--fred", type=Path, default=FRED_DB, help="FRED sqlite path.")
    ap.add_argument("--max-stale-days", type=int, default=3,
                    help="Max acceptable age of latest IBKR prob snapshot.")
    args = ap.parse_args()

    print(f"Data-readiness check  (today={TODAY})")
    rep = Report()
    zip_path = args.zip or find_latest_zip()
    check_ibkr(rep, zip_path, args.max_stale_days)
    check_rule_legs(rep, zip_path, args.max_stale_days)
    check_fred(rep, args.fred, args.max_stale_days)

    print(f"\nOverall: {MARK[rep.worst]} {rep.worst}")
    sys.exit(1 if rep.worst == FAIL else 0)


if __name__ == "__main__":
    main()
