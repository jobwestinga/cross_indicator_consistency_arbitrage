# Cross-Indicator Consistency Arbitrage

Research project: **can macroeconomic identities be traded as consistency
constraints across prediction markets?**

IBKR ForecastTrader prices hundreds of macro event markets (CPI, unemployment,
Fed funds, GDP, recession, FX, ...). Economic identities — Phillips curve,
Okun's law, Taylor rule, Sahm rule, Beveridge curve, UIP, survey/aggregation
coherence — constrain how these markets may move *relative to each other*.
The hypothesis: when two markets price a combination the identity forbids, the
inconsistency closes, and closing it is tradeable. We also test the more
primitive question: does the venue even price single markets coherently
(survival-ladder monotonicity, YES/NO parity)?

> **Status: EXPLORATORY.** No proven edge. Current verdicts live in
> `analysis/report/REPORT.md` (regenerate with `run_all.py`) and in
> [analysis/README.md](analysis/README.md). The improvement backlog is
> [docs/IMPROVEMENTS.md](docs/IMPROVEMENTS.md).

## Architecture

```
VPS (systemd timers)                    local (this repo)
┌────────────────────────┐   export    ┌──────────────────────────────────┐
│ forecast_collector     │ ──────────► │ forecast_analysis_dataset_*.zip  │
│ ForecastTrader HTTP    │   (zip)     │ analysis/  research pipeline     │
│ -> PostgreSQL          │             │ FRED -> analysis/macro/fred.sqlite│
└────────────────────────┘             └──────────────────────────────────┘
```

- **Collector** (`src/forecast_collector/`): production service on the VPS,
  collecting market structure, probabilities and price history continuously.
  Operations manual: [docs/COLLECTOR.md](docs/COLLECTOR.md); private host
  details: `RUNBOOK.local.md` (gitignored).
- **Analysis** (`analysis/`): the research pipeline. Rules are declared in
  [analysis/mappings.yaml](analysis/mappings.yaml); the engine
  ([analysis/rules.py](analysis/rules.py)) turns them into scores, event-study
  validations and costed toy backtests.

## Research quickstart

```bash
pip install -r requirements-analysis.txt

# data freshness audit (bundle + FRED)
python3 analysis/check_readiness.py

# everything: readiness -> static-arb scan -> all rules -> report
python3 analysis/run_all.py --grid

# single rule, step by step
python3 analysis/run_consistency.py --rule taylor
python3 analysis/validate_consistency.py --rule taylor --grid
python3 analysis/backtest.py --rule taylor --cost 0.02
python3 analysis/arbitrage_scan.py
```

Refreshing data (export from VPS, FRED pull): see `RUNBOOK.local.md`.

## Layout

```
.
├── analysis/            research pipeline (see analysis/README.md)
│   ├── mappings.yaml    rule definitions: markets + FRED + score logic
│   ├── rules.py         rule engine (scores, flags, panels)
│   ├── signals.py       implied-series extraction from the bundle
│   ├── run_all.py       one-command pipeline -> analysis/report/REPORT.md
│   └── ...
├── docs/                collector manual, improvement backlog, references
├── src/forecast_collector/   VPS data collector (Python package)
├── sql/                 Postgres schema migrations
├── tests/               collector + analysis test suites
└── deploy/              GitHub-Actions deploy + systemd units
```

## Tests

```bash
python3 -m pytest tests/ -q
```

Analysis tests run everywhere (synthetic bundle fixture); collector tests
skip automatically when server-side deps (psycopg, pydantic-settings, ...)
are not installed locally.
