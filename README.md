# ForecastTrader Collector

HTTP collector for IBKR ForecastTrader public endpoints. The codebase now
supports:

- category-tree discovery for all public markets
- market structure collection for one market or all discovered markets
- contract-details enrichment
- batched open-interest collection
- projected probability collection
- history collection in `backfill` or `incremental` mode
- raw payload capture for every request
- health reporting
- host-level `systemd` timer generation

Realtime/websocket ingestion is intentionally out of scope until the HTTP
pipeline is stable in production.

## Implemented Commands

```bash
python -m forecast_collector.cli migrate
python -m forecast_collector.cli discover-markets
python -m forecast_collector.cli collect-seed-market --underlying-conid 793085688
python -m forecast_collector.cli collect-market-structures --all-discovered
python -m forecast_collector.cli collect-open-interest --all-discovered
python -m forecast_collector.cli collect-probabilities --all-discovered
python -m forecast_collector.cli collect-history --all-discovered --mode backfill
python -m forecast_collector.cli collect-history --all-discovered --mode incremental
python -m forecast_collector.cli report-health
```

The original single-market commands are preserved for debugging and regression
checks.

## Current Validation Markets

Two public markets have already been used to validate the live endpoint
behavior:

- `831072285` (`RCNET`, Northeastern US CPI)
  - useful structure canary
  - sparse upstream data is valid and should not fail collection
- `793085688` (`CBBTC`, BTC Price)
  - active canary for structure, open interest, probabilities, and history

## Quick Start

1. Create `.env` with at least:

```dotenv
DATABASE_URL=postgresql://forecast:forecast@postgres:5432/forecast
IBKR_BASE_URL=https://forecasttrader.interactivebrokers.ie
IBKR_PUBLIC_PREFIX=/tws.proxy/public
IBKR_EXCHANGE=FORECASTX
HISTORY_PERIODS=1week,1month
LOG_LEVEL=INFO
```

`HISTORY_PERIODS` accepts either comma-delimited text or JSON array form:

```dotenv
HISTORY_PERIODS=1week,1month
HISTORY_PERIODS=["1week","1month"]
```

2. Run migrations:

```bash
python -m forecast_collector.cli migrate
```

3. Discover markets:

```bash
python -m forecast_collector.cli discover-markets
```

4. Refresh structure for all discovered markets:

```bash
python -m forecast_collector.cli collect-market-structures --all-discovered
```

5. Collect time-varying data:

```bash
python -m forecast_collector.cli collect-open-interest --all-discovered
python -m forecast_collector.cli collect-probabilities --all-discovered
python -m forecast_collector.cli collect-history --all-discovered --mode incremental
```

## Docker Compose

The repository includes:

- `postgres` for storage
- `collector` for one-shot CLI jobs
- `tester` for running the test suite inside Docker

Typical usage:

```bash
docker compose up -d postgres
docker compose build collector tester
docker compose run --rm tester
docker compose run --rm collector migrate
docker compose run --rm collector discover-markets
docker compose run --rm collector collect-market-structures --all-discovered
docker compose run --rm collector collect-open-interest --all-discovered
docker compose run --rm collector collect-probabilities --all-discovered
docker compose run --rm collector collect-history --all-discovered --mode incremental
```

Run a specific test module:

```bash
docker compose run --rm tester tests/test_repository.py -q
```

## Scheduling

Host-level `systemd` timers are the default scheduling model. Generate unit
files with:

```bash
python -m forecast_collector.scheduler \
  --workdir /srv/cross_indicator_consistency_arbitrage \
  --output-dir deploy/systemd/generated
```

See `deploy/systemd/README.md` for installation steps.

## Layout

```text
.
├── deploy/systemd/
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── samples/
├── sql/
├── src/forecast_collector/
└── tests/
```

## Operational Notes

- Category tree discovery marks disappeared markets inactive but does not delete
  them.
- `contract_history` preserves `period_requested`, so `1week` and `1month`
  responses can coexist for the same timestamp.
- Open-interest batching uses repeated `id=` query params with a configurable
  batch size (`OPEN_INTEREST_BATCH_SIZE`, default `50`).
- Raw responses remain append-only for replay and debugging.
- PostgreSQL advisory locks prevent overlapping runs for the same job type when
  driven by host timers.
