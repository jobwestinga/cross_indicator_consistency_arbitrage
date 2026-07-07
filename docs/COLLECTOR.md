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

In plain language: the collector now has two jobs for history. One job keeps
recent history fresh in small frequent batches. Another job slowly fills missing
history windows and retries sparse gaps over time. Together, those jobs keep
the database up to date without requiring giant all-market sweeps every time.

## Implemented Commands

```bash
python -m forecast_collector.cli migrate
python -m forecast_collector.cli discover-markets
python -m forecast_collector.cli collect-seed-market --underlying-conid 793085688
python -m forecast_collector.cli collect-seed-market --underlying-conid 793085688 --contract-details-limit 6
python -m forecast_collector.cli collect-market-structures --all-discovered
python -m forecast_collector.cli collect-open-interest --all-discovered
python -m forecast_collector.cli collect-probabilities --all-discovered
python -m forecast_collector.cli collect-history --all-discovered --mode incremental --request-limit 500
python -m forecast_collector.cli collect-history --all-discovered --mode backfill --request-limit 1000
python -m forecast_collector.cli export-analysis-dataset --output-dir exports
python -m forecast_collector.cli export-analysis-sqlite --output-dir exports
python -m forecast_collector.cli collect-history --underlying-conid 793085688 --mode backfill --contract-limit 6 --history-periods 1week
python -m forecast_collector.cli report-health
```

The original single-market commands are preserved for debugging and regression
checks.

## Fast Smoke Tests

For day-to-day validation, prefer a small smoke test instead of a full market
run every time:

```bash
python -m forecast_collector.cli collect-seed-market \
  --underlying-conid 793085688 \
  --contract-details-limit 6

python -m forecast_collector.cli collect-open-interest \
  --underlying-conid 793085688

python -m forecast_collector.cli collect-probabilities \
  --underlying-conid 793085688

python -m forecast_collector.cli collect-history \
  --underlying-conid 793085688 \
  --mode backfill \
  --contract-limit 6 \
  --history-periods 1week
```

Run the full unbounded commands only once you are ready for the final canary.

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
HTTP_REQUESTS_PER_SECOND=8
CONTRACT_DETAILS_WORKERS=8
HISTORY_WORKERS=8
OPEN_INTEREST_BATCH_SIZE=100
LOG_LEVEL=INFO
HISTORY_INCREMENTAL_REQUEST_LIMIT=500
HISTORY_BACKFILL_REQUEST_LIMIT=1000
HISTORY_NO_DATA_RETRY_HOURS=24
```

If you already have an older `.env`, remove any legacy throttle such as
`HTTP_REQUESTS_PER_SECOND=1`. Environment values override the faster defaults.

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
python -m forecast_collector.cli collect-history --all-discovered --mode incremental --request-limit 500
python -m forecast_collector.cli collect-history --all-discovered --mode backfill --request-limit 1000
```

## Automatic Upkeep

Normal live operation should look like this:

- `discover-markets` finds newly listed markets.
- `collect-market-structures` pulls any new or changed contract ladders.
- `collect-open-interest` keeps current snapshots fresh.
- `collect-probabilities` keeps current projected probabilities fresh.
- `collect-history --mode incremental` refreshes recent history in bounded batches.
- `collect-history --mode backfill` slowly fills missing history holes in bounded batches.

That means you do not need to wait for a perfect one-shot historical backfill
before going live. The database can start useful and then improve over time as
the scheduled jobs keep running.

## Dataset Export

For sharing with a teammate who does not have server access, generate a zipped
CSV bundle from the VPS:

```bash
python -m forecast_collector.cli export-analysis-dataset --output-dir exports
```

That writes a file such as `exports/forecast_analysis_dataset_20260323T120000Z.zip`
containing:

- `market_categories.csv`
- `markets.csv`
- `contracts.csv`
- `projected_probabilities.csv`
- `open_interest_snapshots.csv`
- `contract_history.csv`
- `manifest.json`

The time-series files are denormalized with market and contract identifiers so
your teammate can load them directly into pandas/R without doing database joins.

Useful filters:

```bash
python -m forecast_collector.cli export-analysis-dataset \
  --output-dir exports \
  --underlying-conid 793085688 \
  --since 2026-03-01
```

If you want a single portable file for local analysis, export the same dataset
as SQLite instead:

```bash
python -m forecast_collector.cli export-analysis-sqlite \
  --output-dir exports \
  --since 2026-03-01
```

That writes a file such as
`exports/forecast_analysis_dataset_20260323T120000Z.sqlite` containing:

- `market_categories`
- `markets`
- `contracts`
- `projected_probabilities`
- `open_interest_snapshots`
- `contract_history`
- `export_manifest`
- `export_tables`

This is the easiest handoff format when you want to copy one file off the VPS
and inspect everything locally with sqlite, pandas, DuckDB, or DB Browser for
SQLite.

To copy a bundle off the VPS:

```bash
scp job090305@your-server:~/cross_indicator_consistency_arbitrage/exports/forecast_analysis_dataset_*.zip .
```

To copy the SQLite export off the VPS:

```bash
scp job090305@your-server:~/cross_indicator_consistency_arbitrage/exports/forecast_analysis_dataset_*.sqlite .
```

## Local Inspection

Once you have a zip bundle on your machine, run the first-pass exploration
script (summary stats + figures) and the readiness audit:

```bash
python3 analysis/explore_dataset.py            # newest local bundle
python3 analysis/check_readiness.py            # PASS/WARN/FAIL freshness audit
```

The full research pipeline is documented in the top-level README and
`analysis/README.md`.

## GitHub Deploy

The clean production model is:

- GitHub is the source of truth for code.
- The server repo stays clean and should not be edited by hand.
- Every push to `main` triggers a GitHub Actions deploy.
- The deploy pulls the latest `main`, rebuilds the Docker services, and runs migrations.

This repository includes:

- `.github/workflows/deploy-prod.yml`
- `deploy/deploy_from_github.sh`

To enable it, add the following in GitHub:

- Repository variable `PROD_HOST`
- Repository variable `PROD_USER`
- Repository variable `PROD_REPO_DIR`
- Optional repository variable `PROD_SSH_PORT`
- Repository secret `PROD_SSH_PRIVATE_KEY`

Typical values:

```text
PROD_HOST=your-server-host-or-ip
PROD_USER=your-server-user
PROD_REPO_DIR=/home/your-server-user/cross_indicator_consistency_arbitrage
PROD_SSH_PORT=22
```

Generate a dedicated deploy key locally:

```bash
ssh-keygen -t ed25519 -C "github-actions-deploy" -f ~/.ssh/forecast_prod_deploy
```

Install the public key on the server:

```bash
cat ~/.ssh/forecast_prod_deploy.pub | ssh your-server-user@your-server-host "mkdir -p ~/.ssh && chmod 700 ~/.ssh && cat >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys"
```

Then copy the private key into the GitHub Actions secret `PROD_SSH_PRIVATE_KEY`.

After that, each push to `main` should deploy automatically. If the workflow
fails because the server repo has tracked local changes, clean those up first so
the server can remain a pure deploy target.

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
docker compose run --rm collector collect-history --all-discovered --mode incremental --request-limit 500
docker compose run --rm collector collect-history --all-discovered --mode backfill --request-limit 1000
docker compose run --rm collector export-analysis-dataset --output-dir exports
docker compose run --rm collector export-analysis-sqlite --output-dir exports
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
  batch size (`OPEN_INTEREST_BATCH_SIZE`, default `100`).
- High-cardinality fetch stages use bounded parallelism:
  `CONTRACT_DETAILS_WORKERS` and `HISTORY_WORKERS` both default to `8`.
- Global request pacing defaults to `HTTP_REQUESTS_PER_SECOND=8`, which is much
  faster than the original MVP while still keeping a global cap in place.
- Scheduled history collection is intentionally bounded. Use
  `HISTORY_INCREMENTAL_REQUEST_LIMIT` to control how many contract-period
  refreshes each incremental run performs, and
  `HISTORY_BACKFILL_REQUEST_LIMIT` to control how aggressively missing history
  holes are filled.
- `HISTORY_NO_DATA_RETRY_HOURS` controls how long the backfill job waits before
  retrying a contract-period that last came back as `no_data`.
- `collect-seed-market --contract-details-limit N` and
  `collect-history --contract-limit N --history-periods ...` are intended for
  fast smoke tests before a full canary.
- History collection now records partial failures and continues when a subset
  of contract-period requests exhaust retries, instead of aborting the whole
  run on the first upstream `500`.
- History collection now tracks state per `(conid, period_requested)`, so the
  scheduler can refresh recent windows and fill specific missing holes instead
  of re-sweeping the full history set every time.
- Raw responses remain append-only for replay and debugging.
- PostgreSQL advisory locks prevent overlapping runs for the same job type when
  driven by host timers.
