# ForecastTrader Collector

Server-side collector for IBKR ForecastTrader public endpoints. The initial
scope is market structure ingestion:

- discover one market by `underlyingConid`
- store market metadata and contract ladders
- enrich every contract through the details endpoint
- capture every HTTP response as raw JSON for auditing and reprocessing

The project is intentionally built around direct HTTP requests to the public
ForecastTrader frontend endpoints. It does not require an IBKR login, browser
automation, or DOM scraping.

## Status

Implemented in this repo:

- Python package scaffold
- Postgres schema migrations
- HTTP client with retry and pacing hooks
- Parsers for market, contract details, history, open interest, and projected
  probabilities
- Repository layer for raw responses and normalized tables
- CLI commands for `migrate`, `collect-seed-market`, `collect-history`,
  `collect-open-interest`, and `collect-probabilities`
- Sample payload fixtures and parser tests

Not yet productionized:

- scheduler execution loop
- category tree discovery
- websocket subscription consumer
- snapshot field decoding

## Quick Start

1. Copy `.env.example` to `.env` and set `DATABASE_URL`.
2. Install dependencies with `pip install -e .`.
3. Run migrations:

```bash
python -m forecast_collector.cli migrate
```

4. Collect a single market:

```bash
python -m forecast_collector.cli collect-seed-market --underlying-conid 766914406
```

5. Collect historical series, open interest, and projected probabilities once
   the base market has been ingested:

```bash
python -m forecast_collector.cli collect-history --underlying-conid 766914406
python -m forecast_collector.cli collect-open-interest --underlying-conid 766914406
python -m forecast_collector.cli collect-probabilities --underlying-conid 766914406
```

## Docker Compose

The repository includes a minimal `docker-compose.yml` with:

- `postgres` for storage
- `collector` as a manual CLI container

Typical usage:

```bash
docker compose up -d postgres
docker compose run --rm collector migrate
docker compose run --rm collector collect-seed-market --underlying-conid 766914406
```

## Layout

```text
.
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── sql/
├── samples/
├── src/forecast_collector/
└── tests/
```

## Notes

- The public host could not be resolved from the local shell in this
  environment, so the included fixtures are synthetic payloads based on the
  endpoint shapes already documented.
- Replace the synthetic sample payloads with real captures from DevTools as
  soon as you have them.
