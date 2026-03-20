CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS collection_runs (
    id BIGSERIAL PRIMARY KEY,
    job_name TEXT NOT NULL,
    status TEXT NOT NULL,
    error_text TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS raw_api_responses (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT REFERENCES collection_runs(id) ON DELETE SET NULL,
    endpoint_name TEXT NOT NULL,
    request_url TEXT NOT NULL,
    query_params JSONB NOT NULL DEFAULT '{}'::JSONB,
    http_status INTEGER NOT NULL,
    response_json JSONB NOT NULL,
    response_sha256 TEXT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS markets (
    underlying_conid BIGINT PRIMARY KEY,
    market_name TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    logo_category TEXT,
    payout NUMERIC,
    exclude_historical_data BOOLEAN,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS contracts (
    conid BIGINT PRIMARY KEY,
    underlying_conid BIGINT NOT NULL REFERENCES markets(underlying_conid) ON DELETE CASCADE,
    side TEXT,
    strike DOUBLE PRECISION,
    strike_label TEXT,
    expiration TEXT,
    question TEXT,
    conid_yes BIGINT,
    conid_no BIGINT,
    product_conid BIGINT,
    market_name TEXT,
    symbol TEXT,
    measured_period TEXT,
    measured_period_units TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS contract_history (
    id BIGSERIAL PRIMARY KEY,
    conid BIGINT NOT NULL REFERENCES contracts(conid) ON DELETE CASCADE,
    ts_utc TIMESTAMPTZ NOT NULL,
    avg DOUBLE PRECISION,
    volume BIGINT,
    chart_step TEXT,
    source TEXT,
    period_requested TEXT NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    UNIQUE (conid, ts_utc)
);

CREATE TABLE IF NOT EXISTS open_interest_snapshots (
    id BIGSERIAL PRIMARY KEY,
    conid BIGINT NOT NULL REFERENCES contracts(conid) ON DELETE CASCADE,
    open_interest BIGINT,
    collected_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS projected_probabilities (
    id BIGSERIAL PRIMARY KEY,
    underlying_conid BIGINT NOT NULL REFERENCES markets(underlying_conid) ON DELETE CASCADE,
    strike DOUBLE PRECISION,
    expiry TEXT,
    probability DOUBLE PRECISION,
    collected_at TIMESTAMPTZ NOT NULL
);

