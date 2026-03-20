CREATE TABLE IF NOT EXISTS market_categories (
    category_key TEXT PRIMARY KEY,
    category_name TEXT NOT NULL,
    parent_category_key TEXT REFERENCES market_categories(category_key) ON DELETE SET NULL,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL
);

ALTER TABLE markets
    ADD COLUMN IF NOT EXISTS product_conid BIGINT,
    ADD COLUMN IF NOT EXISTS category_key TEXT,
    ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS last_discovered_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_structure_collected_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_probabilities_collected_at TIMESTAMPTZ;

DO $$
BEGIN
    ALTER TABLE markets
        ADD CONSTRAINT markets_category_key_fkey
        FOREIGN KEY (category_key)
        REFERENCES market_categories(category_key)
        ON DELETE SET NULL;
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

ALTER TABLE contracts
    ADD COLUMN IF NOT EXISTS expiry_label TEXT,
    ADD COLUMN IF NOT EXISTS time_specifier TEXT,
    ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS last_details_collected_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_open_interest_collected_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_history_collected_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_history_no_data_at TIMESTAMPTZ;

UPDATE markets
SET active = TRUE,
    last_discovered_at = COALESCE(last_discovered_at, last_seen_at),
    last_structure_collected_at = COALESCE(last_structure_collected_at, last_seen_at);

UPDATE contracts
SET active = TRUE,
    last_details_collected_at = COALESCE(last_details_collected_at, last_seen_at);

CREATE INDEX IF NOT EXISTS idx_market_categories_parent
    ON market_categories (parent_category_key);

CREATE INDEX IF NOT EXISTS idx_markets_active
    ON markets (active, underlying_conid);

CREATE INDEX IF NOT EXISTS idx_markets_category_key
    ON markets (category_key);

CREATE INDEX IF NOT EXISTS idx_contracts_active_underlying_conid
    ON contracts (active, underlying_conid);
