ALTER TABLE contract_history
    ADD COLUMN IF NOT EXISTS run_id BIGINT REFERENCES collection_runs(id) ON DELETE SET NULL;

ALTER TABLE contract_history
    DROP CONSTRAINT IF EXISTS contract_history_conid_ts_utc_key;

DO $$
BEGIN
    ALTER TABLE contract_history
        ADD CONSTRAINT contract_history_conid_ts_utc_period_key
        UNIQUE (conid, ts_utc, period_requested);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

DROP INDEX IF EXISTS idx_contract_history_conid_ts;

CREATE INDEX IF NOT EXISTS idx_contract_history_conid_ts_period
    ON contract_history (conid, ts_utc, period_requested);
