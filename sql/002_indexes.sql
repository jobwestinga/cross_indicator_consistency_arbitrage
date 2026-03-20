CREATE INDEX IF NOT EXISTS idx_raw_api_responses_run_id
    ON raw_api_responses (run_id);

CREATE INDEX IF NOT EXISTS idx_contracts_underlying_conid
    ON contracts (underlying_conid);

CREATE INDEX IF NOT EXISTS idx_contract_history_conid_ts
    ON contract_history (conid, ts_utc);

CREATE INDEX IF NOT EXISTS idx_open_interest_snapshots_conid_collected_at
    ON open_interest_snapshots (conid, collected_at DESC);

CREATE INDEX IF NOT EXISTS idx_projected_probabilities_underlying_collected_at
    ON projected_probabilities (underlying_conid, collected_at DESC);
