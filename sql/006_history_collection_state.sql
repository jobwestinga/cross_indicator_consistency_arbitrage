CREATE TABLE IF NOT EXISTS contract_history_collection_state (
    conid BIGINT NOT NULL REFERENCES contracts(conid) ON DELETE CASCADE,
    period_requested TEXT NOT NULL,
    last_collected_at TIMESTAMPTZ NOT NULL,
    last_no_data_at TIMESTAMPTZ,
    PRIMARY KEY (conid, period_requested)
);

CREATE INDEX IF NOT EXISTS idx_contract_history_state_last_collected
    ON contract_history_collection_state (last_collected_at, conid, period_requested);

CREATE INDEX IF NOT EXISTS idx_contract_history_state_last_no_data
    ON contract_history_collection_state (last_no_data_at, conid, period_requested);
