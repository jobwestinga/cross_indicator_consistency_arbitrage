ALTER TABLE collection_runs
    ADD COLUMN IF NOT EXISTS job_args JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS summary_json JSONB NOT NULL DEFAULT '{}'::jsonb;

UPDATE collection_runs
SET job_args = COALESCE(job_args, '{}'::jsonb),
    summary_json = COALESCE(summary_json, '{}'::jsonb);

CREATE INDEX IF NOT EXISTS idx_collection_runs_job_name_started_at
    ON collection_runs (job_name, started_at DESC);
