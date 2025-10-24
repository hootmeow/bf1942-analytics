-- @name analytics_job_runs_schema
-- @type schema
-- @object analytics_job_runs
-- @description Creates supporting tables for recording analytics job execution metrics.
-- @indexes CREATE INDEX IF NOT EXISTS ix_analytics_job_runs_started_at ON analytics_job_runs (started_at DESC);

CREATE TABLE IF NOT EXISTS analytics_job_runs (
    id BIGSERIAL PRIMARY KEY,
    job_name TEXT NOT NULL,
    job_type TEXT NOT NULL,
    object_name TEXT NOT NULL,
    source_file TEXT NOT NULL,
    refresh_sql TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    duration_ms NUMERIC NOT NULL,
    rows_affected BIGINT,
    success BOOLEAN NOT NULL DEFAULT TRUE,
    message TEXT
);

CREATE INDEX IF NOT EXISTS ix_analytics_job_runs_started_at
    ON analytics_job_runs (started_at DESC);

CREATE INDEX IF NOT EXISTS ix_analytics_job_runs_job_name
    ON analytics_job_runs (job_name);
