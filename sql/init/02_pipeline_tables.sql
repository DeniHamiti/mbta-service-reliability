-- Tracks each ingest/pipeline run
CREATE TABLE IF NOT EXISTS staging.pipeline_runs (
    id            SERIAL PRIMARY KEY,
    source        TEXT NOT NULL,        -- e.g. 'gtfs', 'open_data', 'v3_api'
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    rows_loaded   INTEGER,
    status        TEXT NOT NULL DEFAULT 'running',  -- running | success | failed
    error_message TEXT
);

-- Tracks data quality issues found during validation
CREATE TABLE IF NOT EXISTS staging.data_quality_issues (
    id            SERIAL PRIMARY KEY,
    check_name    TEXT NOT NULL,
    table_name    TEXT NOT NULL,
    record_key    TEXT,
    issue_type    TEXT NOT NULL,        -- gap | duplicate | anomaly | referential
    detected_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    details       JSONB
);
