-- Batch/ML lineage and run metadata.
-- Apply to the gold database (HAProxy 5434, database wmata).

CREATE TABLE IF NOT EXISTS public.pipeline_runs (
    run_id          text PRIMARY KEY,
    parent_run_id   text,
    job_name        text NOT NULL,
    layer           text,
    status          text NOT NULL,
    started_at      timestamptz,
    finished_at     timestamptz,
    input_datasets  jsonb,
    output_datasets jsonb,
    source_manifest jsonb,
    row_counts      jsonb,
    git_commit      text,
    error           text
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at
    ON public.pipeline_runs (started_at DESC);
