-- Batch quality gate results.
-- Apply to the gold database (HAProxy 5434, database wmata).

CREATE TABLE IF NOT EXISTS public.data_quality_runs (
    run_id       text NOT NULL,
    layer        text NOT NULL,
    dataset      text NOT NULL,
    check_name   text NOT NULL,
    status       text NOT NULL,
    severity     text NOT NULL,
    observed     double precision,
    threshold    double precision,
    details      jsonb,
    checked_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, layer, dataset, check_name)
);

CREATE INDEX IF NOT EXISTS idx_data_quality_runs_checked_at
    ON public.data_quality_runs (checked_at DESC);
