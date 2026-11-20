-- Retraining support for the gold layer.
-- Apply to the gold cluster (HAProxy 5434, database wmata).

CREATE TABLE IF NOT EXISTS public.training_watermark (
    pipeline            text PRIMARY KEY,
    last_snapshot_ts    timestamptz,
    last_trained_at     timestamptz,
    model_version       text,
    updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.trip_delay_features_staging (
    LIKE public.trip_delay_features INCLUDING DEFAULTS
);
