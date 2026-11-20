-- Batch inference status columns and indexes.
-- Apply to the gold database (HAProxy 5434, database wmata).

ALTER TABLE public.model_predictions
    ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'success',
    ADD COLUMN IF NOT EXISTS error text,
    ADD COLUMN IF NOT EXISTS inference_run_id text;

CREATE INDEX IF NOT EXISTS idx_model_predictions_inference_run
    ON public.model_predictions (inference_run_id);

CREATE INDEX IF NOT EXISTS idx_model_predictions_model_version
    ON public.model_predictions (model_version, predicted_at DESC);
