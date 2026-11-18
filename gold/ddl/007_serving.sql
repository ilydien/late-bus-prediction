-- Read-only serving views for the dashboard/API.
-- Apply to the gold database (HAProxy 5434, database wmata).

CREATE OR REPLACE VIEW public.serving_route_daily AS
SELECT
    route_id,
    service_date,
    trips,
    avg_deviation,
    median_deviation,
    p90_deviation,
    on_time_pct,
    late_pct,
    avg_speed_kmh
FROM public.route_daily_reliability;

CREATE OR REPLACE VIEW public.serving_route_hourly AS
SELECT
    route_id,
    service_date,
    hour_local,
    trip_snapshots,
    trips,
    avg_deviation,
    median_deviation,
    p90_deviation,
    on_time_pct,
    late_pct,
    avg_speed_kmh
FROM public.route_hourly_delay;

CREATE OR REPLACE VIEW public.serving_latest_predictions AS
SELECT
    p.input_record_id,
    p.model_version,
    p.predicted_at,
    p.trip_key,
    p.snapshot_ts,
    p.predicted_delay_min,
    p.actual_delay_min,
    p.abs_error,
    p.predicted_late,
    p.actual_late,
    p.status,
    p.error,
    p.inference_run_id,
    f.route_id,
    f.direction_num,
    f.service_date
FROM public.model_predictions p
LEFT JOIN public.trip_delay_features f
  ON f.trip_key = p.trip_key
 AND f.snapshot_ts = p.snapshot_ts
WHERE p.status = 'success';

CREATE OR REPLACE VIEW public.serving_model_performance AS
SELECT
    service_date,
    model_version,
    n_predictions,
    mae,
    rmse,
    accuracy_late_5,
    avg_error
FROM public.model_performance_daily;

CREATE OR REPLACE VIEW public.serving_freshness AS
SELECT
    (SELECT max(snapshot_ts) FROM public.trip_delay_features) AS max_snapshot_ts,
    (SELECT max(service_date) FROM public.route_daily_reliability) AS max_service_date,
    (SELECT max(predicted_at) FROM public.model_predictions) AS max_predicted_at,
    now() AS generated_at;
