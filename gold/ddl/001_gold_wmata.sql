-- Gold layer DDL for the WMATA trip-delay model.
-- Apply to the gold cluster (HAProxy 5434):
--   psql -h haproxy -p 5434 -U postgres -d wmata -f 001_gold_wmata.sql

CREATE TABLE IF NOT EXISTS public.trip_delay_features (
    trip_key                        text        NOT NULL,
    snapshot_ts                     timestamptz NOT NULL,
    route_id                        text,
    direction_num                   integer,
    service_date                    date,
    hour_local                      integer,
    dow_local                       integer,
    deviation_now                   real,
    speed_kmh                       real,
    progress_pct                    real,
    time_since_start_min            real,
    time_to_end_min                 real,
    trips_total                     integer,
    median_headway_min              real,
    median_scheduled_duration_min   real,
    n_stops                         integer,
    aadt_route_median               real,
    aadt_route_max                  real,
    am_speed_ratio                  real,
    am_speed_route_median           real,
    am_travel_time_per_km           real,
    hour_sin                        real,
    hour_cos                        real,
    dow_sin                         real,
    dow_cos                         real,
    target_deviation_15m            real,
    target_late_5                   smallint,
    split                           text,
    PRIMARY KEY (trip_key, snapshot_ts)
);

CREATE INDEX IF NOT EXISTS idx_trip_delay_features_route_time
    ON public.trip_delay_features (route_id, service_date, hour_local);

CREATE TABLE IF NOT EXISTS public.route_hourly_delay (
    route_id            text    NOT NULL,
    service_date        date    NOT NULL,
    hour_local          integer NOT NULL,
    trip_snapshots      bigint,
    trips               bigint,
    avg_deviation       real,
    median_deviation    real,
    p90_deviation       real,
    on_time_pct         real,
    late_pct            real,
    avg_speed_kmh       real,
    PRIMARY KEY (route_id, service_date, hour_local)
);

CREATE TABLE IF NOT EXISTS public.route_daily_reliability (
    route_id            text NOT NULL,
    service_date        date NOT NULL,
    trips               bigint,
    avg_deviation       real,
    median_deviation    real,
    p90_deviation       real,
    on_time_pct         real,
    late_pct            real,
    avg_speed_kmh       real,
    PRIMARY KEY (route_id, service_date)
);

CREATE TABLE IF NOT EXISTS public.model_predictions (
    input_record_id     text        NOT NULL,
    model_version       text        NOT NULL,
    predicted_at        timestamptz,
    trip_key            text,
    snapshot_ts         timestamptz,
    predicted_delay_min real,
    actual_delay_min    real,
    abs_error           real,
    predicted_late      smallint,
    actual_late         smallint,
    PRIMARY KEY (input_record_id, model_version)
);

CREATE TABLE IF NOT EXISTS public.model_performance_daily (
    service_date        date NOT NULL,
    model_version       text NOT NULL,
    n_predictions       bigint,
    mae                 real,
    rmse                real,
    accuracy_late_5     real,
    avg_error           real,
    PRIMARY KEY (service_date, model_version)
);
