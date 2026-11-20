-- Silver layer DDL (PostgreSQL / Spock). Schema public to match Debezium,
-- dbt and Trino configuration.
-- Apply to database wmata:
--   psql -h haproxy -p 5433 -U postgres -d wmata -f 001_silver_wmata.sql
-- Apply to database traffic:
--   psql -h haproxy -p 5433 -U postgres -d traffic -f 001_silver_traffic.sql

CREATE TABLE IF NOT EXISTS public.bus_trip_snapshot (
    trip_key        text        NOT NULL,
    vehicle_id      text,
    trip_id         text,
    route_id        text,
    direction       text,
    direction_num   integer,
    trip_headsign   text,
    block_number    text,
    event_time      timestamptz,
    snapshot_ts     timestamptz NOT NULL,
    service_date    date,
    hour_local      integer,
    dow_local       integer,
    lat             double precision,
    lon             double precision,
    deviation_min   real,
    trip_start      timestamptz,
    trip_end        timestamptz,
    progress_pct    real,
    speed_kmh       real,
    source_file     text,
    PRIMARY KEY (trip_key, snapshot_ts)
);

CREATE INDEX IF NOT EXISTS idx_bus_trip_snapshot_route_time
    ON public.bus_trip_snapshot (route_id, service_date, hour_local);

CREATE TABLE IF NOT EXISTS public.route_stats (
    route_id                        text PRIMARY KEY,
    route_short_name                text,
    route_long_name                 text,
    route_type                      integer,
    trips_total                     integer,
    median_headway_min              real,
    median_scheduled_duration_min   real,
    n_stops                         integer
);
