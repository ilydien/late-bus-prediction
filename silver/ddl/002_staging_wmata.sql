-- Staging tables for the automatic NATS -> silver consumer.
-- Apply to database wmata:
--   psql -h haproxy -p 5433 -U postgres -d wmata -f 002_staging_wmata.sql

CREATE TABLE IF NOT EXISTS public.bus_trip_snapshot_staging (
    LIKE public.bus_trip_snapshot INCLUDING DEFAULTS
);

CREATE TABLE IF NOT EXISTS public.route_stats_staging (
    LIKE public.route_stats INCLUDING DEFAULTS
);
