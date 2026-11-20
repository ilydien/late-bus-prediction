-- Staging tables for the automatic NATS -> silver consumer.
-- Apply to database traffic:
--   psql -h haproxy -p 5433 -U postgres -d traffic -f 002_staging_traffic.sql

CREATE TABLE IF NOT EXISTS public.ddot_aadt_2020_staging (
    LIKE public.ddot_aadt_2020 INCLUDING DEFAULTS
);

CREATE TABLE IF NOT EXISTS public.ddot_am_speed_2019_staging (
    LIKE public.ddot_am_speed_2019 INCLUDING DEFAULTS
);

CREATE TABLE IF NOT EXISTS public.ddot_taz_staging (
    LIKE public.ddot_taz INCLUDING DEFAULTS
);
