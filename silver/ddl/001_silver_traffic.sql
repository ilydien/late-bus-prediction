-- Silver layer DDL for the traffic domain (DDOT).
-- Apply with:
--   psql -h haproxy -p 5433 -U postgres -d traffic -f 001_silver_traffic.sql

CREATE TABLE IF NOT EXISTS public.ddot_aadt_2020 (
    objectid        integer PRIMARY KEY,
    routeid         text,
    frommeasure     double precision,
    tomeasure       double precision,
    fromdate        text,
    todate          text,
    eventid         text,
    aadt            double precision,
    aadt_year       integer,
    gis_id          text,
    globalid        text,
    creator         text,
    created         text,
    editor          text,
    edited          text,
    shapelen        double precision,
    geometry        text
);

CREATE TABLE IF NOT EXISTS public.ddot_am_speed_2019 (
    objectid                        integer PRIMARY KEY,
    tmc_code                        text,
    time_period                     text,
    tmc_code_speed                  double precision,
    tmc_code_ref_speed              double precision,
    tmc_code_avg_speed              double precision,
    tmc_code_travel_time_minutes    double precision,
    gis_id                          text,
    globalid                        text,
    creator                         text,
    created                         text,
    editor                          text,
    edited                          text,
    shapelen                        double precision,
    geometry                        text
);

CREATE TABLE IF NOT EXISTS public.ddot_taz (
    taz             integer PRIMARY KEY,
    name            text,
    popbytaz        double precision,
    hhbytaz         double precision,
    empbytaz        double precision,
    acres           double precision,
    state_name      text,
    state_fips      text,
    cnty_fips       text,
    fipsstco        text,
    region          text,
    sqmi            double precision,
    jur             integer,
    gis_id          text,
    globalid        text,
    created         text,
    edited          text,
    objectid        integer,
    shapearea       double precision,
    shapelen        double precision,
    geometry        text
);
