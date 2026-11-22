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

CREATE TABLE IF NOT EXISTS public.tomtom_flow_segment (
    corridor_id                 text NOT NULL,
    snapshot_ts                 timestamptz NOT NULL,
    event_time                  timestamptz,
    service_date                date,
    hour_local                  integer,
    dow_local                   integer,
    corridor_name               text,
    route_hint                  text,
    query_lat                   double precision,
    query_lon                   double precision,
    frc                         text,
    current_speed_kmh           double precision,
    free_flow_speed_kmh         double precision,
    speed_ratio                 double precision,
    current_travel_time_sec     integer,
    free_flow_travel_time_sec   integer,
    delay_sec                   integer,
    confidence                  double precision,
    road_closure                boolean,
    geometry                    text,
    source_file                 text,
    PRIMARY KEY (corridor_id, snapshot_ts)
);

CREATE INDEX IF NOT EXISTS idx_tomtom_flow_snapshot_ts
    ON public.tomtom_flow_segment (snapshot_ts DESC);

CREATE INDEX IF NOT EXISTS idx_tomtom_flow_service_date
    ON public.tomtom_flow_segment (service_date);

CREATE TABLE IF NOT EXISTS public.tomtom_traffic_incident (
    incident_id                 text NOT NULL,
    snapshot_ts                 timestamptz NOT NULL,
    event_time                  timestamptz,
    service_date                date,
    hour_local                  integer,
    dow_local                   integer,
    icon_category               integer,
    magnitude_of_delay          integer,
    delay_sec                   integer,
    length_m                    double precision,
    from_location               text,
    to_location                 text,
    road_numbers                text,
    start_time                  timestamptz,
    end_time                    timestamptz,
    last_report_time            timestamptz,
    probability                 text,
    num_reports                 integer,
    description                 text,
    event_code                  integer,
    lat                         double precision,
    lon                         double precision,
    geometry                    text,
    source_file                 text,
    PRIMARY KEY (incident_id, snapshot_ts)
);

CREATE INDEX IF NOT EXISTS idx_tomtom_incident_snapshot_ts
    ON public.tomtom_traffic_incident (snapshot_ts DESC);

CREATE INDEX IF NOT EXISTS idx_tomtom_incident_service_date
    ON public.tomtom_traffic_incident (service_date);
