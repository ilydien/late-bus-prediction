-- DDOT route-level congestion features (gold).
-- Apply to gold cluster (HAProxy 5434, database wmata).

CREATE TABLE IF NOT EXISTS public.route_ddot_features (
    route_id                text PRIMARY KEY,
    aadt_route_median       real,
    aadt_route_max          real,
    am_speed_route_median   real,
    am_speed_ratio          real,
    am_travel_time_per_km   real,
    n_aadt_segments         integer,
    n_speed_segments        integer
);
