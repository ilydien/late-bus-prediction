# Data dictionary

## Silver

### `wmata.bus_trip_snapshot`

| Column | Type | Key | Description |
|---|---|---|---|
| `trip_key` | text | PK | `trip_id|trip_start` |
| `vehicle_id` | text | | vehicle identifier |
| `trip_id` | text | | source trip id |
| `route_id` | text | | route identifier |
| `direction` | text | | direction label |
| `direction_num` | integer | | direction number |
| `trip_headsign` | text | | headsign |
| `block_number` | text | | block |
| `event_time` | timestamptz | | observation time |
| `snapshot_ts` | timestamptz | PK | snapshot time |
| `service_date` | date | | local service date |
| `hour_local` | integer | | local hour |
| `dow_local` | integer | | local day of week |
| `lat` | double | | latitude |
| `lon` | double | | longitude |
| `deviation_min` | real | | schedule deviation in minutes |
| `trip_start` | timestamptz | | scheduled trip start |
| `trip_end` | timestamptz | | scheduled trip end |
| `progress_pct` | real | | trip progress 0–1 |
| `speed_kmh` | real | | derived speed |
| `source_file` | text | | bronze source object |

### `wmata.route_stats`

| Column | Type | Key | Description |
|---|---|---|---|
| `route_id` | text | PK | route |
| `route_short_name` | text | | short name |
| `route_long_name` | text | | long name |
| `route_type` | integer | | GTFS route type |
| `trips_total` | integer | | scheduled trips |
| `median_headway_min` | real | | median headway |
| `median_scheduled_duration_min` | real | | median duration |
| `n_stops` | integer | | distinct stops |

### `traffic.ddot_aadt_2020`

Primary key `objectid`. AADT and road geometry from DDOT.

### `traffic.ddot_am_speed_2019`

Primary key `objectid`. AM peak speed and travel time.

### `traffic.ddot_taz`

Primary key `taz`. Traffic analysis zone attributes.

### `traffic.tomtom_flow_segment`

Grain: one corridor flow snapshot (`(corridor_id, snapshot_ts)`).

| Column | Type | Key | Description |
|---|---|---|---|
| `corridor_id` | text | PK | DC/MD/VA Metrobus corridor identifier |
| `snapshot_ts` | timestamptz | PK | Poll cycle UTC timestamp |
| `event_time` | timestamptz | | Observation timestamp |
| `service_date` | date | | Local (`America/New_York`) service date |
| `hour_local` | integer | | Local hour (`0..23`) |
| `dow_local` | integer | | Local day of week (`0..6`) |
| `corridor_name` | text | | Human-readable corridor label |
| `route_hint` | text | | Associated WMATA bus routes |
| `query_lat` | double | | Queried point latitude |
| `query_lon` | double | | Queried point longitude |
| `frc` | text | | Functional Road Class (`FRC0`–`FRC7`) |
| `current_speed_kmh` | double | | Current traffic speed (km/h) |
| `free_flow_speed_kmh` | double | | Free-flow reference speed (km/h) |
| `speed_ratio` | double | | `current_speed_kmh / free_flow_speed_kmh` |
| `current_travel_time_sec` | integer | | Current segment travel time (seconds) |
| `free_flow_travel_time_sec` | integer | | Free-flow segment travel time (seconds) |
| `delay_sec` | integer | | Segment delay in seconds |
| `confidence` | double | | TomTom speed confidence (`0..1`) |
| `road_closure` | boolean | | Whether segment is closed |
| `geometry` | text | | GeoJSON LineString/Point |
| `source_file` | text | | Bronze S3 key |

### `traffic.tomtom_traffic_incident`

Grain: one active incident per poll snapshot (`(incident_id, snapshot_ts)`).

| Column | Type | Key | Description |
|---|---|---|---|
| `incident_id` | text | PK | TomTom incident identifier |
| `snapshot_ts` | timestamptz | PK | Poll cycle UTC timestamp |
| `event_time` | timestamptz | | Last report or start timestamp |
| `service_date` | date | | Local service date |
| `hour_local` | integer | | Local hour |
| `dow_local` | integer | | Local day of week |
| `icon_category` | integer | | TomTom incident category code |
| `magnitude_of_delay` | integer | | Delay magnitude (`0..4`) |
| `delay_sec` | integer | | Reported delay in seconds |
| `length_m` | double | | Affected road length in meters |
| `from_location` | text | | Start intersection/location |
| `to_location` | text | | End intersection/location |
| `road_numbers` | text | | Comma-separated affected road numbers |
| `start_time` | timestamptz | | Incident start timestamp |
| `end_time` | timestamptz | | Estimated/actual end timestamp |
| `last_report_time` | timestamptz | | Last TomTom report timestamp |
| `probability` | text | | Probability of occurrence |
| `num_reports` | integer | | Number of reports |
| `description` | text | | Concatenated incident descriptions |
| `event_code` | integer | | Primary TomTom event code |
| `lat` | double | | Representative latitude |
| `lon` | double | | Representative longitude |
| `geometry` | text | | GeoJSON geometry |
| `source_file` | text | | Bronze S3 key |

## Gold

### `wmata.trip_delay_features`

Grain: one trip snapshot sampled every 5 minutes.

| Column | Type | Key | Description |
|---|---|---|---|
| `trip_key` | text | PK | trip key |
| `snapshot_ts` | timestamptz | PK | snapshot time |
| `route_id` | text | | route |
| `direction_num` | integer | | direction |
| `service_date` | date | | service date |
| `hour_local` | integer | | hour |
| `dow_local` | integer | | day of week |
| `deviation_now` | real | | current deviation |
| `speed_kmh` | real | | speed |
| `progress_pct` | real | | trip progress |
| `time_since_start_min` | real | | elapsed trip time |
| `time_to_end_min` | real | | remaining trip time |
| `trips_total` | integer | | route scheduled trips |
| `median_headway_min` | real | | route headway |
| `median_scheduled_duration_min` | real | | route duration |
| `n_stops` | integer | | route stops |
| `aadt_route_median` | real | | DDOT AADT median |
| `aadt_route_max` | real | | DDOT AADT max |
| `am_speed_ratio` | real | | DDOT AM speed ratio |
| `am_speed_route_median` | real | | DDOT AM speed median |
| `am_travel_time_per_km` | real | | DDOT AM travel time |
| `hour_sin` | real | | cyclical hour |
| `hour_cos` | real | | cyclical hour |
| `dow_sin` | real | | cyclical day |
| `dow_cos` | real | | cyclical day |
| `target_deviation_15m` | real | | delay 14–16 minutes ahead |
| `target_late_5` | smallint | | 1 if target > 5 minutes |
| `split` | text | | historical split label; training recomputes |

### `wmata.route_hourly_delay`

Grain: `(route_id, service_date, hour_local)`.

### `wmata.route_daily_reliability`

Grain: `(route_id, service_date)`.

### `wmata.route_ddot_features`

Grain: `route_id`. Route-level DDOT spatial features.

### `wmata.model_predictions`

Grain: `(input_record_id, model_version)`.

| Column | Type | Description |
|---|---|---|
| `input_record_id` | text | `trip_key|snapshot_ts` or hash for invalid input |
| `model_version` | text | registered model version, e.g. `v1` |
| `predicted_at` | timestamptz | inference timestamp |
| `trip_key` | text | input trip |
| `snapshot_ts` | timestamptz | input snapshot |
| `predicted_delay_min` | real | model output |
| `actual_delay_min` | real | target if available |
| `abs_error` | real | absolute error if target available |
| `predicted_late` | smallint | `predicted_delay_min > 5` |
| `actual_late` | smallint | `actual_delay_min > 5` |
| `status` | text | `success` or `error` |
| `error` | text | error reason |
| `inference_run_id` | text | links to `pipeline_runs.run_id` |

### `wmata.model_performance_daily`

Grain: `(service_date, model_version)`.

### `wmata.training_watermark`

Pipeline state for incremental gold/retraining.

### `wmata.pipeline_runs`

Batch/ML lineage record. Grain: `run_id`.

### `wmata.data_quality_runs`

Quality check result. Grain: `(run_id, layer, dataset, check_name)`.


## Parquet analytical copy

Path:

```text
s3://wmata/gold_parquet/<table>/version=<run_id>/
```

Tables:

- `trip_delay_features`
- `route_hourly_delay`
- `route_daily_reliability`
- `route_ddot_features`
- `model_predictions`

Manifest:

```text
s3://wmata/gold_parquet/_manifest/<run_id>.json
s3://wmata/gold_parquet/_latest.json
```

The Parquet copy is immutable per run. PostgreSQL remains the ACID serving
layer.
