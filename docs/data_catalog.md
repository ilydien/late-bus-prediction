# Data catalog

| Dataset | Layer | Grain | Owner | Classification | Retention |
|---|---|---|---|---|---|
| `wmata.bus_trip_snapshot` | silver | `trip_key, snapshot_ts` | batch | internal | rolling |
| `wmata.route_stats` | silver | `route_id` | batch | internal | static |
| `traffic.ddot_aadt_2020` | silver | `objectid` | batch | public source | static |
| `traffic.ddot_am_speed_2019` | silver | `objectid` | batch | public source | static |
| `traffic.ddot_taz` | silver | `taz` | batch | public source | static |
| `wmata.trip_delay_features` | gold | `trip_key, snapshot_ts` | batch | internal | rolling |
| `wmata.route_hourly_delay` | gold | `route_id, service_date, hour_local` | batch | internal | rolling |
| `wmata.route_daily_reliability` | gold | `route_id, service_date` | batch | internal | rolling |
| `wmata.route_ddot_features` | gold | `route_id` | batch | internal | static |
| `wmata.model_predictions` | gold | `input_record_id, model_version` | batch | internal | rolling |
| `wmata.model_performance_daily` | gold | `service_date, model_version` | batch | internal | rolling |
| `wmata.pipeline_runs` | governance | `run_id` | batch | internal | retained |
| `wmata.data_quality_runs` | governance | `run_id, layer, dataset, check_name` | batch | internal | retained |

## Refresh

| Dataset | Refresh |
|---|---|
| bronze | on source object |
| silver | NATS consumer batch |
| gold features | daily/manual incremental |
| gold marts | full rebuild |
| model | weekly/manual |

## Lineage

Static dataset lineage: `docs/lineage.json`.

Runtime lineage: `public.pipeline_runs` with `run_id`, input/output datasets,
source manifest, row counts, status, and error.
