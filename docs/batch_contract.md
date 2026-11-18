# Batch contract

This document freezes the batch interface between the batch owner, the
streaming owner, and the dashboard/monitoring owner.

## Sources

| Dataset | Location | Format | Status |
|---|---|---|---|
| WMATA bus positions 2020 | `s3://wmata/bronze_2020/bus_positions/` | `.jsonl.gz` | active |
| GTFS static 2020 | `s3://wmata/reference/gtfs_static/version=20200106/` | zip + JSON | active |
| GTFS static 2026 | `s3://wmata/reference/gtfs_static/version=20260913/` | zip + JSON | reference only |
| DDOT traffic | `s3://traffic/ddot/` | GeoJSON | active |
| WMATA archive 2025 | `s3://wmata/historical/archive/` | `.tar.gz` | raw archive, not processed |

## Bronze event contract

NATS subjects:

```text
wmata.realtime.bronze_data
traffic.realtime.bronze_data
```

Each event should carry:

```json
{
  "event_id": "deterministic-id",
  "event_time": "2020-01-06T12:00:00+00:00",
  "ingest_time": "2026-11-18T12:00:00+00:00",
  "payload_version": "v1",
  "source": "seaweedfs",
  "bucket": "wmata",
  "s3_key": "bronze_2020/bus_positions/....jsonl.gz",
  "etag": "...",
  "event_type": "create",
  "size": 12345
}
```

Bridge events that only contain `key` are normalised by the consumer:

```text
/buckets/wmata/... -> wmata.realtime.bronze_data
/buckets/traffic/... -> traffic.realtime.bronze_data
```

## Silver contract

Database: `wmata` and `traffic` on PostgreSQL through HAProxy `5433`.

### `wmata.bus_trip_snapshot`

- Grain: one trip snapshot.
- Business key: `(trip_key, snapshot_ts)`.
- `trip_key = trip_id|trip_start`.
- `event_time` is observation time.
- `snapshot_ts` is the snapshot time.
- `service_date` is the local service date.
- Update policy: idempotent `ON CONFLICT (trip_key, snapshot_ts) DO UPDATE`.
- Staging table: `bus_trip_snapshot_staging`.

### `wmata.route_stats`

- Grain: one route.
- Primary key: `route_id`.
- Derived from GTFS static.

### `traffic.ddot_*`

- `ddot_aadt_2020` primary key `objectid`.
- `ddot_am_speed_2019` primary key `objectid`.
- `ddot_taz` primary key `taz`.
- Staging tables: `<table>_staging`.

## Event-time and late-data policy

- Consumer dedup key: `bucket|key|etag`.
- Gold incremental reads from `watermark - 20 minutes`.
- Target delay is the average deviation 14–16 minutes ahead for the same
  `trip_key`.
- Events older than `watermark - 20 minutes` are not reprocessed by the
  normal consumer; use a backfill manifest.
- Deletes are currently acknowledged but not propagated to silver. This is a
  known limitation to be handled by the streaming owner if delete semantics
  are required.

## Historical ingest

The `dlt` compose service runs `dlt/ingest_wmata_2020.py` automatically as a
one-shot task on `docker compose --profile dev up -d`. It waits for SeaweedFS
and the bridge, then skips dates already present in
`bronze_2020/bus_positions/*.jsonl.gz`. The same script is also available as
the `bronze_ingest` Dagster asset for scheduled/manual runs and lineage.

## Batch manifest

The consumer writes `/work/bronze_batch.json`:

```json
{
  "created_at": "...",
  "wmata": [{"s3_key": "...", "etag": "..."}],
  "traffic": [{"s3_key": "...", "etag": "..."}]
}
```

Spark reads it with:

```bash
spark-submit ... /opt/spark/jobs/bronze_to_silver.py --manifest /work/bronze_batch.json
```

The manifest is per batch and is overwritten. A permanent manifest/lineage
record is written to `public.pipeline_runs` in the gold database.

## Freshness expectations

- Silver freshness: `max(snapshot_ts)`.
- Gold freshness: `max(snapshot_ts)` in `gold.trip_delay_features`.
- Quality report: `quality/reports/latest.json`.
- Quality results: `gold.public.data_quality_runs`.

## Ownership

| Dataset | Owner | Retention |
|---|---|---|
| bronze WMATA/DDOT | batch | raw archive retained |
| silver WMATA/DDOT | batch | rolling operational |
| gold features/marts | batch | rolling operational |
| MLflow model artifacts | batch/ML | model versions retained |
| dashboard/API | monitoring/dashboard | derived |

## Change policy

Any change to a primary key, event-time field, or silver schema must update
this document and `docs/data_dictionary.md` before the change is merged.
