# Parquet analytical storage

## Location

```text
s3://wmata/gold_parquet/
```

SeaweedFS S3 endpoint:

```text
http://seaweedfs:8333        (container)
http://127.0.0.1:8333        (host)
```

## Layout

```text
gold_parquet/
├── trip_delay_features/
│   └── version=<run_id>/
│       └── service_date=YYYY-MM-DD/
│           └── part-*.snappy.parquet
├── route_hourly_delay/
│   └── version=<run_id>/
│       └── service_date=YYYY-MM-DD/
├── route_daily_reliability/
│   └── version=<run_id>/
│       └── service_date=YYYY-MM-DD/
├── route_ddot_features/
│   └── version=<run_id>/
├── model_predictions/
│   └── version=<run_id>/
│       └── model_version=<version>/
├── _manifest/
│   └── <run_id>.json
└── _latest.json
```

## Schedule

```text
parquet_export_job_schedule
cron: 30 3 * * *
timezone: America/New_York
status: RUNNING
```

The job runs:

```text
gold_trip_delay_features
→ gold_parquet_export
```

If gold has no new data, the gold asset skips and the export still creates a
snapshot of the current gold tables.

## Retention

```text
PARQUET_RETENTION_VERSIONS=7
```

After a successful export, snapshots older than the latest 7 successful
`gold_parquet_export` runs are deleted. `_latest.json` always points to the
newest manifest.

## Snapshot

`version=<run_id>` is the logical snapshot. A snapshot is immutable.

`_latest.json` points to the latest manifest.

## Manifest

```json
{
  "run_id": "...",
  "created_at": "...",
  "git_commit": "...",
  "source_watermark": "2020-02-08 23:59:00+00:00",
  "bucket": "wmata",
  "prefix": "gold_parquet",
  "tables": {
    "trip_delay_features": {
      "rows": 2565962,
      "path": "s3://wmata/gold_parquet/trip_delay_features/version=.../",
      "partition": "service_date"
    }
  }
}
```

## Export

Dagster:

```bash
docker exec -e DAGSTER_HOME=/tmp/dagster_home \
  wmata-lateness-dagster-user-code-1 \
  sh -c 'mkdir -p /tmp/dagster_home && dagster asset materialize \
    -f /opt/dagster/app/definitions.py --select gold_parquet_export'
```

Manual Spark:

```bash
docker compose -f compose.dev.yaml --profile dev run --rm spark-submit \
  --driver-memory 2g /opt/spark/jobs/export_gold_parquet.py \
  --run-id manual-parquet-1
```

## Verify

```bash
docker compose -f compose.dev.yaml --profile dev run --rm spark-submit \
  --driver-memory 1g /opt/spark/jobs/verify_parquet.py \
  --path s3a://wmata/gold_parquet/trip_delay_features/version=<run_id>/
```

## Who reads this

| Consumer | Reads Parquet? | Purpose |
|---|---|---|
| Spark export | writes | snapshot creation |
| Spark verification/benchmark | yes | row counts, performance |
| ML training (future) | optional | reproducible dataset snapshot |
| Analyst via Spark | yes | offline analysis |
| API/dashboard | no | PostgreSQL serving |
| Trino | not yet | needs S3/Hive/Iceberg catalog |

## Consistency

- PostgreSQL is ACID and handles upsert/replay.
- Parquet is an immutable snapshot exported from PostgreSQL.
- Re-running the same `run_id` overwrites the same snapshot with the same
  semantic row counts.
- Schema changes appear in new snapshots; old snapshots retain their schema.
