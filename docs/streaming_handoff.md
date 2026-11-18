# Streaming handoff

The streaming owner can start without waiting for gold/ML.

## Stable inputs

Silver DDL:

- `silver/ddl/001_silver_wmata.sql`
- `silver/ddl/001_silver_traffic.sql`
- `silver/ddl/002_staging_wmata.sql`
- `silver/ddl/002_staging_traffic.sql`

Merge helper:

- `consumer/merge_silver.py`

Sample manifest:

- `tests/fixtures/bronze_manifest_sample.json`

## Event contract

Each NATS event should preserve:

- `event_id`
- `event_time`
- `ingest_time`
- `payload_version`
- `source`
- `bucket`
- `s3_key`
- `etag`
- `event_type`

Dedup key in the current consumer:

```text
bucket|key|etag
```

## What streaming owns

- broker/topic/stream configuration
- consumer lag and offsets
- event-time vs processing-time
- allowed lateness/watermark policy
- DLQ and replay
- stream metrics and alerts

## What batch provides

- silver schema and primary keys
- staging + upsert contract
- manifest format
- expected row counts
- backfill tool
- `pipeline_runs` lineage table

## Known limitation

The current consumer acknowledges delete events but does not delete silver
rows. If delete semantics are required, the streaming owner should add a
tombstone/DLQ path and coordinate a schema change with batch.
