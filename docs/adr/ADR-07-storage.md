# ADR-07: Analytical storage and consistency

## Context

The platform already has an ACID serving layer:

- PostgreSQL silver/gold via Spock
- idempotent `ON CONFLICT` upserts
- backfill and replay
- versioned model artifacts in MLflow

The TBP F-07 requirement asks for:

- Parquet for analytical datasets
- a table format (Iceberg/Delta/Hudi) **or** an equivalent ACID
  database with documented limitations
- evidence of write/update/upsert/replay
- a table version/snapshot
- schema evolution

The local environment is a single Docker host. Running Iceberg/Delta/Hudi
would add JVM/table-format dependencies and operational overhead without a
second storage tier.

## Decision

Use a **two-tier analytical design**:

1. **PostgreSQL gold** remains the ACID serving layer.
   - API/dashboard read from PostgreSQL.
   - Upserts, retries, and backfills are transactional.

2. **Parquet snapshots in SeaweedFS** are the analytical copy.
   - Path:

     ```text
     s3://wmata/gold_parquet/<table>/version=<run_id>/
     ```

   - Partitioned by `service_date` where applicable.
   - Snappy compression.
   - Immutable per `run_id`.

3. A manifest is written for each snapshot:

   ```text
   s3://wmata/gold_parquet/_manifest/<run_id>.json
   s3://wmata/gold_parquet/_latest.json
   ```

   The manifest records row counts, paths, partitions, source watermark, and
   git commit.

4. Runtime lineage is recorded in `public.pipeline_runs`.

## Why not Iceberg/Delta/Hudi?

- The local Docker host has limited RAM/CPU for an additional table-format
  catalog and compaction workload.
- PostgreSQL already provides ACID, upsert, replay, and schema evolution.
- Parquet snapshots give the analytical/reproducibility benefit without
  replacing the serving layer.
- The limitation is documented here: Parquet snapshots are immutable, not a
  transactional table. Updates are made in PostgreSQL and exported as a new
  snapshot.

## Consequences

### Positive

- F-07 Parquet requirement is met.
- Snapshot/version is explicit via `version=<run_id>`.
- ML/audit/benchmark can use a stable snapshot.
- Serving remains low-latency in PostgreSQL.
- No API/dashboard rewrite.

### Negative

- Parquet files are not ACID/table-format; there is no merge-on-read.
- Schema evolution is snapshot-level: old snapshots keep their old schema.
- Trino needs an S3/Hive/Iceberg catalog before querying Parquet directly.
- Retention must be managed manually or by a future job.

## Evidence

- `spark/jobs/export_gold_parquet.py`
- `spark/jobs/verify_parquet.py`
- Dagster `gold_parquet_export`
- `pipeline_runs` row for export
- manifest in `s3://wmata/gold_parquet/_manifest/`
- Parquet files under `version=<run_id>/`
- schema evolution: `model_predictions` includes `status`, `error`,
  `inference_run_id`
- replay: same `run_id` rerun produced the same row counts
