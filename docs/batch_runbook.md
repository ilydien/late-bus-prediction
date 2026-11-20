# Batch runbook

## Services

```bash
docker compose -f compose.dev.yaml --profile dev up -d
docker compose -f compose.dev.yaml --profile dev ps
```

## Quality gate

Silver + gold:

```bash
docker exec -e PYTHONPATH=/opt/dagster/app \
  wmata-lateness-dagster-user-code-1 \
  python -m quality.checks --layer all
```

Staging gate (fails when staging is empty):

```bash
docker exec -e PYTHONPATH=/opt/dagster/app \
  wmata-lateness-dagster-user-code-1 \
  python -m quality.checks --layer gold_staging --fail-on-critical
```

Report:

```text
quality/reports/latest.json
```

Results:

```sql
SELECT * FROM public.data_quality_runs ORDER BY checked_at DESC;
```

## Historical bronze ingest

The `dlt` compose service is now a one-shot ingest that runs automatically on
`docker compose --profile dev up -d`. It skips dates already present in bronze.

Build the dlt image once:

```bash
docker build -t wmata-lateness-dlt -f dlt/Containerfile dlt
```

Manual Dagster asset:

```bash
docker exec -e DAGSTER_HOME=/tmp/dagster_home \
  wmata-lateness-dagster-user-code-1 \
  sh -c 'mkdir -p /tmp/dagster_home && dagster asset materialize \
    -f /opt/dagster/app/definitions.py --select bronze_ingest'
```

Schedule:

```text
batch_ingest_job_schedule
cron: 0 1 * * *
timezone: America/New_York
status: STOPPED
```

The ingest asset:

- runs `dlt/ingest_wmata_2020.py`
- uses the persistent `dlt-state` volume
- scans existing bronze objects and skips dates already present
- records `pipeline_runs` with started/done/skipped day counts

## Great Expectations

Staging gate:

```bash
docker exec -e PYTHONPATH=/opt/dagster/app \
  wmata-lateness-dagster-user-code-1 \
  python -m quality.gx_gate --dataset staging --fail-on-error
```

Gold gate:

```bash
docker exec -e PYTHONPATH=/opt/dagster/app \
  wmata-lateness-dagster-user-code-1 \
  python -m quality.gx_gate --dataset gold --fail-on-error
```

See `docs/great_expectations.md`.

## Gold incremental

```bash
docker exec -e DAGSTER_HOME=/tmp/dagster_home \
  wmata-lateness-dagster-user-code-1 \
  sh -c 'mkdir -p /tmp/dagster_home && dagster asset materialize \
    -f /opt/dagster/app/definitions.py --select gold_trip_delay_features'
```

The asset runs:

```text
silver quality gate
→ Spark incremental
→ gold staging quality gate
→ finalize
→ gold quality gate
→ pipeline_runs
```

## Backfill

Preview:

```bash
docker run --rm --network wmata-lateness_wmata-lateness-dev \
  --env-file .env -v "$PWD/tools:/app/tools:ro" \
  wmata-lateness-consumer python3 /app/tools/backfill_bronze.py \
  --limit 1 --dry-run
```

Publish a limited backfill:

```bash
docker run --rm --network wmata-lateness_wmata-lateness-dev \
  --env-file .env -v "$PWD/tools:/app/tools:ro" \
  wmata-lateness-consumer python3 /app/tools/backfill_bronze.py \
  --since 2020-01-06T00:00:00Z --until 2020-01-07T00:00:00Z --limit 5
```

Gold range backfill:

```bash
docker compose -f compose.dev.yaml --profile dev run --rm spark-submit \
  --driver-memory 2g /opt/spark/jobs/build_gold.py \
  --mode incremental \
  --start-ts 2020-02-08T23:00:00+00:00 \
  --end-ts 2020-02-08T23:10:00+00:00
```

Then merge staging with a run id:

```bash
docker exec wmata-lateness-consumer-1 \
  python3 /app/merge_silver.py --run-id <run-id>
```

## Batch inference

Manual:

```bash
docker run --rm --network wmata-lateness_wmata-lateness-dev \
  --env-file .env wmata-ml \
  python /app/batch_inference.py --model-alias champion --limit 500
```

Dry run:

```bash
docker run --rm --network wmata-lateness_wmata-lateness-dev \
  --env-file .env wmata-ml \
  python /app/batch_inference.py --model-alias champion --limit 5 --dry-run
```

Dagster asset:

```bash
dagster asset materialize -f /opt/dagster/app/definitions.py \
  --select batch_inference
```

Schedule: `batch_inference_job_schedule`, daily 04:00 America/New_York,
STOPPED until live data is available.

## Tests

```bash
docker run --rm --network wmata-lateness_wmata-lateness-dev \
  --env-file .env \
  -v "$PWD/ml:/app" -v "$PWD/tests:/app/tests:ro" \
  -v "$PWD/quality:/app/quality:ro" -v "$PWD/governance:/app/governance:ro" \
  -e PYTHONPATH=/app wmata-ml \
  sh -c 'cd /app && python3 -m unittest discover -s tests -v'
```

## Parquet export

Dagster:

```bash
docker exec -e DAGSTER_HOME=/tmp/dagster_home \
  wmata-lateness-dagster-user-code-1 \
  sh -c 'mkdir -p /tmp/dagster_home && dagster asset materialize \
    -f /opt/dagster/app/definitions.py --select gold_parquet_export'
```

Manual:

```bash
docker compose -f compose.dev.yaml --profile dev run --rm spark-submit \
  --driver-memory 2g /opt/spark/jobs/export_gold_parquet.py \
  --run-id manual-parquet-1
```

Verify:

```bash
docker compose -f compose.dev.yaml --profile dev run --rm spark-submit \
  --driver-memory 1g /opt/spark/jobs/verify_parquet.py \
  --path s3a://wmata/gold_parquet/trip_delay_features/version=<run_id>/
```

See `docs/parquet_storage.md` and `docs/adr/ADR-07-storage.md`.

## Lineage

```sql
SELECT run_id, job_name, layer, status, input_datasets, output_datasets,
       row_counts, started_at, finished_at, error
FROM public.pipeline_runs
ORDER BY started_at DESC;
```

## Schedules

```bash
docker exec -e DAGSTER_HOME=/tmp/dagster_home \
  wmata-lateness-dagster-user-code-1 \
  sh -c 'mkdir -p /tmp/dagster_home && dagster schedule list \
    -f /opt/dagster/app/definitions.py'
```

- `batch_ingest_job_schedule`: daily 01:00 America/New_York, STOPPED
- `batch_inference_job_schedule`: daily 04:00 America/New_York, STOPPED
- `parquet_export_job_schedule`: daily 03:30 America/New_York, RUNNING
- `data_quality_audit_schedule`: daily 05:00 America/New_York, RUNNING
- `weekly_retrain_job_schedule`: Monday 02:00 America/New_York, STOPPED

Enable only after live 2026 data is available.
