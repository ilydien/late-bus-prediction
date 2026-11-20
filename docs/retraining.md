# Weekly retraining (prepared, schedule stopped)

The weekly retraining pipeline is implemented but intentionally **not active**.
It must only be enabled after live 2026 WMATA data is flowing into silver.

## Flow

```
silver.bus_trip_snapshot
        │  Spark incremental read (watermark - 20 min overlap)
        ▼
gold.trip_delay_features_staging
        │  upsert + watermark advance
        ▼
gold.trip_delay_features
        │  rolling 12-week training window
        ▼
MLflow run + registered model version
        │  champion / challenger gate
        ▼
MLflow alias `champion` (deployable) or `challenger`
```

## Tables

- `gold.training_watermark`
  - `last_snapshot_ts`: highest silver snapshot processed into gold.
  - `last_trained_at`: last successful training run.
  - `model_version`: MLflow run id of the last training run.
- `gold.trip_delay_features_staging`
  - Staging table with the same columns as `gold.trip_delay_features`.
  - Spark writes here; the merge is idempotent (`ON CONFLICT` upsert).

DDL: `gold/ddl/003_retrain.sql`.

## Dagster

- Assets:
  - `gold_trip_delay_features`
  - `trip_delay_model`
  - `model_promotion`
- Job: `weekly_retrain_job`
- Schedule: `weekly_retrain_job_schedule`
  - Cron: `0 2 * * 1` (Monday 02:00)
  - Timezone: `America/New_York`
  - Status: **STOPPED**

Heavy work runs in isolated containers from the Dagster user-code container:

- `wmata-spark-gold:latest` — Spark incremental gold build.
- `wmata-ml:latest` — training and promotion.

The Dagster user-code container mounts the Docker socket so it can launch
those containers. This is a local-development convenience; do not expose the
Dagster container publicly with the Docker socket mounted.

Rebuild the images after changing jobs/scripts:

```bash
docker build -t wmata-spark-gold -f spark/gold/Containerfile spark
docker build -t wmata-ml -f ml/Containerfile ml
```

## Training window

`ml/train_trip_delay.py` derives the split from the newest gold service date:

- Window: last `TRAIN_WINDOW_WEEKS` weeks (default `12`).
- Minimum data span: `TRAIN_MIN_WEEKS` weeks (default `4`).
- Test: last `TRAIN_TEST_DAYS` days (default `7`).
- Valid: the `TRAIN_VALID_DAYS` days before test (default `7`).
- Train: everything earlier in the window.

The stored `split` column is ignored for training; it is retained only for
historical inspection. This is required because the 2020 prototype split is
hard-coded to 2020 dates.

## Promotion gate

`ml/promote_model.py` compares the candidate against the current champion:

- MAE must improve by at least **2.5%**.
- RMSE may worsen by at most **2%**.
- F1 (`late > 5`) may drop by at most **0.01**.

Passing candidate gets the `champion` alias. A failing candidate gets the
`challenger` alias and the champion stays unchanged.

Current registry state:

| Alias | Version | Run |
|---|---|---|
| `champion` | v1 (M2) | `2c018fb28a6241f2893deef8f775bcf7` |
| `challenger` | v2 (M3) | `ebd9be7dda4146b9b7d1824df78e506d` |

M3 was not promoted: MAE improved from `2.18757` to `2.18609` (about `0.07%`),
which is below the `2.5%` gate. Both versions have `READY` status and their
artifacts are under `s3://mlflow/...`.

## Manual checks

Incremental gold guard (no new silver data should skip):

```bash
docker exec -e DAGSTER_HOME=/tmp/dagster_home \
  wmata-lateness-dagster-user-code-1 \
  dagster asset materialize -f /opt/dagster/app/definitions.py \
  --select gold_trip_delay_features
```

Promotion check:

```bash
docker run --rm --network wmata-lateness_wmata-lateness-dev \
  -e MLFLOW_TRACKING_URI=http://mlflow:5000 \
  wmata-ml python /app/promote_model.py --candidate-version 2
```

## Activation checklist (after live 2026 data exists)

1. Confirm the live poller is writing 2026 snapshots to silver.
2. Confirm `gold.training_watermark.last_snapshot_ts` advances after a manual
   `gold_trip_delay_features` materialization.
3. Run one manual `trip_delay_model` materialization and verify:
   - a new MLflow run/version is created;
   - `last_trained_at` advances;
   - the candidate either becomes `champion` or `challenger`.
4. Enable the schedule in the Dagster UI, or with the Dagster CLI.
5. Watch the first scheduled run before leaving it unattended.

## Known limitation

Incremental mode updates `gold.trip_delay_features` only. The dashboard marts
(`route_hourly_delay`, `route_daily_reliability`) still require a full gold
rebuild. Retraining does not depend on those marts.
