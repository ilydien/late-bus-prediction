"""Dagster assets for the weekly WMATA retraining pipeline.

Heavy work runs in isolated containers:
  wmata-spark-gold : silver -> gold (incremental)
  wmata-ml         : finalize, train, promote

The schedule is intentionally STOPPED until live 2026 data exists.
"""

import os
import re

import docker
import mlflow
import psycopg2
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
)

from governance import run_log
from quality.checks import run_checks
from quality.gx_gate import run_gx_gate

SPARK_IMAGE = os.environ.get("SPARK_GOLD_IMAGE", "wmata-spark-gold:latest")
ML_IMAGE = os.environ.get("ML_IMAGE", "wmata-ml:latest")
NETWORK = os.environ.get("WMATA_NETWORK", "wmata-lateness_wmata-lateness-dev")

GOLD_HOST = os.environ.get("POSTGRES_GOLD_HOST", "haproxy")
GOLD_PORT = int(os.environ.get("POSTGRES_GOLD_PORT", "5434"))
SILVER_HOST = os.environ.get("POSTGRES_SILVER_HOST", "haproxy")
SILVER_PORT = int(os.environ.get("POSTGRES_SILVER_PORT", "5433"))
DB = os.environ.get("POSTGRES_DB_WMATA", "wmata")
USER = os.environ.get("POSTGRES_GOLD_USER", "postgres")
PASSWORD = os.environ.get("POSTGRES_GOLD_PASSWORD", "postgres")
MLFLOW_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = os.environ.get("MLFLOW_MODEL_NAME", "trip_delay_lgbm")

PG_ENV = {
    "POSTGRES_SILVER_HOST": SILVER_HOST,
    "POSTGRES_SILVER_PORT": str(SILVER_PORT),
    "POSTGRES_GOLD_HOST": GOLD_HOST,
    "POSTGRES_GOLD_PORT": str(GOLD_PORT),
    "POSTGRES_DB_WMATA": DB,
    "POSTGRES_SILVER_USER": USER,
    "POSTGRES_SILVER_PASSWORD": PASSWORD,
    "POSTGRES_GOLD_USER": USER,
    "POSTGRES_GOLD_PASSWORD": PASSWORD,
}
S3_ENV = {
    **PG_ENV,
    "AWS_ACCESS_KEY_ID": os.environ.get("SEAWEEDFS_ACCESS_KEY", "seaweedfs-access-key"),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("SEAWEEDFS_SECRET_KEY", "seaweedfs-secret-key"),
    "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
    "AWS_EC2_METADATA_DISABLED": "true",
}

ML_ENV = {
    **PG_ENV,
    "MLFLOW_TRACKING_URI": MLFLOW_URI,
    "MLFLOW_EXPERIMENT": os.environ.get("MLFLOW_EXPERIMENT", "wmata-trip-delay"),
    "MLFLOW_MODEL_NAME": MODEL_NAME,
    "MLFLOW_S3_ENDPOINT_URL": os.environ.get("MLFLOW_S3_ENDPOINT_URL", "http://seaweedfs:8333"),
    "AWS_ACCESS_KEY_ID": os.environ.get("MLFLOW_KEY_ID", "mlflow-key-id"),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("MLFLOW_SECRET_KEY", "mlflow-secret-key"),
    "AWS_DEFAULT_REGION": "us-east-1",
    "AWS_S3_FORCE_PATH_STYLE": "true",
    "FEATURESET": "with_ddot",
    "MLFLOW_TRIGGERED_BY": "dagster",
    "TRAIN_WINDOW_WEEKS": os.environ.get("TRAIN_WINDOW_WEEKS", "12"),
    "TRAIN_MIN_WEEKS": os.environ.get("TRAIN_MIN_WEEKS", "4"),
    "TRAIN_VALID_DAYS": os.environ.get("TRAIN_VALID_DAYS", "7"),
    "TRAIN_TEST_DAYS": os.environ.get("TRAIN_TEST_DAYS", "7"),
    "GIT_COMMIT": run_log.git_commit(),
}

FEATURE_COLS = [
    "trip_key",
    "snapshot_ts",
    "route_id",
    "direction_num",
    "service_date",
    "hour_local",
    "dow_local",
    "deviation_now",
    "speed_kmh",
    "progress_pct",
    "time_since_start_min",
    "time_to_end_min",
    "trips_total",
    "median_headway_min",
    "median_scheduled_duration_min",
    "n_stops",
    "aadt_route_median",
    "aadt_route_max",
    "am_speed_ratio",
    "am_speed_route_median",
    "am_travel_time_per_km",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "target_deviation_15m",
    "target_late_5",
    "split",
]


def _finalize_gold(context: AssetExecutionContext) -> None:
    cols = ", ".join(FEATURE_COLS)
    updates = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in FEATURE_COLS if c not in ("trip_key", "snapshot_ts")
    )
    with _silver_conn() as silver, silver.cursor() as scur:
        scur.execute("SELECT max(snapshot_ts) FROM public.bus_trip_snapshot")
        silver_max = scur.fetchone()[0]

    with _gold_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM public.trip_delay_features_staging")
        staged = cur.fetchone()[0]
        merged = 0
        if staged:
            cur.execute(
                f"""
                INSERT INTO public.trip_delay_features ({cols})
                SELECT {cols} FROM public.trip_delay_features_staging
                ON CONFLICT (trip_key, snapshot_ts) DO UPDATE SET {updates}
                """
            )
            merged = cur.rowcount
            cur.execute("TRUNCATE public.trip_delay_features_staging")
        cur.execute(
            """
            INSERT INTO public.training_watermark
              (pipeline, last_snapshot_ts, last_trained_at, model_version, updated_at)
            SELECT 'trip_delay_features',
                   GREATEST(
                     (SELECT max(snapshot_ts) FROM public.trip_delay_features),
                     %s::timestamptz
                   ),
                   NULL, NULL, now()
            ON CONFLICT (pipeline) DO UPDATE SET
              last_snapshot_ts = EXCLUDED.last_snapshot_ts,
              updated_at = now()
            """,
            (silver_max,),
        )
        conn.commit()
    context.log.info(f"finalize staged={staged} merged={merged} silver_max={silver_max}")
    return staged, merged, silver_max


def _run(
    context: AssetExecutionContext,
    image: str,
    command: list[str],
    env: dict,
    entrypoint=None,
    volumes=None,
) -> str:
    client = docker.from_env()
    context.log.info(f"running {image}: {' '.join(command)}")
    try:
        logs = client.containers.run(
            image,
            command=command,
            entrypoint=entrypoint,
            network=NETWORK,
            environment=env,
            volumes=volumes,
            remove=True,
            detach=False,
        )
    except docker.errors.ContainerError as exc:
        context.log.error(str(exc))
        raise
    text = logs.decode("utf-8", errors="replace")
    context.log.info(text[-4000:])
    return text


def _gold_conn():
    return psycopg2.connect(
        host=GOLD_HOST, port=GOLD_PORT, dbname=DB, user=USER, password=PASSWORD
    )


def _silver_conn():
    return psycopg2.connect(
        host=SILVER_HOST, port=SILVER_PORT, dbname=DB, user=USER, password=PASSWORD
    )


@asset(
    group_name="retrain",
    retry_policy=RetryPolicy(max_retries=2, delay=10),
)
def gold_trip_delay_features(context: AssetExecutionContext) -> MaterializeResult:
    with _gold_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT last_snapshot_ts FROM public.training_watermark "
            "WHERE pipeline='trip_delay_features'"
        )
        row = cur.fetchone()
        watermark = row[0] if row else None

    with _silver_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT max(snapshot_ts) FROM public.bus_trip_snapshot")
        max_silver = cur.fetchone()[0]

    if watermark is not None and max_silver is not None and max_silver <= watermark:
        return MaterializeResult(
            metadata={
                "skipped": True,
                "reason": "no new silver data",
                "watermark": MetadataValue.text(str(watermark)),
            }
        )

    run_id = context.run_id
    run_log.safe_start_run(
        run_id,
        "gold_trip_delay_features",
        "gold",
        input_datasets=["silver.bus_trip_snapshot", "silver.route_stats"],
        output_datasets=["gold.trip_delay_features"],
        source_manifest={
            "batch_id": run_id,
            "watermark": str(watermark),
            "max_silver": str(max_silver),
        },
    )

    mode = "incremental" if watermark is not None else "full"
    try:
        run_checks(run_id=run_id, layer="silver", fail_on_critical=True)
        _run(
            context,
            SPARK_IMAGE,
            ["--driver-memory", "2g", "/opt/spark/jobs/build_gold.py", "--mode", mode],
            PG_ENV,
            entrypoint="/opt/spark/bin/spark-submit",
        )
        with _gold_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM public.trip_delay_features_staging")
            staged_rows = int(cur.fetchone()[0])
        if staged_rows == 0:
            # No row has a complete 14-16 minute future target yet. Advance the
            # watermark without failing the quality gate on an empty batch.
            staged, merged, silver_max = _finalize_gold(context)
            run_log.safe_finish_run(
                run_id,
                "success",
                row_counts={
                    "staged": staged,
                    "merged": merged,
                    "silver_max": str(silver_max),
                    "note": "empty staging",
                },
            )
            return MaterializeResult(
                metadata={
                    "mode": mode,
                    "skipped": True,
                    "reason": "empty gold staging",
                }
            )
        run_gx_gate(run_id=run_id, dataset="staging", fail_on_error=True)
        run_checks(run_id=run_id, layer="gold_staging", fail_on_critical=True)
        staged, merged, silver_max = _finalize_gold(context)
        run_checks(run_id=run_id, layer="gold", fail_on_critical=True)
        run_log.safe_finish_run(
            run_id,
            "success",
            row_counts={"staged": staged, "merged": merged, "silver_max": str(silver_max)},
        )
    except Exception as exc:  # noqa: BLE001 - record the failure before retrying
        run_log.safe_finish_run(run_id, "failed", error=str(exc))
        raise

    with _gold_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT last_snapshot_ts FROM public.training_watermark "
            "WHERE pipeline='trip_delay_features'"
        )
        new_wm = cur.fetchone()[0]
    return MaterializeResult(
        metadata={"mode": mode, "watermark": MetadataValue.text(str(new_wm))}
    )


@asset(
    deps=[gold_trip_delay_features],
    group_name="retrain",
    retry_policy=RetryPolicy(max_retries=2, delay=10),
)
def trip_delay_model(context: AssetExecutionContext) -> MaterializeResult:
    with _gold_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT last_snapshot_ts, last_trained_at FROM public.training_watermark "
            "WHERE pipeline='trip_delay_features'"
        )
        row = cur.fetchone()
    if not row:
        return MaterializeResult(metadata={"skipped": True, "reason": "no watermark"})
    last_snapshot, last_trained = row
    if last_trained is not None and last_snapshot is not None and last_trained >= last_snapshot:
        return MaterializeResult(
            metadata={
                "skipped": True,
                "reason": "model already trained on this snapshot",
                "last_snapshot": MetadataValue.text(str(last_snapshot)),
            }
        )

    run_id = context.run_id
    run_log.safe_start_run(
        run_id,
        "trip_delay_model",
        "ml",
        input_datasets=["gold.trip_delay_features"],
        output_datasets=["mlflow.trip_delay_lgbm", "gold.model_predictions"],
    )
    try:
        text = _run(context, ML_IMAGE, ["python", "/app/train_trip_delay.py"], ML_ENV)
        match = re.search(r"model_version=([0-9a-f]+)", text)
        mlflow_run_id = match.group(1) if match else "unknown"
        run_log.safe_finish_run(
            run_id,
            "success",
            row_counts={"mlflow_run_id": mlflow_run_id},
        )
    except Exception as exc:  # noqa: BLE001
        run_log.safe_finish_run(run_id, "failed", error=str(exc))
        raise
    return MaterializeResult(
        metadata={"training_log_tail": MetadataValue.text(text[-2000:])}
    )


@asset(deps=[trip_delay_model], group_name="retrain")
def model_promotion(context: AssetExecutionContext) -> MaterializeResult:
    mlflow.set_tracking_uri(MLFLOW_URI)
    client = mlflow.MlflowClient(MLFLOW_URI)
    versions = client.get_latest_versions(MODEL_NAME)
    if not versions:
        return MaterializeResult(metadata={"skipped": True, "reason": "no model versions"})
    candidate = max(versions, key=lambda v: int(v.version)).version
    text = _run(
        context,
        ML_IMAGE,
        ["python", "/app/promote_model.py", "--candidate-version", candidate],
        {**ML_ENV, "MLFLOW_TRACKING_URI": MLFLOW_URI},
    )
    return MaterializeResult(
        metadata={
            "candidate_version": MetadataValue.text(candidate),
            "promotion_log": MetadataValue.text(text[-2000:]),
        }
    )
