"""Export gold marts to versioned Parquet snapshots in SeaweedFS."""

import json
import os
from datetime import datetime, timezone

import boto3
import psycopg2
from botocore.config import Config
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
)

from assets.retrain import S3_ENV, SPARK_IMAGE, _run
from governance import run_log

S3_ENDPOINT = os.environ.get("SEAWEEDFS_S3_ENDPOINT", "http://seaweedfs:8333")
ACCESS = os.environ.get("SEAWEEDFS_ACCESS_KEY", "seaweedfs-access-key")
SECRET = os.environ.get("SEAWEEDFS_SECRET_KEY", "seaweedfs-secret-key")
REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("PARQUET_BUCKET", "wmata")
PREFIX = os.environ.get("PARQUET_PREFIX", "gold_parquet")

GOLD_HOST = os.environ.get("POSTGRES_GOLD_HOST", "haproxy")
GOLD_PORT = int(os.environ.get("POSTGRES_GOLD_PORT", "5434"))
GOLD_DB = os.environ.get("POSTGRES_DB_WMATA", "wmata")
GOLD_USER = os.environ.get("POSTGRES_GOLD_USER", "postgres")
GOLD_PASSWORD = os.environ.get("POSTGRES_GOLD_PASSWORD", "postgres")

TABLES = {
    "trip_delay_features": "service_date",
    "route_hourly_delay": "service_date",
    "route_daily_reliability": "service_date",
    "route_ddot_features": None,
    "model_predictions": "model_version",
}


def _gold_conn():
    return psycopg2.connect(
        host=GOLD_HOST,
        port=GOLD_PORT,
        dbname=GOLD_DB,
        user=GOLD_USER,
        password=GOLD_PASSWORD,
        connect_timeout=10,
    )


def _cleanup_snapshots(context: AssetExecutionContext, run_id: str) -> int:
    keep_n = int(os.environ.get("PARQUET_RETENTION_VERSIONS", "7"))
    if keep_n <= 0:
        return 0
    with _gold_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id FROM public.pipeline_runs
            WHERE job_name = 'gold_parquet_export' AND status = 'success'
            ORDER BY started_at DESC
            """
        )
        all_runs = [row[0] for row in cur.fetchall()]
    if len(all_runs) <= keep_n:
        return 0
    keep = set(all_runs[:keep_n])
    candidates = [r for r in all_runs[keep_n:] if r != run_id and r not in keep]
    if not candidates:
        return 0

    client = _s3()
    deleted = 0
    for table in TABLES:
        for version in candidates:
            prefix = f"{PREFIX}/{table}/version={version}/"
            paginator = client.get_paginator("list_objects_v2")
            keys = []
            for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
                keys.extend(obj["Key"] for obj in page.get("Contents", []))
            if keys:
                client.delete_objects(
                    Bucket=BUCKET,
                    Delete={"Objects": [{"Key": k} for k in keys]},
                )
                deleted += len(keys)
    for version in candidates:
        client.delete_object(Bucket=BUCKET, Key=f"{PREFIX}/_manifest/{version}.json")
    context.log.info(
        "parquet retention kept=%s deleted_versions=%s deleted_objects=%s",
        keep_n,
        len(candidates),
        deleted,
    )
    return len(candidates)


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS,
        aws_secret_access_key=SECRET,
        region_name=REGION,
        config=Config(s3={"addressing_style": "path"}),
    )


@asset(
    deps=["gold_trip_delay_features"],
    group_name="gold",
    retry_policy=RetryPolicy(max_retries=1, delay=30),
)
def gold_parquet_export(context: AssetExecutionContext) -> MaterializeResult:
    run_id = context.run_id
    run_log.safe_start_run(
        run_id,
        "gold_parquet_export",
        "gold",
        input_datasets=list(f"gold.{t}" for t in TABLES),
        output_datasets=[f"s3://{BUCKET}/{PREFIX}"],
        source_manifest={"run_id": run_id, "bucket": BUCKET, "prefix": PREFIX},
    )
    try:
        _run(
            context,
            SPARK_IMAGE,
            [
                "--driver-memory",
                "2g",
                "/opt/spark/jobs/export_gold_parquet.py",
                "--run-id",
                run_id,
            ],
            S3_ENV,
            entrypoint="/opt/spark/bin/spark-submit",
        )

        counts = {}
        with _gold_conn() as conn, conn.cursor() as cur:
            for table in TABLES:
                cur.execute(f"SELECT count(*) FROM public.{table}")
                counts[table] = int(cur.fetchone()[0])
            cur.execute(
                "SELECT last_snapshot_ts FROM public.training_watermark "
                "WHERE pipeline='trip_delay_features'"
            )
            row = cur.fetchone()
            watermark = str(row[0]) if row and row[0] else None

        manifest = {
            "run_id": run_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "git_commit": run_log.git_commit(),
            "source_watermark": watermark,
            "bucket": BUCKET,
            "prefix": PREFIX,
            "tables": {
                table: {
                    "rows": counts[table],
                    "path": f"s3://{BUCKET}/{PREFIX}/{table}/version={run_id}/",
                    "partition": TABLES[table],
                }
                for table in TABLES
            },
        }
        body = json.dumps(manifest, indent=2).encode()
        client = _s3()
        client.put_object(
            Bucket=BUCKET,
            Key=f"{PREFIX}/_manifest/{run_id}.json",
            Body=body,
            ContentType="application/json",
        )
        client.put_object(
            Bucket=BUCKET,
            Key=f"{PREFIX}/_latest.json",
            Body=body,
            ContentType="application/json",
        )
        run_log.safe_finish_run(run_id, "success", row_counts=counts)
        try:
            _cleanup_snapshots(context, run_id)
        except Exception as exc:  # noqa: BLE001 - retention must not fail export
            context.log.warning("parquet retention cleanup failed: %s", exc)
    except Exception as exc:  # noqa: BLE001
        run_log.safe_finish_run(run_id, "failed", error=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "run_id": MetadataValue.text(run_id),
            "manifest": MetadataValue.text(f"s3://{BUCKET}/{PREFIX}/_manifest/{run_id}.json"),
            "tables": MetadataValue.text(json.dumps(counts)),
        }
    )
