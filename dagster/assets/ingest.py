"""Scheduled historical bronze ingestion."""

import os
import re

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
)

from assets.retrain import _run
from governance import run_log

DLT_IMAGE = os.environ.get("DLT_IMAGE", "wmata-lateness-dlt:latest")
DLT_STATE_VOLUME = os.environ.get("DLT_STATE_VOLUME", "wmata-lateness_dlt-state")


@asset(
    group_name="batch",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
)
def bronze_ingest(context: AssetExecutionContext) -> MaterializeResult:
    run_id = context.run_id
    start = os.environ.get("DLT_START_DATE", "2020-01-06")
    end = os.environ.get("DLT_END_DATE", "2020-02-08")
    run_log.safe_start_run(
        run_id,
        "bronze_ingest",
        "bronze",
        input_datasets=["wmata.historical.2020"],
        output_datasets=["s3://wmata/bronze_2020/bus_positions"],
        source_manifest={"batch_id": run_id, "start_date": start, "end_date": end},
    )
    env = {
        "START_DATE": start,
        "END_DATE": end,
        "WMATA_KEY_ID": os.environ.get("WMATA_KEY_ID", "wmata-key-id"),
        "WMATA_SECRET_KEY": os.environ.get("WMATA_SECRET_KEY", "wmata-secret-key"),
        "SEAWEEDFS_S3_ENDPOINT": os.environ.get(
            "SEAWEEDFS_S3_ENDPOINT", "http://seaweedfs:8333"
        ),
        "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
        "DLT_PROJECT_DIR": "/app",
    }
    try:
        text = _run(
            context,
            DLT_IMAGE,
            ["ingest_wmata_2020.py"],
            env,
            volumes={DLT_STATE_VOLUME: {"bind": "/work", "mode": "rw"}},
        )
        started = len(re.findall(r"^START ", text, re.MULTILINE))
        done = len(re.findall(r"^DONE ", text, re.MULTILINE))
        skipped = len(re.findall(r"^SKIP ", text, re.MULTILINE))
        run_log.safe_finish_run(
            run_id,
            "success",
            row_counts={
                "days_started": started,
                "days_done": done,
                "days_skipped": skipped,
            },
        )
    except Exception as exc:  # noqa: BLE001
        run_log.safe_finish_run(run_id, "failed", error=str(exc))
        raise
    return MaterializeResult(
        metadata={
            "start_date": MetadataValue.text(start),
            "end_date": MetadataValue.text(end),
            "days_started": started,
            "days_done": done,
            "days_skipped": skipped,
            "log_tail": MetadataValue.text(text[-2000:]),
        }
    )
