"""Scheduled batch inference asset."""

import json
import os
import re

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
)

from assets.retrain import ML_ENV, ML_IMAGE, NETWORK, _run
from governance import run_log


@asset(
    deps=["model_promotion"],
    group_name="ml",
    retry_policy=RetryPolicy(max_retries=2, delay=10),
)
def batch_inference(context: AssetExecutionContext) -> MaterializeResult:
    run_id = context.run_id
    limit = os.environ.get("INFERENCE_BATCH_SIZE", "500")
    run_log.safe_start_run(
        run_id,
        "batch_inference",
        "ml",
        input_datasets=["gold.trip_delay_features", "mlflow.trip_delay_lgbm@champion"],
        output_datasets=["gold.model_predictions"],
        source_manifest={"limit": limit, "model_alias": "champion"},
    )
    try:
        text = _run(
            context,
            ML_IMAGE,
            [
                "python",
                "/app/batch_inference.py",
                "--model-alias",
                "champion",
                "--limit",
                limit,
                "--run-id",
                run_id,
            ],
            ML_ENV,
        )
        match = re.search(r"INFERENCE_SUMMARY (\{.*\})", text)
        summary = json.loads(match.group(1)) if match else {}
        run_log.safe_finish_run(
            run_id,
            "success",
            row_counts={
                "rows_read": summary.get("rows_read"),
                "success": summary.get("success"),
                "errors": summary.get("errors"),
                "model_version": summary.get("model_version"),
            },
        )
    except Exception as exc:  # noqa: BLE001
        run_log.safe_finish_run(run_id, "failed", error=str(exc))
        raise
    return MaterializeResult(
        metadata={
            "summary": MetadataValue.text(json.dumps(summary)),
            "log_tail": MetadataValue.text(text[-2000:]),
        }
    )
