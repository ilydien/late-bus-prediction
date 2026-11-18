"""Daily data quality audit across silver and gold."""

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


@asset(
    group_name="quality",
    retry_policy=RetryPolicy(max_retries=1, delay=30),
)
def data_quality_audit(context: AssetExecutionContext) -> MaterializeResult:
    run_id = context.run_id
    run_log.safe_start_run(
        run_id,
        "data_quality_audit",
        "quality",
        input_datasets=[
            "silver.wmata.bus_trip_snapshot",
            "silver.wmata.route_stats",
            "gold.wmata.trip_delay_features",
            "gold.wmata.model_predictions",
        ],
        output_datasets=[
            "gold.data_quality_runs",
            "quality.reports.latest",
            "quality.reports.gx_latest",
        ],
    )
    try:
        report = run_checks(
            run_id=run_id,
            layer="all",
            fail_on_critical=True,
        )
        gx = run_gx_gate(
            run_id=run_id,
            dataset="gold",
            fail_on_error=True,
        )
        run_log.safe_finish_run(
            run_id,
            "success",
            row_counts={
                "checks": report["summary"]["total"],
                "pass": report["summary"]["pass"],
                "fail": report["summary"]["fail"],
                "warn": report["summary"]["warn"],
                "gx_success": gx["success"],
            },
        )
    except Exception as exc:  # noqa: BLE001
        run_log.safe_finish_run(run_id, "failed", error=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "summary": MetadataValue.text(str(report["summary"])),
            "gx_success": gx["success"],
            "report": MetadataValue.text("quality/reports/latest.json"),
            "gx_report": MetadataValue.text("quality/reports/gx_latest.json"),
        }
    )
