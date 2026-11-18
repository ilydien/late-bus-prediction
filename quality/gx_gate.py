"""Great Expectations gate for gold staging/features.

Uses GX 1.25 file context under ``gx_quality``. The gate is intentionally
focused on the small gold staging table so it can run on every batch without
scanning 18M silver rows.
"""

from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus

import great_expectations as gx

GOLD_HOST = os.environ.get("POSTGRES_GOLD_HOST", "haproxy")
GOLD_PORT = os.environ.get("POSTGRES_GOLD_PORT", "5434")
GOLD_DB = os.environ.get("POSTGRES_DB_WMATA", "wmata")
GOLD_USER = os.environ.get("POSTGRES_GOLD_USER", "postgres")
GOLD_PASSWORD = os.environ.get("POSTGRES_GOLD_PASSWORD", "postgres")
GX_ROOT = os.environ.get("GX_ROOT", "/opt/dagster/app/gx_quality")
REPORT_DIR = Path(
    os.environ.get("GX_REPORT_DIR", "/opt/dagster/app/quality/reports")
)


def _connection_string() -> str:
    user = quote_plus(GOLD_USER)
    password = quote_plus(GOLD_PASSWORD)
    return (
        f"postgresql+psycopg2://{user}:{password}"
        f"@{GOLD_HOST}:{GOLD_PORT}/{GOLD_DB}"
    )


def _get_or_add_asset(datasource, name: str, table: str):
    try:
        return datasource.get_asset(name)
    except Exception:
        return datasource.add_table_asset(name=name, table_name=table)


def _get_or_add_batch(asset, name: str = "whole"):
    try:
        return asset.get_batch_definition(name)
    except Exception:
        return asset.add_batch_definition_whole_table(name)


def _suite(name: str, table: str):
    suite = gx.ExpectationSuite(name=name)
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)
    )
    for column in ("trip_key", "snapshot_ts", "route_id", "target_deviation_15m"):
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
        )
    suite.add_expectation(
        gx.expectations.ExpectCompoundColumnsToBeUnique(
            column_list=["trip_key", "snapshot_ts"]
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="target_deviation_15m", min_value=-60, max_value=60
        )
    )
    return suite


def run_gx_gate(
    run_id: str | None = None,
    dataset: str = "staging",
    fail_on_error: bool = False,
) -> dict:
    run_id = run_id or str(uuid.uuid4())
    table = "trip_delay_features_staging" if dataset == "staging" else "trip_delay_features"
    asset_name = f"{dataset}_trip_delay_features"
    suite_name = f"{dataset}_trip_delay_features_suite"

    context = gx.get_context(mode="file", project_root_dir=GX_ROOT)
    datasource = context.data_sources.add_or_update_postgres(
        name="gold",
        connection_string=_connection_string(),
    )
    asset = _get_or_add_asset(datasource, asset_name, table)
    batch_definition = _get_or_add_batch(asset)
    suite = _suite(suite_name, table)
    context.suites.add_or_update(suite)

    validation = gx.ValidationDefinition(
        name=f"{dataset}_trip_delay_validation",
        data=batch_definition,
        suite=suite,
    )
    context.validation_definitions.add_or_update(validation)
    checkpoint = gx.Checkpoint(
        name=f"{dataset}_trip_delay_checkpoint",
        validation_definitions=[validation],
    )
    context.checkpoints.add_or_update(checkpoint)
    result = checkpoint.run()
    success = bool(result.success)

    try:
        context.build_data_docs()
    except Exception:
        pass

    summary = {
        "run_id": run_id,
        "dataset": dataset,
        "table": table,
        "success": success,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "checkpoint": f"{dataset}_trip_delay_checkpoint",
    }
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / f"gx_{run_id}.json"
    path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    (REPORT_DIR / "gx_latest.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )

    if fail_on_error and not success:
        raise RuntimeError(f"Great Expectations gate failed for {table}")
    return summary


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", choices=["staging", "gold"], default="staging")
    ap.add_argument("--run-id")
    ap.add_argument("--fail-on-error", action="store_true")
    args = ap.parse_args()
    summary = run_gx_gate(
        run_id=args.run_id,
        dataset=args.dataset,
        fail_on_error=args.fail_on_error,
    )
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()
