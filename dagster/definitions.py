import os

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)

from assets.inference import batch_inference
from assets.ingest import bronze_ingest
from assets.parquet_export import gold_parquet_export
from assets.quality_audit import data_quality_audit
from assets.retrain import gold_trip_delay_features, model_promotion, trip_delay_model
from resources.postgres_io_manager import PostgresIOManager


def _pg(layer: str, db_env: str, db_default: str) -> PostgresIOManager:
    return PostgresIOManager(
        host=os.environ.get(f"POSTGRES_{layer}_HOST", "haproxy"),
        port=int(
            os.environ.get(
                f"POSTGRES_{layer}_PORT", "5433" if layer == "SILVER" else "5434"
            )
        ),
        user=os.environ.get(f"POSTGRES_{layer}_USER", "postgres"),
        password=os.environ.get(f"POSTGRES_{layer}_PASSWORD", "postgres"),
        database=os.environ.get(db_env, db_default),
    )


weekly_retrain_job = define_asset_job(
    "weekly_retrain_job",
    selection=[
        "gold_trip_delay_features",
        "trip_delay_model",
        "model_promotion",
    ],
)

weekly_retrain_schedule = ScheduleDefinition(
    job=weekly_retrain_job,
    cron_schedule="0 2 * * 1",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

batch_inference_job = define_asset_job(
    "batch_inference_job",
    selection=["batch_inference"],
)

batch_inference_schedule = ScheduleDefinition(
    job=batch_inference_job,
    cron_schedule="0 4 * * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

batch_ingest_job = define_asset_job(
    "batch_ingest_job",
    selection=["bronze_ingest"],
)

batch_ingest_schedule = ScheduleDefinition(
    job=batch_ingest_job,
    cron_schedule="0 1 * * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

parquet_export_job = define_asset_job(
    "parquet_export_job",
    selection=[
        "gold_trip_delay_features",
        "gold_parquet_export",
    ],
)

parquet_export_schedule = ScheduleDefinition(
    job=parquet_export_job,
    cron_schedule="30 3 * * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.RUNNING,
)

data_quality_job = define_asset_job(
    "data_quality_audit_job",
    selection=["data_quality_audit"],
)

data_quality_schedule = ScheduleDefinition(
    name="data_quality_audit_schedule",
    job=data_quality_job,
    cron_schedule="0 5 * * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.RUNNING,
)


defs = Definitions(
    assets=[
        bronze_ingest,
        gold_trip_delay_features,
        gold_parquet_export,
        trip_delay_model,
        model_promotion,
        batch_inference,
        data_quality_audit,
    ],
    jobs=[
        weekly_retrain_job,
        batch_inference_job,
        batch_ingest_job,
        parquet_export_job,
        data_quality_job,
    ],
    schedules=[
        weekly_retrain_schedule,
        batch_inference_schedule,
        batch_ingest_schedule,
        parquet_export_schedule,
        data_quality_schedule,
    ],
    resources={
        "silver_wmata_io_manager": _pg("SILVER", "POSTGRES_DB_WMATA", "wmata"),
        "gold_wmata_io_manager": _pg("GOLD", "POSTGRES_DB_WMATA", "wmata"),
        "silver_traffic_io_manager": _pg("SILVER", "POSTGRES_DB_TRAFFIC", "traffic"),
        "gold_traffic_io_manager": _pg("GOLD", "POSTGRES_DB_TRAFFIC", "traffic"),
    },
)

