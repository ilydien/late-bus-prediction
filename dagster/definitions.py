import os

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)

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


defs = Definitions(
    assets=[gold_trip_delay_features, trip_delay_model, model_promotion],
    jobs=[weekly_retrain_job],
    schedules=[weekly_retrain_schedule],
    resources={
        "silver_wmata_io_manager": _pg("SILVER", "POSTGRES_DB_WMATA", "wmata"),
        "gold_wmata_io_manager": _pg("GOLD", "POSTGRES_DB_WMATA", "wmata"),
        "silver_traffic_io_manager": _pg("SILVER", "POSTGRES_DB_TRAFFIC", "traffic"),
        "gold_traffic_io_manager": _pg("GOLD", "POSTGRES_DB_TRAFFIC", "traffic"),
    },
)

