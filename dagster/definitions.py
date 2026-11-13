import os

from dagster import Definitions

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


defs = Definitions(
    assets=[],
    resources={
        "silver_wmata_io_manager": _pg("SILVER", "POSTGRES_DB_WMATA", "wmata"),
        "gold_wmata_io_manager": _pg("GOLD", "POSTGRES_DB_WMATA", "wmata"),
        "silver_traffic_io_manager": _pg("SILVER", "POSTGRES_DB_TRAFFIC", "traffic"),
        "gold_traffic_io_manager": _pg("GOLD", "POSTGRES_DB_TRAFFIC", "traffic"),
    },
)

