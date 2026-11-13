import os

import pandas as pd
from dagster import ConfigurableIOManager
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert


class PostgresIOManager(ConfigurableIOManager):
    host: str = os.environ.get(
        "PG_HOST", os.environ.get("POSTGRES_SILVER_HOST", "haproxy")
    )
    port: int = int(
        os.environ.get("PG_PORT", os.environ.get("POSTGRES_SILVER_PORT", "5433"))
    )
    user: str = os.environ.get(
        "PG_USER", os.environ.get("POSTGRES_SILVER_USER", "postgres")
    )
    password: str = os.environ.get(
        "PG_PASSWORD", os.environ.get("POSTGRES_SILVER_PASSWORD", "postgres")
    )
    database: str = os.environ.get(
        "PG_DB", os.environ.get("POSTGRES_DB_WMATA", "wmata")
    )
    schema_name: str = os.environ.get("PG_SCHEMA", "public")

    def _get_engine(self):
        url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        return create_engine(url, connect_args={"connect_timeout": 10})

    def handle_output(self, context, obj):
        if obj is None or (isinstance(obj, pd.DataFrame) and obj.empty):
            context.log.debug("No rows to write, skipping")
            return

        table = context.asset_key.path[-1]
        schema = self.schema_name

        engine = self._get_engine()
        metadata = MetaData(schema=schema)
        tbl = Table(table, metadata, autoload_with=engine)
        pk_cols = [c.name for c in tbl.primary_key.columns]

        with engine.begin() as conn:
            if pk_cols:
                records = obj.to_dict("records")
                stmt = pg_insert(tbl).values(records)
                update_cols = {
                    c.name: stmt.excluded[c.name]
                    for c in tbl.columns
                    if c.name not in pk_cols
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=pk_cols,
                    set_=update_cols,
                )
                conn.execute(stmt)
            else:
                obj.to_sql(
                    table,
                    conn,
                    schema=schema,
                    if_exists="append",
                    index=False,
                    method="multi",
                )

        context.log.info(f"Wrote {len(obj)} rows to {schema}.{table}")
        context.add_output_metadata(
            {"rows_written": len(obj), "table": f"{schema}.{table}"}
        )

    def load_input(self, context) -> pd.DataFrame:
        table = context.asset_key.path[-1]
        schema = self.schema_name

        engine = self._get_engine()
        with engine.connect() as conn:
            return pd.read_sql_table(table, conn, schema=schema)
