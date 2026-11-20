"""Export gold analytical tables to versioned Parquet snapshots.

Target:
  s3a://wmata/gold_parquet/<table>/version=<run_id>/

The snapshot is immutable. PostgreSQL remains the ACID serving layer.
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession

GOLD_HOST = os.environ.get("POSTGRES_GOLD_HOST", "haproxy")
GOLD_PORT = os.environ.get("POSTGRES_GOLD_PORT", "5434")
GOLD_DB = os.environ.get("POSTGRES_DB_WMATA", "wmata")
GOLD_USER = os.environ.get("POSTGRES_GOLD_USER", "postgres")
GOLD_PASSWORD = os.environ.get("POSTGRES_GOLD_PASSWORD", "postgres")
BUCKET = os.environ.get("PARQUET_BUCKET", "wmata")
PREFIX = os.environ.get("PARQUET_PREFIX", "gold_parquet")

GOLD_URL = f"jdbc:postgresql://{GOLD_HOST}:{GOLD_PORT}/{GOLD_DB}"
PROPS = {
    "user": GOLD_USER,
    "password": GOLD_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# table -> partition column (None = unpartitioned)
TABLES = {
    "trip_delay_features": "service_date",
    "route_hourly_delay": "service_date",
    "route_daily_reliability": "service_date",
    "route_ddot_features": None,
    "model_predictions": "model_version",
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--tables", default=",".join(TABLES))
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("wmata-gold-parquet-export")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    selected = [t.strip() for t in args.tables.split(",") if t.strip()]
    for table in selected:
        partition = TABLES.get(table)
        df = spark.read.jdbc(GOLD_URL, f"public.{table}", properties=PROPS)
        path = f"s3a://{BUCKET}/{PREFIX}/{table}/version={args.run_id}/"
        writer = (
            df.write.mode("overwrite")
            .option("compression", "snappy")
        )
        if partition and partition in df.columns:
            writer = writer.partitionBy(partition)
        writer.parquet(path)
        print(f"EXPORTED {table} rows={df.count()} path={path}", flush=True)

    spark.stop()


if __name__ == "__main__":
    main()
