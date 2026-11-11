"""Connectivity smoke test for the Spark standalone cluster.

Checks that the driver can
  1. reach SeaweedFS through the S3A filesystem, and
  2. reach the silver PostgreSQL cluster through JDBC,

then exits. Run it with:

    docker compose -f compose.dev.yaml --profile dev run --rm spark-submit \
      /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
      /opt/spark/jobs/bronze_check.py
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

BRONZE_BUCKET = os.environ.get("SPARK_BRONZE_BUCKET", "wmata")
PG_URL = os.environ.get("SPARK_PG_URL", "jdbc:postgresql://postgres-silver:5432/wmata")
PG_USER = os.environ.get("POSTGRES_SILVER_USER", "postgres")
PG_PASSWORD = os.environ.get("POSTGRES_SILVER_PASSWORD", "postgres")


def main() -> None:
    spark = SparkSession.builder.appName("wmata-bronze-check").getOrCreate()
    print(f"Spark version: {spark.version}")

    # 1. SeaweedFS over S3A.
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    path = jvm.org.apache.hadoop.fs.Path(f"s3a://{BRONZE_BUCKET}/")
    print(f"s3a://{BRONZE_BUCKET}/ exists: {fs.exists(path)}")

    # 2. PostgreSQL over JDBC. `SELECT 1` proves the connection without
    # depending on any silver table existing yet.
    rows = (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("dbtable", "(SELECT current_database() AS db, current_user AS usr) t")
        .load()
    )
    rows.show(truncate=False)

    spark.stop()
    print("spark connectivity check: OK")


if __name__ == "__main__":
    main()
