"""Read a Parquet snapshot back for verification."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", required=True)
    args = ap.parse_args()
    spark = SparkSession.builder.appName("wmata-parquet-verify").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.parquet(args.path)
    print(f"PARQUET rows={df.count()} path={args.path}", flush=True)
    df.printSchema()
    spark.stop()


if __name__ == "__main__":
    main()
