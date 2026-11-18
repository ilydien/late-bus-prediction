"""Build gold tables for the WMATA trip-delay model.

Source : silver PostgreSQL (HAProxy 5433, database wmata)
Target : gold PostgreSQL (HAProxy 5434, database wmata)

Tables:
  trip_delay_features   one row per trip/snapshot (5-minute sample), with
                        target_deviation_15m and train/valid/test split
  route_hourly_delay    route x date x hour aggregates
  route_daily_reliability route x date aggregates
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession, functions as F, Window

SILVER_HOST = os.environ.get("POSTGRES_SILVER_HOST", "haproxy")
SILVER_PORT = os.environ.get("POSTGRES_SILVER_PORT", "5433")
GOLD_HOST = os.environ.get("POSTGRES_GOLD_HOST", "haproxy")
GOLD_PORT = os.environ.get("POSTGRES_GOLD_PORT", "5434")
USER = os.environ.get("POSTGRES_SILVER_USER", "postgres")
PASSWORD = os.environ.get("POSTGRES_SILVER_PASSWORD", "postgres")

SILVER_URL = f"jdbc:postgresql://{SILVER_HOST}:{SILVER_PORT}/wmata"
GOLD_URL = f"jdbc:postgresql://{GOLD_HOST}:{GOLD_PORT}/wmata"
PROPS = {"user": USER, "password": PASSWORD, "driver": "org.postgresql.Driver"}


def write_gold(df, table: str) -> None:
    (
        df.write.mode("overwrite")
        .option("truncate", "true")
        .option("batchsize", 20000)
        .option("isolationLevel", "NONE")
        .option("reWriteBatchedInserts", "true")
        .jdbc(GOLD_URL, f"public.{table}", properties=PROPS)
    )
    print(f"gold {table} written", flush=True)


def parse_ts(value: str | None):
    if not value:
        return None
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["full", "incremental"], default="full")
    ap.add_argument("--start-ts", help="incremental backfill start (ISO-8601)")
    ap.add_argument("--end-ts", help="incremental backfill end (ISO-8601)")
    args = ap.parse_args()

    backfill_start = parse_ts(args.start_ts)
    backfill_end = parse_ts(args.end_ts)
    if args.mode == "full" and (backfill_start or backfill_end):
        ap.error("--start-ts/--end-ts are only supported with --mode incremental")
    if backfill_end and not backfill_start:
        ap.error("--end-ts requires --start-ts")

    spark = (
        SparkSession.builder.appName("wmata-silver-to-gold")
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    watermark = None
    output_start = None
    read_start = None
    read_end = None
    if args.mode == "incremental":
        rows = spark.read.jdbc(
            GOLD_URL,
            "(SELECT last_snapshot_ts FROM public.training_watermark "
            "WHERE pipeline='trip_delay_features') t",
            properties=PROPS,
        ).collect()
        if rows and rows[0][0] is not None:
            watermark = rows[0][0]

        if backfill_start is not None:
            output_start = backfill_start
            read_start = backfill_start - timedelta(minutes=20)
        elif watermark is not None:
            output_start = watermark
            read_start = watermark - timedelta(minutes=20)
        else:
            read_start = None

        if backfill_end is not None:
            read_end = backfill_end + timedelta(minutes=20)

        if backfill_start is None:
            max_row = spark.read.jdbc(
                SILVER_URL,
                "(SELECT max(snapshot_ts) AS m FROM public.bus_trip_snapshot) t",
                properties=PROPS,
            ).collect()[0]
            max_silver = max_row["m"]
            if watermark is not None and max_silver is not None and max_silver <= watermark:
                print(f"SKIP no new silver data (watermark={watermark})", flush=True)
                spark.stop()
                return

    conditions = []
    if read_start is not None:
        conditions.append(f"snapshot_ts >= '{read_start.isoformat()}'")
    if read_end is not None:
        conditions.append(f"snapshot_ts <= '{read_end.isoformat()}'")
    if conditions:
        dbtable = (
            "(SELECT * FROM public.bus_trip_snapshot WHERE "
            + " AND ".join(conditions)
            + ") t"
        )
        silver = spark.read.jdbc(SILVER_URL, dbtable, properties=PROPS)
    else:
        silver = spark.read.jdbc(SILVER_URL, "public.bus_trip_snapshot", properties=PROPS)
    silver = silver.select(
        "trip_key",
        "snapshot_ts",
        "event_time",
        "trip_start",
        "trip_end",
        "route_id",
        "direction_num",
        "service_date",
        "hour_local",
        "dow_local",
        "deviation_min",
        "speed_kmh",
        "progress_pct",
    )

    routes = spark.read.jdbc(SILVER_URL, "public.route_stats", properties=PROPS)
    routes = routes.select(
        "route_id",
        "trips_total",
        "median_headway_min",
        "median_scheduled_duration_min",
        "n_stops",
    )

    df = silver.join(routes, "route_id", "left")
    ddot = spark.read.jdbc(GOLD_URL, "public.route_ddot_features", properties=PROPS)
    ddot = ddot.select(
        "route_id",
        "aadt_route_median",
        "aadt_route_max",
        "am_speed_route_median",
        "am_speed_ratio",
        "am_travel_time_per_km",
    )
    df = df.join(ddot, "route_id", "left")

    # Target: average deviation 14-16 minutes ahead for the same trip.
    order = F.unix_timestamp("snapshot_ts")
    w = Window.partitionBy("trip_key").orderBy(order)
    df = df.withColumn(
        "target_deviation_15m", F.avg("deviation_min").over(w.rangeBetween(840, 960))
    )
    df = df.withColumn(
        "target_late_5",
        F.when(F.col("target_deviation_15m").isNull(), F.lit(None))
        .when(F.col("target_deviation_15m") > 5, F.lit(1))
        .otherwise(F.lit(0)),
    )
    df = df.filter(F.col("target_deviation_15m").isNotNull())

    df = (
        df.withColumn(
            "time_since_start_min",
            (F.unix_timestamp("event_time") - F.unix_timestamp("trip_start")) / 60.0,
        )
        .withColumn(
            "time_to_end_min",
            (F.unix_timestamp("trip_end") - F.unix_timestamp("event_time")) / 60.0,
        )
        .withColumn("hour_sin", F.sin(2 * 3.141592653589793 * F.col("hour_local") / 24))
        .withColumn("hour_cos", F.cos(2 * 3.141592653589793 * F.col("hour_local") / 24))
        .withColumn("dow_sin", F.sin(2 * 3.141592653589793 * F.col("dow_local") / 7))
        .withColumn("dow_cos", F.cos(2 * 3.141592653589793 * F.col("dow_local") / 7))
        .withColumn(
            "split",
            F.when(F.col("service_date") <= F.lit("2020-01-26").cast("date"), "train")
            .when(F.col("service_date") <= F.lit("2020-02-02").cast("date"), "valid")
            .otherwise("test"),
        )
    )

    # Keep the feature table manageable: one snapshot every 5 minutes.
    df = df.filter(F.minute("snapshot_ts") % 5 == 0)

    features = df.select(
        "trip_key",
        "snapshot_ts",
        "route_id",
        "direction_num",
        "service_date",
        "hour_local",
        "dow_local",
        F.col("deviation_min").alias("deviation_now"),
        "speed_kmh",
        "progress_pct",
        "time_since_start_min",
        "time_to_end_min",
        "trips_total",
        "median_headway_min",
        "median_scheduled_duration_min",
        "n_stops",
        "aadt_route_median",
        "aadt_route_max",
        "am_speed_ratio",
        "am_speed_route_median",
        "am_travel_time_per_km",
        "hour_sin",
        "hour_cos",
        "dow_sin",
        "dow_cos",
        "target_deviation_15m",
        "target_late_5",
        "split",
    )
    if args.mode == "incremental":
        if output_start is not None:
            features = features.filter(F.col("snapshot_ts") > F.lit(output_start))
        if backfill_end is not None:
            features = features.filter(F.col("snapshot_ts") <= F.lit(backfill_end))
        write_gold(features, "trip_delay_features_staging")
        print("incremental staging written; run finalize_gold.py", flush=True)
        spark.stop()
        return

    write_gold(features, "trip_delay_features")

    # Aggregates use the full silver table, not the 5-minute sample.
    agg_base = silver.select(
        "route_id", "service_date", "hour_local", "trip_key", "deviation_min", "speed_kmh"
    )
    on_time = F.avg(F.when(F.col("deviation_min") <= 5, 1).otherwise(0)) * 100
    late = F.avg(F.when(F.col("deviation_min") > 5, 1).otherwise(0)) * 100

    hourly = agg_base.groupBy("route_id", "service_date", "hour_local").agg(
        F.count("*").alias("trip_snapshots"),
        F.countDistinct("trip_key").alias("trips"),
        F.avg("deviation_min").alias("avg_deviation"),
        F.expr("percentile_approx(deviation_min, 0.5)").alias("median_deviation"),
        F.expr("percentile_approx(deviation_min, 0.9)").alias("p90_deviation"),
        on_time.alias("on_time_pct"),
        late.alias("late_pct"),
        F.avg("speed_kmh").alias("avg_speed_kmh"),
    )
    write_gold(hourly, "route_hourly_delay")

    daily = agg_base.groupBy("route_id", "service_date").agg(
        F.countDistinct("trip_key").alias("trips"),
        F.avg("deviation_min").alias("avg_deviation"),
        F.expr("percentile_approx(deviation_min, 0.5)").alias("median_deviation"),
        F.expr("percentile_approx(deviation_min, 0.9)").alias("p90_deviation"),
        on_time.alias("on_time_pct"),
        late.alias("late_pct"),
        F.avg("speed_kmh").alias("avg_speed_kmh"),
    )
    write_gold(daily, "route_daily_reliability")

    spark.stop()


if __name__ == "__main__":
    main()
