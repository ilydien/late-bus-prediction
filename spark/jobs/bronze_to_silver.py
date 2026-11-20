"""Bronze (WMATA 2020 JSONL + DDOT GeoJSON) -> silver Parquet.

Silver WMATA grain: one row per trip per snapshot, including the fields needed
by the LightGBM trip-delay design (deviation, progress, speed, local time,
route). Gold-level joins/aggregates are intentionally out of scope.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import statistics
import zipfile
from datetime import datetime

import boto3
from botocore.config import Config
from pyspark.sql import SparkSession, functions as F, types as T, Window
from pyspark.sql.types import DoubleType

S3_ENDPOINT = os.environ.get("SEAWEEDFS_S3_ENDPOINT", "http://seaweedfs:8333")
ACCESS = os.environ.get("SEAWEEDFS_ACCESS_KEY", "seaweedfs-access-key")
SECRET = os.environ.get("SEAWEEDFS_SECRET_KEY", "seaweedfs-secret-key")
REGION = os.environ.get("AWS_REGION", "us-east-1")
GTFS_ZIP = "reference/gtfs_static/version=20200106/bus-gtfs-static.zip"
PG_HOST = os.environ.get("POSTGRES_SILVER_HOST", "haproxy")
PG_PORT = os.environ.get("POSTGRES_SILVER_PORT", "5433")
PG_USER = os.environ.get("POSTGRES_SILVER_USER", "postgres")
PG_PASSWORD = os.environ.get("POSTGRES_SILVER_PASSWORD", "postgres")

SCHEMA = T.StructType(
    [
        T.StructField("vehicle_id", T.StringType()),
        T.StructField("route_id", T.StringType()),
        T.StructField("trip_id", T.StringType()),
        T.StructField("direction_num", T.IntegerType()),
        T.StructField("direction", T.StringType()),
        T.StructField("trip_headsign", T.StringType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
        T.StructField("deviation_min", T.DoubleType()),
        T.StructField("trip_start", T.StringType()),
        T.StructField("trip_end", T.StringType()),
        T.StructField("block_number", T.StringType()),
        T.StructField("event_time", T.StringType()),
        T.StructField("hour_local", T.IntegerType()),
        T.StructField("dow_local", T.IntegerType()),
        T.StructField("snapshot_ts", T.StringType()),
        T.StructField("service_date", T.StringType()),
        T.StructField("source_file", T.StringType()),
    ]
)


def write_jdbc(df, database: str, table: str) -> None:
    url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{database}"
    props = {
        "user": PG_USER,
        "password": PG_PASSWORD,
        "driver": "org.postgresql.Driver",
    }
    (
        df.write.mode("overwrite")
        .option("truncate", "true")
        .option("batchsize", 20000)
        .option("isolationLevel", "NONE")
        .option("reWriteBatchedInserts", "true")
        .jdbc(url, table, properties=props)
    )
    print(f"silver {database}.{table} written", flush=True)


def s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS,
        aws_secret_access_key=SECRET,
        region_name=REGION,
        config=Config(s3={"addressing_style": "path"}),
    )


def haversine_km(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2):
        return None
    from math import asin, cos, radians, sin, sqrt

    r = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * r * asin(sqrt(a))


HAVERSINE = F.udf(haversine_km, DoubleType())


def silver_wmata(spark: SparkSession, keys: list[str]) -> None:
    paths = [f"s3a://wmata/{k}" for k in keys]
    df = spark.read.schema(SCHEMA).json(paths)

    df = (
        df.withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("snapshot_ts", F.to_timestamp("snapshot_ts"))
        .withColumn("trip_start", F.to_timestamp("trip_start"))
        .withColumn("trip_end", F.to_timestamp("trip_end"))
        .withColumn("service_date", F.to_date("service_date"))
        .withColumn("deviation_min", F.greatest(F.lit(-60.0), F.least(F.lit(60.0), F.col("deviation_min"))))
        .withColumn(
            "trip_key",
            F.concat_ws("|", F.col("trip_id"), F.col("trip_start").cast("string")),
        )
    )

    # 2020 snapshots carry last-reported positions up to ~30 minutes old.
    df = df.filter(
        F.abs(F.unix_timestamp("event_time") - F.unix_timestamp("snapshot_ts")) <= 1800
    )

    dur = F.unix_timestamp("trip_end") - F.unix_timestamp("trip_start")
    elapsed = F.unix_timestamp("event_time") - F.unix_timestamp("trip_start")
    df = df.withColumn(
        "progress_pct",
        F.when(dur > 0, F.greatest(F.lit(0.0), F.least(F.lit(1.0), elapsed / dur))),
    )

    df = df.dropDuplicates(["vehicle_id", "event_time"])

    # Speed between consecutive observations of the same vehicle.
    base = df.select("vehicle_id", "event_time", "lat", "lon")
    w = Window.partitionBy("vehicle_id").orderBy("event_time")
    sp = (
        base.withColumn("prev_lat", F.lag("lat").over(w))
        .withColumn("prev_lon", F.lag("lon").over(w))
        .withColumn("prev_ts", F.lag("event_time").over(w))
        .withColumn("dist_km", HAVERSINE("prev_lat", "prev_lon", "lat", "lon"))
        .withColumn(
            "secs",
            F.unix_timestamp("event_time") - F.unix_timestamp("prev_ts"),
        )
        .withColumn(
            "speed_kmh",
            F.when(
                (F.col("secs") > 0) & (F.col("dist_km") >= 0) & (F.col("dist_km") < 5),
                F.col("dist_km") / (F.col("secs") / 3600.0),
            ),
        )
        .filter((F.col("speed_kmh") >= 0) & (F.col("speed_kmh") <= 120))
        .select("vehicle_id", "event_time", "speed_kmh")
    )
    df = df.join(sp, ["vehicle_id", "event_time"], "left")
    df = df.filter(
        F.col("snapshot_ts").isNotNull() & F.col("trip_key").isNotNull()
    ).dropDuplicates(["trip_key", "snapshot_ts"])

    out = df.select(
        "trip_key",
        "vehicle_id",
        "trip_id",
        "route_id",
        "direction",
        "direction_num",
        "trip_headsign",
        "block_number",
        "event_time",
        "snapshot_ts",
        "service_date",
        "hour_local",
        "dow_local",
        "lat",
        "lon",
        "deviation_min",
        "trip_start",
        "trip_end",
        "progress_pct",
        "speed_kmh",
        "source_file",
    )
    write_jdbc(out, "wmata", "public.bus_trip_snapshot_staging")
    print("silver bus_trip_snapshot_staging written", flush=True)

def parse_hms(value: str) -> int | None:
    try:
        h, m, s = (int(x) for x in value.split(":"))
        return h * 3600 + m * 60 + s
    except Exception:
        return None


def route_stats(spark: SparkSession) -> None:
    client = s3()
    body = client.get_object(Bucket="wmata", Key=GTFS_ZIP)["Body"].read()
    zf = zipfile.ZipFile(io.BytesIO(body))

    routes = {}
    for row in csv.DictReader(io.StringIO(zf.read("routes.txt").decode())):
        routes[row["route_id"]] = row

    trips = {}
    route_trips: dict[str, list[str]] = {}
    for row in csv.DictReader(io.StringIO(zf.read("trips.txt").decode())):
        trips[row["trip_id"]] = row["route_id"]
        route_trips.setdefault(row["route_id"], []).append(row["trip_id"])

    first_dep: dict[str, int] = {}
    duration: dict[str, int] = {}
    stop_set: dict[str, set] = {}
    for row in csv.DictReader(io.StringIO(zf.read("stop_times.txt").decode())):
        trip = row["trip_id"]
        route = trips.get(trip)
        if not route:
            continue
        arr, dep = parse_hms(row["arrival_time"]), parse_hms(row["departure_time"])
        if arr is not None:
            if trip not in first_dep or arr < first_dep[trip]:
                first_dep[trip] = arr
            if trip not in duration:
                duration[trip] = arr
        if dep is not None:
            if trip not in first_dep or dep < first_dep[trip]:
                first_dep[trip] = dep
            duration[trip] = dep
        stop_set.setdefault(route, set()).add(row["stop_id"])

    rows = []
    for route_id, info in routes.items():
        trip_ids = route_trips.get(route_id, [])
        if not trip_ids:
            continue
        starts = sorted(first_dep[t] for t in trip_ids if t in first_dep)
        heads = [b - a for a, b in zip(starts, starts[1:]) if b > a]
        durs = [
            duration[t] - first_dep[t]
            for t in trip_ids
            if t in duration and t in first_dep and duration[t] >= first_dep[t]
        ]
        rows.append(
            {
                "route_id": route_id,
                "route_short_name": info.get("route_short_name"),
                "route_long_name": info.get("route_long_name"),
                "route_type": int(info["route_type"]) if info.get("route_type") else None,
                "trips_total": len(trip_ids),
                "median_headway_min": statistics.median(heads) / 60 if heads else None,
                "median_scheduled_duration_min": statistics.median(durs) / 60 if durs else None,
                "n_stops": len(stop_set.get(route_id, set())),
            }
        )

    df = spark.createDataFrame(rows)
    write_jdbc(df, "wmata", "public.route_stats_staging")
    print(f"silver route_stats_staging rows={len(rows):,}", flush=True)


def silver_ddot(spark: SparkSession, keys: list[str]) -> None:
    client = s3()
    datasets = {
        "ddot/aadt/": (
            "ddot_aadt_2020",
            [
                ("ROUTEID", T.StringType()),
                ("FROMMEASURE", T.DoubleType()),
                ("TOMEASURE", T.DoubleType()),
                ("FROMDATE", T.StringType()),
                ("TODATE", T.StringType()),
                ("EVENTID", T.StringType()),
                ("AADT", T.DoubleType()),
                ("AADT_YEAR", T.IntegerType()),
                ("GIS_ID", T.StringType()),
                ("OBJECTID", T.IntegerType()),
                ("GLOBALID", T.StringType()),
                ("CREATOR", T.StringType()),
                ("CREATED", T.StringType()),
                ("EDITOR", T.StringType()),
                ("EDITED", T.StringType()),
                ("SHAPELEN", T.DoubleType()),
                ("geometry", T.StringType()),
            ],
        ),
        "ddot/am_peak_speed/": (
            "ddot_am_speed_2019",
            [
                ("OBJECTID", T.IntegerType()),
                ("TMC_CODE", T.StringType()),
                ("TIME_PERIOD", T.StringType()),
                ("TMC_CODE_SPEED", T.DoubleType()),
                ("TMC_CODE_REF_SPEED", T.DoubleType()),
                ("TMC_CODE_AVG_SPEED", T.DoubleType()),
                ("TMC_CODE_TRAVEL_TIME_MINUTES", T.DoubleType()),
                ("GIS_ID", T.StringType()),
                ("GLOBALID", T.StringType()),
                ("CREATOR", T.StringType()),
                ("CREATED", T.StringType()),
                ("EDITOR", T.StringType()),
                ("EDITED", T.StringType()),
                ("SHAPELEN", T.DoubleType()),
                ("geometry", T.StringType()),
            ],
        ),
        "ddot/taz/": (
            "ddot_taz",
            [
                ("NAME", T.StringType()),
                ("TAZ", T.IntegerType()),
                ("POPBYTAZ", T.DoubleType()),
                ("HHBYTAZ", T.DoubleType()),
                ("EMPBYTAZ", T.DoubleType()),
                ("ACRES", T.DoubleType()),
                ("STATE_NAME", T.StringType()),
                ("STATE_FIPS", T.StringType()),
                ("CNTY_FIPS", T.StringType()),
                ("FIPSSTCO", T.StringType()),
                ("REGION", T.StringType()),
                ("SQMI", T.DoubleType()),
                ("JUR", T.IntegerType()),
                ("GIS_ID", T.StringType()),
                ("GLOBALID", T.StringType()),
                ("CREATED", T.StringType()),
                ("EDITED", T.StringType()),
                ("OBJECTID", T.IntegerType()),
                ("SHAPEAREA", T.DoubleType()),
                ("SHAPELEN", T.DoubleType()),
                ("geometry", T.StringType()),
            ],
        ),
    }

    grouped: dict[str, tuple[list[dict], list]] = {}
    for key in keys:
        match = next(((v[0], v[1]) for k, v in datasets.items() if key.startswith(k)), None)
        if not match:
            continue
        name, spec = match
        raw = client.get_object(Bucket="traffic", Key=key)["Body"].read()
        doc = json.loads(raw)
        for feature in doc.get("features", []):
            row = dict(feature.get("properties") or {})
            row["geometry"] = json.dumps(feature.get("geometry"))
            grouped.setdefault(name, ([], spec))[0].append(row)

    for name, (rows, spec) in grouped.items():
        if not rows:
            continue
        schema = T.StructType([T.StructField(n, t, True) for n, t in spec])
        def conv(value, dtype):
            if value is None:
                return None
            if isinstance(dtype, T.DoubleType):
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return None
            if isinstance(dtype, T.IntegerType):
                try:
                    return int(value)
                except (TypeError, ValueError):
                    return None
            return str(value)

        tuples = [tuple(conv(r.get(n), t) for n, t in spec) for r in rows]
        df = spark.createDataFrame(tuples, schema=schema)
        df = df.toDF(*[c.lower() for c in df.columns])
        pk = spec[0][0].lower()
        df = df.filter(F.col(pk).isNotNull())
        write_jdbc(df, "traffic", f"public.{name}_staging")
        print(f"silver {name}_staging rows={len(rows):,}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True)
    args = ap.parse_args()
    with open(args.manifest, encoding="utf-8") as fh:
        manifest = json.load(fh)

    spark = (
        SparkSession.builder.appName("wmata-bronze-to-silver")
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    wmata_keys = [x["s3_key"] for x in manifest.get("wmata", [])]
    traffic_keys = [x["s3_key"] for x in manifest.get("traffic", [])]

    if wmata_keys:
        silver_wmata(spark, wmata_keys)
        route_stats(spark)
    if traffic_keys:
        silver_ddot(spark, traffic_keys)

    spark.stop()


if __name__ == "__main__":
    main()
