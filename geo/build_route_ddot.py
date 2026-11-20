"""Build route-level DDOT congestion features.

Joins DDOT AADT 2020 and AM peak speed 2019 to GTFS 2020 route stops within a
150 m buffer, then aggregates to one row per route.
"""

from __future__ import annotations

import csv
import io
import json
import os
import zipfile
from collections import defaultdict

import boto3
import geopandas as gpd
import pandas as pd
import psycopg2
from botocore.config import Config
from psycopg2.extras import execute_values
from shapely.geometry import Point, shape

S3_ENDPOINT = os.environ.get("SEAWEEDFS_S3_ENDPOINT", "http://seaweedfs:8333")
ACCESS = os.environ.get("SEAWEEDFS_ACCESS_KEY", "seaweedfs-access-key")
SECRET = os.environ.get("SEAWEEDFS_SECRET_KEY", "seaweedfs-secret-key")
REGION = os.environ.get("AWS_REGION", "us-east-1")
GTFS_KEY = "reference/gtfs_static/version=20200106/bus-gtfs-static.zip"

SILVER_HOST = os.environ.get("POSTGRES_SILVER_HOST", "haproxy")
SILVER_PORT = os.environ.get("POSTGRES_SILVER_PORT", "5433")
GOLD_HOST = os.environ.get("POSTGRES_GOLD_HOST", "haproxy")
GOLD_PORT = os.environ.get("POSTGRES_GOLD_PORT", "5434")
USER = os.environ.get("POSTGRES_SILVER_USER", "postgres")
PASSWORD = os.environ.get("POSTGRES_SILVER_PASSWORD", "postgres")

BUFFER_M = float(os.environ.get("BUFFER_M", "250"))
CRS = "EPSG:32618"  # UTM 18N, metres


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS,
        aws_secret_access_key=SECRET,
        region_name=REGION,
        config=Config(s3={"addressing_style": "path"}),
    )


def gtfs_route_points() -> gpd.GeoDataFrame:
    raw = s3_client().get_object(Bucket="wmata", Key=GTFS_KEY)["Body"].read()
    zf = zipfile.ZipFile(io.BytesIO(raw))

    trips = {}
    for row in csv.DictReader(io.StringIO(zf.read("trips.txt").decode())):
        trips[row["trip_id"]] = row["route_id"]

    route_stops = defaultdict(set)
    for row in csv.DictReader(io.StringIO(zf.read("stop_times.txt").decode())):
        route = trips.get(row["trip_id"])
        if route:
            route_stops[route].add(row["stop_id"])

    stops = {}
    for row in csv.DictReader(io.StringIO(zf.read("stops.txt").decode())):
        try:
            stops[row["stop_id"]] = (float(row["stop_lon"]), float(row["stop_lat"]))
        except (TypeError, ValueError):
            continue

    rows = []
    for route, stop_ids in route_stops.items():
        for stop_id in stop_ids:
            lonlat = stops.get(stop_id)
            if lonlat:
                rows.append(
                    {"route_id": route, "stop_id": stop_id, "geometry": Point(*lonlat)}
                )
    return gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")


def read_ddot(table: str, columns: str) -> gpd.GeoDataFrame:
    conn = psycopg2.connect(
        host=SILVER_HOST,
        port=SILVER_PORT,
        dbname="traffic",
        user=USER,
        password=PASSWORD,
        connect_timeout=10,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {columns}, geometry FROM public.{table}")
            rows = cur.fetchall()
    finally:
        conn.close()

    names = [c.strip() for c in columns.split(",")] + ["geometry"]
    records = []
    for row in rows:
        rec = dict(zip(names, row))
        if not rec["geometry"]:
            continue
        try:
            geom = json.loads(rec["geometry"])
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(geom, dict):
            continue
        rec["geometry"] = shape(geom)
        records.append(rec)
    return gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")


def main() -> None:
    points = gtfs_route_points().to_crs(CRS)
    print(f"route-stop points: {len(points):,} routes={points['route_id'].nunique()}", flush=True)

    aadt = read_ddot("ddot_aadt_2020", "objectid, aadt").to_crs(CRS)
    aadt = aadt[aadt["aadt"].notna()].copy()
    print(f"aadt segments: {len(aadt):,}", flush=True)

    speed = read_ddot(
        "ddot_am_speed_2019",
        "objectid, tmc_code_avg_speed, tmc_code_ref_speed, tmc_code_travel_time_minutes",
    ).to_crs(CRS)
    speed = speed[speed["tmc_code_avg_speed"].notna()].copy()
    speed["speed_ratio"] = (
        speed["tmc_code_avg_speed"] / speed["tmc_code_ref_speed"]
    )
    print(f"speed segments: {len(speed):,}", flush=True)

    aadt_hit = gpd.sjoin_nearest(
        points[["route_id", "geometry"]],
        aadt[["objectid", "aadt", "geometry"]],
        how="inner",
        max_distance=BUFFER_M,
        distance_col="dist",
    ).drop_duplicates(["route_id", "objectid"])
    aadt_agg = aadt_hit.groupby("route_id").agg(
        aadt_route_median=("aadt", "median"),
        aadt_route_max=("aadt", "max"),
        n_aadt_segments=("objectid", "nunique"),
    )

    speed_hit = gpd.sjoin_nearest(
        points[["route_id", "geometry"]],
        speed[
            [
                "objectid",
                "tmc_code_avg_speed",
                "tmc_code_ref_speed",
                "tmc_code_travel_time_minutes",
                "speed_ratio",
                "geometry",
            ]
        ],
        how="inner",
        max_distance=BUFFER_M,
        distance_col="dist",
    ).drop_duplicates(["route_id", "objectid"])
    speed_agg = speed_hit.groupby("route_id").agg(
        am_speed_route_median=("tmc_code_avg_speed", "median"),
        am_speed_ratio=("speed_ratio", "median"),
        am_travel_time_per_km=("tmc_code_travel_time_minutes", "median"),
        n_speed_segments=("objectid", "nunique"),
    )

    out = aadt_agg.join(speed_agg, how="outer").reset_index()
    print(f"routes with DDOT features: {len(out):,}", flush=True)

    conn = psycopg2.connect(
        host=GOLD_HOST,
        port=GOLD_PORT,
        dbname="wmata",
        user=USER,
        password=PASSWORD,
        connect_timeout=10,
    )
    try:
        rows = [
            (
                r.route_id,
                None if pd.isna(r.aadt_route_median) else float(r.aadt_route_median),
                None if pd.isna(r.aadt_route_max) else float(r.aadt_route_max),
                None if pd.isna(r.am_speed_route_median) else float(r.am_speed_route_median),
                None if pd.isna(r.am_speed_ratio) else float(r.am_speed_ratio),
                None if pd.isna(r.am_travel_time_per_km) else float(r.am_travel_time_per_km),
                int(r.n_aadt_segments) if not pd.isna(r.n_aadt_segments) else 0,
                int(r.n_speed_segments) if not pd.isna(r.n_speed_segments) else 0,
            )
            for r in out.itertuples()
        ]
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO public.route_ddot_features
                  (route_id, aadt_route_median, aadt_route_max,
                   am_speed_route_median, am_speed_ratio, am_travel_time_per_km,
                   n_aadt_segments, n_speed_segments)
                VALUES %s
                ON CONFLICT (route_id) DO UPDATE SET
                  aadt_route_median = EXCLUDED.aadt_route_median,
                  aadt_route_max = EXCLUDED.aadt_route_max,
                  am_speed_route_median = EXCLUDED.am_speed_route_median,
                  am_speed_ratio = EXCLUDED.am_speed_ratio,
                  am_travel_time_per_km = EXCLUDED.am_travel_time_per_km,
                  n_aadt_segments = EXCLUDED.n_aadt_segments,
                  n_speed_segments = EXCLUDED.n_speed_segments
                """,
                rows,
                page_size=500,
            )
        conn.commit()
    finally:
        conn.close()
    print(f"gold.route_ddot_features written rows={len(rows):,}", flush=True)


if __name__ == "__main__":
    main()
