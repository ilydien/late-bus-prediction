"""Merge incremental gold features from staging and advance the watermark."""

from __future__ import annotations

import os

import psycopg2

HOST = os.environ.get("POSTGRES_GOLD_HOST", "haproxy")
PORT = int(os.environ.get("POSTGRES_GOLD_PORT", "5434"))
DB = os.environ.get("POSTGRES_DB_WMATA", "wmata")
USER = os.environ.get("POSTGRES_GOLD_USER", "postgres")
PASSWORD = os.environ.get("POSTGRES_GOLD_PASSWORD", "postgres")
SILVER_HOST = os.environ.get("POSTGRES_SILVER_HOST", "haproxy")
SILVER_PORT = int(os.environ.get("POSTGRES_SILVER_PORT", "5433"))
SILVER_USER = os.environ.get("POSTGRES_SILVER_USER", "postgres")
SILVER_PASSWORD = os.environ.get("POSTGRES_SILVER_PASSWORD", "postgres")

COLS = [
    "trip_key",
    "snapshot_ts",
    "route_id",
    "direction_num",
    "service_date",
    "hour_local",
    "dow_local",
    "deviation_now",
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
]


def main() -> None:
    conn = psycopg2.connect(
        host=HOST, port=PORT, dbname=DB, user=USER, password=PASSWORD, connect_timeout=10
    )
    silver = psycopg2.connect(
        host=SILVER_HOST,
        port=SILVER_PORT,
        dbname=DB,
        user=SILVER_USER,
        password=SILVER_PASSWORD,
        connect_timeout=10,
    )
    try:
        with silver.cursor() as scur:
            scur.execute("SELECT max(snapshot_ts) FROM public.bus_trip_snapshot")
            silver_max = scur.fetchone()[0]
        cols = ", ".join(COLS)
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in COLS if c not in ("trip_key", "snapshot_ts"))
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM public.trip_delay_features_staging")
            staged = cur.fetchone()[0]
            if staged:
                cur.execute(
                    f"""
                    INSERT INTO public.trip_delay_features ({cols})
                    SELECT {cols} FROM public.trip_delay_features_staging
                    ON CONFLICT (trip_key, snapshot_ts) DO UPDATE SET {updates}
                    """
                )
                merged = cur.rowcount
                cur.execute("TRUNCATE public.trip_delay_features_staging")
            else:
                merged = 0

            cur.execute(
                """
                INSERT INTO public.training_watermark
                  (pipeline, last_snapshot_ts, last_trained_at, model_version, updated_at)
                SELECT 'trip_delay_features',
                       GREATEST(
                         (SELECT max(snapshot_ts) FROM public.trip_delay_features),
                         %s::timestamptz
                       ),
                       NULL, NULL, now()
                ON CONFLICT (pipeline) DO UPDATE SET
                  last_snapshot_ts = EXCLUDED.last_snapshot_ts,
                  updated_at = now()
                """,
                (silver_max,),
            )
            conn.commit()
            cur.execute(
                "SELECT last_snapshot_ts FROM public.training_watermark WHERE pipeline='trip_delay_features'"
            )
            wm = cur.fetchone()[0]
        print(f"staged={staged} merged={merged} watermark={wm}", flush=True)
    finally:
        silver.close()
        conn.close()


if __name__ == "__main__":
    main()
