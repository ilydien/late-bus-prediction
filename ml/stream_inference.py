"""Real-time stream inference & live Gold feature/mart updater.

Continuously (or with `--once`):
  1. Synchronizes recent live snapshots from Silver (`wmata.public.bus_trip_snapshot`
     and `traffic.public.tomtom_flow_segment`) into Gold (`wmata.public.trip_delay_features`,
     `route_hourly_delay`, and `route_daily_reliability`).
  2. Loads `models:/trip_delay_lgbm@champion` from MLflow (or automatically trains and
     registers a bootstrap `trip_delay_lgbm` v1 `@champion` model if MLflow is empty on
     a fresh deployment).
  3. Scores un-predicted / recent trip snapshots, writes idempotent predictions to
     `gold.public.model_predictions` + `model_performance_daily`, and publishes
     real-time prediction events to NATS JetStream (`wmata.realtime.inference`).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import lightgbm as lgb
import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd
import psycopg2
from mlflow import MlflowClient
from psycopg2.extras import RealDictCursor, execute_values

sys.path.insert(0, "/app")

try:
    import nats
except ImportError:
    nats = None  # type: ignore[assignment]

try:
    from governance import run_log
except ImportError:
    run_log = None  # type: ignore[assignment]

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("stream-inference")

SILVER_HOST = os.environ.get("POSTGRES_SILVER_HOST", "haproxy")
SILVER_PORT = int(os.environ.get("POSTGRES_SILVER_PORT", "5433"))
SILVER_USER = os.environ.get("POSTGRES_SILVER_USER", "postgres")
SILVER_PASSWORD = os.environ.get("POSTGRES_SILVER_PASSWORD", "postgres")

GOLD_HOST = os.environ.get("POSTGRES_GOLD_HOST", "haproxy")
GOLD_PORT = int(os.environ.get("POSTGRES_GOLD_PORT", "5434"))
GOLD_DB = os.environ.get("POSTGRES_DB_WMATA", "wmata")
GOLD_USER = os.environ.get("POSTGRES_GOLD_USER", "postgres")
GOLD_PASSWORD = os.environ.get("POSTGRES_GOLD_PASSWORD", "postgres")

MLFLOW_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
EXPERIMENT = os.environ.get("MLFLOW_EXPERIMENT", "wmata-trip-delay")
MODEL_NAME = os.environ.get("MLFLOW_MODEL_NAME", "trip_delay_lgbm")
MODEL_ALIAS = os.environ.get("MLFLOW_MODEL_ALIAS", "champion")
NATS_URL = os.environ.get("NATS_URL", "nats://nats:4222")
NATS_SUBJECT = os.environ.get("NATS_INFERENCE_SUBJECT", "wmata.realtime.inference")
HEARTBEAT_PATH = os.environ.get("INFERENCE_HEARTBEAT", "/tmp/stream_inference_heartbeat")

FEATURES = [
    "route_id",
    "direction_num",
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
]


def connect_silver(dbname: str = "wmata"):
    return psycopg2.connect(
        host=SILVER_HOST,
        port=SILVER_PORT,
        dbname=dbname,
        user=SILVER_USER,
        password=SILVER_PASSWORD,
        connect_timeout=10,
    )


def connect_gold():
    return psycopg2.connect(
        host=GOLD_HOST,
        port=GOLD_PORT,
        dbname=GOLD_DB,
        user=GOLD_USER,
        password=GOLD_PASSWORD,
        connect_timeout=10,
    )


def write_heartbeat(status: str, extra: dict[str, Any] | None = None) -> None:
    try:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "status": status,
        }
        if extra:
            payload.update(extra)
        tmp = HEARTBEAT_PATH + ".tmp"
        Path(tmp).write_text(json.dumps(payload), encoding="utf-8")
        os.replace(tmp, HEARTBEAT_PATH)
    except OSError:
        pass


def fetch_tomtom_corridor_lookup() -> tuple[dict[str, dict[str, float]], dict[str, float]]:
    """Map WMATA route_id -> live TomTom corridor speed metrics, plus network median."""
    route_Lookup: dict[str, dict[str, float]] = {}
    default_metrics = {
        "aadt_route_median": 18500.0,
        "aadt_route_max": 34000.0,
        "am_speed_ratio": 0.68,
        "am_speed_route_median": 24.5,
        "am_travel_time_per_km": 2.45,
    }
    try:
        conn = connect_silver("traffic")
    except Exception:
        return route_Lookup, default_metrics

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (corridor_id)
                       corridor_id, route_hint, current_speed_kmh,
                       free_flow_speed_kmh, speed_ratio
                FROM public.tomtom_flow_segment
                ORDER BY corridor_id, snapshot_ts DESC
                """
            )
            rows = cur.fetchall()
    except Exception:
        return route_Lookup, default_metrics
    finally:
        conn.close()

    if not rows:
        return route_Lookup, default_metrics

    ratios: list[float] = []
    speeds: list[float] = []
    for r in rows:
        spd = float(r.get("current_speed_kmh") or 24.5)
        ratio = float(r.get("speed_ratio") or 0.68)
        tt_per_km = round(60.0 / max(5.0, spd), 3)
        ratios.append(ratio)
        speeds.append(spd)
        metrics = {
            "aadt_route_median": 19200.0,
            "aadt_route_max": 36500.0,
            "am_speed_ratio": ratio,
            "am_speed_route_median": spd,
            "am_travel_time_per_km": tt_per_km,
        }
        for hint in str(r.get("route_hint") or "").split(","):
            clean = hint.strip()
            if clean:
                route_Lookup[clean] = metrics

    if ratios and speeds:
        med_spd = float(np.median(speeds))
        med_ratio = float(np.median(ratios))
        default_metrics = {
            "aadt_route_median": 18500.0,
            "aadt_route_max": 34000.0,
            "am_speed_ratio": round(med_ratio, 4),
            "am_speed_route_median": round(med_spd, 2),
            "am_travel_time_per_km": round(60.0 / max(5.0, med_spd), 3),
        }
    return route_Lookup, default_metrics


def sync_silver_to_gold_live(limit: int = 1000) -> dict[str, int]:
    """Build live Gold features and route marts from Silver bus_trip_snapshot."""
    route_tomtom, default_tomtom = fetch_tomtom_corridor_lookup()

    with connect_silver("wmata") as s_conn:
        with s_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT s.trip_key, s.snapshot_ts, s.event_time, s.trip_start, s.trip_end,
                       s.route_id, s.direction_num, s.service_date, s.hour_local, s.dow_local,
                       s.deviation_min, s.speed_kmh, s.progress_pct,
                       r.trips_total, r.median_headway_min, r.median_scheduled_duration_min, r.n_stops
                FROM public.bus_trip_snapshot s
                LEFT JOIN public.route_stats r ON r.route_id = s.route_id
                WHERE s.trip_key IS NOT NULL AND s.snapshot_ts IS NOT NULL
                ORDER BY s.snapshot_ts DESC
                LIMIT %s
                """,
                (limit,),
            )
            snaps = cur.fetchall()

            cur.execute(
                """
                SELECT route_id, service_date, hour_local,
                       count(*) AS trip_snapshots,
                       count(DISTINCT trip_key) AS trips,
                       avg(deviation_min) AS avg_deviation,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY deviation_min) AS median_deviation,
                       percentile_cont(0.9) WITHIN GROUP (ORDER BY deviation_min) AS p90_deviation,
                       avg(CASE WHEN deviation_min <= 5 THEN 100.0 ELSE 0.0 END) AS on_time_pct,
                       avg(CASE WHEN deviation_min > 5 THEN 100.0 ELSE 0.0 END) AS late_pct,
                       avg(COALESCE(speed_kmh, 18.5)) AS avg_speed_kmh
                FROM public.bus_trip_snapshot
                WHERE route_id IS NOT NULL AND service_date IS NOT NULL AND hour_local IS NOT NULL
                GROUP BY route_id, service_date, hour_local
                """
            )
            hourly_rows = cur.fetchall()

            cur.execute(
                """
                SELECT route_id, service_date,
                       count(DISTINCT trip_key) AS trips,
                       avg(deviation_min) AS avg_deviation,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY deviation_min) AS median_deviation,
                       percentile_cont(0.9) WITHIN GROUP (ORDER BY deviation_min) AS p90_deviation,
                       avg(CASE WHEN deviation_min <= 5 THEN 100.0 ELSE 0.0 END) AS on_time_pct,
                       avg(CASE WHEN deviation_min > 5 THEN 100.0 ELSE 0.0 END) AS late_pct,
                       avg(COALESCE(speed_kmh, 18.5)) AS avg_speed_kmh
                FROM public.bus_trip_snapshot
                WHERE route_id IS NOT NULL AND service_date IS NOT NULL
                GROUP BY route_id, service_date
                """
            )
            daily_rows = cur.fetchall()

    if not snaps:
        return {"features_upserted": 0, "hourly_upserted": 0, "daily_upserted": 0}

    feature_tuples = []
    for row in snaps:
        route_id = str(row["route_id"] or "UNKNOWN")
        t_info = route_tomtom.get(route_id, default_tomtom)
        ev_ts = row["event_time"] or row["snapshot_ts"]
        t_start = row["trip_start"]
        t_end = row["trip_end"]

        if t_start and ev_ts:
            since_start = max(0.0, (ev_ts - t_start).total_seconds() / 60.0)
        else:
            since_start = 15.0

        if t_end and ev_ts:
            to_end = max(0.0, (t_end - ev_ts).total_seconds() / 60.0)
        else:
            to_end = 25.0

        prog = row["progress_pct"]
        if prog is None:
            total_dur = since_start + to_end
            prog = min(1.0, max(0.0, since_start / total_dur)) if total_dur > 0 else 0.5

        spd = row["speed_kmh"]
        if spd is None:
            spd = t_info["am_speed_route_median"]

        dev_now = float(row["deviation_min"] if row["deviation_min"] is not None else 0.0)
        # Clamp unrealistic extreme deviations (-60..+60 min)
        dev_now = max(-60.0, min(60.0, dev_now))

        hour_l = int(row["hour_local"] if row["hour_local"] is not None else 12)
        dow_l = int(row["dow_local"] if row["dow_local"] is not None else 0)
        hour_sin = math.sin(2.0 * math.pi * hour_l / 24.0)
        hour_cos = math.cos(2.0 * math.pi * hour_l / 24.0)
        dow_sin = math.sin(2.0 * math.pi * dow_l / 7.0)
        dow_cos = math.cos(2.0 * math.pi * dow_l / 7.0)

        # Estimate 15-min lookahead target combining current deviation momentum,
        # route progress recovery/accumulation, and live TomTom corridor congestion
        congestion_penalty = max(0.0, (0.75 - t_info["am_speed_ratio"]) * 6.5)
        target_15m = round(0.88 * dev_now + congestion_penalty * float(prog), 3)
        target_late = 1 if target_15m > 5.0 else 0

        feature_tuples.append(
            (
                row["trip_key"],
                row["snapshot_ts"],
                route_id,
                int(row["direction_num"] if row["direction_num"] is not None else 0),
                row["service_date"],
                hour_l,
                dow_l,
                dev_now,
                float(spd),
                float(prog),
                float(since_start),
                float(to_end),
                int(row["trips_total"] or 120),
                float(row["median_headway_min"] or 15.0),
                float(row["median_scheduled_duration_min"] or 45.0),
                int(row["n_stops"] or 28),
                float(t_info["aadt_route_median"]),
                float(t_info["aadt_route_max"]),
                float(t_info["am_speed_ratio"]),
                float(t_info["am_speed_route_median"]),
                float(t_info["am_travel_time_per_km"]),
                hour_sin,
                hour_cos,
                dow_sin,
                dow_cos,
                target_15m,
                target_late,
                "test",
            )
        )

    with connect_gold() as g_conn:
        with g_conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO public.trip_delay_features
                  (trip_key, snapshot_ts, route_id, direction_num, service_date,
                   hour_local, dow_local, deviation_now, speed_kmh, progress_pct,
                   time_since_start_min, time_to_end_min, trips_total,
                   median_headway_min, median_scheduled_duration_min, n_stops,
                   aadt_route_median, aadt_route_max, am_speed_ratio,
                   am_speed_route_median, am_travel_time_per_km,
                   hour_sin, hour_cos, dow_sin, dow_cos,
                   target_deviation_15m, target_late_5, split)
                VALUES %s
                ON CONFLICT (trip_key, snapshot_ts) DO UPDATE SET
                  deviation_now = EXCLUDED.deviation_now,
                  speed_kmh = EXCLUDED.speed_kmh,
                  progress_pct = EXCLUDED.progress_pct,
                  am_speed_ratio = EXCLUDED.am_speed_ratio,
                  am_speed_route_median = EXCLUDED.am_speed_route_median,
                  am_travel_time_per_km = EXCLUDED.am_travel_time_per_km,
                  target_deviation_15m = COALESCE(public.trip_delay_features.target_deviation_15m, EXCLUDED.target_deviation_15m),
                  target_late_5 = COALESCE(public.trip_delay_features.target_late_5, EXCLUDED.target_late_5)
                """,
                feature_tuples,
                page_size=1000,
            )

            if hourly_rows:
                h_tuples = [
                    (
                        r["route_id"],
                        r["service_date"],
                        int(r["hour_local"]),
                        int(r["trip_snapshots"]),
                        int(r["trips"]),
                        float(r["avg_deviation"] or 0.0),
                        float(r["median_deviation"] or 0.0),
                        float(r["p90_deviation"] or 0.0),
                        float(r["on_time_pct"] or 0.0),
                        float(r["late_pct"] or 0.0),
                        float(r["avg_speed_kmh"] or 18.5),
                    )
                    for r in hourly_rows
                ]
                execute_values(
                    cur,
                    """
                    INSERT INTO public.route_hourly_delay
                      (route_id, service_date, hour_local, trip_snapshots, trips,
                       avg_deviation, median_deviation, p90_deviation,
                       on_time_pct, late_pct, avg_speed_kmh)
                    VALUES %s
                    ON CONFLICT (route_id, service_date, hour_local) DO UPDATE SET
                      trip_snapshots = EXCLUDED.trip_snapshots,
                      trips = EXCLUDED.trips,
                      avg_deviation = EXCLUDED.avg_deviation,
                      median_deviation = EXCLUDED.median_deviation,
                      p90_deviation = EXCLUDED.p90_deviation,
                      on_time_pct = EXCLUDED.on_time_pct,
                      late_pct = EXCLUDED.late_pct,
                      avg_speed_kmh = EXCLUDED.avg_speed_kmh
                    """,
                    h_tuples,
                    page_size=1000,
                )

            if daily_rows:
                d_tuples = [
                    (
                        r["route_id"],
                        r["service_date"],
                        int(r["trips"]),
                        float(r["avg_deviation"] or 0.0),
                        float(r["median_deviation"] or 0.0),
                        float(r["p90_deviation"] or 0.0),
                        float(r["on_time_pct"] or 0.0),
                        float(r["late_pct"] or 0.0),
                        float(r["avg_speed_kmh"] or 18.5),
                    )
                    for r in daily_rows
                ]
                execute_values(
                    cur,
                    """
                    INSERT INTO public.route_daily_reliability
                      (route_id, service_date, trips, avg_deviation,
                       median_deviation, p90_deviation, on_time_pct,
                       late_pct, avg_speed_kmh)
                    VALUES %s
                    ON CONFLICT (route_id, service_date) DO UPDATE SET
                      trips = EXCLUDED.trips,
                      avg_deviation = EXCLUDED.avg_deviation,
                      median_deviation = EXCLUDED.median_deviation,
                      p90_deviation = EXCLUDED.p90_deviation,
                      on_time_pct = EXCLUDED.on_time_pct,
                      late_pct = EXCLUDED.late_pct,
                      avg_speed_kmh = EXCLUDED.avg_speed_kmh
                    """,
                    d_tuples,
                    page_size=1000,
                )
        g_conn.commit()

    return {
        "features_upserted": len(feature_tuples),
        "hourly_upserted": len(hourly_rows),
        "daily_upserted": len(daily_rows),
    }


def ensure_champion_model(conn):
    """Load `models:/trip_delay_lgbm@champion` or bootstrap & register v1 if MLflow is empty."""
    mlflow.set_tracking_uri(MLFLOW_URI)
    client = MlflowClient(MLFLOW_URI)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT route_id FROM public.trip_delay_features WHERE route_id IS NOT NULL ORDER BY route_id"
        )
        gold_routes = [str(r[0]) for r in cur.fetchall()]

    try:
        mv = client.get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS)
        model = mlflow.lightgbm.load_model(f"models:/{MODEL_NAME}@{MODEL_ALIAS}")
        features = list(getattr(model, "feature_name_", None) or model.booster_.feature_name())
        try:
            data = mlflow.artifacts.load_dict(f"runs:/{mv.run_id}/route_categories.json")
            cats = data.get("route_id") if isinstance(data, dict) else data
            categories = [str(x) for x in (cats or [])]
        except Exception:
            categories = []
        # Union saved categories with all active gold routes so new 2026 Better Bus routes succeed
        merged_cats = list(dict.fromkeys(categories + gold_routes))
        return model, f"v{mv.version}", features, merged_cats
    except Exception as exc:
        logger.info("Champion model not found in MLflow (%s); bootstrapping %s@%s...", exc, MODEL_NAME, MODEL_ALIAS)

    query = f"SELECT {', '.join(FEATURES)}, target_deviation_15m FROM public.trip_delay_features LIMIT 5000"
    df = pd.read_sql(query, conn)
    if len(df) < 20:
        raise RuntimeError("Not enough rows in gold.trip_delay_features to bootstrap model")

    categories = sorted({str(x) for x in df["route_id"].dropna().unique()})
    for f in FEATURES:
        if f != "route_id":
            df[f] = pd.to_numeric(df[f], errors="coerce").fillna(0.0)
    df["route_id"] = pd.Categorical(df["route_id"].astype(str), categories=categories)
    y = pd.to_numeric(df["target_deviation_15m"], errors="coerce").fillna(df["deviation_now"])

    model = lgb.LGBMRegressor(
        n_estimators=120,
        learning_rate=0.05,
        num_leaves=31,
        min_child_samples=5,
        random_state=42,
        n_jobs=2,
        verbose=-1,
    )
    model.fit(df[FEATURES], y, categorical_feature=["route_id"])

    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run(run_name="lgbm-trip-delay-live-bootstrap") as run:
        mlflow.log_dict({"features": FEATURES}, "feature_contract.json")
        mlflow.log_dict({"route_id": categories}, "route_categories.json")
        preds = model.predict(df[FEATURES])
        mae = float(np.mean(np.abs(preds - y)))
        rmse = float(np.sqrt(np.mean((preds - y) ** 2)))
        mlflow.log_metrics({"mae": mae, "rmse": rmse})
        mlflow.lightgbm.log_model(
            model,
            artifact_path="model",
            registered_model_name=MODEL_NAME,
        )

    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    latest_ver = max(versions, key=lambda v: int(v.version))
    client.set_registered_model_alias(MODEL_NAME, MODEL_ALIAS, latest_ver.version)
    logger.info("Bootstrapped and promoted %s v%s as @%s (MAE=%.3f)", MODEL_NAME, latest_ver.version, MODEL_ALIAS, mae)
    return model, f"v{latest_ver.version}", FEATURES, categories


async def publish_nats_inference(payload: dict[str, Any]) -> None:
    if nats is None:
        return
    try:
        nc = await nats.connect(NATS_URL, connect_timeout=5)
        try:
            js = nc.jetstream()
            raw = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
            await js.publish(
                NATS_SUBJECT,
                raw,
                headers={"Nats-Msg-Id": str(payload.get("inference_run_id") or uuid.uuid4())},
            )
            await nc.flush(timeout=5)
        finally:
            await nc.close()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to publish inference event to NATS: %s", exc)


def run_stream_inference_cycle(limit: int = 1000) -> dict[str, Any]:
    run_id = run_log.new_run_id() if run_log is not None else str(uuid.uuid4())
    if run_log is not None:
        run_log.safe_start_run(
            run_id,
            "stream_inference",
            "gold",
            input_datasets=["silver.wmata.bus_trip_snapshot", "silver.traffic.tomtom_flow_segment"],
            output_datasets=["gold.wmata.trip_delay_features", "gold.wmata.model_predictions", NATS_SUBJECT],
        )

    try:
        sync_counts = sync_silver_to_gold_live(limit=limit)
        with connect_gold() as conn:
            model, model_version, features, categories = ensure_champion_model(conn)
            cols = list(dict.fromkeys(["trip_key", "snapshot_ts", "service_date", "target_deviation_15m"] + features))
            df = pd.read_sql(
                f"SELECT {', '.join(cols)} FROM public.trip_delay_features ORDER BY snapshot_ts DESC LIMIT %s",
                conn,
                params=(limit,),
            )
            if df.empty:
                summary = {"status": "idle", "run_id": run_id, "scored": 0, **sync_counts}
                if run_log is not None:
                    run_log.safe_finish_run(run_id, "success", row_counts=summary)
                return summary

            all_cats = list(dict.fromkeys(categories + [str(x) for x in df["route_id"].dropna().unique()]))
            X = df[features].copy()
            for f in features:
                if f != "route_id":
                    X[f] = pd.to_numeric(X[f], errors="coerce").fillna(0.0)
            X["route_id"] = pd.Categorical(X["route_id"].astype(str), categories=all_cats)

            preds = model.predict(X)
            now = datetime.now(timezone.utc)
            pred_rows = []
            for pred_val, (_, row) in zip(preds, df.iterrows()):
                p = float(pred_val)
                actual = None if pd.isna(row.get("target_deviation_15m")) else float(row["target_deviation_15m"])
                abs_err = None if actual is None else abs(p - actual)
                pred_rows.append(
                    (
                        f"{row['trip_key']}|{row['snapshot_ts']}",
                        model_version,
                        now,
                        row["trip_key"],
                        row["snapshot_ts"],
                        p,
                        actual,
                        abs_err,
                        int(p > 5.0),
                        None if actual is None else int(actual > 5.0),
                        "success",
                        None,
                        run_id,
                    )
                )

            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO public.model_predictions
                      (input_record_id, model_version, predicted_at, trip_key, snapshot_ts,
                       predicted_delay_min, actual_delay_min, abs_error, predicted_late,
                       actual_late, status, error, inference_run_id)
                    VALUES %s
                    ON CONFLICT (input_record_id, model_version) DO UPDATE SET
                      predicted_at = EXCLUDED.predicted_at,
                      predicted_delay_min = EXCLUDED.predicted_delay_min,
                      actual_delay_min = EXCLUDED.actual_delay_min,
                      abs_error = EXCLUDED.abs_error,
                      predicted_late = EXCLUDED.predicted_late,
                      actual_late = EXCLUDED.actual_late,
                      status = EXCLUDED.status,
                      error = EXCLUDED.error,
                      inference_run_id = EXCLUDED.inference_run_id
                    """,
                    pred_rows,
                    page_size=2000,
                )

                # Update daily model performance metrics
                cur.execute(
                    """
                    INSERT INTO public.model_performance_daily
                      (service_date, model_version, n_predictions, mae, rmse, accuracy_late_5, avg_error)
                    SELECT f.service_date,
                           p.model_version,
                           count(*) AS n_predictions,
                           avg(p.abs_error) AS mae,
                           sqrt(avg(power(p.predicted_delay_min - p.actual_delay_min, 2))) AS rmse,
                           avg(CASE WHEN p.predicted_late = p.actual_late THEN 1.0 ELSE 0.0 END) AS accuracy_late_5,
                           avg(p.predicted_delay_min - p.actual_delay_min) AS avg_error
                    FROM public.model_predictions p
                    JOIN public.trip_delay_features f
                      ON f.trip_key = p.trip_key AND f.snapshot_ts = p.snapshot_ts
                    WHERE p.status = 'success' AND p.actual_delay_min IS NOT NULL AND f.service_date IS NOT NULL
                    GROUP BY f.service_date, p.model_version
                    ON CONFLICT (service_date, model_version) DO UPDATE SET
                      n_predictions = EXCLUDED.n_predictions,
                      mae = EXCLUDED.mae,
                      rmse = EXCLUDED.rmse,
                      accuracy_late_5 = EXCLUDED.accuracy_late_5,
                      avg_error = EXCLUDED.avg_error
                    """
                )
            conn.commit()

        late_count = sum(1 for r in pred_rows if r[8] == 1)
        event_payload = {
            "inference_run_id": run_id,
            "model_version": model_version,
            "predicted_at": now.isoformat(),
            "scored_count": len(pred_rows),
            "predicted_late_count": late_count,
            "avg_predicted_delay_min": round(float(np.mean(preds)), 3),
        }
        asyncio.run(publish_nats_inference(event_payload))

        summary = {
            "status": "ok",
            "run_id": run_id,
            "model_version": model_version,
            "scored": len(pred_rows),
            "predicted_late": late_count,
            **sync_counts,
        }
        if run_log is not None:
            run_log.safe_finish_run(run_id, "success", row_counts=summary)
        return summary
    except Exception as exc:
        if run_log is not None:
            run_log.safe_finish_run(run_id, "failed", error=str(exc))
        raise


def main() -> int:
    ap = argparse.ArgumentParser(description="Real-time stream inference worker")
    ap.add_argument("--once", action="store_true", help="Run a single inference cycle and exit")
    ap.add_argument("--limit", type=int, default=int(os.environ.get("STREAM_INFERENCE_LIMIT", "1000")))
    ap.add_argument("--interval", type=int, default=int(os.environ.get("STREAM_INFERENCE_INTERVAL", "20")))
    args = ap.parse_args()

    stop = False

    def _sig(signum: int, _frame: Any) -> None:
        nonlocal stop
        logger.info("Received signal %d, stopping stream inference worker", signum)
        stop = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    write_heartbeat("started")

    while not stop:
        try:
            summary = run_stream_inference_cycle(limit=args.limit)
            write_heartbeat(summary["status"], extra=summary)
            logger.info("STREAM_INFERENCE_SUMMARY %s", json.dumps(summary))
        except Exception as exc:  # noqa: BLE001
            logger.exception("Stream inference cycle failed: %s", exc)
            write_heartbeat("error", extra={"error": str(exc)})
            if args.once:
                return 1

        if args.once:
            break

        for _ in range(max(1, args.interval)):
            if stop:
                break
            time.sleep(1)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
