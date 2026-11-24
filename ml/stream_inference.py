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
import bisect
import contextlib
import json
import logging
import math
import os
import signal
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    import lightgbm as lgb
    import mlflow
    import mlflow.lightgbm
    import numpy as np
    import pandas as pd
    import psycopg2
    from mlflow import MlflowClient
    from psycopg2.extras import RealDictCursor, execute_values
except ImportError:  # pragma: no cover - allows pure reconciliation tests without heavy ML deps
    lgb = None  # type: ignore[assignment]
    mlflow = None  # type: ignore[assignment]
    np = None  # type: ignore[assignment]
    pd = None  # type: ignore[assignment]
    psycopg2 = None  # type: ignore[assignment]
    MlflowClient = None  # type: ignore[assignment]
    RealDictCursor = None  # type: ignore[assignment]
    execute_values = None  # type: ignore[assignment]

sys.path.insert(0, "/app")

try:
    import nats
except ImportError:
    nats = None  # type: ignore[assignment]

try:
    from governance import run_log
except ImportError:
    run_log = None  # type: ignore[assignment]

try:
    from shared.logging import run_context, setup_json_logging
    from shared import metrics as vm_metrics
except ImportError:
    run_context = None  # type: ignore[assignment]
    setup_json_logging = None  # type: ignore[assignment]
    vm_metrics = None  # type: ignore[assignment]

SILVER_MERGED_SUBJECT = os.environ.get("SILVER_MERGED_SUBJECT", "realtime.silver.merged")

_LOG_LEVEL = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
if setup_json_logging is not None:
    setup_json_logging("stream-inference", level=_LOG_LEVEL)
else:
    logging.basicConfig(
        level=_LOG_LEVEL,
        format="%(asctime)s %(levelname)s %(message)s",
    )
logger = logging.getLogger("stream-inference")


def _run_ctx(run_id: str):
    return run_context(run_id=run_id) if run_context is not None else contextlib.nullcontext(run_id)

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
    with _run_ctx(run_id):
        if run_log is not None:
            run_log.safe_start_run(
                run_id,
                "stream_inference",
                "gold",
                input_datasets=["silver.wmata.bus_trip_snapshot", "silver.traffic.tomtom_flow_segment"],
                output_datasets=["gold.wmata.trip_delay_features", "gold.wmata.model_predictions", NATS_SUBJECT],
            )

        try:
            return _run_stream_inference_cycle_inner(run_id, limit)
        except Exception as exc:
            if run_log is not None:
                run_log.safe_finish_run(run_id, "failed", error=str(exc))
            if vm_metrics is not None:
                try:
                    vm_metrics.inc_counter(
                        "wmata_pipeline_job_runs_total",
                        labels={
                            "service": "stream-inference",
                            "job": "stream_inference",
                            "domain": "wmata",
                            "layer": "gold",
                            "status": "failed",
                        },
                    )
                except Exception:  # noqa: BLE001
                    pass
            logger.exception(
                "Stream inference cycle failed: %s",
                exc,
                extra={
                    "domain": "wmata",
                    "layer": "gold",
                    "job": "stream_inference",
                    "error_type": type(exc).__name__,
                },
            )
            raise


def _run_stream_inference_cycle_inner(run_id: str, limit: int) -> dict[str, Any]:
    t_cycle = time.monotonic()
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
            logger.info(
                "Stream inference idle (no features)",
                extra={"domain": "wmata", "layer": "gold", "job": "stream_inference", "row_count": 0},
            )
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

            # Reconcile real +15m ground truth before refreshing daily performance
        conn.commit()

    reconciled_count = reconcile_ground_truth_15m(lookback_hours=6)

    with connect_gold() as pconn:
        with pconn.cursor() as cur:
            # Update daily model performance metrics:
            # Prefer true +15m observed actuals (`split = 'live_observed'`) once >= 20 exist
            # for a service_date; fall back to bootstrap targets during cold-start first 14m.
            cur.execute(
                """
                WITH per_day AS (
                    SELECT f.service_date,
                           p.model_version,
                           count(*) FILTER (WHERE f.split = 'live_observed') AS n_obs
                    FROM public.model_predictions p
                    JOIN public.trip_delay_features f
                      ON f.trip_key = p.trip_key AND f.snapshot_ts = p.snapshot_ts
                    WHERE p.status = 'success' AND p.actual_delay_min IS NOT NULL AND f.service_date IS NOT NULL
                    GROUP BY f.service_date, p.model_version
                )
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
                JOIN per_day d
                  ON d.service_date = f.service_date AND d.model_version = p.model_version
                WHERE p.status = 'success'
                  AND p.actual_delay_min IS NOT NULL
                  AND f.service_date IS NOT NULL
                  AND (d.n_obs < 20 OR f.split = 'live_observed')
                GROUP BY f.service_date, p.model_version
                ON CONFLICT (service_date, model_version) DO UPDATE SET
                  n_predictions = EXCLUDED.n_predictions,
                  mae = EXCLUDED.mae,
                  rmse = EXCLUDED.rmse,
                  accuracy_late_5 = EXCLUDED.accuracy_late_5,
                  avg_error = EXCLUDED.avg_error
                """
            )
        pconn.commit()

    late_count = sum(1 for r in pred_rows if r[8] == 1)
    event_payload = {
        "inference_run_id": run_id,
        "model_version": model_version,
        "predicted_at": now.isoformat(),
        "scored_count": len(pred_rows),
        "predicted_late_count": late_count,
        "reconciled_ground_truth": reconciled_count,
        "avg_predicted_delay_min": round(float(np.mean(preds)), 3),
    }
    asyncio.run(publish_nats_inference(event_payload))

    duration_s = round(time.monotonic() - t_cycle, 3)
    if vm_metrics is not None:
        try:
            now_s = time.time()
            vm_metrics.push_metric(
                "wmata_stream_last_event_timestamp",
                now_s,
                labels={"stream": "wmata-inference", "service": "stream-inference"},
            )
            vm_metrics.inc_counter(
                "wmata_stream_inference_scored_total",
                labels={"model_version": model_version},
                amount=float(len(pred_rows)),
            )
            if reconciled_count > 0:
                vm_metrics.inc_counter(
                    "wmata_stream_ground_truth_reconciled_total",
                    labels={"model_version": model_version},
                    amount=float(reconciled_count),
                )
            vm_metrics.inc_counter(
                "wmata_pipeline_job_runs_total",
                labels={
                    "service": "stream-inference",
                    "job": "stream_inference",
                    "domain": "wmata",
                    "layer": "gold",
                    "status": "success",
                },
            )
            vm_metrics.push_metric(
                "wmata_pipeline_job_duration_seconds",
                duration_s,
                labels={
                    "service": "stream-inference",
                    "job": "stream_inference",
                    "domain": "wmata",
                    "layer": "gold",
                },
            )
        except Exception:  # noqa: BLE001
            pass

    summary = {
        "status": "ok",
        "run_id": run_id,
        "model_version": model_version,
        "scored": len(pred_rows),
        "predicted_late": late_count,
        "reconciled_15m": reconciled_count,
        **sync_counts,
    }
    if run_log is not None:
        run_log.safe_finish_run(run_id, "success", row_counts=summary)
    logger.info(
        "Scored %d predictions (%d late, reconciled=%d, model=%s)",
        len(pred_rows),
        late_count,
        reconciled_count,
        model_version,
        extra={
            "domain": "wmata",
            "layer": "gold",
            "job": "stream_inference",
            "duration_ms": round(duration_s * 1000, 2),
            "row_count": len(pred_rows),
        },
    )
    return summary


def match_future_observations(
    predictions: list[dict[str, Any]],
    silver_snaps: list[dict[str, Any]],
    min_ahead_sec: int = 840,
    max_ahead_sec: int = 960,
) -> list[dict[str, Any]]:
    """Match past predictions to true Silver observations 14-16 minutes ahead for the same trip_key."""
    by_trip: dict[str, tuple[list[float], list[float]]] = {}
    grouped_raw: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for snap in silver_snaps:
        tk = snap.get("trip_key")
        ts = snap.get("snapshot_ts")
        dev = snap.get("deviation_min")
        if not tk or ts is None or dev is None:
            continue
        ts_dt = ts if isinstance(ts, datetime) else datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if ts_dt.tzinfo is None:
            ts_dt = ts_dt.replace(tzinfo=timezone.utc)
        grouped_raw[str(tk)].append((ts_dt.timestamp(), max(-60.0, min(60.0, float(dev)))))

    for tk, pairs in grouped_raw.items():
        pairs.sort(key=lambda item: item[0])
        by_trip[tk] = ([p[0] for p in pairs], [p[1] for p in pairs])

    reconciled: list[dict[str, Any]] = []
    for pred in predictions:
        tk = str(pred.get("trip_key") or "")
        if tk not in by_trip:
            continue
        ts = pred.get("snapshot_ts")
        if ts is None:
            continue
        ts_dt = ts if isinstance(ts, datetime) else datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if ts_dt.tzinfo is None:
            ts_dt = ts_dt.replace(tzinfo=timezone.utc)
        base_s = ts_dt.timestamp()
        ts_list, dev_list = by_trip[tk]
        lo = bisect.bisect_left(ts_list, base_s + min_ahead_sec)
        hi = bisect.bisect_right(ts_list, base_s + max_ahead_sec)
        if lo >= hi:
            continue
        vals = dev_list[lo:hi]
        actual = round(sum(vals) / len(vals), 3)
        p_val = float(pred.get("predicted_delay_min") or 0.0)
        reconciled.append(
            {
                "input_record_id": pred["input_record_id"],
                "model_version": pred["model_version"],
                "trip_key": tk,
                "snapshot_ts": pred["snapshot_ts"],
                "actual_delay_min": actual,
                "abs_error": round(abs(p_val - actual), 4),
                "actual_late": int(actual > 5.0),
            }
        )
    return reconciled


def reconcile_ground_truth_15m(lookback_hours: int = 8) -> int:
    """Back-fill true observed +15m actuals from Silver into Gold predictions & features."""
    if psycopg2 is None:
        return 0
    try:
        with connect_silver("wmata") as s_conn:
            with s_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    WITH eligible AS (
                        SELECT trip_key, max(snapshot_ts) AS max_ts
                        FROM public.bus_trip_snapshot
                        WHERE trip_key IS NOT NULL
                          AND snapshot_ts >= now() - (%s || ' hours')::interval
                          AND deviation_min IS NOT NULL
                        GROUP BY trip_key
                        HAVING max(snapshot_ts) - min(snapshot_ts) >= interval '14 minutes'
                    )
                    SELECT s.trip_key, s.snapshot_ts, s.deviation_min, e.max_ts
                    FROM public.bus_trip_snapshot s
                    JOIN eligible e ON e.trip_key = s.trip_key
                    WHERE s.snapshot_ts >= now() - (%s || ' hours')::interval
                      AND s.deviation_min IS NOT NULL
                    """,
                    (str(lookback_hours), str(lookback_hours)),
                )
                silver_snaps = cur.fetchall()

        if not silver_snaps:
            return 0

        eligible_trips = sorted({str(r["trip_key"]) for r in silver_snaps if r.get("trip_key")})
        max_ts_by_trip: dict[str, datetime] = {}
        for r in silver_snaps:
            tk = str(r["trip_key"])
            if tk not in max_ts_by_trip and r.get("max_ts") is not None:
                max_ts_by_trip[tk] = r["max_ts"]

        with connect_gold() as g_conn:
            with g_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT p.input_record_id, p.model_version, p.trip_key,
                           p.snapshot_ts, p.predicted_delay_min
                    FROM public.model_predictions p
                    JOIN public.trip_delay_features f
                      ON f.trip_key = p.trip_key AND f.snapshot_ts = p.snapshot_ts
                    WHERE p.status = 'success'
                      AND p.trip_key = ANY(%s)
                      AND p.snapshot_ts >= now() - (%s || ' hours')::interval
                      AND p.snapshot_ts <= now() - interval '14 minutes'
                      AND COALESCE(f.split, '') <> 'live_observed'
                    ORDER BY p.snapshot_ts DESC
                    LIMIT 20000
                    """,
                    (eligible_trips, str(lookback_hours)),
                )
                pending_preds = [
                    r
                    for r in cur.fetchall()
                    if r["snapshot_ts"] <= max_ts_by_trip.get(str(r["trip_key"]), r["snapshot_ts"]) - timedelta(minutes=14)
                ]

        if not pending_preds:
            return 0

        matched = match_future_observations(pending_preds, silver_snaps)
        if not matched:
            return 0

        pred_updates = [
            (
                m["input_record_id"],
                m["model_version"],
                m["actual_delay_min"],
                m["abs_error"],
                m["actual_late"],
            )
            for m in matched
        ]
        feat_updates = list(
            {
                (m["trip_key"], m["snapshot_ts"]): (
                    m["trip_key"],
                    m["snapshot_ts"],
                    m["actual_delay_min"],
                    m["actual_late"],
                )
                for m in matched
            }.values()
        )

        with connect_gold() as g_conn:
            with g_conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    UPDATE public.model_predictions AS p
                    SET actual_delay_min = v.actual_delay_min,
                        abs_error = v.abs_error,
                        actual_late = v.actual_late
                    FROM (VALUES %s) AS v(input_record_id, model_version, actual_delay_min, abs_error, actual_late)
                    WHERE p.input_record_id = v.input_record_id
                      AND p.model_version = v.model_version
                    """,
                    pred_updates,
                    page_size=1000,
                )
                execute_values(
                    cur,
                    """
                    UPDATE public.trip_delay_features AS f
                    SET target_deviation_15m = v.target_dev,
                        target_late_5 = v.target_late,
                        split = 'live_observed'
                    FROM (VALUES %s) AS v(trip_key, snapshot_ts, target_dev, target_late)
                    WHERE f.trip_key = v.trip_key
                      AND f.snapshot_ts = v.snapshot_ts
                    """,
                    feat_updates,
                    page_size=1000,
                )
            g_conn.commit()
        logger.info("Reconciled %d predictions with observed +15m ground truth", len(matched))
        return len(matched)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Ground-truth +15m reconciliation skipped/failed: %s", exc)
        return 0


async def run_event_driven_loop(limit: int, fallback_interval: int) -> int:
    """Reactively run inference on NATS `realtime.silver.merged` events, with fallback timer."""
    stop = asyncio.Event()
    trigger = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    nc = None
    if nats is not None and NATS_URL:
        try:
            nc = await nats.connect(
                NATS_URL,
                connect_timeout=5,
                max_reconnect_attempts=-1,
                reconnect_time_wait=2,
            )

            async def _on_silver_event(_msg) -> None:
                trigger.set()

            await nc.subscribe(SILVER_MERGED_SUBJECT, cb=_on_silver_event)
            await nc.subscribe("wmata.realtime.silver_data.>", cb=_on_silver_event)
            logger.info(
                "Event-driven inference subscribed to %s & wmata.realtime.silver_data.> (fallback=%ds)",
                SILVER_MERGED_SUBJECT,
                fallback_interval,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("NATS reactive subscription unavailable (%s); using timer fallback", exc)
            nc = None

    write_heartbeat("started")
    try:
        while not stop.is_set():
            trigger.clear()
            try:
                summary = await asyncio.to_thread(run_stream_inference_cycle, limit)
                write_heartbeat(summary["status"], extra=summary)
                logger.info("STREAM_INFERENCE_SUMMARY %s", json.dumps(summary))
            except Exception as exc:  # noqa: BLE001
                logger.exception("Stream inference cycle failed: %s", exc)
                write_heartbeat("error", extra={"error": str(exc)})

            # Wait until a Silver merge/CDC event arrives OR fallback interval expires
            stop_task = asyncio.create_task(stop.wait())
            trig_task = asyncio.create_task(trigger.wait())
            done, pending = await asyncio.wait(
                {stop_task, trig_task},
                timeout=max(5, fallback_interval),
                return_when=asyncio.FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()
            if stop.is_set():
                break
            if trig_task in done:
                # Brief 1.5s debounce to coalesce rapid back-to-back merge events
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=1.5)
    finally:
        if nc is not None:
            with contextlib.suppress(Exception):
                await nc.close()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Real-time stream inference worker")
    ap.add_argument("--once", action="store_true", help="Run a single inference cycle and exit")
    ap.add_argument("--limit", type=int, default=int(os.environ.get("STREAM_INFERENCE_LIMIT", "1000")))
    ap.add_argument("--interval", type=int, default=int(os.environ.get("STREAM_INFERENCE_INTERVAL", "30")))
    args = ap.parse_args()

    if args.once:
        write_heartbeat("started")
        try:
            summary = run_stream_inference_cycle(limit=args.limit)
            write_heartbeat(summary["status"], extra=summary)
            logger.info("STREAM_INFERENCE_SUMMARY %s", json.dumps(summary))
            return 0
        except Exception as exc:  # noqa: BLE001
            logger.exception("Stream inference cycle failed: %s", exc)
            write_heartbeat("error", extra={"error": str(exc)})
            return 1

    return asyncio.run(run_event_driven_loop(limit=args.limit, fallback_interval=args.interval))


if __name__ == "__main__":
    raise SystemExit(main())
