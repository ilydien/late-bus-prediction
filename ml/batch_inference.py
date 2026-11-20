"""Scheduled batch inference for the trip-delay model.

Loads the model at an MLflow alias (default: champion), scores recent rows from
gold.trip_delay_features, and writes idempotent predictions to
gold.model_predictions.

Invalid rows are written with status='error' and an error message, so a batch
run leaves an auditable record instead of silently dropping input.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import uuid
from datetime import datetime, timezone

import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd
import psycopg2
from mlflow import MlflowClient
from psycopg2.extras import execute_values

GOLD_HOST = os.environ.get("POSTGRES_GOLD_HOST", "haproxy")
GOLD_PORT = int(os.environ.get("POSTGRES_GOLD_PORT", "5434"))
GOLD_DB = os.environ.get("POSTGRES_DB_WMATA", "wmata")
GOLD_USER = os.environ.get("POSTGRES_GOLD_USER", "postgres")
GOLD_PASSWORD = os.environ.get("POSTGRES_GOLD_PASSWORD", "postgres")
MLFLOW_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = os.environ.get("MLFLOW_MODEL_NAME", "trip_delay_lgbm")
MODEL_ALIAS = os.environ.get("MLFLOW_MODEL_ALIAS", "champion")


def connect_gold():
    return psycopg2.connect(
        host=GOLD_HOST,
        port=GOLD_PORT,
        dbname=GOLD_DB,
        user=GOLD_USER,
        password=GOLD_PASSWORD,
        connect_timeout=10,
    )


def load_route_categories(client: MlflowClient, run_id: str, conn) -> list[str]:
    """Prefer the category list saved by training; fall back to gold routes."""
    try:
        data = mlflow.artifacts.load_dict(f"runs:/{run_id}/route_categories.json")
        categories = data.get("route_id") if isinstance(data, dict) else data
        if categories:
            return [str(x) for x in categories]
    except Exception:
        pass

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT route_id
            FROM public.trip_delay_features
            WHERE route_id IS NOT NULL AND split = 'train'
            ORDER BY route_id
            """
        )
        categories = [row[0] for row in cur.fetchall()]
        if categories:
            return categories
        cur.execute(
            """
            SELECT DISTINCT route_id
            FROM public.trip_delay_features
            WHERE route_id IS NOT NULL
            ORDER BY route_id
            """
        )
        return [row[0] for row in cur.fetchall()]


def load_model(alias: str, conn, version: str | None = None):
    mlflow.set_tracking_uri(MLFLOW_URI)
    client = MlflowClient(MLFLOW_URI)
    if version:
        mv = client.get_model_version(MODEL_NAME, version)
        model = mlflow.lightgbm.load_model(f"models:/{MODEL_NAME}/{version}")
    else:
        mv = client.get_model_version_by_alias(MODEL_NAME, alias)
        model = mlflow.lightgbm.load_model(f"models:/{MODEL_NAME}@{alias}")
    features = list(getattr(model, "feature_name_", None) or model.booster_.feature_name())
    categories = load_route_categories(client, mv.run_id, conn)
    return model, mv, features, categories


def load_rows(conn, features: list[str], limit: int, since: str | None, until: str | None) -> pd.DataFrame:
    columns = ["trip_key", "snapshot_ts", "target_deviation_15m"] + features
    # Keep order stable and avoid duplicate columns such as route_id.
    columns = list(dict.fromkeys(columns))
    where = []
    params = []
    if since:
        where.append("snapshot_ts >= %s")
        params.append(since)
    if until:
        where.append("snapshot_ts <= %s")
        params.append(until)
    query = f"SELECT {', '.join(columns)} FROM public.trip_delay_features"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY snapshot_ts DESC LIMIT %s"
    params.append(limit)
    return pd.read_sql(query, conn, params=tuple(params))


def prepare_features(df: pd.DataFrame, features: list[str], categories: list[str]):
    """Return valid feature frame and invalid rows with reasons."""
    errors: list[tuple[pd.Series, str]] = []
    valid_mask = []
    category_set = set(categories)
    for _, row in df.iterrows():
        if pd.isna(row.get("trip_key")) or pd.isna(row.get("snapshot_ts")):
            errors.append((row, "missing trip_key/snapshot_ts"))
            valid_mask.append(False)
            continue
        route = row.get("route_id")
        if pd.isna(route) or str(route) not in category_set:
            errors.append((row, f"unknown route_id={route}"))
            valid_mask.append(False)
            continue
        valid_mask.append(True)

    valid = df.loc[valid_mask].copy()
    if not valid.empty:
        for feature in features:
            if feature == "route_id":
                continue
            valid[feature] = pd.to_numeric(valid[feature], errors="coerce")
        valid["route_id"] = pd.Categorical(valid["route_id"], categories=categories)
    return valid, errors


def input_id(row) -> str:
    trip = row.get("trip_key")
    ts = row.get("snapshot_ts")
    if not pd.isna(trip) and not pd.isna(ts):
        return f"{trip}|{ts}"
    payload = json.dumps(
        {k: str(v) for k, v in row.to_dict().items()}, sort_keys=True, default=str
    )
    return "invalid|" + hashlib.sha256(payload.encode()).hexdigest()


def build_rows(
    valid: pd.DataFrame,
    invalid: list[tuple[pd.Series, str]],
    model,
    features: list[str],
    model_version: str,
    inference_run_id: str,
):
    now = datetime.now(timezone.utc)
    rows = []
    if not valid.empty:
        preds = model.predict(valid[features])
        for pred, (_, row) in zip(preds, valid.iterrows()):
            actual = row.get("target_deviation_15m")
            actual = None if pd.isna(actual) else float(actual)
            p = float(pred)
            rows.append(
                (
                    input_id(row),
                    model_version,
                    now,
                    row.get("trip_key"),
                    row.get("snapshot_ts"),
                    p,
                    actual,
                    None if actual is None else abs(p - actual),
                    int(p > 5),
                    None if actual is None else int(actual > 5),
                    "success",
                    None,
                    inference_run_id,
                )
            )
    for row, reason in invalid:
        actual = row.get("target_deviation_15m")
        actual = None if pd.isna(actual) else float(actual)
        rows.append(
            (
                input_id(row),
                model_version,
                now,
                row.get("trip_key"),
                row.get("snapshot_ts"),
                None,
                actual,
                None,
                None,
                None if actual is None else int(actual > 5),
                "error",
                reason,
                inference_run_id,
            )
        )
    return rows


def write_predictions(conn, rows: list[tuple]) -> None:
    if not rows:
        return
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
              trip_key = EXCLUDED.trip_key,
              snapshot_ts = EXCLUDED.snapshot_ts,
              predicted_delay_min = EXCLUDED.predicted_delay_min,
              actual_delay_min = EXCLUDED.actual_delay_min,
              abs_error = EXCLUDED.abs_error,
              predicted_late = EXCLUDED.predicted_late,
              actual_late = EXCLUDED.actual_late,
              status = EXCLUDED.status,
              error = EXCLUDED.error,
              inference_run_id = EXCLUDED.inference_run_id
            """,
            rows,
            page_size=5000,
        )
    conn.commit()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model-alias", default=MODEL_ALIAS)
    ap.add_argument("--model-version", help="registered model version, overrides alias")
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--since")
    ap.add_argument("--until")
    ap.add_argument("--run-id")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    run_id = args.run_id or str(uuid.uuid4())
    conn = connect_gold()
    try:
        model, mv, features, categories = load_model(
            args.model_alias, conn, version=args.model_version
        )
        df = load_rows(conn, features, args.limit, args.since, args.until)
        valid, invalid = prepare_features(df, features, categories)
        rows = build_rows(
            valid,
            invalid,
            model,
            features,
            f"v{mv.version}",
            run_id,
        )
        if not args.dry_run:
            write_predictions(conn, rows)
        summary = {
            "run_id": run_id,
            "model_alias": args.model_alias if not args.model_version else None,
            "model_version": f"v{mv.version}",
            "rows_read": len(df),
            "success": len(valid),
            "errors": len(invalid),
            "dry_run": args.dry_run,
        }
        print("INFERENCE_SUMMARY " + json.dumps(summary), flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
