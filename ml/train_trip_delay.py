"""Train the WMATA trip-delay LightGBM model from gold.trip_delay_features.

Reads gold PostgreSQL (HAProxy 5434), compares against two baselines, logs the
model/metrics to MLflow, and writes test predictions + daily performance back
to gold.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import lightgbm as lgb
import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sklearn.metrics import accuracy_score, f1_score, mean_absolute_error, mean_squared_error, r2_score

GOLD_HOST = os.environ.get("POSTGRES_GOLD_HOST", "haproxy")
GOLD_PORT = int(os.environ.get("POSTGRES_GOLD_PORT", "5434"))
GOLD_DB = os.environ.get("POSTGRES_DB_WMATA", "wmata")
GOLD_USER = os.environ.get("POSTGRES_GOLD_USER", "postgres")
GOLD_PASSWORD = os.environ.get("POSTGRES_GOLD_PASSWORD", "postgres")
MLFLOW_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
EXPERIMENT = os.environ.get("MLFLOW_EXPERIMENT", "wmata-trip-delay")
MODEL_NAME = os.environ.get("MLFLOW_MODEL_NAME", "trip_delay_lgbm")
FEATURESET = os.environ.get("FEATURESET", "without_ddot")
TRIGGERED_BY = os.environ.get("MLFLOW_TRIGGERED_BY", "manual")
WINDOW_WEEKS = int(os.environ.get("TRAIN_WINDOW_WEEKS", "12"))
MIN_WEEKS = int(os.environ.get("TRAIN_MIN_WEEKS", "4"))
VALID_DAYS = int(os.environ.get("TRAIN_VALID_DAYS", "7"))
TEST_DAYS = int(os.environ.get("TRAIN_TEST_DAYS", "7"))

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
TARGET = "target_deviation_15m"

PARAMS = {
    "n_estimators": 2000,
    "learning_rate": 0.05,
    "num_leaves": 63,
    "min_child_samples": 50,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "random_state": 42,
    "n_jobs": -1,
}


def connect():
    return psycopg2.connect(
        host=GOLD_HOST,
        port=GOLD_PORT,
        dbname=GOLD_DB,
        user=GOLD_USER,
        password=GOLD_PASSWORD,
        connect_timeout=10,
    )


def load_data() -> pd.DataFrame:
    cols = ", ".join(
        [
            "trip_key",
            "snapshot_ts",
            "service_date",
            "split",
            TARGET,
            "target_late_5",
        ]
        + FEATURES
    )
    with connect() as conn:
        max_date = pd.read_sql(
            "SELECT max(service_date) AS max_date FROM public.trip_delay_features", conn
        ).iloc[0, 0]
        if max_date is None:
            raise RuntimeError("gold.trip_delay_features is empty")
        window_start = max_date - timedelta(weeks=WINDOW_WEEKS)
        query = (
            f"SELECT {cols} FROM public.trip_delay_features "
            "WHERE service_date >= %s"
        )
        df = pd.read_sql(query, conn, params=(window_start,))

    if df.empty:
        raise RuntimeError(f"no gold rows since {window_start}")
    min_date = pd.to_datetime(df["service_date"]).min().date()
    span_days = (max_date - min_date).days
    if span_days < MIN_WEEKS * 7:
        raise RuntimeError(
            f"training window too short: {span_days} days, need >= {MIN_WEEKS * 7}"
        )

    # Rolling split: last TEST_DAYS days = test, preceding VALID_DAYS = valid.
    test_start = max_date - timedelta(days=TEST_DAYS - 1)
    valid_start = test_start - timedelta(days=VALID_DAYS)
    service_date = pd.to_datetime(df["service_date"]).dt.date
    df["split"] = np.where(
        service_date >= test_start,
        "test",
        np.where(service_date >= valid_start, "valid", "train"),
    )

    numeric = [f for f in FEATURES if f != "route_id"]
    df[numeric] = df[numeric].apply(pd.to_numeric, errors="coerce")
    df["route_id"] = df["route_id"].astype("category")
    df.attrs["max_service_date"] = max_date
    df.attrs["window_start"] = window_start
    df.attrs["valid_start"] = valid_start
    df.attrs["test_start"] = test_start
    return df


def metrics(y_true, y_pred) -> dict:
    mae = float(mean_absolute_error(y_true, y_pred))
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    r2 = float(r2_score(y_true, y_pred))
    actual_late = (np.asarray(y_true) > 5).astype(int)
    pred_late = (np.asarray(y_pred) > 5).astype(int)
    return {
        "mae": mae,
        "rmse": rmse,
        "r2": r2,
        "accuracy_late_5": float(accuracy_score(actual_late, pred_late)),
        "f1_late_5": float(f1_score(actual_late, pred_late, zero_division=0)),
    }


def baseline_persistence(test: pd.DataFrame) -> dict:
    return metrics(test[TARGET], test["deviation_now"].fillna(0))


def baseline_route_hour(train: pd.DataFrame, test: pd.DataFrame) -> dict:
    table = train.groupby(["route_id", "hour_local"], observed=True)[TARGET].mean()
    global_mean = float(train[TARGET].mean())
    pred = [
        table.get((route, hour), global_mean)
        for route, hour in zip(test["route_id"], test["hour_local"])
    ]
    return metrics(test[TARGET], np.asarray(pred))


def write_predictions(conn, test: pd.DataFrame, pred: np.ndarray, model_version: str) -> None:
    now = datetime.now(timezone.utc)
    rows = [
        (
            f"{trip_key}|{snapshot_ts}",
            model_version,
            now,
            trip_key,
            snapshot_ts,
            float(p),
            float(actual),
            abs(float(p) - float(actual)),
            int(float(p) > 5),
            int(float(actual) > 5),
        )
        for trip_key, snapshot_ts, p, actual in zip(
            test["trip_key"], test["snapshot_ts"], pred, test[TARGET]
        )
    ]
    sql = """
        INSERT INTO public.model_predictions
          (input_record_id, model_version, predicted_at, trip_key, snapshot_ts,
           predicted_delay_min, actual_delay_min, abs_error,
           predicted_late, actual_late)
        VALUES %s
        ON CONFLICT (input_record_id, model_version) DO UPDATE SET
          predicted_at = EXCLUDED.predicted_at,
          predicted_delay_min = EXCLUDED.predicted_delay_min,
          actual_delay_min = EXCLUDED.actual_delay_min,
          abs_error = EXCLUDED.abs_error,
          predicted_late = EXCLUDED.predicted_late,
          actual_late = EXCLUDED.actual_late
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=5000)
    conn.commit()

    # Daily performance for the test window.
    perf = (
        pd.DataFrame(
            {
                "service_date": pd.to_datetime(test["service_date"]).dt.date,
                "abs_error": np.abs(pred - test[TARGET].to_numpy()),
                "sq_error": (pred - test[TARGET].to_numpy()) ** 2,
                "pred_late": (pred > 5).astype(int),
                "actual_late": (test[TARGET].to_numpy() > 5).astype(int),
            }
        )
        .groupby("service_date")
        .agg(
            n_predictions=("abs_error", "size"),
            mae=("abs_error", "mean"),
            rmse=("sq_error", lambda x: float(np.sqrt(x.mean()))),
            accuracy_late_5=(
                "pred_late",
                lambda x: float(
                    accuracy_score(
                        test.loc[x.index, TARGET].gt(5).astype(int), x
                    )
                ),
            ),
        )
        .reset_index()
    )
    perf_rows = [
        (
            row.service_date,
            model_version,
            int(row.n_predictions),
            float(row.mae),
            float(row.rmse),
            float(row.accuracy_late_5),
            None,
        )
        for row in perf.itertuples()
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO public.model_performance_daily
              (service_date, model_version, n_predictions, mae, rmse,
               accuracy_late_5, avg_error)
            VALUES %s
            ON CONFLICT (service_date, model_version) DO UPDATE SET
              n_predictions = EXCLUDED.n_predictions,
              mae = EXCLUDED.mae,
              rmse = EXCLUDED.rmse,
              accuracy_late_5 = EXCLUDED.accuracy_late_5,
              avg_error = EXCLUDED.avg_error
            """,
            perf_rows,
            page_size=100,
        )
    conn.commit()


def main() -> None:
    df = load_data()
    train = df[df["split"] == "train"].copy()
    valid = df[df["split"] == "valid"].copy()
    test = df[df["split"] == "test"].copy()
    for name, part in (("train", train), ("valid", valid), ("test", test)):
        if part.empty:
            raise RuntimeError(
                f"empty {name} split (window={WINDOW_WEEKS}w, "
                f"valid_days={VALID_DAYS}, test_days={TEST_DAYS})"
            )

    max_date = df.attrs["max_service_date"]
    run_name = os.environ.get("MLFLOW_RUN_NAME") or (
        f"lgbm-trip-delay-{max_date.isoformat()}-{FEATURESET}"
    )

    X_train, y_train = train[FEATURES], train[TARGET]
    X_valid, y_valid = valid[FEATURES], valid[TARGET]
    X_test, y_test = test[FEATURES], test[TARGET]

    base_p = baseline_persistence(test)
    base_r = baseline_route_hour(train, test)

    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run(run_name=run_name) as run:
        mlflow.log_params({f"lgbm_{k}": v for k, v in PARAMS.items()})
        mlflow.log_params(
            {
                "n_features": len(FEATURES),
                "train_rows": len(train),
                "valid_rows": len(valid),
                "test_rows": len(test),
                "train_window_weeks": WINDOW_WEEKS,
                "train_min_weeks": MIN_WEEKS,
                "train_valid_days": VALID_DAYS,
                "train_test_days": TEST_DAYS,
                "window_start": str(df.attrs["window_start"]),
                "max_service_date": str(max_date),
            }
        )
        mlflow.set_tags(
            {
                "data_source": "gold.trip_delay_features",
                "dataset_version": f"gold-{max_date.isoformat()}",
                "model_family": "lightgbm",
                "target": TARGET,
                "featureset": FEATURESET,
                "triggered_by": TRIGGERED_BY,
            }
        )
        mlflow.log_metrics({f"baseline_persistence_{k}": v for k, v in base_p.items()})
        mlflow.log_metrics({f"baseline_route_hour_{k}": v for k, v in base_r.items()})
        mlflow.log_dict(
            {
                "features": FEATURES,
                "split": {
                    "train": [str(train["service_date"].min()), str(train["service_date"].max())],
                    "valid": [str(valid["service_date"].min()), str(valid["service_date"].max())],
                    "test": [str(test["service_date"].min()), str(test["service_date"].max())],
                },
            },
            "feature_contract.json",
        )

        model = lgb.LGBMRegressor(**PARAMS)
        model.fit(
            X_train,
            y_train,
            eval_set=[(X_valid, y_valid)],
            eval_metric="l1",
            categorical_feature=["route_id"],
            callbacks=[lgb.early_stopping(100), lgb.log_evaluation(100)],
        )
        pred = model.predict(X_test)
        m = metrics(y_test, pred)
        mlflow.log_metrics(m)
        mlflow.lightgbm.log_model(
            model,
            artifact_path="model",
            registered_model_name=MODEL_NAME,
            input_example=X_test.head(5),
        )

        print("TEST METRICS", json.dumps(m, indent=2), flush=True)
        print("BASELINE persistence", json.dumps(base_p, indent=2), flush=True)
        print("BASELINE route_hour", json.dumps(base_r, indent=2), flush=True)

        with connect() as conn:
            write_predictions(conn, test, pred, run.info.run_id)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.training_watermark
                      (pipeline, last_snapshot_ts, last_trained_at, model_version, updated_at)
                    SELECT 'trip_delay_features', max(snapshot_ts), now(), %s, now()
                    FROM public.trip_delay_features
                    ON CONFLICT (pipeline) DO UPDATE SET
                      last_trained_at = now(),
                      model_version = EXCLUDED.model_version,
                      updated_at = now()
                    """,
                    (run.info.run_id,),
                )
            conn.commit()
        print(f"predictions written model_version={run.info.run_id}", flush=True)


if __name__ == "__main__":
    main()
