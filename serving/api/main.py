from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from psycopg2 import connect
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel

import os

GOLD_HOST = os.environ.get("POSTGRES_GOLD_HOST", "haproxy")
GOLD_PORT = int(os.environ.get("POSTGRES_GOLD_PORT", "5436"))
GOLD_DB = os.environ.get("POSTGRES_DB_WMATA", "wmata")
GOLD_USER = os.environ.get("POSTGRES_GOLD_USER", "postgres")
GOLD_PASSWORD = os.environ.get("POSTGRES_GOLD_PASSWORD", "postgres")


class Freshness(BaseModel):
    max_snapshot_ts: str | None = None
    max_service_date: str | None = None
    max_predicted_at: str | None = None
    generated_at: str | None = None


class DataResponse(BaseModel):
    data: list[dict[str, Any]]
    freshness: Freshness


app = FastAPI(
    title="WMATA Serving API",
    version="1.0.0",
    description="Read-only serving API over the WMATA gold marts.",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def get_conn():
    conn = connect(
        host=GOLD_HOST,
        port=GOLD_PORT,
        dbname=GOLD_DB,
        user=GOLD_USER,
        password=GOLD_PASSWORD,
        connect_timeout=10,
    )
    try:
        yield conn
    finally:
        conn.close()


def _validate_range(start_date: date, end_date: date) -> None:
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")


def _fetch(conn, sql: str, params=()) -> list[dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


def _freshness(conn) -> dict:
    rows = _fetch(conn, "SELECT * FROM public.serving_freshness")
    if not rows:
        return {}
    return {
        key: (str(value) if value is not None else None)
        for key, value in rows[0].items()
    }


@app.get("/health")
def health(conn=Depends(get_conn)):
    try:
        _fetch(conn, "SELECT 1 AS ok")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc
    return {"status": "ok"}


@app.get("/v1/freshness", response_model=Freshness)
def freshness(conn=Depends(get_conn)):
    return _freshness(conn)


@app.get("/v1/routes/delay", response_model=DataResponse)
def routes_delay(
    start_date: date,
    end_date: date,
    route_id: str | None = None,
    conn=Depends(get_conn),
):
    _validate_range(start_date, end_date)
    sql = """
        SELECT route_id, service_date, trips, avg_deviation, median_deviation,
               p90_deviation, on_time_pct, late_pct, avg_speed_kmh
        FROM public.serving_route_daily
        WHERE service_date BETWEEN %s AND %s
    """
    params: list = [start_date, end_date]
    if route_id:
        sql += " AND route_id = %s"
        params.append(route_id)
    sql += " ORDER BY service_date, route_id"
    return {"data": _fetch(conn, sql, params), "freshness": _freshness(conn)}


@app.get("/v1/reliability/daily", response_model=DataResponse)
def reliability_daily(
    start_date: date,
    end_date: date,
    route_id: str | None = None,
    conn=Depends(get_conn),
):
    _validate_range(start_date, end_date)
    sql = """
        SELECT route_id, service_date, trips, on_time_pct, late_pct,
               avg_deviation, p90_deviation
        FROM public.serving_route_daily
        WHERE service_date BETWEEN %s AND %s
    """
    params: list = [start_date, end_date]
    if route_id:
        sql += " AND route_id = %s"
        params.append(route_id)
    sql += " ORDER BY service_date, route_id"
    return {"data": _fetch(conn, sql, params), "freshness": _freshness(conn)}


@app.get("/v1/routes/hourly", response_model=DataResponse)
def routes_hourly(
    start_date: date,
    end_date: date,
    route_id: str | None = None,
    conn=Depends(get_conn),
):
    _validate_range(start_date, end_date)
    sql = """
        SELECT route_id, service_date, hour_local, trips, avg_deviation,
               on_time_pct, late_pct, avg_speed_kmh
        FROM public.serving_route_hourly
        WHERE service_date BETWEEN %s AND %s
    """
    params: list = [start_date, end_date]
    if route_id:
        sql += " AND route_id = %s"
        params.append(route_id)
    sql += " ORDER BY service_date, hour_local, route_id"
    return {"data": _fetch(conn, sql, params), "freshness": _freshness(conn)}


@app.get("/v1/predictions/latest", response_model=DataResponse)
def latest_predictions(
    route_id: str | None = None,
    limit: int = Query(50, ge=1, le=500),
    conn=Depends(get_conn),
):
    sql = """
        SELECT input_record_id, model_version, predicted_at, trip_key, snapshot_ts,
               route_id, predicted_delay_min, actual_delay_min, abs_error,
               predicted_late, actual_late, inference_run_id
        FROM public.serving_latest_predictions
    """
    params: list = []
    if route_id:
        sql += " WHERE route_id = %s"
        params.append(route_id)
    sql += " ORDER BY predicted_at DESC LIMIT %s"
    params.append(limit)
    return {"data": _fetch(conn, sql, params), "freshness": _freshness(conn)}


@app.get("/v1/model/performance", response_model=DataResponse)
def model_performance(
    start_date: date | None = None,
    end_date: date | None = None,
    conn=Depends(get_conn),
):
    if start_date and end_date:
        _validate_range(start_date, end_date)
    sql = """
        SELECT service_date, model_version, n_predictions, mae, rmse,
               accuracy_late_5, avg_error
        FROM public.serving_model_performance
        WHERE 1=1
    """
    params: list = []
    if start_date:
        sql += " AND service_date >= %s"
        params.append(start_date)
    if end_date:
        sql += " AND service_date <= %s"
        params.append(end_date)
    sql += " ORDER BY service_date DESC, model_version"
    return {"data": _fetch(conn, sql, params), "freshness": _freshness(conn)}
