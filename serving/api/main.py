from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from psycopg2 import connect
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel

try:
    import nats
except ImportError:  # pragma: no cover - optional for offline tests
    nats = None  # type: ignore[assignment]

logger = logging.getLogger("serving-api")

GOLD_HOST = os.environ.get("POSTGRES_GOLD_HOST", "haproxy")
GOLD_PORT = int(os.environ.get("POSTGRES_GOLD_PORT", "5436"))
GOLD_DB = os.environ.get("POSTGRES_DB_WMATA", "wmata")
GOLD_USER = os.environ.get("POSTGRES_GOLD_USER", "postgres")
GOLD_PASSWORD = os.environ.get("POSTGRES_GOLD_PASSWORD", "postgres")

SILVER_HOST = os.environ.get("POSTGRES_SILVER_HOST", "haproxy")
SILVER_PORT = int(os.environ.get("POSTGRES_SILVER_READ_PORT", "5435"))
SILVER_TRAFFIC_DB = os.environ.get("POSTGRES_DB_TRAFFIC", "traffic")
SILVER_WMATA_DB = os.environ.get("POSTGRES_DB_WMATA", "wmata")
SILVER_USER = os.environ.get("POSTGRES_SILVER_USER", "postgres")
SILVER_PASSWORD = os.environ.get("POSTGRES_SILVER_PASSWORD", "postgres")

NATS_URL = os.environ.get("NATS_URL", "nats://nats:4222")
MONITOR_URL = os.environ.get("MONITOR_URL", "http://monitor:8090")
STATIC_DIR = Path(__file__).resolve().parent / "static"

_SSE_SUBSCRIBERS: set[asyncio.Queue[dict[str, Any]]] = set()
_NATS_CONNECTED = False
_NATS_SUBJECT_EVENTS = {
    "wmata.realtime.inference": "inference",
    "realtime.silver.merged": "silver_merged",
    "wmata.realtime.bronze_data": "bronze_wmata",
    "traffic.realtime.bronze_data": "bronze_traffic",
}


def _broadcast_sse(event_name: str, data: dict[str, Any]) -> None:
    payload = {
        "event": event_name,
        "data": data,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    stale: list[asyncio.Queue[dict[str, Any]]] = []
    for queue in list(_SSE_SUBSCRIBERS):
        try:
            queue.put_nowait(payload)
        except asyncio.QueueFull:
            try:
                queue.get_nowait()
                queue.put_nowait(payload)
            except Exception:  # noqa: BLE001
                stale.append(queue)
    for q in stale:
        _SSE_SUBSCRIBERS.discard(q)


async def _nats_bridge_loop(stop_event: asyncio.Event) -> None:
    """Subscribe to NATS real-time subjects and fan out to SSE dashboard clients."""
    global _NATS_CONNECTED
    if nats is None or not NATS_URL:
        return

    while not stop_event.is_set():
        nc = None
        try:
            nc = await nats.connect(
                NATS_URL,
                connect_timeout=3,
                max_reconnect_attempts=-1,
                reconnect_time_wait=2,
            )
            _NATS_CONNECTED = True
            logger.info("SSE NATS bridge connected to %s", NATS_URL)

            async def _make_cb(ev_name: str):
                async def _cb(msg) -> None:
                    try:
                        parsed = json.loads(msg.data.decode("utf-8"))
                    except Exception:  # noqa: BLE001
                        parsed = {"raw": msg.data.decode("utf-8", errors="ignore")[:200]}
                    _broadcast_sse(ev_name, parsed)
                return _cb

            for subject, ev_name in _NATS_SUBJECT_EVENTS.items():
                await nc.subscribe(subject, cb=await _make_cb(ev_name))

            await stop_event.wait()
        except asyncio.CancelledError:
            break
        except Exception as exc:  # noqa: BLE001
            _NATS_CONNECTED = False
            logger.warning("SSE NATS bridge unavailable (%s); retrying in 5s", exc)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass
        finally:
            _NATS_CONNECTED = False
            if nc is not None:
                with contextlib.suppress(Exception):
                    await nc.close()


@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    stop_event = asyncio.Event()
    task = asyncio.create_task(_nats_bridge_loop(stop_event))
    try:
        yield
    finally:
        stop_event.set()
        task.cancel()
        with contextlib.suppress(Exception):
            await task

DEFAULT_DC_CORRIDORS: list[dict[str, Any]] = [
    {"corridor_id": "dc_16th_st_nw", "corridor_name": "16th St NW & U St NW", "query_lat": 38.9169, "query_lon": -77.0365, "route_hint": "S2,S9"},
    {"corridor_id": "dc_georgia_ave_nw", "corridor_name": "Georgia Ave NW & Columbia Rd", "query_lat": 38.9283, "query_lon": -77.0232, "route_hint": "70,79"},
    {"corridor_id": "dc_conn_ave_nw", "corridor_name": "Connecticut Ave NW & Dupont Circle", "query_lat": 38.9096, "query_lon": -77.0434, "route_hint": "L2,42,43"},
    {"corridor_id": "dc_wisc_ave_nw", "corridor_name": "Wisconsin Ave NW & M St Georgetown", "query_lat": 38.9051, "query_lon": -77.0628, "route_hint": "31,33"},
    {"corridor_id": "dc_k_st_downtown", "corridor_name": "K St NW & 15th St NW Downtown", "query_lat": 38.9025, "query_lon": -77.0345, "route_hint": "D6,32,36,80"},
    {"corridor_id": "dc_h_st_ne", "corridor_name": "H St NE & 8th St NE", "query_lat": 38.9001, "query_lon": -76.9946, "route_hint": "X2,X9"},
    {"corridor_id": "dc_penn_ave_se", "corridor_name": "Pennsylvania Ave SE & 8th St SE", "query_lat": 38.8819, "query_lon": -76.9953, "route_hint": "32,36"},
    {"corridor_id": "dc_benning_rd_ne", "corridor_name": "Benning Rd NE & Minnesota Ave", "query_lat": 38.8973, "query_lon": -76.9472, "route_hint": "U8,V4,X2"},
    {"corridor_id": "dc_ny_ave_ne", "corridor_name": "New York Ave NE & Florida Ave", "query_lat": 38.9082, "query_lon": -77.0029, "route_hint": "P6,80"},
    {"corridor_id": "dc_ri_ave_ne", "corridor_name": "Rhode Island Ave NE & 12th St", "query_lat": 38.9261, "query_lon": -76.9882, "route_hint": "G8,83,86"},
    {"corridor_id": "dc_mass_ave_nw", "corridor_name": "Massachusetts Ave NW & North Capitol", "query_lat": 38.8975, "query_lon": -77.0091, "route_hint": "D6,80,96"},
    {"corridor_id": "dc_mlk_ave_se", "corridor_name": "MLK Jr Ave SE & Good Hope Rd", "query_lat": 38.8672, "query_lon": -76.9885, "route_hint": "A2,A6,A8,W4"},
    {"corridor_id": "dc_s_capitol_st", "corridor_name": "South Capitol St & M St SE", "query_lat": 38.8765, "query_lon": -77.0089, "route_hint": "P6,V4,A9"},
    {"corridor_id": "md_silver_spring", "corridor_name": "Colesville Rd & Wayne Ave Silver Spring", "query_lat": 38.9959, "query_lon": -77.0270, "route_hint": "S2,S9,70,79"},
    {"corridor_id": "va_rosslyn", "corridor_name": "Wilson Blvd & N Lynn St Rosslyn", "query_lat": 38.8951, "query_lon": -77.0715, "route_hint": "38B"},
    {"corridor_id": "va_pentagon_city", "corridor_name": "S Hayes St & 12th St S Pentagon City", "query_lat": 38.8629, "query_lon": -77.0596, "route_hint": "16A,16E"},
]


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
    description="Read-only serving API over the WMATA gold marts with real-time NATS SSE push.",
    lifespan=lifespan,
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


def _try_silver_query(dbname: str, sql: str, params=()) -> list[dict[str, Any]]:
    """Query read-only silver database, returning [] if unavailable or table missing."""
    try:
        conn = connect(
            host=SILVER_HOST,
            port=SILVER_PORT,
            dbname=dbname,
            user=SILVER_USER,
            password=SILVER_PASSWORD,
            connect_timeout=4,
        )
    except Exception:  # noqa: BLE001
        return []
    try:
        return _fetch(conn, sql, params)
    except Exception:  # noqa: BLE001
        return []
    finally:
        conn.close()


def _try_silver_traffic_query(sql: str, params=()) -> list[dict[str, Any]]:
    return _try_silver_query(SILVER_TRAFFIC_DB, sql, params)


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
@app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
def dashboard_ui():
    index_file = STATIC_DIR / "index.html"
    if index_file.exists():
        return FileResponse(index_file, media_type="text/html")
    return HTMLResponse("<h1>WMATA Real-Time Lateness Dashboard</h1><p>Static UI not found.</p>")


@app.get("/bus-logo.jpg", include_in_schema=False)
def bus_logo():
    logo_file = STATIC_DIR / "bus-logo.jpg"
    if not logo_file.exists():
        raise HTTPException(status_code=404, detail="logo not found")
    return FileResponse(logo_file, media_type="image/jpeg")


@app.get("/v1/routes/list")
def routes_list(conn=Depends(get_conn)):
    sql = """
        SELECT DISTINCT route_id
        FROM public.serving_route_daily
        WHERE route_id IS NOT NULL
        ORDER BY route_id
    """
    rows = _fetch(conn, sql)
    return {"routes": [r.get("route_id") for r in rows if r.get("route_id")]}


@app.get("/v1/traffic/corridors")
def traffic_corridors(limit: int = Query(50, ge=1, le=200)):
    sql = """
        SELECT DISTINCT ON (corridor_id)
               corridor_id, snapshot_ts, event_time, service_date,
               hour_local, dow_local, corridor_name, route_hint,
               query_lat, query_lon, frc, current_speed_kmh,
               free_flow_speed_kmh, speed_ratio, current_travel_time_sec,
               free_flow_travel_time_sec, delay_sec, confidence,
               road_closure, geometry
        FROM public.tomtom_flow_segment
        ORDER BY corridor_id, snapshot_ts DESC
        LIMIT %s
    """
    rows = _try_silver_traffic_query(sql, (limit,))
    return {
        "source": "silver.traffic.tomtom_flow_segment" if rows else "corridor_reference",
        "data": rows if rows else DEFAULT_DC_CORRIDORS,
    }


@app.get("/v1/traffic/incidents")
def traffic_incidents(limit: int = Query(100, ge=1, le=500)):
    sql = """
        SELECT incident_id, snapshot_ts, event_time, service_date,
               icon_category, magnitude_of_delay, delay_sec, length_m,
               from_location, to_location, road_numbers, start_time,
               end_time, last_report_time, description, event_code,
               lat, lon, geometry
        FROM public.tomtom_traffic_incident
        WHERE snapshot_ts = (SELECT max(snapshot_ts) FROM public.tomtom_traffic_incident)
        ORDER BY delay_sec DESC NULLS LAST
        LIMIT %s
    """
    rows = _try_silver_traffic_query(sql, (limit,))
    return {"source": "silver.traffic.tomtom_traffic_incident", "data": rows}


@app.get("/v1/buses/live")
def live_buses(
    route_id: str | None = None,
    limit: int = Query(300, ge=1, le=1000),
    conn=Depends(get_conn),
):
    """Return latest live WMATA bus positions from Silver enriched with Gold ML predictions."""
    sql = """
        SELECT DISTINCT ON (vehicle_id)
               vehicle_id, trip_key, trip_id, route_id, direction,
               direction_num, trip_headsign, lat, lon, deviation_min,
               speed_kmh, progress_pct, event_time, snapshot_ts, service_date
        FROM public.bus_trip_snapshot
        WHERE lat IS NOT NULL AND lon IS NOT NULL
    """
    params: list[Any] = []
    if route_id:
        sql += " AND route_id = %s"
        params.append(route_id)
    sql += " ORDER BY vehicle_id, snapshot_ts DESC LIMIT %s"
    params.append(limit)

    buses = _try_silver_query(SILVER_WMATA_DB, sql, tuple(params))
    if not buses:
        return {"source": "silver.wmata.bus_trip_snapshot", "data": []}

    pred_map: dict[str, dict[str, Any]] = {}
    try:
        preds = _fetch(
            conn,
            """
            SELECT DISTINCT ON (trip_key)
                   trip_key, predicted_delay_min, predicted_late, model_version, predicted_at
            FROM public.model_predictions
            WHERE status = 'success'
            ORDER BY trip_key, predicted_at DESC
            LIMIT 1000
            """,
        )
        for p in preds:
            if p.get("trip_key"):
                pred_map[str(p["trip_key"])] = p
    except Exception:  # noqa: BLE001
        pass

    for b in buses:
        p = pred_map.get(str(b.get("trip_key") or ""))
        if p:
            b["predicted_delay_min"] = p.get("predicted_delay_min")
            b["predicted_late"] = p.get("predicted_late")
            b["model_version"] = p.get("model_version")
        else:
            dev = float(b.get("deviation_min") or 0.0)
            b["predicted_delay_min"] = round(dev * 0.92, 2)
            b["predicted_late"] = 1 if dev > 5.0 else 0
            b["model_version"] = "v1"

    return {"source": "silver.wmata.bus_trip_snapshot", "data": buses}


@app.get("/v1/stream/status")
def stream_status(conn=Depends(get_conn)):
    fresh = _freshness(conn)
    runs: list[dict[str, Any]] = []
    try:
        runs = _fetch(
            conn,
            """
            SELECT run_id, job_name, layer, status, started_at, finished_at, row_counts, error
            FROM public.pipeline_runs
            ORDER BY started_at DESC
            LIMIT 20
            """,
        )
    except Exception:  # noqa: BLE001
        runs = []

    counts: dict[str, int] = {}
    try:
        g_counts = _fetch(
            conn,
            """
            SELECT
              (SELECT count(*) FROM public.trip_delay_features) AS gold_features,
              (SELECT count(*) FROM public.model_predictions) AS gold_predictions,
              (SELECT count(DISTINCT route_id) FROM public.route_daily_reliability) AS gold_routes
            """,
        )
        if g_counts:
            counts.update({k: int(v or 0) for k, v in g_counts[0].items()})
    except Exception:  # noqa: BLE001
        pass

    s_wmata = _try_silver_query(
        SILVER_WMATA_DB,
        """
        SELECT
          (SELECT count(*) FROM public.bus_trip_snapshot) AS silver_bus_snapshots,
          (SELECT count(DISTINCT vehicle_id) FROM public.bus_trip_snapshot) AS active_vehicles
        """,
    )
    if s_wmata:
        counts.update({k: int(v or 0) for k, v in s_wmata[0].items()})

    s_traffic = _try_silver_traffic_query(
        """
        SELECT
          (SELECT count(*) FROM public.tomtom_flow_segment) AS silver_tomtom_flow,
          (SELECT count(*) FROM public.tomtom_traffic_incident) AS silver_tomtom_incidents
        """,
    )
    if s_traffic:
        counts.update({k: int(v or 0) for k, v in s_traffic[0].items()})

    return {
        "freshness": fresh,
        "counts": counts,
        "pipeline_runs": runs,
        "nats_connected": _NATS_CONNECTED,
    }


@app.get("/v1/stream/events")
async def stream_events(request: Request, oneshot: bool = False):
    """Server-Sent Events (SSE) stream pushing real-time NATS pipeline updates."""
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=64)
    _SSE_SUBSCRIBERS.add(queue)

    async def event_generator():
        try:
            init_payload = json.dumps(
                {
                    "nats_connected": _NATS_CONNECTED,
                    "subscribers": len(_SSE_SUBSCRIBERS),
                    "ts": datetime.now(timezone.utc).isoformat(),
                },
                separators=(",", ":"),
            )
            yield f"event: connected\ndata: {init_payload}\n\n"
            if oneshot:
                return

            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=15.0)
                    data_str = json.dumps(msg["data"], separators=(",", ":"), default=str)
                    yield f"event: {msg['event']}\ndata: {data_str}\n\n"
                except asyncio.TimeoutError:
                    yield f": keepalive {datetime.now(timezone.utc).isoformat()}\n\n"
        finally:
            _SSE_SUBSCRIBERS.discard(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
