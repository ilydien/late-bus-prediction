from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import os
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
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
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

_INCIDENT_CATEGORIES: dict[int, str] = {
    0: "Incident",
    1: "Accident",
    2: "Fog",
    3: "Road Hazard",
    4: "Rain",
    5: "Ice",
    6: "Traffic Jam",
    7: "Lane Closed",
    8: "Road Closed",
    9: "Road Works",
    10: "Wind",
    11: "Flooding",
    14: "Disabled Vehicle",
}


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2.0) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2.0) ** 2
    )
    return 2.0 * r * math.asin(min(1.0, math.sqrt(max(0.0, a))))


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val is None:
            return default
        f = float(val)
        return default if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return default


def _parse_iso_dt(val: Any) -> datetime | None:
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    if not val:
        return None
    try:
        dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _diagnose_bus_cause(
    dev: float,
    progress: float | None,
    gps_speed: float | None,
    corridor_ratio: float | None,
    corridor_name: str | None,
    near_incident: bool,
    incident_cat: str | None,
    incident_road: str | None,
    incident_dist_m: int | None,
) -> tuple[str, str]:
    """Return (primary_cause_label, plain_language_reason) for a live bus."""
    if dev <= 2.0:
        if dev < -1.0:
            return ("Early / Fast", f"Running {abs(dev):.1f}m ahead of schedule")
        return ("On-Time", "Operating within normal schedule window")
    if dev <= 5.0:
        if near_incident and incident_cat:
            return ("Watch: Near Incident", f"Moderate delay near {incident_cat} ({incident_dist_m}m)")
        if corridor_ratio is not None and corridor_ratio < 0.72:
            return ("Watch: Dense Traffic", f"Slowed along {corridor_name or 'corridor'} ({int(corridor_ratio * 100)}% flow)")
        return ("Minor Delay", f"+{dev:.1f}m behind schedule (within 5m SLA)")

    # Late > 5m — determine primary root cause
    if near_incident and incident_cat:
        road_str = f" on {incident_road}" if incident_road else ""
        return (
            "Nearby Incident",
            f"Within {incident_dist_m or 0}m of active {incident_cat}{road_str}",
        )
    if (corridor_ratio is not None and corridor_ratio < 0.72) or (
        gps_speed is not None and gps_speed < 10.5
    ):
        if corridor_ratio is not None and corridor_ratio < 0.72:
            return (
                "Traffic Congestion",
                f"{corridor_name or 'Corridor'} flow at {int(corridor_ratio * 100)}% free-flow",
            )
        if gps_speed is not None and gps_speed < 1.5:
            return (
                "Traffic Congestion",
                f"Bus stalled / heavy stop dwell ({gps_speed:.1f} km/h live GPS speed)",
            )
        return (
            "Traffic Congestion",
            f"Slow live operating speed ({gps_speed:.1f} km/h)",
        )
    if progress is not None and progress >= 0.60:
        return (
            "Trip Cascading",
            f"Delay compounded by {int(progress * 100)}% trip progress (stop dwell / bunching)",
        )
    return (
        "Schedule / Dispatch",
        f"Already +{dev:.1f}m late early in trip ({int((progress or 0.25) * 100)}% progress)",
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
        SELECT p.input_record_id, p.model_version, p.predicted_at, p.trip_key, p.snapshot_ts,
               p.route_id, p.predicted_delay_min, p.actual_delay_min, p.abs_error,
               p.predicted_late, p.actual_late, p.inference_run_id,
               f.deviation_now, f.progress_pct, f.speed_kmh, f.am_speed_ratio
        FROM public.serving_latest_predictions p
        LEFT JOIN public.trip_delay_features f
          ON f.trip_key = p.trip_key AND f.snapshot_ts = p.snapshot_ts
    """
    params: list = []
    if route_id:
        sql += " WHERE p.route_id = %s"
        params.append(route_id)
    sql += " ORDER BY p.predicted_at DESC LIMIT %s"
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
@app.get("/live-map", response_class=HTMLResponse, include_in_schema=False)
@app.get("/root-cause", response_class=HTMLResponse, include_in_schema=False)
@app.get("/ml-forecast", response_class=HTMLResponse, include_in_schema=False)
@app.get("/pipeline", response_class=HTMLResponse, include_in_schema=False)
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


def _fetch_latest_corridors(limit: int = 50) -> list[dict[str, Any]]:
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
    return _try_silver_traffic_query(sql, (limit,))


def _fetch_latest_incidents(limit: int = 200) -> list[dict[str, Any]]:
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
    return _try_silver_traffic_query(sql, (limit,))


def _fetch_live_buses_with_gps_speed(
    route_id: str | None = None,
    limit: int = 600,
) -> list[dict[str, Any]]:
    """Fetch latest live WMATA bus per vehicle plus GPS-derived speed from its prior snapshot."""
    where_clause = "WHERE lat IS NOT NULL AND lon IS NOT NULL"
    params: list[Any] = []
    if route_id:
        where_clause += " AND route_id = %s"
        params.append(route_id)
    params.append(limit)

    sql = f"""
        WITH ranked AS (
            SELECT vehicle_id, trip_key, trip_id, route_id, direction,
                   direction_num, trip_headsign, lat, lon, deviation_min,
                   speed_kmh, progress_pct, event_time, snapshot_ts, service_date,
                   ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY snapshot_ts DESC) AS rn
            FROM public.bus_trip_snapshot
            {where_clause}
        )
        SELECT a.vehicle_id, a.trip_key, a.trip_id, a.route_id, a.direction,
               a.direction_num, a.trip_headsign, a.lat, a.lon, a.deviation_min,
               a.progress_pct, a.event_time, a.snapshot_ts, a.service_date,
               COALESCE(
                   a.speed_kmh,
                   CASE
                       WHEN b.lat IS NOT NULL
                        AND EXTRACT(EPOCH FROM (a.snapshot_ts - b.snapshot_ts)) BETWEEN 10 AND 900
                       THEN LEAST(
                           75.0,
                           (111.32 * sqrt(
                               power(a.lat - b.lat, 2)
                               + power((a.lon - b.lon) * cos(radians(a.lat)), 2)
                           )) / (EXTRACT(EPOCH FROM (a.snapshot_ts - b.snapshot_ts)) / 3600.0)
                       )
                       ELSE NULL
                   END
               ) AS speed_kmh
        FROM ranked a
        LEFT JOIN ranked b ON a.vehicle_id = b.vehicle_id AND b.rn = 2
        WHERE a.rn = 1
        ORDER BY a.snapshot_ts DESC, a.vehicle_id
        LIMIT %s
    """
    return _try_silver_query(SILVER_WMATA_DB, sql, tuple(params))


def _enrich_buses_with_traffic(
    buses: list[dict[str, Any]],
    corridors: list[dict[str, Any]],
    incidents: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Attach nearest TomTom corridor, nearest active incident, and diagnosed delay cause."""
    corridor_pts = [
        (
            _safe_float(c.get("query_lat")),
            _safe_float(c.get("query_lon")),
            c,
        )
        for c in corridors
        if c.get("query_lat") is not None and c.get("query_lon") is not None
    ]
    incident_pts = [
        (
            _safe_float(inc.get("lat")),
            _safe_float(inc.get("lon")),
            inc,
        )
        for inc in incidents
        if inc.get("lat") is not None and inc.get("lon") is not None
    ]

    for b in buses:
        lat = _safe_float(b.get("lat"))
        lon = _safe_float(b.get("lon"))
        dev = _safe_float(b.get("deviation_min"))
        prog = None if b.get("progress_pct") is None else _safe_float(b.get("progress_pct"))
        spd = None if b.get("speed_kmh") is None else round(_safe_float(b.get("speed_kmh")), 1)
        b["speed_kmh"] = spd

        best_c = None
        best_c_dist = 999.0
        for clat, clon, c in corridor_pts:
            d = _haversine_km(lat, lon, clat, clon)
            if d < best_c_dist:
                best_c_dist = d
                best_c = c

        corridor_ratio = None
        corridor_name = None
        corridor_delay_sec = None
        if best_c is not None and best_c_dist <= 4.5:
            corridor_ratio = _safe_float(best_c.get("speed_ratio"), 0.85)
            corridor_name = best_c.get("corridor_name") or best_c.get("corridor_id")
            corridor_delay_sec = int(_safe_float(best_c.get("delay_sec"), 0))
            b["corridor_name"] = corridor_name
            b["corridor_speed_ratio"] = round(corridor_ratio, 3)
            b["corridor_speed_kmh"] = round(_safe_float(best_c.get("current_speed_kmh"), 20.0), 1)
            b["corridor_delay_sec"] = corridor_delay_sec
            b["corridor_distance_km"] = round(best_c_dist, 2)
        else:
            b["corridor_name"] = None
            b["corridor_speed_ratio"] = None
            b["corridor_speed_kmh"] = None
            b["corridor_delay_sec"] = None

        best_inc = None
        best_inc_dist = 999.0
        for ilat, ilon, inc in incident_pts:
            d = _haversine_km(lat, lon, ilat, ilon)
            if d < best_inc_dist:
                best_inc_dist = d
                best_inc = inc

        near_incident = best_inc is not None and best_inc_dist <= 1.5
        incident_cat = None
        incident_road = None
        incident_dist_m = None
        if near_incident and best_inc is not None:
            incident_cat = _INCIDENT_CATEGORIES.get(int(_safe_float(best_inc.get("icon_category"), 0)), "Incident")
            incident_road = best_inc.get("road_numbers") or best_inc.get("from_location")
            incident_dist_m = int(round(best_inc_dist * 1000))
            b["near_incident"] = True
            b["incident_category"] = incident_cat
            b["incident_road"] = incident_road
            b["incident_distance_m"] = incident_dist_m
            b["incident_delay_sec"] = int(_safe_float(best_inc.get("delay_sec"), 0))
        else:
            b["near_incident"] = False
            b["incident_category"] = None
            b["incident_road"] = None
            b["incident_distance_m"] = None
            b["incident_delay_sec"] = None

        cause, detail = _diagnose_bus_cause(
            dev=dev,
            progress=prog,
            gps_speed=spd,
            corridor_ratio=corridor_ratio,
            corridor_name=corridor_name,
            near_incident=near_incident,
            incident_cat=incident_cat,
            incident_road=incident_road,
            incident_dist_m=incident_dist_m,
        )
        b["diagnosed_cause"] = cause
        b["cause_detail"] = detail

    return buses


@app.get("/v1/traffic/corridors")
def traffic_corridors(limit: int = Query(50, ge=1, le=200)):
    rows = _fetch_latest_corridors(limit=limit)
    return {
        "source": "silver.traffic.tomtom_flow_segment" if rows else "corridor_reference",
        "data": rows if rows else DEFAULT_DC_CORRIDORS,
    }


@app.get("/v1/traffic/incidents")
def traffic_incidents(limit: int = Query(100, ge=1, le=500)):
    rows = _fetch_latest_incidents(limit=limit)
    return {"source": "silver.traffic.tomtom_traffic_incident", "data": rows}


@app.get("/v1/buses/live")
def live_buses(
    route_id: str | None = None,
    limit: int = Query(300, ge=1, le=1000),
    conn=Depends(get_conn),
):
    """Return latest live WMATA bus positions enriched with GPS speed, TomTom proximity, ML predictions & cause."""
    buses = _fetch_live_buses_with_gps_speed(route_id=route_id, limit=limit)
    if not buses:
        return {"source": "silver.wmata.bus_trip_snapshot", "data": []}

    corridors = _fetch_latest_corridors(limit=50)
    incidents = _fetch_latest_incidents(limit=250)
    _enrich_buses_with_traffic(buses, corridors, incidents)

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
            dev = _safe_float(b.get("deviation_min"))
            b["predicted_delay_min"] = round(dev * 0.92, 2)
            b["predicted_late"] = 1 if dev > 5.0 else 0
            b["model_version"] = "v1"

    return {"source": "silver.wmata.bus_trip_snapshot", "data": buses}


@app.get("/v1/analytics/root-cause")
def analytics_root_cause(
    start_date: date,
    end_date: date,
    route_id: str | None = None,
    conn=Depends(get_conn),
):
    """Diagnose WHY buses are late by joining Gold features/marts with Silver GPS & TomTom proximity."""
    _validate_range(start_date, end_date)
    fresh = _freshness(conn)

    where_feat = "WHERE service_date BETWEEN %s AND %s AND deviation_now IS NOT NULL"
    feat_params: list[Any] = [start_date, end_date]
    if route_id:
        where_feat += " AND route_id = %s"
        feat_params.append(route_id)

    # 1. Delay status distribution (5 intuitive buckets)
    delay_distribution = [
        {"key": "early", "label": "Early (< -1m)", "count": 0, "pct": 0.0},
        {"key": "on_time", "label": "On-Time (-1 to +2m)", "count": 0, "pct": 0.0},
        {"key": "slight", "label": "Slightly Late (+2 to +5m)", "count": 0, "pct": 0.0},
        {"key": "late", "label": "Late (+5 to +10m)", "count": 0, "pct": 0.0},
        {"key": "severe", "label": "Severe (> +10m)", "count": 0, "pct": 0.0},
    ]
    try:
        dist_rows = _fetch(
            conn,
            f"""
            SELECT
              count(*) FILTER (WHERE deviation_now < -1) AS early,
              count(*) FILTER (WHERE deviation_now >= -1 AND deviation_now <= 2) AS on_time,
              count(*) FILTER (WHERE deviation_now > 2 AND deviation_now <= 5) AS slight,
              count(*) FILTER (WHERE deviation_now > 5 AND deviation_now <= 10) AS late,
              count(*) FILTER (WHERE deviation_now > 10) AS severe,
              count(*) AS total
            FROM public.trip_delay_features
            {where_feat}
            """,
            feat_params,
        )
        if dist_rows and dist_rows[0].get("total"):
            d0 = dist_rows[0]
            tot = max(1, int(d0.get("total") or 0))
            for item in delay_distribution:
                cnt = int(d0.get(item["key"]) or 0)
                item["count"] = cnt
                item["pct"] = round(100.0 * cnt / tot, 1)
    except Exception:  # noqa: BLE001
        pass

    # 2. Trip progress cascading curve (0-20% Start -> 80-100% End)
    stage_labels = [
        (0, "0–20% Start"),
        (1, "20–40% Early"),
        (2, "40–60% Mid"),
        (3, "60–80% Late"),
        (4, "80–100% End"),
    ]
    progress_curve: list[dict[str, Any]] = []
    try:
        prog_rows = _fetch(
            conn,
            f"""
            SELECT
              LEAST(4, GREATEST(0, floor(COALESCE(progress_pct, 0.5) * 5)::int)) AS bucket,
              count(*) AS samples,
              avg(deviation_now) AS avg_delay,
              100.0 * avg(CASE WHEN deviation_now > 5 THEN 1.0 ELSE 0.0 END) AS late_pct
            FROM public.trip_delay_features
            {where_feat}
            GROUP BY 1
            ORDER BY 1
            """,
            feat_params,
        )
        by_bucket = {int(r["bucket"]): r for r in prog_rows if r.get("bucket") is not None}
        for b_idx, lbl in stage_labels:
            row = by_bucket.get(b_idx)
            progress_curve.append(
                {
                    "stage": lbl,
                    "samples": int(row["samples"]) if row else 0,
                    "avg_delay": round(_safe_float(row["avg_delay"]), 2) if row else 0.0,
                    "late_pct": round(_safe_float(row["late_pct"]), 1) if row else 0.0,
                }
            )
    except Exception:  # noqa: BLE001
        progress_curve = [{"stage": lbl, "samples": 0, "avg_delay": 0.0, "late_pct": 0.0} for _, lbl in stage_labels]

    # 3. Real feature correlations against target +15m delay from Gold features
    feature_drivers: list[dict[str, Any]] = []
    try:
        corr_rows = _fetch(
            conn,
            f"""
            SELECT
              corr(deviation_now, target_deviation_15m) AS c_deviation_now,
              corr(progress_pct, target_deviation_15m) AS c_progress_pct,
              corr(am_speed_ratio, target_deviation_15m) AS c_am_speed_ratio,
              corr(speed_kmh, target_deviation_15m) AS c_speed_kmh,
              corr(time_since_start_min, target_deviation_15m) AS c_time_since_start,
              corr(hour_local::double precision, target_deviation_15m) AS c_hour_local,
              corr(median_headway_min, target_deviation_15m) AS c_headway
            FROM public.trip_delay_features
            {where_feat} AND target_deviation_15m IS NOT NULL
            """,
            feat_params,
        )
        if corr_rows:
            c0 = corr_rows[0]
            specs = [
                ("c_deviation_now", "Current Schedule Momentum", "deviation_now", "Existing delay carries forward strongly over the next 15 min"),
                ("c_progress_pct", "Trip Progress (Cascading)", "progress_pct", "Delays compound toward the middle and end of a trip"),
                ("c_time_since_start", "Elapsed Trip Time", "time_since_start_min", "Longer time in service increases stop-dwell exposure"),
                ("c_hour_local", "Time of Day (Peak Period)", "hour_local", "Evening rush hours show substantially higher delay rates"),
                ("c_am_speed_ratio", "Corridor Traffic Flow (TomTom)", "am_speed_ratio", "Lower free-flow speed ratio increases predicted delay"),
                ("c_speed_kmh", "Operating Speed (km/h)", "speed_kmh", "Slower fleet speed corresponds to heavier delay buildup"),
                ("c_headway", "Scheduled Headway (min)", "median_headway_min", "Longer headways amplify bunching and passenger load"),
            ]
            for col_key, label, raw_feat, desc in specs:
                val = c0.get(col_key)
                if val is None:
                    continue
                fval = _safe_float(val, 0.0)
                if abs(fval) < 0.001:
                    continue
                feature_drivers.append(
                    {
                        "feature": raw_feat,
                        "label": label,
                        "correlation": round(fval, 3),
                        "impact_pct": round(min(100.0, abs(fval) * 100.0), 1),
                        "direction": "increases_delay" if fval >= 0 else "reduces_delay",
                        "description": desc,
                    }
                )
            feature_drivers.sort(key=lambda x: abs(x["correlation"]), reverse=True)
    except Exception:  # noqa: BLE001
        pass

    # 4. Gold daily route metrics & per-route progress features
    route_daily_map: dict[str, dict[str, Any]] = {}
    try:
        r_rows = _fetch(
            conn,
            f"""
            SELECT route_id,
                   sum(trips) AS trips,
                   avg(avg_deviation) AS avg_delay,
                   avg(median_deviation) AS median_delay,
                   avg(p90_deviation) AS p90_delay,
                   avg(on_time_pct) AS on_time_pct,
                   avg(late_pct) AS late_pct,
                   avg(avg_speed_kmh) AS mart_speed_kmh
            FROM public.serving_route_daily
            WHERE service_date BETWEEN %s AND %s
            {"AND route_id = %s" if route_id else ""}
            GROUP BY route_id
            """,
            feat_params,
        )
        for r in r_rows:
            if r.get("route_id"):
                route_daily_map[str(r["route_id"])] = r
    except Exception:  # noqa: BLE001
        pass

    route_feat_map: dict[str, dict[str, Any]] = {}
    try:
        rf_rows = _fetch(
            conn,
            f"""
            SELECT route_id,
                   avg(progress_pct) FILTER (WHERE deviation_now > 5) AS avg_progress_late,
                   avg(progress_pct) AS avg_progress
            FROM public.trip_delay_features
            {where_feat}
            GROUP BY route_id
            """,
            feat_params,
        )
        for r in rf_rows:
            if r.get("route_id"):
                route_feat_map[str(r["route_id"])] = r
    except Exception:  # noqa: BLE001
        pass

    # 5. Live Silver spatial enrichment (GPS speed + TomTom corridor & incident proximity)
    corridors = _fetch_latest_corridors(limit=50)
    incidents = _fetch_latest_incidents(limit=250)
    live_bus_rows = _fetch_live_buses_with_gps_speed(route_id=route_id, limit=800)
    _enrich_buses_with_traffic(live_bus_rows, corridors, incidents)

    # Spatial incident category breakdown
    inc_cat_map: dict[str, dict[str, Any]] = {}
    for inc in incidents:
        cat = _INCIDENT_CATEGORIES.get(int(_safe_float(inc.get("icon_category"), 0)), "Incident")
        slot = inc_cat_map.setdefault(cat, {"category": cat, "count": 0, "delay_sum_sec": 0.0, "length_m": 0.0})
        slot["count"] += 1
        slot["delay_sum_sec"] += _safe_float(inc.get("delay_sec"), 0.0)
        slot["length_m"] += _safe_float(inc.get("length_m"), 0.0)
    incident_breakdown = sorted(
        [
            {
                "category": v["category"],
                "count": v["count"],
                "avg_delay_min": round((v["delay_sum_sec"] / max(1, v["count"])) / 60.0, 1),
                "total_length_km": round(v["length_m"] / 1000.0, 1),
            }
            for v in inc_cat_map.values()
        ],
        key=lambda x: x["count"],
        reverse=True,
    )

    # Driver attribution across late live buses (deviation > 5m)
    cause_meta = {
        "Nearby Incident": "Bus is within 1.5 km of an active TomTom traffic incident (jam, closure, or roadworks)",
        "Traffic Congestion": "Nearest DC corridor is below 72% free-flow speed or bus GPS speed dropped below 10.5 km/h",
        "Trip Cascading": "Delay accumulated past 60% of the trip via stop dwell time and schedule bunching",
        "Schedule / Dispatch": "Already >5m behind in the first 60% of the trip despite normal nearby traffic flow",
    }
    cause_stats: dict[str, dict[str, Any]] = {
        k: {"cause": k, "count": 0, "delay_sum": 0.0, "description": desc}
        for k, desc in cause_meta.items()
    }
    congestion_buckets = {
        "free_flow": {"tier": "Free-Flow (≥ 78%)", "buses": 0, "delay_sum": 0.0, "late_count": 0},
        "moderate": {"tier": "Moderate Flow (65–77%)", "buses": 0, "delay_sum": 0.0, "late_count": 0},
        "congested": {"tier": "Congested (< 65%)", "buses": 0, "delay_sum": 0.0, "late_count": 0},
    }

    route_live_agg: dict[str, dict[str, Any]] = {}
    total_late_live = 0
    late_near_incidents = 0

    for b in live_bus_rows:
        dev = _safe_float(b.get("deviation_min"))
        rt = str(b.get("route_id") or "")
        spd = b.get("speed_kmh")
        c_ratio = b.get("corridor_speed_ratio")
        near_inc = bool(b.get("near_incident"))

        # Congestion tier stats
        if c_ratio is not None:
            key = "free_flow" if c_ratio >= 0.78 else ("moderate" if c_ratio >= 0.65 else "congested")
            cb = congestion_buckets[key]
            cb["buses"] += 1
            cb["delay_sum"] += dev
            if dev > 5.0:
                cb["late_count"] += 1

        if rt:
            ra = route_live_agg.setdefault(
                rt,
                {
                    "live_buses": 0,
                    "late_buses": 0,
                    "gps_speeds": [],
                    "corridor_ratios": [],
                    "corridor_names": [],
                    "near_inc_buses": 0,
                    "near_inc_late": 0,
                    "cause_counts": {},
                    "cause_reasons": {},
                },
            )
            ra["live_buses"] += 1
            if spd is not None:
                ra["gps_speeds"].append(float(spd))
            if c_ratio is not None:
                ra["corridor_ratios"].append(float(c_ratio))
            if b.get("corridor_name"):
                ra["corridor_names"].append(str(b["corridor_name"]))
            if near_inc:
                ra["near_inc_buses"] += 1
            if dev > 5.0:
                ra["late_buses"] += 1
                if near_inc:
                    ra["near_inc_late"] += 1
                cause = str(b.get("diagnosed_cause") or "Trip Cascading")
                ra["cause_counts"][cause] = ra["cause_counts"].get(cause, 0) + 1
                if cause not in ra["cause_reasons"] and b.get("cause_detail"):
                    ra["cause_reasons"][cause] = str(b.get("cause_detail"))

        if dev > 5.0:
            total_late_live += 1
            if near_inc:
                late_near_incidents += 1
            cause = str(b.get("diagnosed_cause") or "Trip Cascading")
            if cause in cause_stats:
                cause_stats[cause]["count"] += 1
                cause_stats[cause]["delay_sum"] += dev

    driver_attribution = []
    for v in cause_stats.values():
        cnt = v["count"]
        driver_attribution.append(
            {
                "cause": v["cause"],
                "count": cnt,
                "share_pct": round(100.0 * cnt / max(1, total_late_live), 1) if total_late_live else 0.0,
                "avg_delay": round(v["delay_sum"] / max(1, cnt), 2) if cnt else 0.0,
                "description": v["description"],
            }
        )
    driver_attribution.sort(key=lambda x: x["count"], reverse=True)

    congestion_tiers = [
        {
            "tier": v["tier"],
            "buses": v["buses"],
            "avg_delay": round(v["delay_sum"] / max(1, v["buses"]), 2) if v["buses"] else 0.0,
            "late_pct": round(100.0 * v["late_count"] / max(1, v["buses"]), 1) if v["buses"] else 0.0,
        }
        for v in congestion_buckets.values()
    ]

    # 6. Per-route diagnostic breakdown (combining Gold metrics + live GPS speed + spatial causes)
    route_diagnostics: list[dict[str, Any]] = []
    for rt, dm in route_daily_map.items():
        avg_delay = round(_safe_float(dm.get("avg_delay")), 2)
        p90_delay = round(_safe_float(dm.get("p90_delay"), avg_delay), 2)
        on_time_raw = _safe_float(dm.get("on_time_pct"), 0.0)
        on_time_pct = round(on_time_raw * 100.0 if 0 < on_time_raw <= 1.0 else on_time_raw, 1)
        trips = int(_safe_float(dm.get("trips"), 0))

        rf = route_feat_map.get(rt, {})
        avg_prog_late = rf.get("avg_progress_late")
        avg_prog_late_f = None if avg_prog_late is None else round(_safe_float(avg_prog_late), 2)

        rl = route_live_agg.get(rt, {})
        gps_list = rl.get("gps_speeds") or []
        c_ratios = rl.get("corridor_ratios") or []
        c_names = rl.get("corridor_names") or []

        avg_gps_spd = round(sum(gps_list) / len(gps_list), 1) if gps_list else round(_safe_float(dm.get("mart_speed_kmh"), 18.5), 1)
        avg_c_ratio = round(sum(c_ratios) / len(c_ratios), 3) if c_ratios else None
        top_corridor = max(set(c_names), key=c_names.count) if c_names else None
        near_inc_cnt = int(rl.get("near_inc_buses") or 0)
        cause_counts: dict[str, int] = rl.get("cause_counts") or {}

        if avg_delay <= 2.5 and on_time_pct >= 80.0:
            primary_cause = "Healthy"
            cause_detail = f"Meeting 80% SLA ({on_time_pct:.0f}% on-time, avg {avg_gps_spd:.1f} km/h)"
        elif cause_counts:
            primary_cause = max(cause_counts.items(), key=lambda kv: kv[1])[0]
            cause_detail = (rl.get("cause_reasons") or {}).get(primary_cause) or cause_meta.get(primary_cause, "")
        elif avg_c_ratio is not None and avg_c_ratio < 0.72:
            primary_cause = "Traffic Congestion"
            cause_detail = f"{top_corridor or 'Corridor'} flow at {int(avg_c_ratio * 100)}% free-flow"
        elif avg_prog_late_f is not None and avg_prog_late_f >= 0.60:
            primary_cause = "Trip Cascading"
            cause_detail = f"Late snapshots concentrate at {int(avg_prog_late_f * 100)}% trip progress"
        elif avg_delay > 5.0 or on_time_pct < 70.0:
            primary_cause = "Schedule / Dispatch"
            cause_detail = f"Persistent delay (P90 {p90_delay:.1f}m) despite normal corridor speeds"
        else:
            primary_cause = "Minor Variance"
            cause_detail = f"Moderate delay (+{avg_delay:.1f}m avg, {on_time_pct:.0f}% on-time)"

        route_diagnostics.append(
            {
                "route_id": rt,
                "trips": trips,
                "avg_delay": avg_delay,
                "p90_delay": p90_delay,
                "on_time_pct": on_time_pct,
                "avg_speed_kmh": avg_gps_spd,
                "speed_source": "live_gps" if gps_list else "mart_avg",
                "corridor_name": top_corridor,
                "corridor_speed_ratio": avg_c_ratio,
                "avg_progress_late": avg_prog_late_f,
                "nearby_incidents": near_inc_cnt,
                "live_buses": int(rl.get("live_buses") or 0),
                "primary_cause": primary_cause,
                "cause_detail": cause_detail,
            }
        )
    route_diagnostics.sort(key=lambda x: x["avg_delay"], reverse=True)

    # 7. Executive summary object for Page 1 smart banner & spotlight cards
    avg_on_time = (
        round(sum(r["on_time_pct"] for r in route_diagnostics) / len(route_diagnostics), 1)
        if route_diagnostics
        else 0.0
    )
    avg_flow_ratio = (
        round(
            100.0 * sum(_safe_float(c.get("speed_ratio"), 0.8) for c in corridors) / len(corridors),
            1,
        )
        if corridors
        else 0.0
    )
    worst_corridor = (
        min(corridors, key=lambda c: _safe_float(c.get("speed_ratio"), 1.0)) if corridors else None
    )
    top_incident = incidents[0] if incidents else None
    dominant_driver = driver_attribution[0] if driver_attribution and driver_attribution[0]["count"] > 0 else None

    start_stage = progress_curve[0] if len(progress_curve) >= 1 else {"avg_delay": 0.0, "late_pct": 0.0}
    end_stage = progress_curve[-1] if len(progress_curve) >= 5 else {"avg_delay": 0.0, "late_pct": 0.0}

    executive_summary = {
        "on_time_pct": avg_on_time,
        "sla_target_pct": 80.0,
        "sla_gap_pct": round(avg_on_time - 80.0, 1),
        "dominant_cause": dominant_driver["cause"] if dominant_driver else "Trip Cascading",
        "dominant_cause_pct": dominant_driver["share_pct"] if dominant_driver else 0.0,
        "total_late_live_buses": total_late_live,
        "late_buses_near_incidents": late_near_incidents,
        "active_incidents": len(incidents),
        "avg_flow_pct": avg_flow_ratio,
        "start_stage_late_pct": start_stage["late_pct"],
        "end_stage_late_pct": end_stage["late_pct"],
        "start_stage_avg_delay": start_stage["avg_delay"],
        "end_stage_avg_delay": end_stage["avg_delay"],
        "worst_route": route_diagnostics[0] if route_diagnostics else None,
        "worst_corridor": {
            "corridor_name": worst_corridor.get("corridor_name"),
            "speed_ratio_pct": round(_safe_float(worst_corridor.get("speed_ratio"), 0.0) * 100.0, 0),
            "current_speed_kmh": round(_safe_float(worst_corridor.get("current_speed_kmh"), 0.0), 1),
            "delay_sec": int(_safe_float(worst_corridor.get("delay_sec"), 0)),
            "query_lat": worst_corridor.get("query_lat"),
            "query_lon": worst_corridor.get("query_lon"),
        }
        if worst_corridor
        else None,
        "top_incident": {
            "category": _INCIDENT_CATEGORIES.get(int(_safe_float(top_incident.get("icon_category"), 0)), "Incident"),
            "road": top_incident.get("road_numbers") or top_incident.get("from_location") or "DC Roadway",
            "from_location": top_incident.get("from_location"),
            "delay_min": round(_safe_float(top_incident.get("delay_sec"), 0.0) / 60.0, 1),
            "length_m": int(_safe_float(top_incident.get("length_m"), 0)),
            "lat": top_incident.get("lat"),
            "lon": top_incident.get("lon"),
        }
        if top_incident
        else None,
    }

    return {
        "executive_summary": executive_summary,
        "delay_distribution": delay_distribution,
        "progress_curve": progress_curve,
        "congestion_tiers": congestion_tiers,
        "driver_attribution": driver_attribution,
        "incident_breakdown": incident_breakdown,
        "feature_drivers": feature_drivers,
        "route_diagnostics": route_diagnostics,
        "freshness": fresh,
    }


@app.get("/v1/stream/status")
def stream_status(conn=Depends(get_conn)):
    fresh = _freshness(conn)
    now_utc = datetime.now(timezone.utc)
    runs: list[dict[str, Any]] = []
    try:
        runs = _fetch(
            conn,
            """
            SELECT run_id, job_name, layer, status, started_at, finished_at, row_counts, error
            FROM public.pipeline_runs
            ORDER BY started_at DESC
            LIMIT 30
            """,
        )
        for r in runs:
            st_dt = _parse_iso_dt(r.get("started_at"))
            fn_dt = _parse_iso_dt(r.get("finished_at"))
            if st_dt and fn_dt:
                r["duration_sec"] = round(max(0.0, (fn_dt - st_dt).total_seconds()), 1)
            else:
                r["duration_sec"] = None
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
          (SELECT count(*) FROM public.tomtom_traffic_incident) AS silver_tomtom_incidents,
          (SELECT max(snapshot_ts) FROM public.tomtom_flow_segment) AS tomtom_max_ts
        """,
    )
    tomtom_max_ts = None
    if s_traffic:
        row0 = s_traffic[0]
        counts["silver_tomtom_flow"] = int(row0.get("silver_tomtom_flow") or 0)
        counts["silver_tomtom_incidents"] = int(row0.get("silver_tomtom_incidents") or 0)
        if row0.get("tomtom_max_ts") is not None:
            tomtom_max_ts = str(row0["tomtom_max_ts"])

    snap_dt = _parse_iso_dt(fresh.get("max_snapshot_ts"))
    pred_dt = _parse_iso_dt(fresh.get("max_predicted_at"))
    tomtom_dt = _parse_iso_dt(tomtom_max_ts)

    telemetry = {
        "snapshot_age_sec": int(max(0.0, (now_utc - snap_dt).total_seconds())) if snap_dt else None,
        "prediction_age_sec": int(max(0.0, (now_utc - pred_dt).total_seconds())) if pred_dt else None,
        "tomtom_age_sec": int(max(0.0, (now_utc - tomtom_dt).total_seconds())) if tomtom_dt else None,
        "tomtom_max_ts": tomtom_max_ts,
        "sse_subscribers": len(_SSE_SUBSCRIBERS),
        "server_time_utc": now_utc.isoformat(),
    }

    return {
        "freshness": fresh,
        "counts": counts,
        "pipeline_runs": runs,
        "telemetry": telemetry,
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
