"""Live WMATA 2026 BusPositions poller -> SeaweedFS Bronze (`s3://wmata/bronze_2026/bus_positions/`).

Polls `https://api.wmata.com/Bus.svc/json/jBusPositions` using `WMATA_API_KEY`,
normalizes timestamps from Eastern time (`America/New_York`) to UTC matching the
bronze contract (`dlt/ingest_wmata_2020.py` and `spark/jobs/bronze_to_silver.py`),
and uploads gzipped JSONL files to SeaweedFS.

SeaweedFS filer webhook automatically forwards object creation events to
`seaweedfs-bridge` -> NATS JetStream (`wmata.realtime.bronze_data`), where
`consumer` triggers Spark `bronze_to_silver.py` and `merge_silver.py`.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import os
import signal
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

sys.path.insert(0, "/app")

try:
    from governance import run_log
except ImportError:
    run_log = None  # type: ignore[assignment]

try:
    from shared.logging import setup_json_logging
    from shared.metrics import inc_counter, push_metric, track_job
except ImportError:
    setup_json_logging = None  # type: ignore[assignment]
    inc_counter = None  # type: ignore[assignment]
    push_metric = None  # type: ignore[assignment]
    track_job = None  # type: ignore[assignment]

if setup_json_logging is not None:
    setup_json_logging("wmata-poller")
else:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )

logger = logging.getLogger("wmata-poller")

EASTERN = ZoneInfo("America/New_York")
WMATA_BUS_POSITIONS_URL = "https://api.wmata.com/Bus.svc/json/jBusPositions"


def _to_utc(value: str | None, assume_tz: timezone | ZoneInfo = EASTERN) -> str | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=assume_tz)
        return dt.astimezone(timezone.utc).isoformat()
    except ValueError:
        return None


def _local_parts(value: str | None, fallback_utc: datetime) -> tuple[str, int, int]:
    if value:
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=EASTERN)
            dt_east = dt.astimezone(EASTERN)
            return dt_east.date().isoformat(), dt_east.hour, dt_east.weekday()
        except ValueError:
            pass
    dt_east = fallback_utc.astimezone(EASTERN)
    return dt_east.date().isoformat(), dt_east.hour, dt_east.weekday()


def fetch_bus_positions(api_key: str, timeout: int = 20, retries: int = 3) -> dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        req = urllib.request.Request(
            WMATA_BUS_POSITIONS_URL,
            headers={
                "api_key": api_key,
                "Accept": "application/json",
                "User-Agent": "wmata-live-poller/1.0",
            },
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last_exc = exc
            if exc.code in (401, 403, 429):
                raise
            if attempt < retries:
                time.sleep(2 * attempt)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < retries:
                time.sleep(2 * attempt)
    raise RuntimeError(f"WMATA jBusPositions GET failed after {retries} attempts") from last_exc


def normalize_bus_positions(
    payload: dict[str, Any],
    snapshot_dt: datetime,
    source_file: str = "",
) -> list[dict[str, Any]]:
    snap_utc = snapshot_dt.astimezone(timezone.utc)
    snap_iso = snap_utc.isoformat()
    records: list[dict[str, Any]] = []

    for bus in payload.get("BusPositions") or []:
        if not isinstance(bus, dict):
            continue
        event_local = bus.get("DateTime")
        event_utc_str = _to_utc(event_local) or snap_iso
        # Keep snapshot_ts aligned with observation window so bronze_to_silver's
        # |event_time - snapshot_ts| <= 1800s filter retains live observations
        # regardless of host clock drift.
        service_date, hour_local, dow_local = _local_parts(event_local, snap_utc)
        records.append(
            {
                "vehicle_id": str(bus.get("VehicleID")) if bus.get("VehicleID") is not None else None,
                "route_id": str(bus.get("RouteID")) if bus.get("RouteID") is not None else None,
                "trip_id": str(bus.get("TripID")) if bus.get("TripID") is not None else None,
                "direction_num": int(bus["DirectionNum"]) if bus.get("DirectionNum") is not None else None,
                "direction": bus.get("DirectionText"),
                "trip_headsign": bus.get("TripHeadsign"),
                "lat": float(bus["Lat"]) if bus.get("Lat") is not None else None,
                "lon": float(bus["Lon"]) if bus.get("Lon") is not None else None,
                "deviation_min": float(bus["Deviation"]) if bus.get("Deviation") is not None else 0.0,
                "trip_start": _to_utc(bus.get("TripStartTime")),
                "trip_end": _to_utc(bus.get("TripEndTime")),
                "block_number": str(bus.get("BlockNumber")) if bus.get("BlockNumber") is not None else None,
                "event_time": event_utc_str,
                "hour_local": hour_local,
                "dow_local": dow_local,
                "snapshot_ts": event_utc_str,
                "service_date": service_date,
                "source_file": source_file,
            }
        )
    return records


def encode_jsonl_gz(records: list[dict[str, Any]]) -> bytes:
    buf = "\n".join(json.dumps(r, separators=(",", ":")) for r in records) + "\n"
    return gzip.compress(buf.encode("utf-8"))


def _s3_client():
    import boto3
    from botocore.config import Config

    endpoint = os.environ.get("SEAWEEDFS_S3_ENDPOINT", "http://seaweedfs:8333")
    access = (
        os.environ.get("WMATA_KEY_ID")
        or os.environ.get("SEAWEEDFS_ACCESS_KEY")
        or "seaweedfs-access-key"
    )
    secret = (
        os.environ.get("WMATA_SECRET_KEY")
        or os.environ.get("SEAWEEDFS_SECRET_KEY")
        or "seaweedfs-secret-key"
    )
    region = os.environ.get("AWS_REGION", "us-east-1")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name=region,
        config=Config(s3={"addressing_style": "path"}),
    )


def upload_wmata_bronze(
    bucket: str,
    records: list[dict[str, Any]],
    snapshot_dt: datetime,
    s3_client: Any = None,
) -> tuple[str, int]:
    provisional = encode_jsonl_gz(records)
    digest = hashlib.sha256(provisional).hexdigest()[:10]
    service_date = records[0].get("service_date") or snapshot_dt.date().isoformat()
    ts_slug = snapshot_dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"bronze_2026/bus_positions/date={service_date}/bus_positions_{ts_slug}_{digest}.jsonl.gz"
    for rec in records:
        rec["source_file"] = key
    payload_bytes = encode_jsonl_gz(records)
    client = s3_client if s3_client is not None else _s3_client()
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=payload_bytes,
        ContentType="application/x-ndjson",
        ContentEncoding="gzip",
    )
    return key, len(payload_bytes)


def write_heartbeat(path: str, status: str, extra: dict[str, Any] | None = None) -> None:
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "status": status,
        }
        if extra:
            payload.update(extra)
        tmp = path + ".tmp"
        Path(tmp).write_text(json.dumps(payload), encoding="utf-8")
        os.replace(tmp, path)
    except OSError as exc:
        logger.warning("Heartbeat write failed: %s", exc)


def run_poll_cycle(
    api_key: str,
    bucket: str = "wmata",
    dry_run: bool = False,
    s3_client: Any = None,
) -> dict[str, Any]:
    snapshot_dt = datetime.now(timezone.utc)
    run_id = run_log.new_run_id() if run_log is not None else f"wmata-live-{int(time.time())}"
    if run_log is not None and not dry_run:
        run_log.safe_start_run(
            run_id,
            "wmata_live_poller",
            "bronze",
            input_datasets=["wmata.api.jBusPositions"],
            output_datasets=[f"s3://{bucket}/bronze_2026/bus_positions"],
            source_manifest={"snapshot_ts": snapshot_dt.isoformat()},
        )

    try:
        raw = fetch_bus_positions(api_key)
        records = normalize_bus_positions(raw, snapshot_dt)
        if not records:
            raise RuntimeError("WMATA jBusPositions returned 0 bus records")

        uploaded_key = ""
        byte_size = 0
        if not dry_run:
            uploaded_key, byte_size = upload_wmata_bronze(
                bucket, records, snapshot_dt, s3_client=s3_client
            )
            logger.info(
                "Uploaded live WMATA bus snapshot s3://%s/%s (%d buses, %d bytes)",
                bucket,
                uploaded_key,
                len(records),
                byte_size,
            )

        if push_metric is not None:
            push_metric(
                "wmata_stream_last_event_timestamp",
                time.time(),
                {"stream": "wmata-bronze", "source": "wmata-live"},
            )

        distinct_routes = len({r["route_id"] for r in records if r.get("route_id")})
        summary = {
            "status": "ok",
            "run_id": run_id,
            "snapshot_ts": snapshot_dt.isoformat(),
            "bus_positions": len(records),
            "distinct_routes": distinct_routes,
            "uploaded_key": uploaded_key,
            "bytes": byte_size,
        }
        if run_log is not None and not dry_run:
            run_log.safe_finish_run(
                run_id,
                "success",
                row_counts={"bus_positions": len(records), "distinct_routes": distinct_routes},
                output_datasets=[f"s3://{bucket}/{uploaded_key}"] if uploaded_key else None,
            )
        return summary
    except Exception as exc:
        if run_log is not None and not dry_run:
            run_log.safe_finish_run(run_id, "failed", error=str(exc))
        raise


def main() -> int:
    ap = argparse.ArgumentParser(description="Live WMATA BusPositions Poller -> SeaweedFS Bronze")
    ap.add_argument("--once", action="store_true", help="Run one poll cycle and exit")
    ap.add_argument("--dry-run", action="store_true", help="Fetch without writing to S3")
    ap.add_argument(
        "--interval",
        type=int,
        default=int(os.environ.get("WMATA_POLL_INTERVAL", "60")),
        help="Poll interval in seconds (default 60s)",
    )
    args = ap.parse_args()

    bucket = os.environ.get("SEAWEEDFS_BUCKET_WMATA_BRONZE", "wmata")
    heartbeat_path = os.environ.get("WMATA_HEARTBEAT", "/work/wmata_poller_heartbeat")

    stop = False

    def _handle_sig(signum: int, _frame: Any) -> None:
        nonlocal stop
        logger.info("Received signal %d, stopping WMATA live poller", signum)
        stop = True

    signal.signal(signal.SIGTERM, _handle_sig)
    signal.signal(signal.SIGINT, _handle_sig)

    write_heartbeat(heartbeat_path, "started")

    while not stop:
        api_key = os.environ.get("WMATA_API_KEY", "").strip()
        if not api_key:
            write_heartbeat(heartbeat_path, "waiting_for_api_key")
            logger.warning("WMATA_API_KEY is not set in .env; waiting...")
            if args.once:
                return 1
            for _ in range(max(1, args.interval)):
                if stop:
                    break
                time.sleep(1)
            continue

        try:
            if track_job is not None:
                with track_job("wmata-poller", "wmata_live_poll", domain="wmata", layer="bronze"):
                    result = run_poll_cycle(api_key=api_key, bucket=bucket, dry_run=args.dry_run)
            else:
                result = run_poll_cycle(api_key=api_key, bucket=bucket, dry_run=args.dry_run)
            write_heartbeat(heartbeat_path, result["status"], extra=result)
            logger.info("WMATA live poll summary: %s", json.dumps(result))
        except Exception as exc:  # noqa: BLE001
            logger.exception("WMATA live poll failed: %s", exc)
            write_heartbeat(heartbeat_path, "error", extra={"error": str(exc)})
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
