"""NATS JetStream -> Spark -> silver, as a long-running streaming consumer.

For each micro-batch:
  1. collect bronze object events from durable pull subscriptions;
  2. validate JSON, route delete events to tombstone stream, enforce event-time
     watermark + allowed lateness policy, and route invalid/missing objects to DLQ;
  3. truncate the silver staging tables;
  4. run the Spark bronze -> silver job (writes staging only);
  5. upsert staging into the real silver tables;
  6. update TTL-pruned dedup state + event-time watermarks, ack NATS messages,
     publish persistent JetStream `realtime.silver.merged`, and emit metrics.

A message is acked only after steps 4 and 5 succeed. On batch failure:
  - items reaching CONSUMER_MAX_DELIVER are routed to `wmata-dlq` and acked
    (preventing infinite poison-pill retry loops);
  - remaining items are nacked with backoff delay for retry.
"""

from __future__ import annotations

import asyncio
import contextlib
import glob
import hashlib
import importlib
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

for _cand in (*(str(p) for p in Path(__file__).resolve().parents), "/app"):
    if _cand not in sys.path:
        sys.path.insert(0, _cand)

try:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
except ImportError:  # pragma: no cover - allows pure helper tests without boto3
    boto3 = None  # type: ignore[assignment]
    Config = None  # type: ignore[assignment]
    ClientError = Exception  # type: ignore[assignment,misc]

try:
    import nats
    from nats.errors import TimeoutError as NatsTimeout
    from nats.js.api import RetentionPolicy, StorageType, StreamConfig
except ImportError:  # pragma: no cover - allows pure helper tests without nats-py
    nats = None  # type: ignore[assignment]
    NatsTimeout = TimeoutError  # type: ignore[assignment,misc]
    RetentionPolicy = None  # type: ignore[assignment]
    StorageType = None  # type: ignore[assignment]
    StreamConfig = None  # type: ignore[assignment]

try:
    from governance import run_log
except ImportError:  # pragma: no cover
    run_log = None  # type: ignore[assignment]

try:
    from shared.logging import run_context, setup_json_logging
    from shared import metrics as vm_metrics
except ImportError:  # pragma: no cover
    run_context = None  # type: ignore[assignment]
    setup_json_logging = None  # type: ignore[assignment]
    vm_metrics = None  # type: ignore[assignment]

_LOG_LEVEL = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
if setup_json_logging is not None:
    setup_json_logging("bronze-consumer", level=_LOG_LEVEL)
else:
    logging.basicConfig(
        level=_LOG_LEVEL,
        format="%(asctime)s %(levelname)s %(message)s",
    )
logger = logging.getLogger("bronze-consumer")


def _run_ctx(run_id: str):
    return run_context(run_id=run_id) if run_context is not None else contextlib.nullcontext(run_id)

try:
    import merge_silver as silver_merger
except ImportError:  # pragma: no cover - imported as consumer.bronze_consumer in tests
    try:
        from consumer import merge_silver as silver_merger  # type: ignore[no-redef]
    except ImportError:
        silver_merger = None  # type: ignore[assignment]

NATS_URL = os.environ.get("NATS_URL", "nats://nats:4222")
SPARK_HOME = os.environ.get("SPARK_HOME", "/opt/spark")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
SPARK_JOB = os.environ.get("SPARK_JOB", "/opt/spark/jobs/bronze_to_silver.py")
SPARK_APP_NAME = os.environ.get("SPARK_APP_NAME", "wmata-silver-consumer")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "1g")
SPARK_SHUFFLE_PARTITIONS = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8")
SPARK_CORES_MAX = os.environ.get("SPARK_CORES_MAX", "").strip()
SPARK_MODE = os.environ.get("CONSUMER_SPARK_MODE", "persistent").strip().lower()
if SPARK_MODE not in ("persistent", "submit"):
    SPARK_MODE = "persistent"
SPARK_WARMUP = os.environ.get("CONSUMER_SPARK_WARMUP", "true").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
MERGE_SCRIPT = os.environ.get("SILVER_MERGE", "/app/merge_silver.py")
STATE_PATH = os.environ.get("CONSUMER_STATE", "/work/consumer_state.json")
MANIFEST = os.environ.get("BRONZE_MANIFEST", "/work/bronze_batch.json")
HEARTBEAT_PATH = os.environ.get("CONSUMER_HEARTBEAT", "/work/consumer_heartbeat")

S3_ENDPOINT = os.environ.get("SEAWEEDFS_S3_ENDPOINT", "http://seaweedfs:8333")
ACCESS = os.environ.get("SEAWEEDFS_ACCESS_KEY", "seaweedfs-access-key")
SECRET = os.environ.get("SEAWEEDFS_SECRET_KEY", "seaweedfs-secret-key")
REGION = os.environ.get("AWS_REGION", "us-east-1")

POLL_INTERVAL = float(os.environ.get("CONSUMER_POLL_INTERVAL", "5"))
FETCH_TIMEOUT = float(os.environ.get("CONSUMER_FETCH_TIMEOUT", "2"))
ERROR_BACKOFF = float(os.environ.get("CONSUMER_ERROR_BACKOFF", "30"))
BATCH_SIZE = int(os.environ.get("CONSUMER_BATCH_SIZE", "200"))

MAX_DELIVER = int(os.environ.get("CONSUMER_MAX_DELIVER", "3"))
ALLOWED_LATENESS_MIN = int(os.environ.get("CONSUMER_ALLOWED_LATENESS_MIN", "20"))
DEDUP_TTL_HOURS = float(os.environ.get("CONSUMER_DEDUP_TTL_HOURS", "72"))
DEDUP_MAX_KEYS = int(os.environ.get("CONSUMER_DEDUP_MAX_KEYS", "50000"))

DLQ_STREAM = os.environ.get("DLQ_STREAM", "wmata-dlq")
REALTIME_EVENTS_STREAM = os.environ.get("REALTIME_EVENTS_STREAM", "realtime-events")
SILVER_MERGED_SUBJECT = os.environ.get("SILVER_MERGED_SUBJECT", "realtime.silver.merged")

WMATA_PREFIXES = tuple(
    p.strip()
    for p in os.environ.get("WMATA_BRONZE_PREFIXES", "bronze_").split(",")
    if p.strip()
)
TRAFFIC_PREFIX = os.environ.get("TRAFFIC_BRONZE_PREFIX", "ddot/")
TOMTOM_PREFIX = os.environ.get("TOMTOM_BRONZE_PREFIX", "tomtom/")

S3 = (
    boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS,
        aws_secret_access_key=SECRET,
        region_name=REGION,
        config=Config(s3={"addressing_style": "path"}),
    )
    if boto3 is not None
    else None
)

SUBS = [
    ("wmata", "wmata.realtime.bronze_data", "wmata-bronze", "silver-wmata"),
    ("traffic", "traffic.realtime.bronze_data", "traffic-bronze", "silver-traffic"),
]


# ---------------------------------------------------------------------------
# Pure streaming contract, watermark, DLQ, tombstone & dedup state helpers
# ---------------------------------------------------------------------------
def wanted(bucket: str, key: str) -> bool:
    if bucket == "wmata":
        return (
            key.endswith(".jsonl.gz")
            and "/bus_positions/" in key
            and key.startswith(WMATA_PREFIXES)
        )
    if bucket == "traffic":
        if key.startswith(TRAFFIC_PREFIX) and key.endswith(".geojson"):
            return True
        if (
            key.startswith(TOMTOM_PREFIX)
            and key.endswith(".jsonl.gz")
            and ("/flow/" in key or "/incidents/" in key)
        ):
            return True
        return False
    return False


def s3_key_of(bucket: str, event: dict) -> str | None:
    if event.get("s3_key"):
        return event["s3_key"]
    raw = event.get("key") or ""
    prefix = f"/buckets/{bucket}/"
    if raw.startswith(prefix):
        return raw[len(prefix):]
    return None


def event_etag(event: dict) -> str:
    etag = event.get("etag")
    if etag:
        return str(etag)
    message = event.get("message") or event.get("message_data") or {}
    for side in ("new_entry", "old_entry"):
        entry = message.get(side) or {}
        attrs = entry.get("attributes") or {}
        for name in ("md5", "e_tag", "etag"):
            if attrs.get(name):
                return str(attrs[name])
    return ""


def dedup_key(bucket: str, key: str, event: dict) -> str:
    return f"{bucket}|{key}|{event_etag(event)}"


def parse_iso_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def extract_event_time(event: dict) -> datetime | None:
    """Extract canonical event-time from bridge envelope or SeaweedFS metadata."""
    for field in ("event_time", "ingest_time"):
        parsed = parse_iso_timestamp(event.get(field))
        if parsed is not None:
            return parsed
    message = event.get("message") or event.get("message_data") or {}
    for side in ("new_entry", "old_entry"):
        entry = message.get(side) or {}
        attrs = entry.get("attributes") or {}
        mtime = attrs.get("mtime")
        if mtime is not None:
            try:
                return datetime.fromtimestamp(float(mtime), tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                pass
    return None


def is_delete_event(event: dict) -> bool:
    ev_type = str(event.get("event_type") or "").strip().lower()
    if ev_type in ("delete", "remove", "deleted"):
        return True
    message = event.get("message") or event.get("message_data") or {}
    if isinstance(message, dict) and message.get("old_entry") and not message.get("new_entry"):
        return True
    return False


def is_live_stream_event(bucket: str, key: str, event: dict) -> bool:
    """Return True if event belongs to live continuous stream subject to watermark policy."""
    source = str(event.get("source") or "").strip().lower()
    if source in ("backfill", "replay"):
        return False
    if bucket == "wmata":
        # Historical 2020 archive files bypass live event-time watermark rejection
        return not key.startswith("bronze_2020/")
    if bucket == "traffic":
        return key.startswith(TOMTOM_PREFIX)
    return False


def is_late_event(
    event_ts: datetime | None,
    watermark_ts: datetime | None,
    allowed_lateness_min: int = ALLOWED_LATENESS_MIN,
) -> bool:
    """Check whether event_ts falls behind watermark_ts - allowed_lateness_min."""
    if event_ts is None or watermark_ts is None or allowed_lateness_min <= 0:
        return False
    cutoff = watermark_ts - timedelta(minutes=allowed_lateness_min)
    return event_ts < cutoff


def get_delivery_count(msg: Any, fallback_count: int = 1) -> int:
    try:
        meta = getattr(msg, "metadata", None)
        if meta is not None and getattr(meta, "num_delivered", None):
            return max(1, int(meta.num_delivered))
    except Exception:  # noqa: BLE001
        pass
    return max(1, int(fallback_count))


def build_dlq_payload(
    bucket: str,
    reason: str,
    raw_data: bytes | str | None = None,
    event: dict | None = None,
    delivery_count: int = 1,
    error: str | None = None,
    s3_key: str | None = None,
) -> dict[str, Any]:
    now_iso = datetime.now(timezone.utc).isoformat()
    raw_text = None
    if raw_data is not None:
        if isinstance(raw_data, (bytes, bytearray)):
            raw_text = raw_data.decode("utf-8", errors="replace")[:4000]
        else:
            raw_text = str(raw_data)[:4000]

    ev = event if isinstance(event, dict) else {}
    resolved_key = s3_key or s3_key_of(bucket, ev)
    return {
        "dlq_ts": now_iso,
        "bucket": bucket,
        "original_subject": f"{bucket}.realtime.bronze_data",
        "reason": reason,
        "delivery_count": int(delivery_count),
        "error": error,
        "s3_key": resolved_key,
        "etag": event_etag(ev) if ev else "",
        "event_id": ev.get("event_id"),
        "event_time": ev.get("event_time"),
        "original_event": ev if ev else None,
        "raw_payload": raw_text if not ev else None,
    }


def build_tombstone_payload(bucket: str, key: str, event: dict) -> dict[str, Any]:
    now_iso = datetime.now(timezone.utc).isoformat()
    ev_ts = extract_event_time(event)
    return {
        "tombstone_ts": now_iso,
        "bucket": bucket,
        "s3_key": key,
        "etag": event_etag(event),
        "event_id": event.get("event_id"),
        "event_type": str(event.get("event_type") or "delete"),
        "event_time": ev_ts.isoformat() if ev_ts else now_iso,
        "source": str(event.get("source") or "seaweedfs"),
    }


def prune_dedup_keys(
    keys_map: dict[str, float],
    now_ts: float | None = None,
    ttl_hours: float = DEDUP_TTL_HOURS,
    max_keys: int = DEDUP_MAX_KEYS,
) -> dict[str, float]:
    """Prune expired dedup keys by TTL and enforce an upper bound on memory/state size."""
    ref_now = time.time() if now_ts is None else float(now_ts)
    cutoff = ref_now - max(1.0, float(ttl_hours)) * 3600.0
    kept = {k: float(ts) for k, ts in keys_map.items() if float(ts) >= cutoff}
    if len(kept) > max_keys:
        # Keep the most recently seen keys
        newest = sorted(kept.items(), key=lambda kv: kv[1], reverse=True)[:max_keys]
        kept = dict(newest)
    return kept


def load_consumer_state(path: str = STATE_PATH) -> dict[str, Any]:
    """Load consumer dedup map + per-bucket event-time watermarks (migrating legacy list format)."""
    now_ts = time.time()
    try:
        with open(path, encoding="utf-8") as fh:
            raw = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return {"keys": {}, "watermarks": {}}

    raw_keys = raw.get("keys", {})
    if isinstance(raw_keys, list):
        keys_map = {str(k): now_ts for k in raw_keys}
    elif isinstance(raw_keys, dict):
        keys_map = {str(k): float(v) for k, v in raw_keys.items()}
    else:
        keys_map = {}

    watermarks_raw = raw.get("watermarks") or {}
    watermarks = (
        {str(b): str(ts) for b, ts in watermarks_raw.items() if ts}
        if isinstance(watermarks_raw, dict)
        else {}
    )
    return {
        "keys": prune_dedup_keys(keys_map, now_ts=now_ts),
        "watermarks": watermarks,
    }


def save_consumer_state(state_obj: dict[str, Any], path: str = STATE_PATH) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    pruned = prune_dedup_keys(state_obj.get("keys") or {})
    state_obj["keys"] = pruned
    payload = {
        "keys": pruned,
        "watermarks": state_obj.get("watermarks") or {},
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh)
    os.replace(tmp, path)


# Backward-compatible wrappers
def load_state() -> set[str]:
    return set(load_consumer_state(STATE_PATH)["keys"].keys())


def save_state(keys: set[str]) -> None:
    now_ts = time.time()
    save_consumer_state({"keys": {k: now_ts for k in keys}, "watermarks": {}}, STATE_PATH)


def write_heartbeat(status: str, extra: dict[str, Any] | None = None) -> None:
    try:
        os.makedirs(os.path.dirname(HEARTBEAT_PATH) or ".", exist_ok=True)
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "status": status,
        }
        if extra:
            payload.update(extra)
        tmp = HEARTBEAT_PATH + ".tmp"
        Path(tmp).write_text(json.dumps(payload), encoding="utf-8")
        os.replace(tmp, HEARTBEAT_PATH)
    except OSError as exc:  # noqa: BLE001 - heartbeat is best effort
        logger.warning("Heartbeat write failed: %s", exc, extra={"domain": "infra", "layer": "silver", "job": "silver_consumer"})


# ---------------------------------------------------------------------------
# Metrics helpers (VictoriaMetrics best-effort)
# ---------------------------------------------------------------------------
def _emit_counter(name: str, labels: dict[str, str], amount: float = 1.0) -> None:
    if vm_metrics is None:
        return
    try:
        vm_metrics.inc_counter(name, labels=labels, amount=amount)
    except Exception:  # noqa: BLE001
        pass


def _emit_gauge(name: str, value: float, labels: dict[str, str]) -> None:
    if vm_metrics is None:
        return
    try:
        vm_metrics.push_metric(name, value, labels=labels)
    except Exception:  # noqa: BLE001
        pass


# ---------------------------------------------------------------------------
# JetStream stream bootstrap, DLQ, Tombstone & ack/nak helpers
# ---------------------------------------------------------------------------
async def ensure_aux_streams(js) -> None:
    """Idempotently ensure DLQ and persistent realtime-events JetStream streams exist."""
    if StreamConfig is None:
        return
    specs = [
        (
            DLQ_STREAM,
            ["wmata.dlq.>", "traffic.dlq.>"],
            7 * 24 * 3600,
            1073741824,
        ),
        (
            REALTIME_EVENTS_STREAM,
            [SILVER_MERGED_SUBJECT, "realtime.tombstone.>"],
            7 * 24 * 3600,
            536870912,
        ),
    ]
    for name, subjects, max_age_sec, max_bytes in specs:
        try:
            await js.stream_info(name)
        except Exception:  # noqa: BLE001
            try:
                cfg = StreamConfig(
                    name=name,
                    subjects=subjects,
                    retention=RetentionPolicy.LIMITS,
                    storage=StorageType.FILE,
                    max_age=max_age_sec,
                    max_bytes=max_bytes,
                    duplicate_window=120,
                )
                await js.add_stream(cfg)
                logger.info("Created JetStream stream %s (%s)", name, subjects, extra={"domain": "infra", "layer": "silver", "job": "silver_consumer"})
            except Exception as exc:  # noqa: BLE001
                logger.warning("Could not ensure stream %s: %s", name, exc, extra={"domain": "infra", "layer": "silver", "job": "silver_consumer"})


async def publish_dlq(
    js,
    bucket: str,
    reason: str,
    raw_data: bytes | str | None = None,
    event: dict | None = None,
    delivery_count: int = 1,
    error: str | None = None,
    s3_key: str | None = None,
) -> None:
    payload = build_dlq_payload(
        bucket=bucket,
        reason=reason,
        raw_data=raw_data,
        event=event,
        delivery_count=delivery_count,
        error=error,
        s3_key=s3_key,
    )
    subject = f"{bucket}.dlq.{reason}"
    body = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
    msg_id = hashlib.sha256(
        f"{subject}|{payload.get('s3_key') or ''}|{payload.get('etag') or ''}|{reason}|{body[:120]!r}".encode()
    ).hexdigest()
    try:
        await js.publish(subject, body, headers={"Nats-Msg-Id": msg_id})
        logger.warning("Routed to DLQ %s key=%s err=%s", subject, payload.get("s3_key"), error, extra={"domain": bucket, "layer": "silver", "job": "silver_consumer"})
    except Exception as exc:  # noqa: BLE001
        logger.warning("DLQ publish failed (%s): %s", subject, exc, extra={"domain": bucket, "layer": "silver", "job": "silver_consumer"})
    _emit_counter("wmata_stream_dlq_total", {"bucket": bucket, "reason": reason})


async def publish_tombstone(js, bucket: str, key: str, event: dict) -> None:
    payload = build_tombstone_payload(bucket, key, event)
    subject = f"realtime.tombstone.{bucket}"
    body = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
    msg_id = hashlib.sha256(f"{subject}|{key}|{payload['etag']}".encode()).hexdigest()
    try:
        await js.publish(subject, body, headers={"Nats-Msg-Id": msg_id})
        logger.info("Published tombstone %s key=%s", subject, key, extra={"domain": bucket, "layer": "silver", "job": "silver_consumer"})
    except Exception as exc:  # noqa: BLE001
        logger.warning("Tombstone publish failed (%s): %s", subject, exc, extra={"domain": bucket, "layer": "silver", "job": "silver_consumer"})
    _emit_counter("wmata_stream_tombstones_total", {"bucket": bucket})


async def safe_ack(msg) -> None:
    try:
        await msg.ack()
    except Exception as exc:  # noqa: BLE001 - connection may be closing
        logger.warning("Ack failed: %s", exc, extra={"domain": "infra", "layer": "silver", "job": "silver_consumer"})


async def safe_nak(msg, delay: float | None = None) -> None:
    try:
        if delay is not None:
            await msg.nak(delay=delay)
        else:
            await msg.nak()
    except TypeError:
        try:
            await msg.nak()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Nak failed: %s", exc, extra={"domain": "infra", "layer": "silver", "job": "silver_consumer"})
    except Exception as exc:  # noqa: BLE001 - connection may be closing
        logger.warning("Nak failed: %s", exc, extra={"domain": "infra", "layer": "silver", "job": "silver_consumer"})


async def object_exists(bucket: str, key: str) -> bool:
    for attempt in range(3):
        try:
            await asyncio.to_thread(S3.head_object, Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchKey", "NotFound"):
                if attempt < 2:
                    await asyncio.sleep(1)
                    continue
                return False
            raise
    return False


def write_manifest(batch: list[dict]) -> dict:
    payload = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "wmata": [
            {"s3_key": item["s3_key"], "etag": item["etag"]}
            for item in batch
            if item["bucket"] == "wmata"
        ],
        "traffic": [
            {"s3_key": item["s3_key"], "etag": item["etag"]}
            for item in batch
            if item["bucket"] == "traffic"
        ],
    }
    os.makedirs(os.path.dirname(MANIFEST) or ".", exist_ok=True)
    tmp = MANIFEST + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    os.replace(tmp, MANIFEST)
    return payload


async def report_consumer_lag(subs: dict[str, Any]) -> dict[str, int]:
    """Query JetStream consumer pending counts and emit lag gauges."""
    pending_map: dict[str, int] = {}
    for bucket, _subject, stream_name, durable in SUBS:
        sub = subs.get(bucket)
        if sub is None:
            continue
        try:
            info = await sub.consumer_info()
            pending = int(getattr(info, "num_pending", 0) or 0)
            pending_map[durable] = pending
            _emit_gauge(
                "jetstream_consumer_num_pending",
                float(pending),
                {"stream_name": stream_name, "consumer_name": durable},
            )
        except Exception:  # noqa: BLE001
            pass
    return pending_map


async def collect(
    js,
    sub,
    bucket: str,
    state_keys: dict[str, float] | set[str],
    batch: list[dict],
    seen: set[str],
    watermarks: dict[str, str] | None = None,
    delivery_tracker: dict[str, int] | None = None,
) -> None:
    wm_ts = parse_iso_timestamp((watermarks or {}).get(bucket))
    while len(batch) < BATCH_SIZE:
        try:
            msgs = await sub.fetch(BATCH_SIZE, timeout=FETCH_TIMEOUT)
        except (NatsTimeout, asyncio.TimeoutError):
            break
        except Exception as exc:  # noqa: BLE001 - reconnect/retry on next loop
            logger.warning("Fetch %s failed: %s: %s", bucket, type(exc).__name__, exc, extra={"domain": bucket, "layer": "silver", "job": "silver_consumer"})
            break

        for msg in msgs:
            try:
                event = json.loads(msg.data)
                if not isinstance(event, dict):
                    raise json.JSONDecodeError("expected JSON object", "", 0)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                await publish_dlq(
                    js,
                    bucket=bucket,
                    reason="malformed_json",
                    raw_data=getattr(msg, "data", b""),
                    delivery_count=get_delivery_count(msg),
                    error=str(exc),
                )
                await safe_ack(msg)
                continue

            key = s3_key_of(bucket, event)
            if not key or not wanted(bucket, key):
                await safe_ack(msg)
                continue

            # Delete / Tombstone handling
            if is_delete_event(event):
                await publish_tombstone(js, bucket, key, event)
                _emit_counter("wmata_stream_events_total", {"bucket": bucket, "status": "tombstone"})
                await safe_ack(msg)
                continue

            dedup = dedup_key(bucket, key, event)
            if dedup in state_keys or dedup in seen:
                _emit_counter("wmata_stream_events_total", {"bucket": bucket, "status": "deduped"})
                await safe_ack(msg)
                continue

            # Event-time watermark & allowed lateness enforcement for live streams
            ev_ts = extract_event_time(event)
            if is_live_stream_event(bucket, key, event) and is_late_event(
                ev_ts, wm_ts, ALLOWED_LATENESS_MIN
            ):
                await publish_dlq(
                    js,
                    bucket=bucket,
                    reason="late_event",
                    event=event,
                    delivery_count=get_delivery_count(msg),
                    error=(
                        f"event_time {ev_ts.isoformat() if ev_ts else None} older than "
                        f"watermark {wm_ts.isoformat() if wm_ts else None} - {ALLOWED_LATENESS_MIN}m"
                    ),
                    s3_key=key,
                )
                _emit_counter("wmata_stream_late_events_total", {"bucket": bucket})
                _emit_counter("wmata_stream_events_total", {"bucket": bucket, "status": "late"})
                await safe_ack(msg)
                continue

            try:
                exists = await object_exists(bucket, key)
            except Exception as exc:  # noqa: BLE001 - transient S3 error
                logger.warning("HEAD %s/%s failed: %s", bucket, key, exc, extra={"domain": bucket, "layer": "silver", "job": "silver_consumer"})
                await safe_nak(msg, delay=5)
                continue

            if not exists:
                await publish_dlq(
                    js,
                    bucket=bucket,
                    reason="missing_s3_object",
                    event=event,
                    delivery_count=get_delivery_count(msg),
                    error=f"S3 object not found after retries: {bucket}/{key}",
                    s3_key=key,
                )
                _emit_counter("wmata_stream_events_total", {"bucket": bucket, "status": "missing"})
                await safe_ack(msg)
                continue

            seen.add(dedup)
            fallback_del = 1
            if delivery_tracker is not None:
                delivery_tracker[dedup] = delivery_tracker.get(dedup, 0) + 1
                fallback_del = delivery_tracker[dedup]
            batch.append(
                {
                    "bucket": bucket,
                    "s3_key": key,
                    "etag": event_etag(event),
                    "dedup": dedup,
                    "event": event,
                    "event_time": ev_ts,
                    "delivery_count": get_delivery_count(msg, fallback_count=fallback_del),
                    "msg": msg,
                }
            )


async def run_cmd(cmd: list[str], label: str) -> None:
    logger.info("Running command (%s): %s", label, " ".join(cmd), extra={"domain": "infra", "layer": "silver", "job": "silver_consumer"})
    proc = await asyncio.create_subprocess_exec(*cmd)
    rc = await proc.wait()
    if rc != 0:
        raise RuntimeError(f"{label} failed with rc={rc}")


# ---------------------------------------------------------------------------
# Persistent SparkSession runner (eliminates per-batch JVM startup latency)
# ---------------------------------------------------------------------------
_FATAL_SPARK_ERROR_TYPES = frozenset(
    {
        "Py4JNetworkError",
        "ConnectionRefusedError",
        "ConnectionResetError",
        "BrokenPipeError",
        "EOFError",
    }
)

_FATAL_SPARK_ERROR_MARKERS = (
    "cannot call methods on a stopped sparkcontext",
    "sparkcontext was shut down",
    "java gateway process exited",
    "answer from java side is empty",
    "outofmemoryerror",
    "rpcenv stopped",
    "master has been removed",
    "connection refused",
    "broken pipe",
)


def _is_fatal_spark_error(exc: BaseException) -> bool:
    if type(exc).__name__ in _FATAL_SPARK_ERROR_TYPES:
        return True
    msg = str(exc).lower()
    return any(marker in msg for marker in _FATAL_SPARK_ERROR_MARKERS)


def ensure_pyspark_paths(
    spark_home: str = SPARK_HOME,
    spark_job: str = SPARK_JOB,
    master: str = SPARK_MASTER,
    driver_memory: str = SPARK_DRIVER_MEMORY,
    cores_max: str = SPARK_CORES_MAX,
    configure_env: bool = True,
) -> list[str]:
    """Ensure PySpark, Py4J, and Spark job directory are importable on sys.path."""
    candidates: list[str] = []
    if spark_home:
        py_dir = os.path.join(spark_home, "python")
        if os.path.isdir(py_dir):
            candidates.append(py_dir)
        py4j_zips = sorted(glob.glob(os.path.join(py_dir, "lib", "py4j-*.zip")))
        candidates.extend(py4j_zips)
    job_dir = os.path.dirname(os.path.abspath(spark_job)) if spark_job else ""
    if job_dir and os.path.isdir(job_dir):
        candidates.append(job_dir)

    added: list[str] = []
    for path in reversed(candidates):
        if path not in sys.path:
            sys.path.insert(0, path)
            added.append(path)

    if configure_env:
        if spark_home and os.path.isdir(spark_home):
            os.environ.setdefault("SPARK_HOME", spark_home)
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        submit_parts = ["--master", master, "--driver-memory", driver_memory]
        if cores_max:
            submit_parts.extend(["--conf", f"spark.cores.max={cores_max}"])
        submit_parts.append("pyspark-shell")
        os.environ.setdefault("PYSPARK_SUBMIT_ARGS", " ".join(submit_parts))

    return list(reversed(added))


class SparkRunner:
    """Manages a long-lived SparkSession with automatic recovery on JVM failure."""

    def __init__(
        self,
        master: str = SPARK_MASTER,
        app_name: str = SPARK_APP_NAME,
        driver_memory: str = SPARK_DRIVER_MEMORY,
        shuffle_partitions: str = SPARK_SHUFFLE_PARTITIONS,
        cores_max: str = SPARK_CORES_MAX,
        spark_home: str = SPARK_HOME,
        spark_job: str = SPARK_JOB,
        session_factory=None,
        manifest_processor=None,
    ) -> None:
        self.master = master
        self.app_name = app_name
        self.driver_memory = driver_memory
        self.shuffle_partitions = str(shuffle_partitions)
        self.cores_max = cores_max
        self.spark_home = spark_home
        self.spark_job = spark_job
        self._session_factory = session_factory
        self._manifest_processor = manifest_processor
        self._spark: Any = None
        self._job_module: Any = None

    def is_alive(self) -> bool:
        if self._spark is None:
            return False
        sc = getattr(self._spark, "sparkContext", None)
        if sc is None:
            return True
        try:
            jsc = getattr(sc, "_jsc", None)
            if jsc is None:
                return False
            return not bool(jsc.sc().isStopped())
        except Exception:  # noqa: BLE001
            return False

    def _create_session(self) -> Any:
        t0 = time.monotonic()
        if self._session_factory is not None:
            spark = self._session_factory()
        else:
            ensure_pyspark_paths(
                spark_home=self.spark_home,
                spark_job=self.spark_job,
                master=self.master,
                driver_memory=self.driver_memory,
                cores_max=self.cores_max,
                configure_env=True,
            )
            from pyspark.sql import SparkSession

            builder = (
                SparkSession.builder.appName(self.app_name)
                .master(self.master)
                .config("spark.driver.memory", self.driver_memory)
                .config("spark.sql.shuffle.partitions", self.shuffle_partitions)
            )
            if self.cores_max:
                builder = builder.config("spark.cores.max", self.cores_max)
            spark = builder.getOrCreate()
            try:
                spark.sparkContext.setLogLevel("WARN")
            except Exception:  # noqa: BLE001
                pass
        elapsed = time.monotonic() - t0
        logger.info(
            "SparkSession ready master=%s app=%s shuffle_partitions=%s init=%.1fs",
            self.master,
            self.app_name,
            self.shuffle_partitions,
            elapsed,
            extra={"domain": "infra", "layer": "silver", "job": "silver_consumer", "duration_ms": round(elapsed * 1000, 2)},
        )
        return spark

    def get_session(self) -> Any:
        if self._spark is not None:
            if self.is_alive():
                return self._spark
            logger.warning("Persistent SparkSession died; recreating", extra={"domain": "infra", "layer": "silver", "job": "silver_consumer"})
            self.reset(hard=True)
        self._spark = self._create_session()
        return self._spark

    def _get_manifest_processor(self):
        if self._manifest_processor is not None:
            return self._manifest_processor
        if self._job_module is None:
            ensure_pyspark_paths(
                spark_home=self.spark_home,
                spark_job=self.spark_job,
                master=self.master,
                driver_memory=self.driver_memory,
                cores_max=self.cores_max,
                configure_env=True,
            )
            mod_name = Path(self.spark_job).stem or "bronze_to_silver"
            self._job_module = importlib.import_module(mod_name)
        return self._job_module.process_manifest

    def reset_job_caches(self) -> None:
        if self._job_module is not None and hasattr(self._job_module, "reset_caches"):
            try:
                self._job_module.reset_caches()
            except Exception:  # noqa: BLE001
                pass

    def run_manifest(self, manifest: dict) -> dict[str, int]:
        spark = self.get_session()
        processor = self._get_manifest_processor()
        try:
            return processor(spark, manifest)
        except Exception as exc:
            self.reset_job_caches()
            if _is_fatal_spark_error(exc) or not self.is_alive():
                logger.warning(
                    "SparkSession unhealthy after error (%s); resetting session for next batch",
                    type(exc).__name__,
                    extra={"domain": "infra", "layer": "silver", "job": "silver_consumer", "error_type": type(exc).__name__},
                )
                self.reset(hard=True)
            raise

    def reset(self, hard: bool = False) -> None:
        spark = self._spark
        self._spark = None
        self.reset_job_caches()
        if spark is not None:
            try:
                spark.stop()
            except Exception as exc:  # noqa: BLE001
                logger.warning("SparkSession stop error: %s", exc, extra={"domain": "infra", "layer": "silver", "job": "silver_consumer"})
        if hard and self._session_factory is None:
            try:
                from pyspark import SparkContext

                SparkContext._active_spark_context = None
                gw = getattr(SparkContext, "_gateway", None)
                if gw is not None:
                    try:
                        gw.shutdown()
                    except Exception:  # noqa: BLE001
                        pass
                SparkContext._gateway = None
                SparkContext._jvm = None
            except Exception:  # noqa: BLE001
                pass

    def stop(self) -> None:
        was_active = self._spark is not None
        self.reset(hard=False)
        if was_active:
            logger.info("SparkSession stopped cleanly", extra={"domain": "infra", "layer": "silver", "job": "silver_consumer"})


async def run_batch_persistent(
    spark_runner: SparkRunner,
    manifest_payload: dict,
    run_id: str,
    merger: Any = None,
) -> dict[str, Any]:
    """Execute truncate -> in-process Spark transform -> merge without spawning JVMs."""
    m = merger if merger is not None else silver_merger
    if m is None:
        raise RuntimeError("merge_silver module unavailable for persistent Spark mode")
    await asyncio.to_thread(m.truncate_all)
    spark_counts = await asyncio.to_thread(spark_runner.run_manifest, manifest_payload)
    try:
        merge_counts = await asyncio.to_thread(m.merge_all, run_id)
    except Exception:
        spark_runner.reset_job_caches()
        raise
    return {"spark": spark_counts, "merge": merge_counts}


async def sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


async def handle_batch_failure(
    js,
    batch: list[dict],
    error: str,
    max_deliver: int = MAX_DELIVER,
    backoff: float = ERROR_BACKOFF,
) -> tuple[int, int]:
    """Route poison-pill messages (delivery_count >= max_deliver) to DLQ; nak the rest."""
    dlq_count = 0
    retried_count = 0
    for item in batch:
        d_cnt = int(item.get("delivery_count") or 1)
        if d_cnt >= max_deliver:
            await publish_dlq(
                js,
                bucket=item["bucket"],
                reason="max_deliver_exceeded",
                event=item.get("event"),
                delivery_count=d_cnt,
                error=error,
                s3_key=item.get("s3_key"),
            )
            await safe_ack(item["msg"])
            dlq_count += 1
        else:
            await safe_nak(item["msg"], delay=backoff)
            retried_count += 1
    return dlq_count, retried_count


async def main() -> int:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    nc = await nats.connect(
        NATS_URL,
        max_reconnect_attempts=-1,
        reconnect_time_wait=2,
    )
    js = nc.jetstream()
    await ensure_aux_streams(js)

    subs = {}
    for bucket, subject, stream, durable in SUBS:
        subs[bucket] = await js.pull_subscribe(
            subject, durable=durable, stream=stream
        )

    state_obj = load_consumer_state(STATE_PATH)
    delivery_tracker: dict[str, int] = {}
    spark_runner = SparkRunner() if SPARK_MODE == "persistent" else None
    logger.info(
        "Consumer started spark_mode=%s state_keys=%d watermarks=%s max_deliver=%d allowed_lateness=%dm",
        SPARK_MODE,
        len(state_obj["keys"]),
        state_obj["watermarks"],
        MAX_DELIVER,
        ALLOWED_LATENESS_MIN,
        extra={"domain": "infra", "layer": "silver", "job": "silver_consumer"},
    )
    write_heartbeat("started", extra={"spark_mode": SPARK_MODE})

    if spark_runner is not None and SPARK_WARMUP:
        write_heartbeat("warming_up", extra={"spark_mode": SPARK_MODE})
        try:
            await asyncio.to_thread(spark_runner.get_session)
        except Exception as exc:  # noqa: BLE001 - retry lazily on first batch
            logger.warning("Spark warmup failed (will retry on first batch): %s", exc, extra={"domain": "infra", "layer": "silver", "job": "silver_consumer", "error_type": type(exc).__name__})

    try:
        while not stop.is_set():
            batch: list[dict] = []
            seen: set[str] = set()
            for bucket, _, _, _ in SUBS:
                await collect(
                    js,
                    subs[bucket],
                    bucket,
                    state_obj["keys"],
                    batch,
                    seen,
                    watermarks=state_obj["watermarks"],
                    delivery_tracker=delivery_tracker,
                )

            pending_lag = await report_consumer_lag(subs)
            if not batch:
                write_heartbeat(
                    "idle",
                    extra={"spark_mode": SPARK_MODE, "pending_lag": pending_lag},
                )
                await sleep_or_stop(stop, POLL_INTERVAL)
                continue

            batch_start = time.monotonic()
            counts = {
                bucket: sum(1 for item in batch if item["bucket"] == bucket)
                for bucket, _, _, _ in SUBS
            }
            manifest_payload = write_manifest(batch)
            run_id = run_log.new_run_id() if run_log is not None else f"run-{int(time.time())}"
            batch_domain = "traffic" if counts["traffic"] > 0 and counts["wmata"] == 0 else "wmata"
            with _run_ctx(run_id):
                logger.info(
                    "Collected batch wmata=%d traffic=%d",
                    counts["wmata"],
                    counts["traffic"],
                    extra={
                        "domain": batch_domain,
                        "layer": "silver",
                        "job": "silver_consumer",
                        "row_count": len(batch),
                    },
                )
                if run_log is not None:
                    run_log.safe_start_run(
                        run_id,
                        "silver_consumer",
                        "silver",
                        input_datasets=["bronze.wmata", "bronze.traffic"],
                        output_datasets=["silver.wmata", "silver.traffic"],
                        source_manifest={**manifest_payload, "batch_id": run_id},
                    )

                try:
                    if spark_runner is not None:
                        await run_batch_persistent(spark_runner, manifest_payload, run_id)
                    else:
                        await run_cmd(
                            [sys.executable, MERGE_SCRIPT, "--truncate-only"],
                            "truncate staging",
                        )
                        await run_cmd(
                            [
                                "/opt/spark/bin/spark-submit",
                                "--master",
                                SPARK_MASTER,
                                "--conf",
                                f"spark.sql.shuffle.partitions={SPARK_SHUFFLE_PARTITIONS}",
                                "--conf",
                                f"spark.driver.memory={SPARK_DRIVER_MEMORY}",
                                SPARK_JOB,
                                "--manifest",
                                MANIFEST,
                            ],
                            "spark bronze->silver",
                        )
                        await run_cmd(
                            [sys.executable, MERGE_SCRIPT, "--run-id", run_id],
                            "merge staging",
                        )
                except Exception as exc:  # noqa: BLE001 - retry or DLQ poison pills
                    dur_ms = round((time.monotonic() - batch_start) * 1000, 2)
                    logger.exception(
                        "Batch failed: %s",
                        exc,
                        extra={
                            "domain": batch_domain,
                            "layer": "silver",
                            "job": "silver_consumer",
                            "duration_ms": dur_ms,
                            "error_type": type(exc).__name__,
                        },
                    )
                    if run_log is not None:
                        run_log.safe_finish_run(run_id, "failed", error=str(exc))
                    _emit_counter(
                        "wmata_pipeline_job_runs_total",
                        {"service": "consumer", "job": "silver_consumer", "domain": "wmata", "layer": "silver", "status": "failed"},
                    )
                    dlq_n, retry_n = await handle_batch_failure(js, batch, str(exc))
                    write_heartbeat(
                        "error",
                        extra={"spark_mode": SPARK_MODE, "dlq": dlq_n, "retried": retry_n},
                    )
                    await sleep_or_stop(stop, ERROR_BACKOFF)
                    continue

                duration = time.monotonic() - batch_start
                now_dt = datetime.now(timezone.utc)
                now_ts = now_dt.timestamp()

                if run_log is not None:
                    run_log.safe_finish_run(run_id, "success")

                # Advance dedup keys + per-bucket live watermarks
                for item in batch:
                    state_obj["keys"][item["dedup"]] = now_ts
                    delivery_tracker.pop(item["dedup"], None)
                    bkt = item["bucket"]
                    ev_ts = item.get("event_time")
                    if ev_ts is not None and is_live_stream_event(bkt, item["s3_key"], item.get("event") or {}):
                        cur_wm = parse_iso_timestamp(state_obj["watermarks"].get(bkt))
                        if cur_wm is None or ev_ts > cur_wm:
                            state_obj["watermarks"][bkt] = ev_ts.isoformat()
                        lag_s = max(0.0, (now_dt - ev_ts).total_seconds())
                        _emit_gauge("wmata_stream_processing_lag_seconds", round(lag_s, 2), {"bucket": bkt})

                try:
                    save_consumer_state(state_obj, STATE_PATH)
                except Exception as exc:  # noqa: BLE001 - dedup state is best effort
                    logger.warning("Could not save consumer state: %s", exc, extra={"domain": batch_domain, "layer": "silver", "job": "silver_consumer"})

                for item in batch:
                    await safe_ack(item["msg"])

                # Publish persistent JetStream event for downstream inference & SSE
                source_tag = "tomtom" if counts["traffic"] > 0 and counts["wmata"] == 0 else "wmata"
                notify = json.dumps(
                    {
                        "run_id": run_id,
                        "source": source_tag,
                        "spark_mode": SPARK_MODE,
                        "wmata_files": counts["wmata"],
                        "traffic_files": counts["traffic"],
                        "duration_sec": round(duration, 2),
                        "watermarks": state_obj["watermarks"],
                        "merged_at": now_dt.isoformat(),
                    },
                    separators=(",", ":"),
                ).encode("utf-8")
                try:
                    await js.publish(
                        SILVER_MERGED_SUBJECT,
                        notify,
                        headers={"Nats-Msg-Id": run_id},
                    )
                    await nc.flush(timeout=10)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("JetStream publish failed (%s): %s", SILVER_MERGED_SUBJECT, exc, extra={"domain": batch_domain, "layer": "silver", "job": "silver_consumer"})
                    try:
                        await nc.publish(SILVER_MERGED_SUBJECT, notify)
                        await nc.flush(timeout=5)
                    except Exception:  # noqa: BLE001
                        pass

                # Emit stream & pipeline metrics to VictoriaMetrics
                _emit_gauge("wmata_stream_batch_duration_seconds", round(duration, 3), {"service": "consumer"})
                _emit_counter(
                    "wmata_pipeline_job_runs_total",
                    {"service": "consumer", "job": "silver_consumer", "domain": "wmata", "layer": "silver", "status": "success"},
                )
                for bkt, cnt in counts.items():
                    if cnt > 0:
                        _emit_counter("wmata_stream_events_total", {"bucket": bkt, "status": "processed"}, amount=float(cnt))
                        _emit_gauge("wmata_stream_last_event_timestamp", now_ts, {"stream": f"silver-{bkt}", "service": "consumer"})

                write_heartbeat(
                    "ok",
                    extra={
                        "spark_mode": SPARK_MODE,
                        "wmata": counts["wmata"],
                        "traffic": counts["traffic"],
                        "duration_sec": round(duration, 2),
                        "watermarks": state_obj["watermarks"],
                        "pending_lag": pending_lag,
                    },
                )
                logger.info(
                    "Acked batch wmata=%d traffic=%d dur=%.1fs mode=%s",
                    counts["wmata"],
                    counts["traffic"],
                    duration,
                    SPARK_MODE,
                    extra={
                        "domain": batch_domain,
                        "layer": "silver",
                        "job": "silver_consumer",
                        "duration_ms": round(duration * 1000, 2),
                        "row_count": len(batch),
                    },
                )
    finally:
        if spark_runner is not None:
            try:
                await asyncio.to_thread(spark_runner.stop)
            except Exception:  # noqa: BLE001
                pass
        try:
            await nc.flush(timeout=5)
        except Exception:  # noqa: BLE001 - best effort shutdown
            pass
        try:
            await nc.close()
        except Exception:  # noqa: BLE001
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
