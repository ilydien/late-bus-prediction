"""NATS JetStream -> Spark -> silver, as a long-running consumer.

For each batch:
  1. collect bronze object events from the durable pull subscriptions;
  2. truncate the silver staging tables;
  3. run the Spark bronze -> silver job (writes staging only);
  4. upsert staging into the real silver tables;
  5. save the dedup state and ack the NATS messages.

A message is acked only after steps 3 and 4 succeed. On failure the messages
are nacked and retried. Staging + upsert makes retries idempotent.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
import nats
from botocore.config import Config
from botocore.exceptions import ClientError
from nats.errors import TimeoutError as NatsTimeout

from governance import run_log

NATS_URL = os.environ.get("NATS_URL", "nats://nats:4222")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
SPARK_JOB = os.environ.get("SPARK_JOB", "/opt/spark/jobs/bronze_to_silver.py")
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

WMATA_PREFIXES = tuple(
    p.strip()
    for p in os.environ.get("WMATA_BRONZE_PREFIXES", "bronze_").split(",")
    if p.strip()
)
TRAFFIC_PREFIX = os.environ.get("TRAFFIC_BRONZE_PREFIX", "ddot/")

S3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=ACCESS,
    aws_secret_access_key=SECRET,
    region_name=REGION,
    config=Config(s3={"addressing_style": "path"}),
)

SUBS = [
    ("wmata", "wmata.realtime.bronze_data", "wmata-bronze", "silver-wmata"),
    ("traffic", "traffic.realtime.bronze_data", "traffic-bronze", "silver-traffic"),
]


def wanted(bucket: str, key: str) -> bool:
    if bucket == "wmata":
        return (
            key.endswith(".jsonl.gz")
            and "/bus_positions/" in key
            and key.startswith(WMATA_PREFIXES)
        )
    if bucket == "traffic":
        return key.startswith(TRAFFIC_PREFIX) and key.endswith(".geojson")
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
    message = event.get("message") or {}
    for side in ("new_entry", "old_entry"):
        entry = message.get(side) or {}
        attrs = entry.get("attributes") or {}
        for name in ("md5", "e_tag", "etag"):
            if attrs.get(name):
                return str(attrs[name])
    return ""


def dedup_key(bucket: str, key: str, event: dict) -> str:
    return f"{bucket}|{key}|{event_etag(event)}"


def load_state() -> set[str]:
    try:
        with open(STATE_PATH, encoding="utf-8") as fh:
            return set(json.load(fh).get("keys", []))
    except (OSError, json.JSONDecodeError):
        return set()


def save_state(keys: set[str]) -> None:
    os.makedirs(os.path.dirname(STATE_PATH) or ".", exist_ok=True)
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"keys": sorted(keys)}, fh)
    os.replace(tmp, STATE_PATH)


def write_heartbeat(status: str) -> None:
    try:
        os.makedirs(os.path.dirname(HEARTBEAT_PATH) or ".", exist_ok=True)
        tmp = HEARTBEAT_PATH + ".tmp"
        Path(tmp).write_text(
            json.dumps(
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "status": status,
                }
            ),
            encoding="utf-8",
        )
        os.replace(tmp, HEARTBEAT_PATH)
    except OSError as exc:  # noqa: BLE001 - heartbeat is best effort
        print(f"WARN heartbeat failed: {exc}", flush=True)


async def safe_ack(msg) -> None:
    try:
        await msg.ack()
    except Exception as exc:  # noqa: BLE001 - connection may be closing
        print(f"WARN ack failed: {exc}", flush=True)


async def safe_nak(msg) -> None:
    try:
        await msg.nak()
    except Exception as exc:  # noqa: BLE001 - connection may be closing
        print(f"WARN nak failed: {exc}", flush=True)


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


async def collect(js, sub, bucket: str, state: set[str], batch: list[dict], seen: set[str]) -> None:
    while len(batch) < BATCH_SIZE:
        try:
            msgs = await sub.fetch(BATCH_SIZE, timeout=FETCH_TIMEOUT)
        except (NatsTimeout, asyncio.TimeoutError):
            break
        except Exception as exc:  # noqa: BLE001 - reconnect/retry on next loop
            print(
                f"WARN fetch {bucket} failed: {type(exc).__name__}: {exc}",
                flush=True,
            )
            break

        for msg in msgs:
            try:
                event = json.loads(msg.data)
            except json.JSONDecodeError:
                await safe_ack(msg)
                continue

            key = s3_key_of(bucket, event)
            if not key or not wanted(bucket, key):
                await safe_ack(msg)
                continue

            dedup = dedup_key(bucket, key, event)
            if dedup in state or dedup in seen:
                await safe_ack(msg)
                continue

            try:
                exists = await object_exists(bucket, key)
            except Exception as exc:  # noqa: BLE001 - transient S3 error
                print(f"WARN head {bucket}/{key} failed: {exc}", flush=True)
                await safe_nak(msg)
                continue

            if not exists:
                print(f"SKIP missing {bucket}/{key}", flush=True)
                await safe_ack(msg)
                continue

            seen.add(dedup)
            batch.append(
                {
                    "bucket": bucket,
                    "s3_key": key,
                    "etag": event_etag(event),
                    "dedup": dedup,
                    "msg": msg,
                }
            )


async def run_cmd(cmd: list[str], label: str) -> None:
    print(f"RUN {label}: {' '.join(cmd)}", flush=True)
    proc = await asyncio.create_subprocess_exec(*cmd)
    rc = await proc.wait()
    if rc != 0:
        raise RuntimeError(f"{label} failed with rc={rc}")


async def sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


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
    subs = {}
    for bucket, subject, stream, durable in SUBS:
        subs[bucket] = await js.pull_subscribe(
            subject, durable=durable, stream=stream
        )

    state = load_state()
    print(f"consumer started state_keys={len(state)}", flush=True)
    write_heartbeat("started")

    try:
        while not stop.is_set():
            batch: list[dict] = []
            seen: set[str] = set()
            for bucket, _, _, _ in SUBS:
                await collect(js, subs[bucket], bucket, state, batch, seen)

            if not batch:
                write_heartbeat("idle")
                await sleep_or_stop(stop, POLL_INTERVAL)
                continue

            counts = {
                bucket: sum(1 for item in batch if item["bucket"] == bucket)
                for bucket, _, _, _ in SUBS
            }
            print(
                f"collected wmata={counts['wmata']} traffic={counts['traffic']}",
                flush=True,
            )
            manifest_payload = write_manifest(batch)
            run_id = run_log.new_run_id()
            run_log.safe_start_run(
                run_id,
                "silver_consumer",
                "silver",
                input_datasets=["bronze.wmata", "bronze.traffic"],
                output_datasets=["silver.wmata", "silver.traffic"],
                source_manifest={**manifest_payload, "batch_id": run_id},
            )

            try:
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
                        "spark.sql.shuffle.partitions=32",
                        "--conf",
                        "spark.driver.memory=2g",
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
            except Exception as exc:  # noqa: BLE001 - retry the whole batch
                print(f"BATCH FAILED: {exc}", flush=True)
                run_log.safe_finish_run(run_id, "failed", error=str(exc))
                for item in batch:
                    await safe_nak(item["msg"])
                write_heartbeat("error")
                await sleep_or_stop(stop, ERROR_BACKOFF)
                continue

            run_log.safe_finish_run(run_id, "success")
            for item in batch:
                state.add(item["dedup"])
            try:
                save_state(state)
            except Exception as exc:  # noqa: BLE001 - dedup state is best effort
                print(f"WARN could not save consumer state: {exc}", flush=True)

            for item in batch:
                await safe_ack(item["msg"])
            try:
                await nc.flush(timeout=10)
            except Exception as exc:  # noqa: BLE001 - ack will be retried by NATS
                print(f"WARN flush failed: {exc}", flush=True)
            write_heartbeat("ok")
            print(f"ACKED wmata={counts['wmata']} traffic={counts['traffic']}", flush=True)
    finally:
        try:
            await nc.flush(timeout=5)
        except Exception:  # noqa: BLE001 - best effort shutdown
            pass
        try:
            await nc.close()
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
