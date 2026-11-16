"""NATS JetStream -> Spark -> silver.

Pulls bronze object events, dedups by object key, writes a manifest, then runs
the Spark silver job. Messages are acked only after Spark succeeds.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

import nats
from nats.errors import TimeoutError as NatsTimeout

import boto3
from botocore.config import Config

NATS_URL = os.environ.get("NATS_URL", "nats://nats:4222")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
SPARK_JOB = os.environ.get("SPARK_JOB", "/opt/spark/jobs/bronze_to_silver.py")
STATE_PATH = os.environ.get("CONSUMER_STATE", "/work/bronze_consumer_state.json")
MANIFEST = os.environ.get("BRONZE_MANIFEST", "/tmp/bronze_batch.json")
S3_ENDPOINT = os.environ.get("SEAWEEDFS_S3_ENDPOINT", "http://seaweedfs:8333")
ACCESS = os.environ.get("SEAWEEDFS_ACCESS_KEY", "seaweedfs-access-key")
SECRET = os.environ.get("SEAWEEDFS_SECRET_KEY", "seaweedfs-secret-key")
REGION = os.environ.get("AWS_REGION", "us-east-1")

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
        return key.startswith("bronze_2020/bus_positions/") and key.endswith(".jsonl.gz")
    if bucket == "traffic":
        return key.startswith("ddot/") and key.endswith(".geojson")
    return False


def s3_key_of(bucket: str, event: dict) -> str | None:
    if event.get("s3_key"):
        return event["s3_key"]
    raw = event.get("key") or ""
    prefix = f"/buckets/{bucket}/"
    if raw.startswith(prefix):
        return raw[len(prefix):]
    return None


def load_state() -> set[str]:
    try:
        with open(STATE_PATH, encoding="utf-8") as fh:
            return set(json.load(fh).get("keys", []))
    except (OSError, json.JSONDecodeError):
        return set()


def save_state(keys: set[str]) -> None:
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"keys": sorted(keys)}, fh)
    os.replace(tmp, STATE_PATH)


async def collect(js, bucket, subject, stream, durable, state, batch, acked):
    sub = await js.pull_subscribe(subject, durable=durable, stream=stream)
    while True:
        try:
            msgs = await sub.fetch(200, timeout=3)
        except NatsTimeout:
            break
        for msg in msgs:
            try:
                event = json.loads(msg.data)
            except json.JSONDecodeError:
                await msg.ack()
                continue
            key = s3_key_of(bucket, event)
            if not key or not wanted(bucket, key):
                await msg.ack()
                continue
            try:
                S3.head_object(Bucket=bucket, Key=key)
            except Exception:
                print(f"SKIP missing {bucket}/{key}", flush=True)
                await msg.ack()
                continue
            dedup = f"{bucket}|{key}"
            if dedup in state:
                await msg.ack()
                continue
            state.add(dedup)
            batch.setdefault(bucket, []).append(
                {"s3_key": key, "etag": event.get("etag")}
            )
            acked[bucket].append(msg)


async def main() -> int:
    nc = await nats.connect(NATS_URL, max_reconnect_attempts=10)
    js = nc.jetstream()
    state = load_state()
    batch: dict[str, list] = {"wmata": [], "traffic": []}
    acked: dict[str, list] = {"wmata": [], "traffic": []}

    try:
        for bucket, subject, stream, durable in SUBS:
            await collect(js, bucket, subject, stream, durable, state, batch, acked)

        total = sum(len(v) for v in batch.values())
        print(
            f"collected wmata={len(batch['wmata'])} traffic={len(batch['traffic'])}",
            flush=True,
        )
        if total == 0:
            for msgs in acked.values():
                for m in msgs:
                    await m.ack()
            await nc.flush(timeout=10)
            return 0

        with open(MANIFEST, "w", encoding="utf-8") as fh:
            json.dump(
                {
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "wmata": batch["wmata"],
                    "traffic": batch["traffic"],
                },
                fh,
                indent=2,
            )

        cmd = [
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
        ]
        print("RUN", " ".join(cmd), flush=True)
        proc = await asyncio.create_subprocess_exec(*cmd)
        rc = await proc.wait()
        if rc != 0:
            print(f"SPARK FAILED rc={rc}", flush=True)
            for msgs in acked.values():
                for m in msgs:
                    await m.nak()
            return 1

        for msgs in acked.values():
            for m in msgs:
                await m.ack()
        await nc.flush(timeout=10)
        try:
            save_state(state)
        except Exception as exc:  # noqa: BLE001 - state is best-effort
            print(f"WARN could not save consumer state: {exc}", flush=True)
        print("ACKED", flush=True)
        return 0
    finally:
        try:
            await nc.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
