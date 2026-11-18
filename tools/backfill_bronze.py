"""Backfill bronze object events into NATS JetStream.

Existing objects in SeaweedFS do not trigger the filer webhook again, so this
one-shot publisher scans the selected prefixes and emits the same logical event
the bridge would have emitted on upload.

Event id is deterministic (bucket|key|etag|last_modified) so a rerun can be
deduplicated by the consumer (JetStream only dedupes inside its 2 minute window).
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from datetime import datetime, timezone

import boto3
import nats
from botocore.config import Config

NATS_URL = os.environ.get("NATS_URL", "nats://nats:4222")
ENDPOINT = os.environ.get("SEAWEEDFS_S3_ENDPOINT", "http://seaweedfs:8333")
ACCESS = os.environ.get("SEAWEEDFS_ACCESS_KEY", "seaweedfs-access-key")
SECRET = os.environ.get("SEAWEEDFS_SECRET_KEY", "seaweedfs-secret-key")
REGION = os.environ.get("AWS_REGION", "us-east-1")
LOG_PATH = os.environ.get("BACKFILL_LOG", "/work/backfill_bronze.json")

# bucket -> (prefix, suffix filter)
TARGETS = {
    "wmata": ("bronze_2020/bus_positions/", ".jsonl.gz"),
    "traffic": ("ddot/", ".geojson"),
}
SUBJECTS = {
    "wmata": "wmata.realtime.bronze_data",
    "traffic": "traffic.realtime.bronze_data",
}


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS,
        aws_secret_access_key=SECRET,
        region_name=REGION,
        config=Config(s3={"addressing_style": "path"}),
    )


def objects(client, bucket: str, prefix: str, suffix: str, since=None, until=None):
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(suffix):
                continue
            modified = obj["LastModified"]
            if since is not None and modified < since:
                continue
            if until is not None and modified > until:
                continue
            yield key, obj["ETag"].strip('"'), modified, obj["Size"]


def make_event(bucket: str, key: str, etag: str, last_modified: datetime, size: int) -> dict:
    event_id = hashlib.sha256(
        f"{bucket}|{key}|{etag}|{last_modified.isoformat()}".encode()
    ).hexdigest()
    return {
        "event_id": event_id,
        "event_time": last_modified.astimezone(timezone.utc).isoformat(),
        "ingest_time": datetime.now(timezone.utc).isoformat(),
        "payload_version": "v1",
        "source": "seaweedfs",
        "bucket": bucket,
        "key": f"/buckets/{bucket}/{key}",
        "s3_key": key,
        "event_type": "create",
        "size": size,
        "etag": etag,
    }


def parse_dt(value: str | None):
    if not value:
        return None
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wmata-prefix", default=TARGETS["wmata"][0])
    ap.add_argument("--traffic-prefix", default=TARGETS["traffic"][0])
    ap.add_argument("--since")
    ap.add_argument("--until")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    since = parse_dt(args.since)
    until = parse_dt(args.until)
    targets = {
        "wmata": (args.wmata_prefix, TARGETS["wmata"][1]),
        "traffic": (args.traffic_prefix, TARGETS["traffic"][1]),
    }

    client = s3_client()
    nc = await nats.connect(NATS_URL, max_reconnect_attempts=10)
    js = nc.jetstream()
    log = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "since": str(since) if since else None,
        "until": str(until) if until else None,
        "dry_run": args.dry_run,
        "events": [],
    }

    try:
        for bucket, (prefix, suffix) in targets.items():
            subject = SUBJECTS[bucket]
            count = 0
            for key, etag, modified, size in objects(
                client, bucket, prefix, suffix, since=since, until=until
            ):
                if args.limit and count >= args.limit:
                    break
                event = make_event(bucket, key, etag, modified, size)
                if not args.dry_run:
                    await js.publish(
                        subject,
                        json.dumps(event, separators=(",", ":")).encode(),
                        headers={"Nats-Msg-Id": event["event_id"]},
                    )
                log["events"].append(
                    {"bucket": bucket, "key": key, "event_id": event["event_id"]}
                )
                count += 1
                print(f"{'DRY' if args.dry_run else 'PUB'} {bucket}/{key}", flush=True)
            print(f"{bucket}: {count} events -> {subject}", flush=True)
            log[f"{bucket}_count"] = count
        if not args.dry_run:
            await nc.flush(timeout=10)
    finally:
        await nc.drain()
        log["finished_at"] = datetime.now(timezone.utc).isoformat()
        log["total"] = len(log["events"])
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
        with open(LOG_PATH, "w", encoding="utf-8") as fh:
            json.dump(log, fh, indent=2)
        print(f"log: {LOG_PATH} total={log['total']}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
