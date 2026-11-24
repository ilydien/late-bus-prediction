"""Inspect and replay Dead-Letter Queue (DLQ) messages from NATS JetStream `wmata-dlq`.

Usage:
    # List DLQ messages
    python3 tools/replay_dlq.py --list --limit 20

    # Dry-run replay of late/failed WMATA events
    python3 tools/replay_dlq.py --replay --bucket wmata --reason max_deliver_exceeded --dry-run

    # Replay messages back onto bronze subjects (tagged source="replay")
    python3 tools/replay_dlq.py --replay --bucket wmata --limit 50
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any

try:
    import nats
except ImportError:  # pragma: no cover
    nats = None  # type: ignore[assignment]

NATS_URL = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
DLQ_STREAM = os.environ.get("DLQ_STREAM", "wmata-dlq")


def build_replay_envelope(dlq_record: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    """Convert a DLQ JSON record back into a replayable bronze event envelope."""
    bucket = str(dlq_record.get("bucket") or "").strip()
    if bucket not in ("wmata", "traffic"):
        return None
    orig = dlq_record.get("original_event")
    s3_key = dlq_record.get("s3_key")
    if isinstance(orig, dict):
        event = dict(orig)
    elif s3_key:
        event = {"bucket": bucket, "s3_key": s3_key, "etag": dlq_record.get("etag") or ""}
    else:
        return None

    now_iso = datetime.now(timezone.utc).isoformat()
    event["bucket"] = bucket
    if s3_key:
        event["s3_key"] = s3_key
    event["source"] = "replay"
    event["replayed_at"] = now_iso
    event["replayed_from_reason"] = dlq_record.get("reason")
    event["payload_version"] = event.get("payload_version") or "v1"
    target_subject = str(dlq_record.get("original_subject") or f"{bucket}.realtime.bronze_data")
    return target_subject, event


def matches_filter(
    dlq_record: dict[str, Any],
    bucket: str | None = None,
    reason: str | None = None,
) -> bool:
    if bucket and str(dlq_record.get("bucket") or "") != bucket:
        return False
    if reason and str(dlq_record.get("reason") or "") != reason:
        return False
    return True


async def run_dlq_tool(args: argparse.Namespace) -> int:
    if nats is None:
        raise RuntimeError("nats-py is required to connect to NATS JetStream")

    nc = await nats.connect(args.nats_url, connect_timeout=5)
    try:
        js = nc.jetstream()
        try:
            info = await js.stream_info(args.stream)
        except Exception as exc:  # noqa: BLE001
            print(f"DLQ stream '{args.stream}' not found: {exc}")
            return 1

        total = int(info.state.messages or 0)
        first_seq = int(info.state.first_seq or 1)
        last_seq = int(info.state.last_seq or 0)
        print(f"Stream={args.stream} messages={total} seq={first_seq}..{last_seq}")
        if total == 0:
            return 0

        matched = 0
        replayed = 0
        for seq in range(first_seq, last_seq + 1):
            if matched >= args.limit:
                break
            try:
                raw_msg = await js.get_msg(args.stream, seq=seq)
            except Exception:  # noqa: BLE001
                continue
            try:
                rec = json.loads(raw_msg.data.decode("utf-8"))
            except Exception:  # noqa: BLE001
                continue
            if not matches_filter(rec, bucket=args.bucket, reason=args.reason):
                continue

            matched += 1
            if args.list or args.dry_run:
                print(
                    f"  seq={seq} bucket={rec.get('bucket')} reason={rec.get('reason')} "
                    f"key={rec.get('s3_key')} ts={rec.get('dlq_ts')} err={rec.get('error')}"
                )
            if args.replay:
                built = build_replay_envelope(rec)
                if built is None:
                    print(f"  SKIP seq={seq} (non-replayable payload)")
                    continue
                target_subject, replay_ev = built
                if args.dry_run:
                    print(f"  DRY-RUN -> {target_subject} key={replay_ev.get('s3_key')}")
                else:
                    body = json.dumps(replay_ev, separators=(",", ":"), default=str).encode("utf-8")
                    msg_id = hashlib.sha256(f"replay|{seq}|{body[:120]!r}".encode()).hexdigest()
                    await js.publish(target_subject, body, headers={"Nats-Msg-Id": msg_id})
                    if args.delete_after_replay:
                        await js.delete_msg(args.stream, seq)
                    replayed += 1
                    print(f"  REPLAYED seq={seq} -> {target_subject} key={replay_ev.get('s3_key')}")

        if args.replay and not args.dry_run:
            await nc.flush(timeout=5)
            print(f"Done: matched={matched} replayed={replayed}")
        else:
            print(f"Done: matched={matched}")
        return 0
    finally:
        await nc.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Inspect and replay NATS JetStream DLQ messages")
    ap.add_argument("--nats-url", default=NATS_URL)
    ap.add_argument("--stream", default=DLQ_STREAM)
    ap.add_argument("--bucket", choices=["wmata", "traffic"], default=None)
    ap.add_argument("--reason", default=None)
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--list", action="store_true", help="List matching DLQ records")
    ap.add_argument("--replay", action="store_true", help="Replay matching records onto bronze subjects")
    ap.add_argument("--dry-run", action="store_true", help="Preview replay without publishing")
    ap.add_argument(
        "--delete-after-replay",
        action="store_true",
        help="Remove replayed sequence numbers from the DLQ stream",
    )
    args = ap.parse_args()
    if not args.list and not args.replay:
        args.list = True
    return asyncio.run(run_dlq_tool(args))


if __name__ == "__main__":
    raise SystemExit(main())
