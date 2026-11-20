"""Ingest WMATA 2020 bus-position archive into SeaweedFS bronze via dlt.

Source : http://busdata-00-us-west-2.s3.amazonaws.com/<date>.tar.gz (public)
Target : s3://wmata/bronze_2020/bus_positions/...  (SeaweedFS S3 gateway)
Label  : Deviation (minutes vs schedule) is captured as deviation_min.

No WMATA API key is used for this dataset.
"""

from __future__ import annotations

import gzip
import json
import os
import tarfile
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from typing import Iterator
from zoneinfo import ZoneInfo

import boto3
import dlt
import requests
from botocore.config import Config
from dlt.common.configuration.specs import AwsCredentials
from dlt.common.configuration.specs.aws_credentials import AwsCredentialsWithoutDefaults
from dlt.destinations import filesystem

SOURCE = "http://busdata-00-us-west-2.s3.amazonaws.com"
START = os.environ.get("START_DATE", "2020-01-06")
END = os.environ.get("END_DATE", "2020-02-08")
STATE_FILE = "/work/dlt_2020_done.json"
PIPELINES_DIR = "/work/dlt_state"
EASTERN = ZoneInfo("America/New_York")


def _to_utc(value: str | None, assume_tz: timezone | ZoneInfo = EASTERN) -> str | None:
    """WMATA payload timestamps carry no offset and are Eastern local time."""
    if not value:
        return None
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=assume_tz)
    return dt.astimezone(timezone.utc).isoformat()


def _local_parts(value: str | None) -> tuple[int | None, int | None]:
    if not value:
        return None, None
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=EASTERN)
    dt = dt.astimezone(EASTERN)
    return dt.hour, dt.weekday()


class SeaweedCredentials(AwsCredentials):
    """AwsCredentials with path-style addressing for the SeaweedFS S3 gateway."""

    def to_s3fs_credentials(self):
        creds = super().to_s3fs_credentials()
        # s3fs passes top-level endpoint_url to botocore Session; it belongs in
        # client_kwargs (boto3 client accepts it there).
        creds.pop("endpoint_url", None)
        creds.setdefault("client_kwargs", {})["endpoint_url"] = self.endpoint_url
        creds.setdefault("config_kwargs", {}).setdefault("s3", {})[
            "addressing_style"
        ] = "path"
        return creds


# dlt resolves credentials back to the base spec, so patch the base method too.
_orig_to_s3fs = AwsCredentialsWithoutDefaults.to_s3fs_credentials


def _seaweed_to_s3fs(self):
    creds = _orig_to_s3fs(self)
    creds.pop("endpoint_url", None)
    if self.endpoint_url:
        creds.setdefault("client_kwargs", {})["endpoint_url"] = self.endpoint_url
        creds.setdefault("config_kwargs", {}).setdefault("s3", {})[
            "addressing_style"
        ] = "path"
    return creds


AwsCredentialsWithoutDefaults.to_s3fs_credentials = _seaweed_to_s3fs


def _download(url: str, dest: str, attempts: int = 3) -> None:
    last: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            with requests.get(url, stream=True, timeout=300) as resp:
                resp.raise_for_status()
                with open(dest, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=1 << 20):
                        if chunk:
                            fh.write(chunk)
            return
        except Exception as exc:  # noqa: BLE001 - retry network failures
            last = exc
            print(f"download attempt {attempt}/{attempts} failed for {url}: {exc}", flush=True)
            time.sleep(10 * attempt)
    raise RuntimeError(f"failed to download {url}") from last


def _records(day: str) -> Iterator[dict]:
    tar_path = os.path.join(tempfile.gettempdir(), f"wmata-{day}.tar.gz")
    _download(f"{SOURCE}/{day}.tar.gz", tar_path)
    try:
        with tarfile.open(tar_path, mode="r|gz") as tf:
            for member in tf:
                if not member.isfile() or not member.name.endswith(".json"):
                    continue
                if "/buses" not in member.name:
                    continue
                handle = tf.extractfile(member)
                if handle is None:
                    continue
                payload = json.load(handle)
                snap = member.name.rsplit("buses", 1)[-1].removesuffix(".json")
                for bus in payload.get("BusPositions", []):
                    event_local = bus.get("DateTime")
                    hour_local, dow_local = _local_parts(event_local)
                    yield {
                        "vehicle_id": bus.get("VehicleID"),
                        "route_id": bus.get("RouteID"),
                        "trip_id": bus.get("TripID"),
                        "direction_num": bus.get("DirectionNum"),
                        "direction": bus.get("DirectionText"),
                        "trip_headsign": bus.get("TripHeadsign"),
                        "lat": bus.get("Lat"),
                        "lon": bus.get("Lon"),
                        "deviation_min": bus.get("Deviation"),
                        "trip_start": _to_utc(bus.get("TripStartTime")),
                        "trip_end": _to_utc(bus.get("TripEndTime")),
                        "block_number": bus.get("BlockNumber"),
                        "event_time": _to_utc(event_local),
                        "hour_local": hour_local,
                        "dow_local": dow_local,
                        "snapshot_ts": _to_utc(snap, assume_tz=timezone.utc),
                        "service_date": day,
                        "source_file": member.name,
                    }
    finally:
        try:
            os.remove(tar_path)
        except OSError:
            pass


@dlt.resource(name="bus_positions", write_disposition="append")
def bus_positions(day: str) -> Iterator[dict]:
    yield from _records(day)


def _dates(start: str, end: str) -> list[str]:
    d = date.fromisoformat(start)
    stop = date.fromisoformat(end)
    out = []
    while d <= stop:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _existing_bronze_dates() -> set[str]:
    """Dates already present in bronze, so a first run does not duplicate data."""
    try:
        client = boto3.client(
            "s3",
            endpoint_url=os.environ.get("SEAWEEDFS_S3_ENDPOINT", "http://seaweedfs:8333"),
            aws_access_key_id=os.environ["WMATA_KEY_ID"],
            aws_secret_access_key=os.environ["WMATA_SECRET_KEY"],
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
            config=Config(s3={"addressing_style": "path"}),
        )
        dates: set[str] = set()
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket="wmata", Prefix="bronze_2020/bus_positions/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".jsonl.gz"):
                    continue
                body = client.get_object(Bucket="wmata", Key=key)["Body"]
                try:
                    with gzip.GzipFile(fileobj=body) as gz:
                        line = gz.readline()
                    record = json.loads(line)
                    service_date = record.get("service_date")
                    if service_date:
                        dates.add(service_date)
                except Exception:
                    continue
        return dates
    except Exception as exc:  # noqa: BLE001 - fall back to dlt state
        print(f"WARN existing-date scan failed: {exc}", flush=True)
        return set()


def _load_done() -> dict:
    try:
        with open(STATE_FILE, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return {}


def _save_done(done: dict) -> None:
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(done, fh, indent=2)
    os.replace(tmp, STATE_FILE)


def main() -> None:
    credentials = SeaweedCredentials(
        aws_access_key_id=os.environ["WMATA_KEY_ID"],
        aws_secret_access_key=os.environ["WMATA_SECRET_KEY"],
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        endpoint_url=os.environ.get("SEAWEEDFS_S3_ENDPOINT", "http://seaweedfs:8333"),
    )

    destination = filesystem(
        bucket_url="s3://wmata",
        credentials=credentials,
    )

    pipeline = dlt.pipeline(
        pipeline_name="wmata_2020",
        destination=destination,
        dataset_name="bronze_2020",
        pipelines_dir=PIPELINES_DIR,
    )

    done = _load_done()
    existing = _existing_bronze_dates()
    print(f"existing bronze dates={len(existing)}", flush=True)
    for day in _dates(START, END):
        if done.get(day, {}).get("status") == "ok":
            print(f"SKIP {day}", flush=True)
            continue
        if day in existing:
            done[day] = {
                "status": "ok",
                "loaded_at": datetime.now(timezone.utc).isoformat(),
                "reason": "existing bronze object",
            }
            _save_done(done)
            print(f"SKIP existing {day}", flush=True)
            continue
        print(f"START {day}", flush=True)
        try:
            info = pipeline.run(bus_positions(day), table_name="bus_positions")
            print(info, flush=True)
            done[day] = {
                "status": "ok",
                "loaded_at": datetime.now(timezone.utc).isoformat(),
            }
            print(f"DONE {day}", flush=True)
        except Exception as exc:  # noqa: BLE001 - continue with the next day
            print(f"ERROR {day}: {exc}", flush=True)
            done[day] = {
                "status": "error",
                "error": str(exc)[:500],
                "at": datetime.now(timezone.utc).isoformat(),
            }
        _save_done(done)

    print("ALL DONE", flush=True)


if __name__ == "__main__":
    main()
