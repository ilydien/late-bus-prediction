"""Lightweight Prometheus-format metrics helper targeting VictoriaMetrics."""

from __future__ import annotations

import contextlib
import logging
import os
import threading
import time
import urllib.request
from typing import Iterator

from shared.logging import run_context

logger = logging.getLogger(__name__)

_COUNTER_LOCK = threading.Lock()
_COUNTERS: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}


def _vm_import_url() -> str:
    base = os.environ.get("VM_URL", "http://victoriametrics:8428").rstrip("/")
    return f"{base}/api/v1/import/prometheus"


def _format_labels(labels: dict[str, str] | None) -> str:
    if not labels:
        return ""
    escaped = []
    for k, v in sorted(labels.items()):
        clean_v = str(v).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")
        escaped.append(f'{k}="{clean_v}"')
    return "{" + ",".join(escaped) + "}"


def format_prometheus_line(
    name: str,
    value: float | int,
    labels: dict[str, str] | None = None,
    timestamp_ms: int | None = None,
) -> str:
    """Render a single metric sample in Prometheus text exposition format."""
    label_str = _format_labels(labels)
    ts_part = f" {timestamp_ms}" if timestamp_ms is not None else ""
    return f"{name}{label_str} {float(value)}{ts_part}\n"


def push_metrics_batch(lines: list[str], timeout: int = 5) -> bool:
    """Push multiple Prometheus exposition lines to VictoriaMetrics in one POST."""
    if not lines:
        return True
    payload = "".join(lines).encode("utf-8")
    req = urllib.request.Request(
        _vm_import_url(),
        data=payload,
        headers={"Content-Type": "text/plain; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return 200 <= resp.status < 300
    except Exception as exc:
        logger.warning("Failed to push metrics batch to VictoriaMetrics: %s", exc)
        return False


def push_metric(
    name: str,
    value: float | int,
    labels: dict[str, str] | None = None,
    timestamp_ms: int | None = None,
) -> bool:
    """Push a single metric sample to VictoriaMetrics."""
    ts = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
    return push_metrics_batch([format_prometheus_line(name, value, labels, ts)])


def inc_counter(
    name: str,
    labels: dict[str, str] | None = None,
    amount: float = 1.0,
) -> float:
    """Increment an in-process monotonic counter and push the updated value."""
    frozen = tuple(sorted((str(k), str(v)) for k, v in (labels or {}).items()))
    key = (name, frozen)
    with _COUNTER_LOCK:
        total = _COUNTERS.get(key, 0.0) + amount
        _COUNTERS[key] = total
    push_metric(name, total, dict(frozen))
    return total


@contextlib.contextmanager
def track_job(
    service: str,
    job: str,
    domain: str = "wmata",
    layer: str = "silver",
) -> Iterator[str]:
    """Bind run_id, emit JSON lifecycle logs, and record job metrics to VictoriaMetrics."""
    base_labels = {"service": service, "job": job, "domain": domain, "layer": layer}
    start = time.monotonic()
    with run_context(prefix=job) as rid:
        logger.info("Job started: %s", job, extra={"domain": domain, "layer": layer, "job": job})
        try:
            yield rid
        except Exception as exc:
            duration = time.monotonic() - start
            inc_counter("wmata_pipeline_job_runs_total", {**base_labels, "status": "failed"})
            push_metric("wmata_pipeline_job_duration_seconds", round(duration, 4), base_labels)
            logger.exception(
                "Job failed: %s",
                job,
                extra={
                    "domain": domain,
                    "layer": layer,
                    "job": job,
                    "duration_ms": round(duration * 1000, 2),
                    "error_type": type(exc).__name__,
                },
            )
            raise
        else:
            duration = time.monotonic() - start
            now_s = time.time()
            inc_counter("wmata_pipeline_job_runs_total", {**base_labels, "status": "success"})
            push_metrics_batch(
                [
                    format_prometheus_line(
                        "wmata_pipeline_job_duration_seconds",
                        round(duration, 4),
                        base_labels,
                        int(now_s * 1000),
                    ),
                    format_prometheus_line(
                        "wmata_pipeline_job_last_success_timestamp",
                        now_s,
                        base_labels,
                        int(now_s * 1000),
                    ),
                ]
            )
            logger.info(
                "Job completed: %s",
                job,
                extra={
                    "domain": domain,
                    "layer": layer,
                    "job": job,
                    "duration_ms": round(duration * 1000, 2),
                },
            )
