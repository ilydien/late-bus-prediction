"""Platform health probe, Prometheus metrics endpoint, and vmalert -> Telegram relay."""

from __future__ import annotations

import json
import logging
import os
import shutil
import signal
import socket
import sys
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable

import psutil

sys.path.insert(0, "/app")

from shared.alerts import send_alert
from shared.logging import run_context, setup_json_logging

setup_json_logging("health-monitor")
logger = logging.getLogger("health-monitor")

CHECK_INTERVAL = int(os.environ.get("CHECK_INTERVAL", "30"))
MONITOR_PORT = int(os.environ.get("MONITOR_PORT", "8090"))

POSTGRES_INFRA_HOST = os.environ.get("POSTGRES_INFRA_HOST", "postgres-infra")
POSTGRES_INFRA_PORT = int(os.environ.get("POSTGRES_INFRA_PORT", "5432"))
POSTGRES_SILVER_HOST = os.environ.get("POSTGRES_SILVER_HOST", "haproxy")
POSTGRES_SILVER_PORT = int(os.environ.get("POSTGRES_SILVER_PORT", "5433"))
POSTGRES_GOLD_HOST = os.environ.get("POSTGRES_GOLD_HOST", "haproxy")
POSTGRES_GOLD_PORT = int(os.environ.get("POSTGRES_GOLD_PORT", "5434"))

HAPROXY_STATS_URL = os.environ.get("HAPROXY_STATS_URL", "http://haproxy:8404")
SEAWEEDFS_URL = os.environ.get("SEAWEEDFS_URL", "http://seaweedfs:9333")
SEAWEEDFS_BRIDGE_URL = os.environ.get("SEAWEEDFS_BRIDGE_URL", "http://seaweedfs-bridge:8080")
NATS_MON_URL = os.environ.get("NATS_MON_URL", "http://nats:8222")
DEBEZIUM_WMATA_URL = os.environ.get("DEBEZIUM_WMATA_URL", "http://debezium-wmata:8080")
DEBEZIUM_TRAFFIC_URL = os.environ.get("DEBEZIUM_TRAFFIC_URL", "http://debezium-traffic:8080")
DAGSTER_URL = os.environ.get("DAGSTER_URL", "http://dagster-webserver:3000")
MLFLOW_URL = os.environ.get("MLFLOW_URL", "http://mlflow:5000")
TRINO_URL = os.environ.get("TRINO_URL", "http://trino:8080")
VM_URL = os.environ.get("VM_URL", "http://victoriametrics:8428")
VL_URL = os.environ.get("VL_URL", "http://victorialogs:9428")
VECTOR_URL = os.environ.get("VECTOR_URL", "http://vector:8686")
VMALERT_URL = os.environ.get("VMALERT_URL", "http://vmalert:8880")
SERVING_API_URL = os.environ.get("SERVING_API_URL", "http://serving-api:8000")
SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL", "http://spark-master:8080")
SPARK_WORKER_URL = os.environ.get("SPARK_WORKER_URL", "http://spark-worker:8081")
CADVISOR_URL = os.environ.get("CADVISOR_URL", "http://cadvisor:8080")
NATS_EXPORTER_URL = os.environ.get("NATS_EXPORTER_URL", "http://nats-exporter:7777")
DAGSTER_USER_CODE_HOST = os.environ.get("DAGSTER_USER_CODE_HOST", "dagster-user-code")
DAGSTER_USER_CODE_PORT = int(os.environ.get("DAGSTER_USER_CODE_PORT", "4000"))
CONSUMER_HEARTBEAT_PATH = os.environ.get("CONSUMER_HEARTBEAT_PATH", "/work/consumer/consumer_heartbeat")
WMATA_POLLER_HEARTBEAT_PATH = os.environ.get("WMATA_POLLER_HEARTBEAT_PATH", "/work/dlt/wmata_poller_heartbeat")
TOMTOM_POLLER_HEARTBEAT_PATH = os.environ.get("TOMTOM_POLLER_HEARTBEAT_PATH", "/work/dlt/tomtom_poller_heartbeat")
HEARTBEAT_MAX_AGE_SEC = int(os.environ.get("HEARTBEAT_MAX_AGE_SEC", "1800"))

_STATE_LOCK = threading.Lock()
_latest_checks: dict[str, dict[str, object]] = {}
_latest_host_metrics: dict[str, float] = {}
_last_run_ts: float = 0.0
_alert_counts: dict[tuple[str, str], int] = {}
_firing_keys: set[str] = set()


def _http_ok(url: str, timeout: int = 5) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return 200 <= resp.status < 400
    except Exception:
        return False


def _tcp_ok(host: str, port: int, timeout: int = 3) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def _heartbeat_ok(path: str, max_age_s: int = HEARTBEAT_MAX_AGE_SEC) -> bool:
    try:
        mtime = os.path.getmtime(path)
        return (time.time() - mtime) < max_age_s
    except OSError:
        return False


def _cadvisor_up(compose_service: str, max_age_s: int = 120) -> bool:
    rows = _query_vm(
        f'time() - max(container_last_seen{{container_label_com_docker_compose_service="{compose_service}"}})'
    )
    if not rows:
        return False
    return float(rows[0].get("value", 9999.0)) < max_age_s


def _telegram_configured() -> bool:
    return bool(os.environ.get("TELEGRAM_BOT_API_KEY", "").strip() and os.environ.get("TELEGRAM_CHAT_ID", "").strip())


def _build_checks() -> list[tuple[str, Callable[[], bool]]]:
    return [
        ("postgres-infra", lambda: _tcp_ok(POSTGRES_INFRA_HOST, POSTGRES_INFRA_PORT)),
        ("haproxy-silver-rw", lambda: _tcp_ok(POSTGRES_SILVER_HOST, POSTGRES_SILVER_PORT)),
        ("haproxy-gold-rw", lambda: _tcp_ok(POSTGRES_GOLD_HOST, POSTGRES_GOLD_PORT)),
        ("haproxy-stats", lambda: _http_ok(f"{HAPROXY_STATS_URL}/")),
        ("seaweedfs", lambda: _http_ok(f"{SEAWEEDFS_URL}/cluster/status")),
        ("seaweedfs-bridge", lambda: _http_ok(f"{SEAWEEDFS_BRIDGE_URL}/health")),
        ("nats", lambda: _http_ok(f"{NATS_MON_URL}/healthz")),
        ("nats-exporter", lambda: _http_ok(f"{NATS_EXPORTER_URL}/metrics")),
        ("debezium-wmata", lambda: _http_ok(f"{DEBEZIUM_WMATA_URL}/q/health/ready")),
        ("debezium-traffic", lambda: _http_ok(f"{DEBEZIUM_TRAFFIC_URL}/q/health/ready")),
        ("dagster-webserver", lambda: _http_ok(f"{DAGSTER_URL}/dagit_info")),
        ("dagster-user-code", lambda: _tcp_ok(DAGSTER_USER_CODE_HOST, DAGSTER_USER_CODE_PORT)),
        ("mlflow", lambda: _http_ok(f"{MLFLOW_URL}/health")),
        ("trino", lambda: _http_ok(f"{TRINO_URL}/v1/info")),
        ("spark-master", lambda: _http_ok(f"{SPARK_MASTER_URL}/")),
        ("spark-worker", lambda: _http_ok(f"{SPARK_WORKER_URL}/")),
        ("serving-api", lambda: _http_ok(f"{SERVING_API_URL}/health")),
        ("consumer", lambda: _heartbeat_ok(CONSUMER_HEARTBEAT_PATH)),
        ("wmata-poller", lambda: _heartbeat_ok(WMATA_POLLER_HEARTBEAT_PATH)),
        ("tomtom-poller", lambda: _heartbeat_ok(TOMTOM_POLLER_HEARTBEAT_PATH)),
        ("stream-inference", lambda: _cadvisor_up("stream-inference")),
        ("victoriametrics", lambda: _http_ok(f"{VM_URL}/-/healthy")),
        ("victorialogs", lambda: _http_ok(f"{VL_URL}/health")),
        ("vector", lambda: _http_ok(f"{VECTOR_URL}/health")),
        ("vmalert", lambda: _http_ok(f"{VMALERT_URL}/-/healthy")),
        ("cadvisor", lambda: _http_ok(f"{CADVISOR_URL}/healthz")),
    ]


def run_checks() -> dict[str, dict[str, object]]:
    """Run all service health probes and update in-memory metrics snapshot."""
    global _last_run_ts
    results: dict[str, dict[str, object]] = {}

    for name, fn in _build_checks():
        t0 = time.monotonic()
        ok = fn()
        elapsed = round(time.monotonic() - t0, 4)
        results[name] = {"up": ok, "status": "ok" if ok else "down", "duration_s": elapsed}
        if not ok:
            logger.warning("Service check failed: %s", name, extra={"domain": "infra", "layer": "ops", "job": "health_probe"})

    cpu = psutil.cpu_percent(interval=None)
    mem = psutil.virtual_memory().percent
    disk = shutil.disk_usage("/")
    disk_pct = round((disk.used / disk.total) * 100.0, 2)

    with _STATE_LOCK:
        _latest_checks.clear()
        _latest_checks.update(results)
        _latest_host_metrics.clear()
        _latest_host_metrics.update(
            {
                "cpu_percent": float(cpu),
                "memory_percent": float(mem),
                "disk_percent": float(disk_pct),
            }
        )
        _last_run_ts = time.time()

    up_count = sum(1 for item in results.values() if item["up"])
    logger.info(
        "Health probe completed: %d/%d services UP",
        up_count,
        len(results),
        extra={"domain": "infra", "layer": "ops", "job": "health_probe"},
    )
    return results


def render_prometheus_metrics() -> str:
    """Render current health probe and alert relay counters in Prometheus exposition format."""
    lines = [
        "# HELP wmata_service_up Service health status (1=up, 0=down)",
        "# TYPE wmata_service_up gauge",
        "# HELP wmata_service_check_duration_seconds Health check duration in seconds",
        "# TYPE wmata_service_check_duration_seconds gauge",
    ]
    with _STATE_LOCK:
        checks_copy = dict(_latest_checks)
        host_copy = dict(_latest_host_metrics)
        alert_copy = dict(_alert_counts)
        last_ts = _last_run_ts

    for svc, info in sorted(checks_copy.items()):
        up_val = 1 if info["up"] else 0
        lines.append(f'wmata_service_up{{service="{svc}"}} {up_val}')
        lines.append(f'wmata_service_check_duration_seconds{{service="{svc}"}} {info["duration_s"]}')

    lines.extend(
        [
            "# HELP wmata_monitor_last_run_timestamp Unix timestamp of last health probe cycle",
            "# TYPE wmata_monitor_last_run_timestamp gauge",
            f"wmata_monitor_last_run_timestamp {last_ts:.3f}",
        ]
    )

    for metric_key, prom_name in (
        ("cpu_percent", "wmata_host_cpu_percent"),
        ("memory_percent", "wmata_host_memory_percent"),
        ("disk_percent", "wmata_host_disk_percent"),
    ):
        if metric_key in host_copy:
            lines.append(f"# TYPE {prom_name} gauge")
            lines.append(f"{prom_name} {host_copy[metric_key]}")

    if alert_copy:
        lines.append("# HELP wmata_alert_notifications_total Alert notifications processed by relay")
        lines.append("# TYPE wmata_alert_notifications_total counter")
        for (alertname, status), count in sorted(alert_copy.items()):
            lines.append(
                f'wmata_alert_notifications_total{{alertname="{alertname}",status="{status}"}} {count}'
            )

    return "\n".join(lines) + "\n"


def _parse_iso_ts(value: str | None) -> float | None:
    if not value or value.startswith("0001-"):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.timestamp()
    except ValueError:
        return None


def _is_alert_resolved(alert: dict, top_status: str | None = None) -> bool:
    if top_status and top_status.lower() == "resolved":
        return True
    if str(alert.get("status", "")).lower() == "resolved":
        return True
    ends_ts = _parse_iso_ts(alert.get("endsAt"))
    if ends_ts is not None and ends_ts <= time.time() + 2.0:
        return True
    return False


def handle_alertmanager_payload(payload: object) -> int:
    """Process vmalert / Alertmanager webhook payload and dispatch Telegram notifications."""
    top_status: str | None = None
    if isinstance(payload, list):
        alerts = payload
    elif isinstance(payload, dict):
        top_status = payload.get("status")
        alerts = payload.get("alerts", [])
    else:
        return 0

    processed = 0
    for alert in alerts:
        if not isinstance(alert, dict):
            continue
        labels = alert.get("labels", {})
        annotations = alert.get("annotations", {})
        alert_name = str(labels.get("alertname", "UnknownAlert"))
        severity = str(labels.get("severity", "CRITICAL")).upper()
        service = str(labels.get("service", labels.get("job", "platform")))
        summary = str(annotations.get("summary", alert_name))
        description = str(annotations.get("description", ""))
        runbook = annotations.get("runbook")

        resolved = _is_alert_resolved(alert, top_status)
        status = "RESOLVED" if resolved else "FIRING"

        label_sig = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        dedup_key = f"{alert_name}:{label_sig}"

        with _STATE_LOCK:
            was_firing = dedup_key in _firing_keys
            if resolved:
                _firing_keys.discard(dedup_key)
            else:
                _firing_keys.add(dedup_key)
            _alert_counts[(alert_name, status)] = _alert_counts.get((alert_name, status), 0) + 1

        if resolved and not was_firing:
            processed += 1
            continue

        body_parts = [f"*Service:* `{service}`", f"*Summary:* {summary}"]
        if description:
            body_parts.append(f"*Detail:* {description}")
        message = "\n".join(body_parts)

        logger.info(
            "Alert relay %s: %s (%s)",
            status,
            alert_name,
            service,
            extra={"domain": "infra", "layer": "ops", "job": "alert_relay"},
        )
        send_alert(
            message=message,
            level=severity,
            throttle_key=dedup_key,
            throttle_seconds=300,
            alert_name=alert_name,
            status=status,
            runbook=str(runbook) if runbook else None,
        )
        processed += 1

    return processed


def _fetch_json(url: str, timeout: int = 5) -> object | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None


def _query_vm(promql: str) -> list[dict[str, object]]:
    encoded = urllib.parse.urlencode({"query": promql})
    data = _fetch_json(f"{VM_URL.rstrip('/')}/api/v1/query?{encoded}")
    if not isinstance(data, dict):
        return []
    result = data.get("data", {}).get("result", [])
    rows: list[dict[str, object]] = []
    for item in result:
        val = item.get("value", [None, None])
        try:
            num = float(val[1])
        except (TypeError, ValueError, IndexError):
            num = 0.0
        rows.append({"labels": item.get("metric", {}), "value": num})
    return rows


def build_monitoring_summary() -> dict[str, object]:
    """Build a unified monitoring JSON snapshot for the React dashboard."""
    with _STATE_LOCK:
        services = dict(_latest_checks)
        host = dict(_latest_host_metrics)
        last_run = _last_run_ts

    alerts_raw = _fetch_json(f"{VMALERT_URL.rstrip('/')}/api/v1/alerts")
    alerts = alerts_raw.get("data", {}).get("alerts", []) if isinstance(alerts_raw, dict) else []

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "last_probe_ts": last_run,
        "telegram_configured": _telegram_configured(),
        "services": services,
        "host": host,
        "alerts": alerts,
        "metrics": {
            "container_cpu_pct": _query_vm('sum by (name) (rate(container_cpu_usage_seconds_total{name!=""}[1m])) * 100'),
            "container_memory_bytes": _query_vm('container_memory_working_set_bytes{name!=""}'),
            "nats_stream_messages": _query_vm("jetstream_stream_total_messages"),
            "nats_consumer_pending": _query_vm("jetstream_consumer_num_pending"),
            "pg_up": _query_vm("pg_up"),
            "spock_replicating": _query_vm("spock_subscription_is_replicating"),
            "haproxy_active_servers": _query_vm('haproxy_backend_active_servers{proxy=~"silver_.*|gold_.*"}'),
        },
    }


def query_logs(
    service: str = "",
    level: str = "",
    run_id: str = "",
    limit: int = 50,
) -> dict[str, object]:
    """Query structured logs from VictoriaLogs for dashboard log exploration."""
    terms: list[str] = []
    for key, val in (("service", service), ("level", level), ("run_id", run_id)):
        clean = val.strip().replace('"', "")
        if clean:
            terms.append(f'{key}:"{clean}"')
    logsql = " ".join(terms) if terms else "*"
    bounded_limit = max(1, min(int(limit), 200))
    params = urllib.parse.urlencode({"query": logsql, "limit": str(bounded_limit)})
    url = f"{VL_URL.rstrip('/')}/select/logsql/query?{params}"

    entries: list[dict[str, object]] = []
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            for raw_line in resp.read().decode("utf-8").splitlines():
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except Exception as exc:
        logger.warning("VictoriaLogs query failed: %s", exc)

    return {"query": logsql, "count": len(entries), "logs": entries}


class MonitorHandler(BaseHTTPRequestHandler):
    """HTTP handler serving /health, /metrics, /api/monitoring/*, and /api/v2/alerts."""

    server_version = "wmata-health-monitor/1.0"

    def log_message(self, fmt: str, *args: object) -> None:
        logger.debug("http: " + fmt, *args)

    def _send_bytes(self, status: int, content_type: str, data: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(data)

    def do_OPTIONS(self) -> None:
        self._send_bytes(204, "text/plain; charset=utf-8", b"")

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        if path == "/health":
            with _STATE_LOCK:
                checks = {k: v["status"] for k, v in _latest_checks.items()}
                last_run = _last_run_ts
            body = json.dumps(
                {
                    "status": "ok",
                    "service": "health-monitor",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "last_probe_ts": last_run,
                    "telegram_configured": _telegram_configured(),
                    "checks": checks,
                }
            ).encode("utf-8")
            self._send_bytes(200, "application/json", body)
        elif path == "/metrics":
            body = render_prometheus_metrics().encode("utf-8")
            self._send_bytes(200, "text/plain; version=0.0.4; charset=utf-8", body)
        elif path == "/api/monitoring/summary":
            body = json.dumps(build_monitoring_summary()).encode("utf-8")
            self._send_bytes(200, "application/json", body)
        elif path == "/api/monitoring/logs":
            qs = urllib.parse.parse_qs(parsed.query)
            limit_raw = qs.get("limit", ["50"])[0]
            try:
                limit = int(limit_raw)
            except ValueError:
                limit = 50
            result = query_logs(
                service=qs.get("service", [""])[0],
                level=qs.get("level", [""])[0],
                run_id=qs.get("run_id", [""])[0],
                limit=limit,
            )
            self._send_bytes(200, "application/json", json.dumps(result).encode("utf-8"))
        else:
            self._send_bytes(404, "text/plain; charset=utf-8", b"not found")

    def do_POST(self) -> None:
        if self.path == "/api/monitoring/test-alert":
            try:
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length > 0 else b"{}"
                body = json.loads(raw) if raw.strip() else {}
            except (ValueError, json.JSONDecodeError):
                self._send_bytes(400, "application/json", b'{"error":"invalid json"}')
                return

            status = str(body.get("status", "FIRING")).upper()
            if status not in ("FIRING", "RESOLVED"):
                status = "FIRING"
            level = str(body.get("level", "INFO")).upper()
            service = str(body.get("service", "health-monitor"))
            custom_msg = str(body.get("message", "Smoke test alert from WMATA health-monitor"))

            with run_context("test-alert"):
                configured = _telegram_configured()
                msg = f"*Service:* `{service}`\n*Summary:* {custom_msg}"
                sent = send_alert(
                    message=msg,
                    level=level,
                    throttle_key=None if body.get("force", True) else f"test:{service}",
                    alert_name="SmokeTestAlert",
                    status=status,
                    runbook="docs/runbooks/alerts.md",
                )
                logger.info(
                    "Test alert dispatched status=%s configured=%s sent=%s",
                    status,
                    configured,
                    sent,
                    extra={"domain": "infra", "layer": "ops", "job": "test_alert"},
                )
            resp_status = 200 if (sent or not configured) else 502
            payload = {
                "status": "sent" if sent else ("not_configured" if not configured else "failed"),
                "telegram_configured": configured,
                "sent": sent,
                "alert_status": status,
            }
            self._send_bytes(resp_status, "application/json", json.dumps(payload).encode("utf-8"))
            return

        if self.path not in ("/api/v2/alerts", "/api/v1/alerts", "/alert-webhook"):
            self._send_bytes(404, "text/plain; charset=utf-8", b"not found")
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            payload = json.loads(raw)
        except (ValueError, json.JSONDecodeError):
            self._send_bytes(400, "application/json", b'{"error":"invalid json"}')
            return

        with run_context("alert"):
            count = handle_alertmanager_payload(payload)
        resp = json.dumps({"status": "ok", "processed": count}).encode("utf-8")
        self._send_bytes(200, "application/json", resp)


def _probe_loop(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        with run_context("probe"):
            try:
                run_checks()
            except Exception as exc:
                logger.exception(
                    "Health check loop error: %s",
                    exc,
                    extra={"domain": "infra", "layer": "ops", "job": "health_probe"},
                )
        stop_event.wait(CHECK_INTERVAL)


def main() -> None:
    stop_event = threading.Event()
    server = ThreadingHTTPServer(("0.0.0.0", MONITOR_PORT), MonitorHandler)
    server.daemon_threads = True

    def _shutdown(signum: int, _frame: object) -> None:
        logger.info("Received signal %s, shutting down", signum)
        stop_event.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    # Initial probe before serving requests
    with run_context("probe-init"):
        run_checks()

    probe_thread = threading.Thread(target=_probe_loop, args=(stop_event,), daemon=True)
    probe_thread.start()

    logger.info("Health monitor listening on :%d (interval=%ds)", MONITOR_PORT, CHECK_INTERVAL)
    try:
        server.serve_forever()
    finally:
        stop_event.set()
        server.server_close()


if __name__ == "__main__":
    main()
