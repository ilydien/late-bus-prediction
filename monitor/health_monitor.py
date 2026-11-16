import logging
import os
import shutil
import sys
import time
import urllib.request
import urllib.error

import psutil

sys.path.insert(0, "/app")

from shared.alerts import send_alert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("health-monitor")

CHECK_INTERVAL = int(os.environ.get("CHECK_INTERVAL", "60"))
POSTGRES_DB_INFRA = os.environ.get("POSTGRES_DB_INFRA", "infra")
POSTGRES_DB_WMATA = os.environ.get("POSTGRES_DB_WMATA", "wmata")
POSTGRES_DB_TRAFFIC = os.environ.get("POSTGRES_DB_TRAFFIC", "traffic")
POSTGRES_INFRA_HOST = os.environ.get("POSTGRES_INFRA_HOST", "postgres-infra")
POSTGRES_INFRA_PORT = int(os.environ.get("POSTGRES_INFRA_PORT", "5432"))
POSTGRES_INFRA_USER = os.environ.get("POSTGRES_INFRA_USER", "postgres")
POSTGRES_INFRA_PASSWORD = os.environ.get("POSTGRES_INFRA_PASSWORD", "postgres")
POSTGRES_SILVER_HOST = os.environ.get("POSTGRES_SILVER_HOST", "haproxy")
POSTGRES_SILVER_PORT = int(os.environ.get("POSTGRES_SILVER_PORT", "5433"))
POSTGRES_SILVER_USER = os.environ.get("POSTGRES_SILVER_USER", "postgres")
POSTGRES_SILVER_PASSWORD = os.environ.get("POSTGRES_SILVER_PASSWORD", "postgres")
POSTGRES_GOLD_HOST = os.environ.get("POSTGRES_GOLD_HOST", "haproxy")
POSTGRES_GOLD_PORT = int(os.environ.get("POSTGRES_GOLD_PORT", "5434"))
POSTGRES_GOLD_USER = os.environ.get("POSTGRES_GOLD_USER", "postgres")
POSTGRES_GOLD_PASSWORD = os.environ.get("POSTGRES_GOLD_PASSWORD", "postgres")
NATS_URL = os.environ.get("NATS_URL", "nats://nats:8222")
DEBEZIUM_URL = os.environ.get("DEBEZIUM_URL", "http://debezium-wmata:8080")
VM_URL = os.environ.get("VM_URL", "http://victoriametrics:8428")
VL_URL = os.environ.get("VL_URL", "http://victorialogs:9428")
DAGSTER_URL = os.environ.get("DAGSTER_URL", "http://dagster-webserver:3000")
MLFLOW_URL = os.environ.get("MLFLOW_URL", "http://mlflow:5000")
INFERENCE_URL = os.environ.get("INFERENCE_URL", "http://inference:8001")
SEAWEEDFS_URL = os.environ.get("SEAWEEDFS_URL", "http://seaweedfs:9333")
DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "http://dashboard-backend:8000")
VM_API_URL = f"{VM_URL}/api/v1/import/prometheus"

_failures: dict[str, int] = {}
FAILURE_THRESHOLD = 2

CPU_THRESHOLD = 90.0
MEMORY_THRESHOLD = 80.0
DISK_THRESHOLD = 80.0


def _push_metric(name: str, value: float, labels: dict | None = None) -> None:
    label_str = (
        ",".join(f'{k}="{v}"' for k, v in sorted(labels.items())) if labels else ""
    )
    now = int(time.time())
    line = f"{name}{{{label_str}}} {value} {now}\n"
    try:
        req = urllib.request.Request(
            VM_API_URL,
            data=line.encode(),
            headers={"Content-Type": "text/plain"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        logger.warning("Failed to push %s: %s", name, e)


def _http_get(url: str, timeout: int = 5) -> bool:
    try:
        resp = urllib.request.urlopen(url, timeout=timeout)
        return 200 <= resp.status < 400
    except Exception:
        return False


def _check_postgres_infra() -> bool:
    import socket

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect((POSTGRES_INFRA_HOST, POSTGRES_INFRA_PORT))
        s.close()
        return True
    except Exception:
        return False


def _check_postgres_silver() -> bool:
    import socket

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect((POSTGRES_SILVER_HOST, POSTGRES_SILVER_PORT))
        s.close()
        return True
    except Exception:
        return False


def _check_postgres_gold() -> bool:
    import socket

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect((POSTGRES_GOLD_HOST, POSTGRES_GOLD_PORT))
        s.close()
        return True
    except Exception:
        return False


def _check_nats() -> bool:
    return _http_get(f"http://{NATS_URL.split('//')[-1]}/healthz")


def _check_debezium() -> bool:
    return _http_get(f"{DEBEZIUM_URL}/q/health/ready", timeout=5)


def _check_victoriametrics() -> bool:
    return _http_get(f"{VM_URL}/-/healthy")


def _check_victorialogs() -> bool:
    return _http_get(f"{VL_URL}/health")


def _check_dagster() -> bool:
    return _http_get(f"{DAGSTER_URL}/dagit_info")


def _check_mlflow() -> bool:
    return _http_get(f"{MLFLOW_URL}/health")


def _check_inference() -> bool:
    return _http_get(f"{INFERENCE_URL}/health")


def _check_seaweedfs() -> bool:
    return _http_get(f"{SEAWEEDFS_URL}/cluster/status")


def _check_dashboard() -> bool:
    return _http_get(f"{DASHBOARD_URL}/health")


def _check_vm_disk() -> bool:
    try:
        url = f"{VM_URL}/api/v1/query?query=min(vm_free_disk_space_bytes)"
        resp = urllib.request.urlopen(url, timeout=5)
        data = resp.json()
        results = data.get("data", {}).get("result", [])
        if results:
            free_bytes = float(results[0]["value"][1])
            free_gb = free_bytes / (1024**3)
            if free_gb < 1.0:
                send_alert(
                    f"VictoriaMetrics disk low: *{free_gb:.1f}GB* free",
                    level="CRITICAL",
                    throttle_key="vm_disk_low",
                    throttle_seconds=1800,
                )
                return False
        return True
    except Exception:
        return True


def _check_system_resources() -> None:
    try:
        cpu = psutil.cpu_percent(interval=1)
        mem = psutil.virtual_memory().percent
        disk = shutil.disk_usage("/")
        disk_pct = (disk.used / disk.total) * 100

        _push_metric("system_cpu_percent", cpu)
        _push_metric("system_memory_percent", mem)
        _push_metric("system_disk_percent", disk_pct)

        if cpu > CPU_THRESHOLD:
            send_alert(
                f"System CPU usage *{cpu:.1f}%* exceeds {CPU_THRESHOLD}%",
                level="CRITICAL",
                throttle_key="system_cpu",
                throttle_seconds=600,
            )
        if mem > MEMORY_THRESHOLD:
            send_alert(
                f"System RAM usage *{mem:.1f}%* exceeds {MEMORY_THRESHOLD}%",
                level="CRITICAL",
                throttle_key="system_mem",
                throttle_seconds=600,
            )
        if disk_pct > DISK_THRESHOLD:
            send_alert(
                f"System disk usage *{disk_pct:.1f}%* exceeds {DISK_THRESHOLD}%",
                level="CRITICAL",
                throttle_key="system_disk",
                throttle_seconds=600,
            )
    except Exception as e:
        logger.warning("System resource check failed: %s", e)


def _alert_if_persistent(name: str, ok: bool) -> None:
    if ok:
        _failures.pop(name, None)
        return
    _failures[name] = _failures.get(name, 0) + 1
    if _failures[name] == FAILURE_THRESHOLD:
        send_alert(
            f"Service *{name}* is DOWN (failed {FAILURE_THRESHOLD} consecutive checks)",
            level="CRITICAL",
            throttle_key=f"monitor_{name}",
            throttle_seconds=600,
        )
    elif _failures[name] > FAILURE_THRESHOLD and _failures[name] % 10 == 0:
        send_alert(
            f"Service *{name}* still DOWN ({_failures[name]} consecutive failures)",
            level="CRITICAL",
            throttle_key=f"monitor_{name}",
            throttle_seconds=600,
        )


def _push_health_metrics(checks_results: list[tuple[str, bool]]) -> None:
    for name, ok in checks_results:
        _push_metric("service_up", 1.0 if ok else 0.0, {"service": name})


def run_checks() -> list[tuple[str, bool]]:
    checks = [
        ("PostgreSQL Infra", _check_postgres_infra),
        ("PostgreSQL Silver", _check_postgres_silver),
        ("PostgreSQL Gold", _check_postgres_gold),
        ("NATS", _check_nats),
        ("Debezium", _check_debezium),
        ("VictoriaMetrics", _check_victoriametrics),
        ("VictoriaLogs", _check_victorialogs),
        ("Dagster", _check_dagster),
        ("MLflow", _check_mlflow),
        ("Inference", _check_inference),
        ("SeaweedFS", _check_seaweedfs),
        ("Dashboard Backend", _check_dashboard),
    ]
    results = []
    for name, check_fn in checks:
        ok = check_fn()
        _alert_if_persistent(name, ok)
        results.append((name, ok))

    _push_health_metrics(results)
    _check_vm_disk()
    _check_system_resources()

    return results


def main() -> None:
    logger.info(
        "Health monitor started (interval=%ds, threshold=%d)",
        CHECK_INTERVAL,
        FAILURE_THRESHOLD,
    )
    while True:
        try:
            run_checks()
        except Exception as e:
            logger.exception("Health check loop error: %s", e)
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
