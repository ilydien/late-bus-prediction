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
_accuracy_iteration = 0
ACCURACY_INTERVAL = 5

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


def _get_pg_connection():
    import psycopg2

    return psycopg2.connect(
        dbname_wmata=POSTGRES_DB_WMATA,
        dbname_traffic=POSTGRES_DB_TRAFFIC,
        infra_host=POSTGRES_INFRA_HOST,
        infra_port=POSTGRES_INFRA_PORT,
        infra_dbname=POSTGRES_DB_INFRA,
        infra_user=POSTGRES_INFRA_USER,
        infra_password=POSTGRES_INFRA_PASSWORD,
        silver_host=POSTGRES_SILVER_HOST,
        silver_port=POSTGRES_SILVER_PORT,
        silver_user=POSTGRES_SILVER_USER,
        silver_password=POSTGRES_SILVER_PASSWORD,
        gold_host=POSTGRES_GOLD_HOST,
        gold_port=POSTGRES_GOLD_PORT,
        gold_user=POSTGRES_GOLD_USER,
        gold_password=POSTGRES_GOLD_PASSWORD,
    )


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
    try:
        url = f"{VM_URL}/api/v1/query?query=demand_staleness_seconds"
        resp = urllib.request.urlopen(url, timeout=5)
        data = resp.json()
        results = data.get("data", {}).get("result", [])
        if not results:
            return False
        ts = float(results[0]["value"][0])
        return (time.time() - ts) < 120
    except Exception:
        return False


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


def _check_inference_staleness() -> bool:
    try:
        url = f"{VM_URL}/api/v1/query?query=max(demand_staleness_seconds)"
        resp = urllib.request.urlopen(url, timeout=5)
        data = resp.json()
        results = data.get("data", {}).get("result", [])
        if results:
            staleness = float(results[0]["value"][1])
            if staleness > 900:
                send_alert(
                    f"Inference staleness alert from VM: *{int(staleness)}s*",
                    level="WARNING",
                    throttle_key="vm_staleness",
                    throttle_seconds=600,
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


def _push_pipeline_latency() -> None:
    try:
        conn = _get_pg_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT COALESCE(MAX(ABS(EXTRACT(EPOCH FROM (updated_at - time)))), 0)
            FROM silver.demand_5min
            WHERE updated_at > NOW() - INTERVAL '5 minutes'
        """)
        row = cur.fetchone()
        latency = float(row[0]) if row else 0.0
        cur.close()
        conn.close()
        _push_metric("pipeline_latency_seconds", latency)
    except Exception as e:
        logger.warning("Pipeline latency query failed: %s", e)


def _evaluate_accuracy() -> None:
    try:
        conn = _get_pg_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT p.region_id, p.created_at,
                   p.horizon_h01, p.horizon_h02, p.horizon_h03, p.horizon_h04,
                   p.horizon_h05, p.horizon_h06, p.horizon_h07, p.horizon_h08,
                   p.horizon_h09, p.horizon_h10, p.horizon_h11, p.horizon_h12,
                   p.horizon_h13, p.horizon_h14, p.horizon_h15, p.horizon_h16,
                   p.horizon_h17, p.horizon_h18, p.horizon_h19, p.horizon_h20,
                   p.horizon_h21, p.horizon_h22, p.horizon_h23, p.horizon_h24
            FROM silver.predictions p
            WHERE p.created_at >= NOW() - INTERVAL '48 hours'
              AND p.created_at < NOW() - INTERVAL '24 hours'
            ORDER BY p.created_at DESC
            LIMIT 50
        """)
        predictions = cur.fetchall()

        if not predictions:
            cur.close()
            conn.close()
            return

        regions = {}
        for row in predictions:
            region_id = row[0]
            created_at = row[1]
            horizons = row[2:]

            for h_idx, predicted in enumerate(horizons):
                if predicted is None or predicted <= 0:
                    continue

                h = h_idx + 1
                expected_time = created_at + __import__("datetime").timedelta(hours=h)

                cur.execute(
                    """
                    SELECT demand_mw FROM silver.features_ml
                    WHERE region_id = %s AND time = %s
                """,
                    (region_id, expected_time),
                )
                actual_row = cur.fetchone()

                if actual_row and actual_row[0] and actual_row[0] > 0:
                    actual = actual_row[0]
                    mape = abs((actual - predicted) / actual) * 100

                    key = (region_id, h)
                    if key not in regions:
                        regions[key] = []
                    regions[key].append(mape)

        for (region_id, horizon), mapes in regions.items():
            avg_mape = sum(mapes) / len(mapes)
            _push_metric(
                "prediction_mape",
                avg_mape,
                {
                    "region": region_id,
                    "horizon": str(horizon),
                },
            )

        cur.close()
        conn.close()
        logger.info(
            "Accuracy evaluation done: %d region/horizon combinations", len(regions)
        )
    except Exception as e:
        logger.warning("Accuracy evaluation failed: %s", e)


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
    _check_inference_staleness()
    _push_pipeline_latency()
    _check_system_resources()

    return results


def main() -> None:
    global _accuracy_iteration
    logger.info(
        "Health monitor started (interval=%ds, threshold=%d, accuracy_every=%d)",
        CHECK_INTERVAL,
        FAILURE_THRESHOLD,
        ACCURACY_INTERVAL,
    )
    while True:
        try:
            run_checks()

            _accuracy_iteration += 1
            if _accuracy_iteration % ACCURACY_INTERVAL == 0:
                _evaluate_accuracy()
                _accuracy_iteration = 0
        except Exception as e:
            logger.exception("Health check loop error: %s", e)
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
