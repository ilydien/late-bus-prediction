"""Unit tests for monitor.health_monitor endpoints and alert relay."""

from __future__ import annotations

import json
from io import BytesIO
from unittest.mock import patch

from monitor import health_monitor


def test_run_checks_and_render_prometheus_metrics():
    with (
        patch("monitor.health_monitor._tcp_ok", return_value=True),
        patch("monitor.health_monitor._http_ok", return_value=True),
        patch("monitor.health_monitor._heartbeat_ok", return_value=True),
        patch("monitor.health_monitor._cadvisor_up", return_value=True),
    ):
        results = health_monitor.run_checks()

    for expected in (
        "postgres-infra",
        "mlflow",
        "vector",
        "serving-api",
        "vmalert",
        "spark-master",
        "spark-worker",
        "dagster-daemon",
        "dagster-user-code",
        "consumer",
        "wmata-poller",
        "tomtom-poller",
        "stream-inference",
    ):
        assert expected in results
    assert all(item["up"] is True for item in results.values())

    metrics_text = health_monitor.render_prometheus_metrics()
    assert 'wmata_service_up{service="mlflow"} 1' in metrics_text
    assert 'wmata_service_up{service="serving-api"} 1' in metrics_text
    assert 'wmata_service_up{service="consumer"} 1' in metrics_text
    assert "wmata_host_cpu_percent" in metrics_text
    assert "cpu_cores" in health_monitor._latest_host_metrics
    assert "memory_used_gb" in health_monitor._latest_host_metrics
    assert "disk_total_gb" in health_monitor._latest_host_metrics


def test_handle_alertmanager_firing_and_resolved_lifecycle():
    health_monitor._firing_keys.clear()
    health_monitor._alert_counts.clear()

    firing_payload = [
        {
            "labels": {
                "alertname": "ServiceDown",
                "severity": "critical",
                "service": "seaweedfs-bridge",
            },
            "annotations": {
                "summary": "Service seaweedfs-bridge is DOWN",
                "description": "Health probe failed for 1 minute",
                "runbook": "docs/runbooks/alerts.md#alert-c-servicedown-resourcehigh",
            },
            "startsAt": "2026-11-15T07:00:00Z",
            "endsAt": "2099-01-01T00:00:00Z",
        }
    ]

    resolved_payload = [
        {
            "labels": {
                "alertname": "ServiceDown",
                "severity": "critical",
                "service": "seaweedfs-bridge",
            },
            "annotations": {
                "summary": "Service seaweedfs-bridge is DOWN",
            },
            "startsAt": "2026-11-15T07:00:00Z",
            "endsAt": "2026-11-15T07:05:00Z",
        }
    ]

    with patch("monitor.health_monitor.send_alert", return_value=True) as mock_send:
        assert health_monitor.handle_alertmanager_payload(firing_payload) == 1
        assert mock_send.call_count == 1
        assert mock_send.call_args.kwargs["status"] == "FIRING"
        assert mock_send.call_args.kwargs["alert_name"] == "ServiceDown"

        assert health_monitor.handle_alertmanager_payload(resolved_payload) == 1
        assert mock_send.call_count == 2
        assert mock_send.call_args.kwargs["status"] == "RESOLVED"

        # A duplicate resolved when not firing should not spam Telegram
        assert health_monitor.handle_alertmanager_payload(resolved_payload) == 1
        assert mock_send.call_count == 2


def test_build_monitoring_summary_and_query_logs():
    def fake_fetch(url: str, timeout: int = 5):
        if "/api/v1/alerts" in url:
            return {"data": {"alerts": [{"name": "ServiceDown", "state": "inactive"}]}}
        if "/api/v1/query" in url:
            return {"data": {"result": [{"metric": {"layer": "silver"}, "value": [1700000000, "1"]}]}}
        return None

    with patch("monitor.health_monitor._fetch_json", side_effect=fake_fetch):
        summary = health_monitor.build_monitoring_summary()

    assert "services" in summary
    assert "metrics" in summary
    assert len(summary["alerts"]) == 1
    assert summary["metrics"]["spock_replicating"][0]["value"] == 1.0

    ndjson = b'{"_msg":"ok","run_id":"run-123","service":"health-monitor"}\n'
    with patch("monitor.health_monitor.urllib.request.urlopen") as mock_open:
        mock_open.return_value.__enter__.return_value.read.return_value = ndjson
        logs = health_monitor.query_logs(service="health-monitor", run_id="run-123", limit=10)

    assert logs["count"] == 1
    assert logs["logs"][0]["run_id"] == "run-123"
    assert 'service:"health-monitor"' in logs["query"]
    assert 'run_id:"run-123"' in logs["query"]


def test_heartbeat_and_cadvisor_probes(tmp_path, monkeypatch):
    hb = tmp_path / "hb"
    hb.write_text('{"status":"ok"}', encoding="utf-8")
    assert health_monitor._heartbeat_ok(str(hb), max_age_s=60) is True
    assert health_monitor._heartbeat_ok(str(tmp_path / "missing"), max_age_s=60) is False

    with patch("monitor.health_monitor._query_vm", return_value=[{"labels": {}, "value": 15.0}]):
        assert health_monitor._cadvisor_up("stream-inference", max_age_s=120) is True
    with patch("monitor.health_monitor._query_vm", return_value=[{"labels": {}, "value": 250.0}]):
        assert health_monitor._cadvisor_up("stream-inference", max_age_s=120) is False

    monkeypatch.delenv("TELEGRAM_BOT_API_KEY", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    assert health_monitor._telegram_configured() is False
    monkeypatch.setenv("TELEGRAM_BOT_API_KEY", "bot-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")
    assert health_monitor._telegram_configured() is True


def test_http_test_alert_endpoint(monkeypatch):
    import threading
    import urllib.request
    from http.server import ThreadingHTTPServer

    server = ThreadingHTTPServer(("127.0.0.1", 0), health_monitor.MonitorHandler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        monkeypatch.setenv("TELEGRAM_BOT_API_KEY", "bot-token")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/monitoring/test-alert",
            data=json.dumps({"status": "FIRING", "level": "INFO", "message": "test"}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with patch("monitor.health_monitor.send_alert", return_value=True) as mock_send:
            with urllib.request.urlopen(req, timeout=5) as resp:
                assert resp.status == 200
                body = json.loads(resp.read().decode("utf-8"))
        assert body["status"] == "sent"
        assert body["telegram_configured"] is True
        assert body["sent"] is True
        assert mock_send.call_args.kwargs["alert_name"] == "SmokeTestAlert"
    finally:
        server.shutdown()
        server.server_close()
