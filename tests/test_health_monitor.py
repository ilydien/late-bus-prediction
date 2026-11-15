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
    ):
        results = health_monitor.run_checks()

    assert "postgres-infra" in results
    assert "mlflow" in results
    assert "vector" in results
    assert all(item["up"] is True for item in results.values())

    metrics_text = health_monitor.render_prometheus_metrics()
    assert 'wmata_service_up{service="mlflow"} 1' in metrics_text
    assert 'wmata_service_up{service="nats"} 1' in metrics_text
    assert "wmata_host_cpu_percent" in metrics_text


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
