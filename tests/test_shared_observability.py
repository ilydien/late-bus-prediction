"""Unit tests for shared logging, metrics, and alert helpers."""

from __future__ import annotations

import json
import logging
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest

from shared.alerts import AlertThrottle, format_alert_message, send_alert
from shared.logging import JsonFormatter, get_run_id, new_run_id, run_context
from shared.metrics import format_prometheus_line, track_job


def test_json_formatter_includes_run_id_and_extra_fields():
    formatter = JsonFormatter("test-service")
    with run_context("job") as rid:
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname=__file__,
            lineno=42,
            msg="Processed %d rows",
            args=(10,),
            exc_info=None,
        )
        record.domain = "wmata"
        record.layer = "silver"
        record.row_count = 10

        parsed = json.loads(formatter.format(record))

    assert parsed["service"] == "test-service"
    assert parsed["level"] == "INFO"
    assert parsed["run_id"] == rid
    assert parsed["message"] == "Processed 10 rows"
    assert parsed["domain"] == "wmata"
    assert parsed["layer"] == "silver"
    assert parsed["row_count"] == 10
    assert "timestamp" in parsed


def test_run_context_restores_previous_run_id():
    outer = new_run_id("outer")
    assert get_run_id() == outer
    with run_context("inner") as inner:
        assert get_run_id() == inner
        assert inner.startswith("inner-")
    assert get_run_id() == outer


def test_format_prometheus_line_escapes_labels():
    line = format_prometheus_line(
        "wmata_test_metric",
        3.5,
        labels={"service": 'api"1', "domain": "wmata"},
        timestamp_ms=1700000000000,
    )
    assert line == 'wmata_test_metric{domain="wmata",service="api\\"1"} 3.5 1700000000000\n'


def test_track_job_success_and_failure_emit_metrics():
    with patch("shared.metrics.urllib.request.urlopen") as mock_urlopen:
        mock_resp = MagicMock()
        mock_resp.status = 204
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        with track_job(service="dagster-etl", job="silver_bus", domain="wmata", layer="silver") as rid:
            assert rid.startswith("silver_bus-")

        assert mock_urlopen.call_count >= 2

        with pytest.raises(RuntimeError):
            with track_job(service="dagster-etl", job="silver_bus", domain="wmata", layer="silver"):
                raise RuntimeError("boom")


def test_alert_throttle_and_formatting():
    throttle = AlertThrottle()
    assert throttle.should_send("k1", cooldown_seconds=60) is True
    assert throttle.should_send("k1", cooldown_seconds=60) is False

    firing = format_alert_message(
        "Service down",
        level="CRITICAL",
        alert_name="ServiceDown",
        status="FIRING",
        runbook="docs/runbooks/alerts.md",
    )
    assert "[CRITICAL] WMATA-Platform Alert — ServiceDown" in firing
    assert "docs/runbooks/alerts.md" in firing

    resolved = format_alert_message(
        "Service recovered",
        level="CRITICAL",
        alert_name="ServiceDown",
        status="RESOLVED",
    )
    assert "[RESOLVED] WMATA-Platform Alert — ServiceDown" in resolved


def test_send_alert_returns_false_without_credentials(monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_API_KEY", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    assert send_alert("test message", throttle_key=None) is False
