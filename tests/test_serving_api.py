import io
import json
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from fastapi.testclient import TestClient

import main


class FakeCursor:
    def __init__(self):
        self.sql = ""

    def execute(self, sql, params=()):
        self.sql = sql

    def fetchall(self):
        if "serving_freshness" in self.sql:
            return [
                {
                    "max_snapshot_ts": datetime(2020, 2, 8, 23, 59, tzinfo=timezone.utc),
                    "max_service_date": datetime(2020, 2, 8).date(),
                    "max_predicted_at": datetime(2026, 11, 18, tzinfo=timezone.utc),
                    "generated_at": datetime(2026, 11, 18, tzinfo=timezone.utc),
                }
            ]
        if "SELECT 1" in self.sql:
            return [{"ok": 1}]
        if "FROM public.pipeline_runs" in self.sql and "GROUP BY r.job_name" in self.sql:
            return [
                {
                    "job_name": "silver_consumer",
                    "layer": "silver",
                    "total_runs": 10,
                    "success_runs": 9,
                    "failed_runs": 1,
                    "running_runs": 0,
                    "avg_duration_sec": 3.5,
                    "p95_duration_sec": 7.2,
                    "max_duration_sec": 8.0,
                    "latest_status": "success",
                    "latest_started_at": datetime(2026, 11, 24, 9, 0, tzinfo=timezone.utc),
                    "latest_error": None,
                }
            ]
        if "FROM public.pipeline_runs" in self.sql and "lower(status) = 'failed'" in self.sql:
            return [
                {
                    "run_id": "run-fail-1",
                    "job_name": "silver_consumer",
                    "layer": "silver",
                    "status": "failed",
                    "started_at": datetime(2026, 11, 24, 8, 55, tzinfo=timezone.utc),
                    "finished_at": datetime(2026, 11, 24, 8, 56, tzinfo=timezone.utc),
                    "error": "boom",
                }
            ]
        if "FROM public.data_quality_runs" in self.sql:
            return [
                {
                    "run_id": "dq-1",
                    "layer": "gold",
                    "dataset": "trip_delay_features",
                    "check_name": "no_null_pk",
                    "status": "pass",
                    "severity": "critical",
                    "observed": 0.0,
                    "threshold": 0.0,
                    "checked_at": datetime(2026, 11, 24, 5, 0, tzinfo=timezone.utc),
                }
            ]
        return [
            {
                "route_id": "F8",
                "service_date": datetime(2020, 2, 8).date(),
                "trips": 1,
                "avg_deviation": 1.0,
                "on_time_pct": 1.0,
            }
        ]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def cursor(self, cursor_factory=None):
        return FakeCursor()

    def close(self):
        pass


def fake_conn():
    yield FakeConn()


class ServingApiTest(unittest.TestCase):
    def setUp(self):
        main.app.dependency_overrides[main.get_conn] = fake_conn
        self.client = TestClient(main.app)

    def tearDown(self):
        main.app.dependency_overrides.clear()

    def test_health(self):
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ok")

    def test_invalid_date_format(self):
        response = self.client.get(
            "/v1/routes/delay?start_date=bad&end_date=2020-02-08"
        )
        self.assertEqual(response.status_code, 422)

    def test_start_after_end(self):
        response = self.client.get(
            "/v1/routes/delay?start_date=2020-02-09&end_date=2020-02-08"
        )
        self.assertEqual(response.status_code, 400)

    def test_routes_delay(self):
        response = self.client.get(
            "/v1/routes/delay?start_date=2020-02-01&end_date=2020-02-08&route_id=F8"
        )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn("data", body)
        self.assertIn("freshness", body)
        self.assertEqual(body["data"][0]["route_id"], "F8")

    def test_freshness(self):
        response = self.client.get("/v1/freshness")
        self.assertEqual(response.status_code, 200)
        self.assertIn("max_snapshot_ts", response.json())

    def test_limit_validation(self):
        response = self.client.get("/v1/predictions/latest?limit=0")
        self.assertEqual(response.status_code, 422)

    def test_dashboard_html(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("WMATA", response.text)
        self.assertIn("TomTom", response.text)

    def test_routes_list(self):
        response = self.client.get("/v1/routes/list")
        self.assertEqual(response.status_code, 200)
        self.assertIn("F8", response.json()["routes"])

    def test_traffic_corridors_fallback(self):
        response = self.client.get("/v1/traffic/corridors")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn("data", body)
        self.assertGreaterEqual(len(body["data"]), 16)

    def test_stream_status(self):
        response = self.client.get("/v1/stream/status")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn("freshness", body)
        self.assertIn("pipeline_runs", body)

    def test_stream_events_sse_oneshot(self):
        response = self.client.get("/v1/stream/events?oneshot=true")
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/event-stream", response.headers.get("content-type", ""))
        self.assertIn("event: connected", response.text)

    def test_multipage_routes_and_static_assets(self):
        for route in ("/", "/dashboard", "/live-map", "/root-cause", "/ml-forecast", "/pipeline"):
            resp = self.client.get(route)
            self.assertEqual(resp.status_code, 200, f"failed route {route}")
            self.assertIn("MetroPulse", resp.text)
        pipe_resp = self.client.get("/pipeline")
        for marker in ("ops-signal-strip", "ops-svc-grid", "pipePerfChart", "ops-logs-tbody"):
            self.assertIn(marker, pipe_resp.text)
        css_resp = self.client.get("/static/styles.css")
        self.assertEqual(css_resp.status_code, 200)
        js_resp = self.client.get("/static/app.js")
        self.assertEqual(js_resp.status_code, 200)

    def test_ops_overview(self):
        def fake_fetch(url: str, timeout: float = 2.0):
            if "/api/monitoring/summary" in url:
                return {
                    "telegram_configured": True,
                    "last_probe_ts": 1795501869.0,
                    "services": {
                        "serving-api": {"up": True, "status": "ok", "duration_s": 0.004},
                        "nats": {"up": True, "status": "ok", "duration_s": 0.002},
                    },
                    "host": {
                        "cpu_percent": 4.5,
                        "cpu_cores": 8.0,
                        "cpu_used_cores": 0.36,
                        "memory_percent": 50.0,
                        "memory_used_gb": 8.0,
                        "memory_total_gb": 16.0,
                        "disk_percent": 60.0,
                        "disk_used_gb": 120.0,
                        "disk_total_gb": 200.0,
                    },
                    "metrics": {
                        "spock_replicating": [
                            {"labels": {"layer": "silver", "datname": "wmata", "subscription_name": "sub_1"}, "value": 1.0}
                        ],
                        "haproxy_active_servers": [
                            {"labels": {"proxy": "silver_rw_backend"}, "value": 1.0}
                        ],
                        "container_cpu_pct": [
                            {"labels": {"name": "wmata-lateness-serving-api-1"}, "value": 2.4}
                        ],
                        "container_memory_bytes": [
                            {"labels": {"name": "wmata-lateness-serving-api-1"}, "value": 104857600.0}
                        ],
                    },
                }
            if "/api/v1/rules" in url:
                return {
                    "data": {
                        "groups": [
                            {
                                "name": "wmata_platform_alerts",
                                "rules": [
                                    {
                                        "name": "ServiceDown",
                                        "state": "firing",
                                        "health": "ok",
                                        "labels": {"severity": "critical"},
                                        "annotations": {"summary": "Down"},
                                        "alerts": [{"state": "firing", "annotations": {"summary": "Down"}}],
                                    }
                                ],
                            }
                        ]
                    }
                }
            return None

        with patch.object(main, "_fetch_json_url", side_effect=fake_fetch):
            response = self.client.get("/v1/ops/overview")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["monitor"]["reachable"])
        self.assertTrue(body["monitor"]["telegram_configured"])
        self.assertEqual(body["monitor"]["up_count"], 2)
        self.assertEqual(body["monitor"]["host"]["memory_used_gb"], 8.0)
        self.assertEqual(body["alerts"]["firing_count"], 1)
        self.assertEqual(body["replication"]["spock"][0]["is_replicating"], 1)
        self.assertEqual(body["replication"]["haproxy"][0]["active_servers"], 1)
        self.assertEqual(body["resources"]["top_memory"][0]["name"], "serving-api")
        self.assertEqual(body["pipeline_performance"]["by_job"][0]["job_name"], "silver_consumer")
        self.assertEqual(body["pipeline_performance"]["recent_errors"][0]["run_id"], "run-fail-1")
        self.assertEqual(body["data_quality"]["summary"]["pass"], 1)

    def test_ops_logs(self):
        ndjson = (
            json.dumps(
                {
                    "_time": "2026-11-24T09:00:00Z",
                    "level": "ERROR",
                    "service": "stream-inference",
                    "run_id": "run-abc",
                    "log_format": "json",
                    "error_type": "RuntimeError",
                    "_msg": "Inference failed",
                }
            )
            + "\n"
        ).encode("utf-8")

        class FakeResp:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return ndjson

        with patch("main.urllib.request.urlopen", return_value=FakeResp()) as mock_open:
            response = self.client.get(
                "/v1/ops/logs?service=stream-inference&level=ERROR&run_id=run-abc&log_format=json&q=failed&limit=10"
            )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["reachable"])
        self.assertEqual(body["count"], 1)
        self.assertEqual(body["logs"][0]["service"], "stream-inference")
        self.assertEqual(body["logs"][0]["run_id"], "run-abc")
        self.assertIn('service:"stream-inference"', body["query"])
        self.assertIn('level:"ERROR"', body["query"])
        self.assertTrue(mock_open.called)

    def test_analytics_root_cause(self):
        response = self.client.get(
            "/v1/analytics/root-cause?start_date=2020-02-01&end_date=2020-02-08"
        )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        for key in (
            "executive_summary",
            "delay_distribution",
            "progress_curve",
            "congestion_tiers",
            "driver_attribution",
            "incident_breakdown",
            "feature_drivers",
            "route_diagnostics",
            "freshness",
        ):
            self.assertIn(key, body)
        self.assertEqual(len(body["delay_distribution"]), 5)
        self.assertEqual(len(body["progress_curve"]), 5)


if __name__ == "__main__":
    unittest.main()
