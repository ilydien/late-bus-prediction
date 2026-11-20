import unittest
from datetime import datetime, timezone

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


if __name__ == "__main__":
    unittest.main()
