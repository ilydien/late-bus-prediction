import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import quality.checks as checks


class FakeConn:
    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def fake_scalar(conn, sql, params=()):
    if "max(snapshot_ts)" in sql:
        return datetime.now(timezone.utc)
    return 1


class QualityChecksIntegrationTest(unittest.TestCase):
    def test_silver_gold_staging_checks_run(self):
        results = []
        with patch.object(checks, "connect", return_value=FakeConn()), patch.object(
            checks, "scalar", side_effect=fake_scalar
        ):
            checks.silver_checks(results)
            checks.gold_checks(results)
            checks.staging_checks(results)
        self.assertGreater(len(results), 20)
        self.assertTrue(all("status" in r for r in results))

    def test_summarize_warning(self):
        result = checks.summarize(
            [
                {"status": "warn", "severity": "warning"},
                {"status": "pass", "severity": "critical"},
            ]
        )
        self.assertEqual(result["overall"], "warn")


if __name__ == "__main__":
    unittest.main()
