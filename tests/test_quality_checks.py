import unittest

from quality.checks import max_check, min_check, summarize


class CheckHelpersTest(unittest.TestCase):
    def test_max_check_pass(self):
        self.assertEqual(max_check(0, 0), "pass")

    def test_max_check_critical_fail(self):
        self.assertEqual(max_check(1, 0, "critical"), "fail")

    def test_max_check_warning(self):
        self.assertEqual(max_check(10, 1, "warning"), "warn")

    def test_min_check_pass(self):
        self.assertEqual(min_check(1, 1), "pass")

    def test_min_check_fail(self):
        self.assertEqual(min_check(0, 1), "fail")

    def test_summary_critical(self):
        results = [
            {"status": "fail", "severity": "critical"},
            {"status": "pass", "severity": "critical"},
        ]
        self.assertEqual(summarize(results)["overall"], "fail")

    def test_summary_warning(self):
        results = [
            {"status": "warn", "severity": "warning"},
            {"status": "pass", "severity": "critical"},
        ]
        self.assertEqual(summarize(results)["overall"], "warn")

    def test_summary_pass(self):
        results = [{"status": "pass", "severity": "critical"}]
        self.assertEqual(summarize(results)["overall"], "pass")


if __name__ == "__main__":
    unittest.main()
