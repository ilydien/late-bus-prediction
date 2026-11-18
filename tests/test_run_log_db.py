import unittest
from unittest.mock import patch

import governance.run_log as run_log


class FakeCursor:
    description = [
        ("run_id",),
        ("parent_run_id",),
        ("job_name",),
        ("layer",),
        ("status",),
        ("started_at",),
        ("finished_at",),
        ("input_datasets",),
        ("output_datasets",),
        ("source_manifest",),
        ("row_counts",),
        ("git_commit",),
        ("error",),
    ]

    def execute(self, *args, **kwargs):
        pass

    def fetchone(self):
        return (
            "r1",
            None,
            "job",
            "batch",
            "success",
            None,
            None,
            [],
            [],
            {},
            {},
            "abc",
            None,
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def cursor(self):
        return FakeCursor()

    def commit(self):
        pass

    def close(self):
        pass


class RunLogDbTest(unittest.TestCase):
    def test_run_log_round_trip(self):
        with patch.object(run_log, "connect", return_value=FakeConn()):
            run_log.start_run(
                "r1",
                "job",
                "batch",
                input_datasets=["in"],
                output_datasets=["out"],
                source_manifest={"x": 1},
            )
            run_log.finish_run("r1", "success", row_counts={"rows": 1})
            run_log.record_row_counts("r1", {"rows": 2})
            result = run_log.get_run("r1")
        self.assertEqual(result["run_id"], "r1")
        self.assertEqual(result["job_name"], "job")


if __name__ == "__main__":
    unittest.main()
