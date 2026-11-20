import unittest

import pandas as pd

from batch_inference import input_id, prepare_features


class BatchInferenceTest(unittest.TestCase):
    def test_invalid_route_is_marked(self):
        df = pd.DataFrame(
            [
                {
                    "trip_key": "t1",
                    "snapshot_ts": pd.Timestamp("2020-01-01T00:00:00Z"),
                    "route_id": "UNKNOWN",
                    "x": 1.0,
                }
            ]
        )
        valid, invalid = prepare_features(df, ["route_id", "x"], ["A", "B"])
        self.assertTrue(valid.empty)
        self.assertEqual(len(invalid), 1)
        self.assertIn("unknown route_id", invalid[0][1])

    def test_missing_key_is_marked(self):
        df = pd.DataFrame(
            [
                {
                    "trip_key": None,
                    "snapshot_ts": pd.Timestamp("2020-01-01T00:00:00Z"),
                    "route_id": "A",
                    "x": 1.0,
                }
            ]
        )
        valid, invalid = prepare_features(df, ["route_id", "x"], ["A", "B"])
        self.assertTrue(valid.empty)
        self.assertEqual(len(invalid), 1)
        self.assertIn("missing trip_key", invalid[0][1])

    def test_valid_row(self):
        df = pd.DataFrame(
            [
                {
                    "trip_key": "t1",
                    "snapshot_ts": pd.Timestamp("2020-01-01T00:00:00Z"),
                    "route_id": "A",
                    "x": 1.0,
                }
            ]
        )
        valid, invalid = prepare_features(df, ["route_id", "x"], ["A", "B"])
        self.assertEqual(len(valid), 1)
        self.assertEqual(invalid, [])

    def test_input_id_is_stable(self):
        row = pd.Series(
            {
                "trip_key": "t1",
                "snapshot_ts": pd.Timestamp("2020-01-01T00:00:00Z"),
            }
        )
        self.assertEqual(input_id(row), "t1|2020-01-01 00:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
