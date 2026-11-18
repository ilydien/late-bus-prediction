import unittest
from datetime import date

import pandas as pd

from train_trip_delay import baseline_persistence, metrics, split_for_dates


class TrainHelpersTest(unittest.TestCase):
    def test_split_boundaries(self):
        dates = pd.Series(
            [
                date(2020, 1, 6),
                date(2020, 1, 25),
                date(2020, 1, 26),
                date(2020, 2, 1),
                date(2020, 2, 2),
                date(2020, 2, 8),
            ]
        )
        labels = split_for_dates(dates, date(2020, 2, 8), valid_days=7, test_days=7)
        self.assertEqual(list(labels), ["train", "train", "valid", "valid", "test", "test"])

    def test_metrics_keys(self):
        result = metrics([0, 5, 10], [1, 4, 12])
        self.assertEqual(
            set(result), {"mae", "rmse", "r2", "accuracy_late_5", "f1_late_5"}
        )
        self.assertGreater(result["mae"], 0)

    def test_baseline_persistence(self):
        df = pd.DataFrame({"target_deviation_15m": [2.0, 4.0], "deviation_now": [1.0, 5.0]})
        result = baseline_persistence(df)
        self.assertIn("mae", result)
        self.assertIn("f1_late_5", result)


if __name__ == "__main__":
    unittest.main()
