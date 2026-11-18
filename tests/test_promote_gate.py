import unittest

from promote_model import promotion_decision


class PromotionGateTest(unittest.TestCase):
    def test_promote_when_mae_improves(self):
        cand = {"mae": 9.0, "rmse": 12.0, "f1_late_5": 0.80}
        champ = {"mae": 10.0, "rmse": 12.0, "f1_late_5": 0.80}
        result = promotion_decision(cand, champ)
        self.assertTrue(result["promote"])
        self.assertTrue(result["mae_ok"])

    def test_reject_when_mae_improvement_too_small(self):
        cand = {"mae": 9.9, "rmse": 12.0, "f1_late_5": 0.80}
        champ = {"mae": 10.0, "rmse": 12.0, "f1_late_5": 0.80}
        result = promotion_decision(cand, champ)
        self.assertFalse(result["promote"])
        self.assertFalse(result["mae_ok"])

    def test_reject_when_rmse_worsens_too_much(self):
        cand = {"mae": 9.0, "rmse": 13.0, "f1_late_5": 0.80}
        champ = {"mae": 10.0, "rmse": 12.0, "f1_late_5": 0.80}
        result = promotion_decision(cand, champ)
        self.assertFalse(result["promote"])
        self.assertFalse(result["rmse_ok"])

    def test_reject_when_f1_drops_too_much(self):
        cand = {"mae": 9.0, "rmse": 12.0, "f1_late_5": 0.70}
        champ = {"mae": 10.0, "rmse": 12.0, "f1_late_5": 0.80}
        result = promotion_decision(cand, champ)
        self.assertFalse(result["promote"])
        self.assertFalse(result["f1_ok"])


if __name__ == "__main__":
    unittest.main()
