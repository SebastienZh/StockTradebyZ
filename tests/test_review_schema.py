import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "agent"))

from review_schema import ReviewModelOutput, finalize_review


class ReviewSchemaTests(unittest.TestCase):
    def _output(self, volume_score: int = 4):
        return ReviewModelOutput.model_validate(
            {
                "trend": {"score": 4, "evidence": ["日周趋势向上"]},
                "price_position": {"score": 4, "evidence": ["中位突破"]},
                "volume_behavior": {"score": volume_score, "evidence": ["上涨放量"]},
                "previous_abnormal_move": {"score": 4, "evidence": ["存在放量阳线"]},
                "fundamentals": {"status": "missing"},
                "signal_type": "trend_start",
                "invalidation_conditions": ["跌破平台且放量"],
                "comment": "周线向上，日线中位突破且量价健康，主要风险是回落失守平台。",
            }
        )

    def test_total_is_computed_by_code(self):
        result = finalize_review(self._output(), code="600519", metadata={})
        self.assertEqual(result["total_score"], 4.0)
        self.assertEqual(result["verdict"], "PASS")

    def test_volume_one_forces_fail(self):
        result = finalize_review(self._output(volume_score=1), code="600519", metadata={})
        self.assertEqual(result["verdict"], "FAIL")

    def test_hard_fundamental_risk_forces_fail(self):
        output = self._output()
        output.hard_risk_flags = ["liquidity_or_debt_crisis"]
        result = finalize_review(output, code="600519", metadata={})
        self.assertEqual(result["verdict"], "FAIL")


if __name__ == "__main__":
    unittest.main()
