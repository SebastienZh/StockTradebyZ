from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "agent"))

from glm_review import load_config, parse_model_output


class GLMReviewTests(unittest.TestCase):
    def test_default_models(self):
        config = load_config(ROOT / "config" / "glm_review.yaml")
        self.assertEqual(config["model"], "glm-5v-turbo")
        self.assertEqual(config["fallback_model"], "glm-4.6v-flash")

    def test_structured_output_is_validated(self):
        payload = {
            "trend": {"score": 4, "evidence": ["周线趋势向上"]},
            "price_position": {"score": 3, "evidence": ["接近前高"]},
            "volume_behavior": {"score": 4, "evidence": ["上涨放量"]},
            "previous_abnormal_move": {"score": 3, "evidence": ["存在温和异动"]},
            "fundamentals": {"status": "missing"},
            "signal_type": "trend_start",
            "invalidation_conditions": ["放量跌破平台"],
            "comment": "周线向上，日线接近前高且量价尚可，需防止放量跌破平台。",
        }
        parsed = parse_model_output(json.dumps(payload, ensure_ascii=False))
        self.assertEqual(parsed.trend.score, 4)


if __name__ == "__main__":
    unittest.main()
