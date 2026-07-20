from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "agent"))

from kimi_review import load_config, parse_model_output, request_options


class KimiReviewTests(unittest.TestCase):
    def test_default_model_and_endpoint(self):
        config = load_config(ROOT / "config" / "kimi_review.yaml")
        self.assertEqual(config["model"], "kimi-k3")
        self.assertEqual(config["base_url"], "https://api.moonshot.cn/v1")

    def test_k3_options_do_not_force_temperature(self):
        options = request_options({"reasoning_effort": "high", "max_tokens": 16384})
        self.assertEqual(options["reasoning_effort"], "high")
        self.assertNotIn("temperature", options)
        self.assertEqual(options["response_format"]["type"], "json_schema")

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
        self.assertEqual(parsed.volume_behavior.score, 4)


if __name__ == "__main__":
    unittest.main()
