from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd
import yaml


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.fetch_kline import _normalize_akshare_kline


class FetchKlineTests(unittest.TestCase):
    def test_akshare_schema_normalization(self):
        raw = pd.DataFrame(
            {
                "日期": ["2026-07-17"],
                "股票代码": ["600519"],
                "开盘": [1400.0],
                "收盘": [1410.0],
                "最高": [1420.0],
                "最低": [1390.0],
                "成交量": [12345],
                "成交额": [1000000],
            }
        )
        actual = _normalize_akshare_kline(raw)
        self.assertEqual(
            list(actual.columns), ["date", "open", "close", "high", "low", "volume"]
        )
        self.assertEqual(actual.iloc[0]["close"], 1410.0)

    def test_default_config_uses_akshare_without_key(self):
        config = yaml.safe_load((ROOT / "config" / "fetch_kline.yaml").read_text("utf-8"))
        self.assertEqual(config["provider"], "akshare")
        self.assertLessEqual(config["workers"], 2)


if __name__ == "__main__":
    unittest.main()
