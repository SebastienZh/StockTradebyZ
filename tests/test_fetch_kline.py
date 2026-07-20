from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd
import yaml


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from unittest.mock import patch

from pipeline.fetch_kline import (
    ProviderCircuitBreaker,
    _get_kline_with_fallback,
    _normalize_akshare_kline,
)


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

    def test_default_config_uses_keyless_fallback_chain(self):
        config = yaml.safe_load((ROOT / "config" / "fetch_kline.yaml").read_text("utf-8"))
        self.assertEqual(config["provider"], "auto")
        self.assertEqual(config["providers"], ["akshare", "sina", "baostock"])
        self.assertLessEqual(config["workers"], 2)

    @patch("pipeline.fetch_kline._PROVIDER_BREAKER", new_callable=ProviderCircuitBreaker)
    @patch("pipeline.fetch_kline._get_kline")
    def test_network_failure_opens_circuit_and_falls_back(self, get_kline, breaker):
        expected = pd.DataFrame({"date": [pd.Timestamp("2026-07-17")]})
        get_kline.side_effect = [
            ConnectionError("Remote end closed connection without response"),
            expected,
            expected,
        ]
        actual, source = _get_kline_with_fallback(
            ["akshare", "sina"], "600519", "20260701", "20260720", adjust="qfq", timeout=20
        )
        self.assertIs(actual, expected)
        self.assertEqual(source, "sina")
        self.assertFalse(breaker.available("akshare"))

        _, source = _get_kline_with_fallback(
            ["akshare", "sina"], "000001", "20260701", "20260720", adjust="qfq", timeout=20
        )
        self.assertEqual(source, "sina")
        self.assertEqual([call.args[0] for call in get_kline.call_args_list], ["akshare", "sina", "sina"])

    @patch("pipeline.fetch_kline._PROVIDER_BREAKER", new_callable=ProviderCircuitBreaker)
    @patch("pipeline.fetch_kline._get_kline")
    def test_empty_result_falls_back_without_opening_circuit(self, get_kline, breaker):
        expected = pd.DataFrame({"date": [pd.Timestamp("2026-07-17")]})
        get_kline.side_effect = [pd.DataFrame(), expected]
        actual, source = _get_kline_with_fallback(
            ["akshare", "sina"], "600519", "20260701", "20260720", adjust="qfq", timeout=20
        )
        self.assertIs(actual, expected)
        self.assertEqual(source, "sina")
        self.assertTrue(breaker.available("akshare"))


if __name__ == "__main__":
    unittest.main()
