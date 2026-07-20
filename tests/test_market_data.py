import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.market_data.models import CanonicalSymbol, Market
from pipeline.market_data.quality import compare_sources, validate_bars
from pipeline.market_data.router import RoutedBars
from pipeline.market_data.store import MarketDataStore


def bars(scale: float = 1.0) -> pd.DataFrame:
    close = [10.0 * scale, 10.2 * scale, 10.1 * scale, 10.5 * scale, 10.7 * scale]
    return pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=5),
            "open": close,
            "high": [x * 1.01 for x in close],
            "low": [x * 0.99 for x in close],
            "close": close,
            "volume": [100] * 5,
            "turnover": [1000] * 5,
            "adj_close": close,
            "currency": ["CNY"] * 5,
            "market": ["CN"] * 5,
            "symbol": ["600519"] * 5,
            "source": ["test"] * 5,
            "fetched_at": [datetime.now(timezone.utc).isoformat()] * 5,
        }
    )


class MarketDataTests(unittest.TestCase):
    def test_symbol_normalization(self):
        self.assertEqual(CanonicalSymbol.parse("CN:600519").key, "CN:600519")
        self.assertEqual(CanonicalSymbol(Market.HK, "700").key, "HK:00700")

    def test_quality_and_adjustment_anchor_comparison(self):
        self.assertTrue(validate_bars(bars()).passed)
        comparison = compare_sources(bars(), bars(scale=10.0))
        self.assertTrue(comparison["passed"])

    def test_store_writes_snapshot_and_curated_parquet(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            symbol = CanonicalSymbol(Market.CN, "600519")
            routed = RoutedBars(bars(), "test", [{"provider": "test", "ok": True}])
            paths = MarketDataStore(tmp, Path(tmp) / "legacy").save(symbol, routed)
            self.assertTrue(Path(paths["raw"]).exists())
            self.assertTrue(Path(paths["curated"]).exists())
            self.assertTrue(Path(paths["quality"]).exists())
            self.assertTrue(Path(paths["legacy"]).exists())


if __name__ == "__main__":
    unittest.main()
