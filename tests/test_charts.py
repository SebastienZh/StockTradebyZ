import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dashboard"))

from components.charts import make_daily_chart, make_weekly_chart


class ChartTests(unittest.TestCase):
    def test_daily_and_weekly_figures_build(self):
        count = 320
        close = 100 + np.linspace(0, 30, count) + np.sin(np.arange(count) / 8)
        df = pd.DataFrame(
            {
                "date": pd.bdate_range("2025-01-01", periods=count),
                "open": close - 0.3,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 1_000_000 + np.arange(count) * 100,
            }
        )
        daily = make_daily_chart(df, "TEST", bars=120)
        weekly = make_weekly_chart(df, "TEST", bars=60)
        self.assertGreaterEqual(len(daily.data), 3)
        self.assertGreaterEqual(len(weekly.data), 3)


if __name__ == "__main__":
    unittest.main()
