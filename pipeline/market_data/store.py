from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .models import CanonicalSymbol
from .quality import normalize_bars
from .router import RoutedBars


class MarketDataStore:
    """Immutable raw snapshots plus a curated latest dataset and legacy CSV bridge."""

    def __init__(self, root: str | Path, legacy_cn_dir: str | Path | None = None) -> None:
        self.root = Path(root)
        self.legacy_cn_dir = Path(legacy_cn_dir) if legacy_cn_dir else None

    def save(self, symbol: CanonicalSymbol, routed: RoutedBars) -> dict[str, str]:
        bars = normalize_bars(routed.bars)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        raw_dir = self.root / "raw" / routed.provider / symbol.market.value / symbol.ticker
        curated_dir = self.root / "curated" / "bars" / symbol.market.value
        report_dir = self.root / "quality" / symbol.market.value
        raw_dir.mkdir(parents=True, exist_ok=True)
        curated_dir.mkdir(parents=True, exist_ok=True)
        report_dir.mkdir(parents=True, exist_ok=True)

        raw_path = raw_dir / f"{stamp}.parquet"
        curated_path = curated_dir / f"{symbol.ticker}.parquet"
        report_path = report_dir / f"{symbol.ticker}_{stamp}.json"
        bars.to_parquet(raw_path, index=False)

        if curated_path.exists():
            previous = pd.read_parquet(curated_path)
            bars = (
                pd.concat([previous, bars], ignore_index=True)
                .sort_values("date")
                .drop_duplicates("date", keep="last")
            )
        bars.to_parquet(curated_path, index=False)
        report_path.write_text(
            json.dumps(
                {
                    "symbol": symbol.key,
                    "selected_provider": routed.provider,
                    "attempts": routed.attempts,
                    "cross_check": routed.cross_check,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        paths = {"raw": str(raw_path), "curated": str(curated_path), "quality": str(report_path)}
        if symbol.market.value == "CN" and self.legacy_cn_dir is not None:
            self.legacy_cn_dir.mkdir(parents=True, exist_ok=True)
            legacy = bars[["date", "open", "close", "high", "low", "volume"]].copy()
            legacy_path = self.legacy_cn_dir / f"{symbol.ticker}.csv"
            legacy.to_csv(legacy_path, index=False)
            paths["legacy"] = str(legacy_path)
        return paths
