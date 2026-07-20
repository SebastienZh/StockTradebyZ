from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .models import BarRequest
from .providers import PROVIDER_TYPES, MarketDataProvider
from .quality import compare_sources, validate_bars


@dataclass(slots=True)
class RoutedBars:
    bars: pd.DataFrame
    provider: str
    attempts: list[dict]
    cross_check: dict | None = None


class MarketDataRouter:
    """Lazy provider router with ordered fallback and optional independent cross-check."""

    def __init__(self, routes: dict[str, list[str]], provider_options: dict | None = None) -> None:
        self.routes = {str(k).upper(): list(v) for k, v in routes.items()}
        self.provider_options = provider_options or {}
        self._instances: dict[str, MarketDataProvider] = {}

    def _provider(self, name: str) -> MarketDataProvider:
        if name not in PROVIDER_TYPES:
            raise KeyError(f"未知数据源：{name}")
        if name not in self._instances:
            self._instances[name] = PROVIDER_TYPES[name](**self.provider_options.get(name, {}))
        return self._instances[name]

    def fetch(self, request: BarRequest, *, cross_check: bool = True) -> RoutedBars:
        names = self.routes.get(request.symbol.market.value, [])
        if not names:
            raise ValueError(f"市场 {request.symbol.market.value} 未配置数据源")

        attempts: list[dict] = []
        successful: list[tuple[str, pd.DataFrame]] = []
        for name in names:
            try:
                provider = self._provider(name)
                if not provider.supports(request.symbol.market):
                    raise ValueError("数据源不支持该市场")
                bars = provider.fetch_daily(request)
                quality = validate_bars(bars)
                attempts.append({"provider": name, "ok": True, "quality": quality.to_dict()})
                successful.append((name, bars))
                if not cross_check or len(successful) >= 2:
                    break
            except Exception as exc:
                attempts.append({"provider": name, "ok": False, "error": str(exc)[:300]})

        if not successful:
            raise RuntimeError(f"所有数据源均失败：{attempts}")
        check = None
        if len(successful) >= 2:
            check = compare_sources(successful[0][1], successful[1][1])
        return RoutedBars(successful[0][1], successful[0][0], attempts, check)
