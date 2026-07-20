from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Market(str, Enum):
    CN = "CN"
    HK = "HK"
    US = "US"


@dataclass(frozen=True, slots=True)
class CanonicalSymbol:
    """Provider-independent instrument id, e.g. CN:600519 or HK:00700."""

    market: Market
    ticker: str

    def __post_init__(self) -> None:
        ticker = str(self.ticker).strip().upper()
        if self.market is Market.CN:
            ticker = ticker.split(".")[0].zfill(6)
            if len(ticker) != 6 or not ticker.isdigit():
                raise ValueError(f"无效 A 股代码：{self.ticker}")
        elif self.market is Market.HK:
            ticker = ticker.split(".")[0].zfill(5)
            if len(ticker) != 5 or not ticker.isdigit():
                raise ValueError(f"无效港股代码：{self.ticker}")
        elif not ticker:
            raise ValueError("美股 ticker 不能为空")
        object.__setattr__(self, "ticker", ticker)

    @property
    def key(self) -> str:
        return f"{self.market.value}:{self.ticker}"

    @classmethod
    def parse(cls, value: str, default_market: Market | str | None = None) -> "CanonicalSymbol":
        raw = str(value).strip()
        if ":" in raw:
            market, ticker = raw.split(":", 1)
            return cls(Market(market.upper()), ticker)
        if default_market is None:
            raise ValueError(f"代码缺少市场前缀：{value}，示例 CN:600519")
        return cls(Market(str(default_market).upper()), raw)


@dataclass(frozen=True, slots=True)
class BarRequest:
    symbol: CanonicalSymbol
    start: str
    end: str
    adjusted: bool = True


REQUIRED_BAR_COLUMNS = (
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "turnover",
    "adj_close",
    "currency",
    "market",
    "symbol",
    "source",
    "fetched_at",
)
