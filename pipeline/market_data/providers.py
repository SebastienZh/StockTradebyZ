from __future__ import annotations

import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone

import pandas as pd

from .models import BarRequest, Market
from .quality import normalize_bars


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _finish(
    df: pd.DataFrame,
    request: BarRequest,
    source: str,
    *,
    currency: str,
    volume_scale: float = 1.0,
    turnover_scale: float = 1.0,
) -> pd.DataFrame:
    out = df.copy()
    out["volume"] = pd.to_numeric(out.get("volume"), errors="coerce") * volume_scale
    if "turnover" not in out:
        out["turnover"] = pd.NA
    out["turnover"] = pd.to_numeric(out["turnover"], errors="coerce") * turnover_scale
    if "adj_close" not in out:
        out["adj_close"] = out["close"]
    out["currency"] = currency
    out["market"] = request.symbol.market.value
    out["symbol"] = request.symbol.ticker
    out["source"] = source
    out["fetched_at"] = _now()
    return normalize_bars(out)


class MarketDataProvider(ABC):
    name: str
    supported_markets: frozenset[Market]

    def supports(self, market: Market) -> bool:
        return market in self.supported_markets

    @abstractmethod
    def fetch_daily(self, request: BarRequest) -> pd.DataFrame:
        raise NotImplementedError


class TushareProvider(MarketDataProvider):
    name = "tushare"
    supported_markets = frozenset({Market.CN})

    def __init__(self) -> None:
        import tushare as ts

        token = os.getenv("TUSHARE_TOKEN")
        if not token:
            raise RuntimeError("缺少 TUSHARE_TOKEN")
        ts.set_token(token)
        self.ts = ts
        self.pro = ts.pro_api()

    @staticmethod
    def _code(ticker: str) -> str:
        if ticker.startswith(("60", "68", "9")):
            return f"{ticker}.SH"
        if ticker.startswith(("4", "8")):
            return f"{ticker}.BJ"
        return f"{ticker}.SZ"

    def fetch_daily(self, request: BarRequest) -> pd.DataFrame:
        ts_code = self._code(request.symbol.ticker)
        raw = self.pro.daily(ts_code=ts_code, start_date=request.start, end_date=request.end)
        if raw is None or raw.empty:
            raise ValueError(f"Tushare 无数据：{ts_code}")
        raw = raw.rename(columns={"trade_date": "date", "vol": "volume", "amount": "turnover"})
        raw["adj_close"] = raw["close"]
        if request.adjusted:
            try:
                factor = self.pro.adj_factor(
                    ts_code=ts_code, start_date=request.start, end_date=request.end
                ).rename(columns={"trade_date": "date"})
                raw = raw.merge(factor[["date", "adj_factor"]], on="date", how="left")
                anchor = pd.to_numeric(raw["adj_factor"], errors="coerce").dropna().iloc[0]
                raw["adj_close"] = raw["close"] * raw["adj_factor"] / anchor
                for col in ("open", "high", "low", "close"):
                    raw[col] = raw[col] * raw["adj_factor"] / anchor
            except Exception:
                pass
        return _finish(
            raw,
            request,
            self.name,
            currency="CNY",
            volume_scale=100.0,   # Tushare 日线 vol 单位为手
            turnover_scale=1000.0,  # amount 单位为千元
        )


class BaoStockProvider(MarketDataProvider):
    name = "baostock"
    supported_markets = frozenset({Market.CN})

    def fetch_daily(self, request: BarRequest) -> pd.DataFrame:
        import baostock as bs

        prefix = "sh" if request.symbol.ticker.startswith(("60", "68", "9")) else "sz"
        code = f"{prefix}.{request.symbol.ticker}"
        login = bs.login()
        if login.error_code != "0":
            raise RuntimeError(login.error_msg)
        try:
            rs = bs.query_history_k_data_plus(
                code,
                "date,open,high,low,close,volume,amount",
                start_date=pd.Timestamp(request.start).strftime("%Y-%m-%d"),
                end_date=pd.Timestamp(request.end).strftime("%Y-%m-%d"),
                frequency="d",
                adjustflag="2" if request.adjusted else "3",
            )
            rows: list[list[str]] = []
            while rs.error_code == "0" and rs.next():
                rows.append(rs.get_row_data())
            if rs.error_code != "0":
                raise RuntimeError(rs.error_msg)
            raw = pd.DataFrame(rows, columns=rs.fields).rename(columns={"amount": "turnover"})
            return _finish(raw, request, self.name, currency="CNY")
        finally:
            bs.logout()


class AkShareProvider(MarketDataProvider):
    name = "akshare"
    supported_markets = frozenset({Market.CN, Market.HK})

    _COLS = {
        "日期": "date",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "成交量": "volume",
        "成交额": "turnover",
    }

    def fetch_daily(self, request: BarRequest) -> pd.DataFrame:
        import akshare as ak

        kwargs = {
            "symbol": request.symbol.ticker,
            "period": "daily",
            "start_date": pd.Timestamp(request.start).strftime("%Y%m%d"),
            "end_date": pd.Timestamp(request.end).strftime("%Y%m%d"),
            "adjust": "qfq" if request.adjusted else "",
        }
        if request.symbol.market is Market.CN:
            raw = ak.stock_zh_a_hist(**kwargs)
            currency = "CNY"
        else:
            kwargs["symbol"] = request.symbol.ticker.lstrip("0") or "0"
            raw = ak.stock_hk_hist(**kwargs)
            currency = "HKD"
        raw = raw.rename(columns=self._COLS)
        return _finish(raw, request, self.name, currency=currency)


class YFinanceProvider(MarketDataProvider):
    name = "yfinance"
    supported_markets = frozenset({Market.HK, Market.US})

    @staticmethod
    def _ticker(request: BarRequest) -> str:
        if request.symbol.market is Market.HK:
            return f"{(request.symbol.ticker.lstrip('0') or '0').zfill(4)}.HK"
        return request.symbol.ticker

    def fetch_daily(self, request: BarRequest) -> pd.DataFrame:
        import yfinance as yf

        ticker = self._ticker(request)
        end_exclusive = (pd.Timestamp(request.end) + pd.Timedelta(days=1)).date().isoformat()
        raw = yf.download(
            ticker,
            start=pd.Timestamp(request.start).date().isoformat(),
            end=end_exclusive,
            auto_adjust=False,
            actions=False,
            progress=False,
            threads=False,
        )
        if raw.empty:
            raise ValueError(f"yfinance 无数据：{ticker}")
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)
        raw = raw.reset_index().rename(
            columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Adj Close": "adj_close",
                "Volume": "volume",
            }
        )
        raw["turnover"] = pd.NA
        currency = "HKD" if request.symbol.market is Market.HK else "USD"
        return _finish(raw, request, self.name, currency=currency)


class MassiveProvider(MarketDataProvider):
    name = "massive"
    supported_markets = frozenset({Market.US})

    def __init__(self, base_url: str = "https://api.massive.com") -> None:
        self.api_key = os.getenv("MASSIVE_API_KEY") or os.getenv("POLYGON_API_KEY")
        if not self.api_key:
            raise RuntimeError("缺少 MASSIVE_API_KEY")
        self.base_url = base_url.rstrip("/")

    def fetch_daily(self, request: BarRequest) -> pd.DataFrame:
        import requests

        start = pd.Timestamp(request.start).strftime("%Y-%m-%d")
        end = pd.Timestamp(request.end).strftime("%Y-%m-%d")
        url = f"{self.base_url}/v2/aggs/ticker/{request.symbol.ticker}/range/1/day/{start}/{end}"
        response = requests.get(
            url,
            params={"adjusted": str(request.adjusted).lower(), "sort": "asc", "limit": 50000},
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        rows = payload.get("results") or []
        raw = pd.DataFrame(rows).rename(
            columns={"t": "date", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"}
        )
        if raw.empty:
            raise ValueError(f"Massive 无数据：{request.symbol.ticker}")
        raw["date"] = pd.to_datetime(raw["date"], unit="ms", utc=True).dt.tz_convert(None)
        raw["turnover"] = pd.NA
        return _finish(raw, request, self.name, currency="USD")


class FutuProvider(MarketDataProvider):
    name = "futu"
    supported_markets = frozenset({Market.CN, Market.HK, Market.US})

    def __init__(self, host: str = "127.0.0.1", port: int = 11111) -> None:
        self.host = host
        self.port = int(port)

    @staticmethod
    def _code(request: BarRequest) -> str:
        if request.symbol.market is Market.HK:
            return f"HK.{request.symbol.ticker}"
        if request.symbol.market is Market.US:
            return f"US.{request.symbol.ticker}"
        ticker = request.symbol.ticker
        prefix = "SH" if ticker.startswith(("60", "68", "9")) else "SZ"
        return f"{prefix}.{ticker}"

    def fetch_daily(self, request: BarRequest) -> pd.DataFrame:
        from futu import AuType, KLType, OpenQuoteContext, RET_OK

        context = OpenQuoteContext(host=self.host, port=self.port)
        pages = []
        page_key = None
        try:
            while True:
                ret, data, page_key = context.request_history_kline(
                    self._code(request),
                    start=pd.Timestamp(request.start).strftime("%Y-%m-%d"),
                    end=pd.Timestamp(request.end).strftime("%Y-%m-%d"),
                    ktype=KLType.K_DAY,
                    autype=AuType.QFQ if request.adjusted else AuType.NONE,
                    max_count=1000,
                    page_req_key=page_key,
                )
                if ret != RET_OK:
                    raise RuntimeError(str(data))
                pages.append(data)
                if page_key is None:
                    break
        finally:
            context.close()
        raw = pd.concat(pages, ignore_index=True).rename(
            columns={"time_key": "date", "turnover": "turnover"}
        )
        currency = {Market.CN: "CNY", Market.HK: "HKD", Market.US: "USD"}[request.symbol.market]
        return _finish(raw, request, self.name, currency=currency)


PROVIDER_TYPES = {
    "tushare": TushareProvider,
    "baostock": BaoStockProvider,
    "akshare": AkShareProvider,
    "yfinance": YFinanceProvider,
    "massive": MassiveProvider,
    "futu": FutuProvider,
}
