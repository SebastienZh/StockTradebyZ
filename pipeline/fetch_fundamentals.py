from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
import yaml
from dotenv import load_dotenv

from pipeline.market_data import CanonicalSymbol, Market


ROOT = Path(__file__).resolve().parent.parent


def _json_value(value):
    if value is None or pd.isna(value):
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


class TushareFundamentals:
    def __init__(self) -> None:
        import tushare as ts

        token = os.getenv("TUSHARE_TOKEN")
        if not token:
            raise RuntimeError("缺少 TUSHARE_TOKEN")
        ts.set_token(token)
        self.pro = ts.pro_api()

    @staticmethod
    def _code(ticker: str) -> str:
        if ticker.startswith(("60", "68", "9")):
            return f"{ticker}.SH"
        if ticker.startswith(("4", "8")):
            return f"{ticker}.BJ"
        return f"{ticker}.SZ"

    def fetch(self, symbol: CanonicalSymbol, as_of: str) -> dict:
        end = pd.Timestamp(as_of).strftime("%Y%m%d")
        start = (pd.Timestamp(as_of) - pd.DateOffset(years=3)).strftime("%Y%m%d")
        df = self.pro.fina_indicator(ts_code=self._code(symbol.ticker), start_date=start, end_date=end)
        if df is None or df.empty:
            raise ValueError("Tushare 未返回财务指标")
        if "ann_date" not in df:
            raise ValueError("财务指标缺少 ann_date，无法执行点时过滤")
        df = df[pd.to_datetime(df["ann_date"], errors="coerce") <= pd.Timestamp(as_of)]
        df = df.sort_values(["ann_date", "end_date"], ascending=False).head(12)
        fields = [
            "ann_date", "end_date", "eps", "dt_eps", "roe", "roe_dt",
            "grossprofit_margin", "netprofit_margin", "debt_to_assets",
            "current_ratio", "quick_ratio", "ocfps", "revenue_ps",
            "q_sales_yoy", "q_profit_yoy", "q_netprofit_yoy",
        ]
        available = [c for c in fields if c in df.columns]
        records = [
            {key: _json_value(value) for key, value in row.items()}
            for row in df[available].to_dict(orient="records")
        ]
        return {
            "status": "verified",
            "source": "tushare",
            "market": "CN",
            "symbol": symbol.ticker,
            "available_at": pd.Timestamp(records[0]["ann_date"]).date().isoformat(),
            "as_of": pd.Timestamp(as_of).date().isoformat(),
            "latest": records[0],
            "history": records,
        }


class SecEdgarFundamentals:
    TAGS = {
        "revenue": ("Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"),
        "net_income": ("NetIncomeLoss",),
        "assets": ("Assets",),
        "liabilities": ("Liabilities",),
        "equity": ("StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"),
        "operating_cash_flow": ("NetCashProvidedByUsedInOperatingActivities",),
    }

    def __init__(self) -> None:
        user_agent = os.getenv("SEC_USER_AGENT")
        if not user_agent:
            raise RuntimeError("缺少 SEC_USER_AGENT；格式应包含应用名称和联系邮箱")
        self.headers = {"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"}

    def _get(self, url: str) -> dict:
        response = requests.get(url, headers=self.headers, timeout=30)
        response.raise_for_status()
        return response.json()

    def _cik(self, ticker: str) -> int:
        payload = self._get("https://www.sec.gov/files/company_tickers.json")
        for item in payload.values():
            if str(item.get("ticker", "")).upper() == ticker.upper():
                return int(item["cik_str"])
        raise ValueError(f"SEC ticker 映射不存在：{ticker}")

    @staticmethod
    def _latest_observation(facts: dict, tags: tuple[str, ...], as_of: pd.Timestamp) -> dict | None:
        us_gaap = facts.get("facts", {}).get("us-gaap", {})
        rows = []
        for tag in tags:
            node = us_gaap.get(tag) or {}
            units = node.get("units") or {}
            for unit in ("USD", "USD/shares", "shares"):
                for obs in units.get(unit, []):
                    filed = pd.to_datetime(obs.get("filed"), errors="coerce")
                    if pd.isna(filed) or filed > as_of:
                        continue
                    if obs.get("form") not in {"10-K", "10-Q", "20-F", "40-F"}:
                        continue
                    rows.append({"tag": tag, "unit": unit, **obs})
            if rows:
                break
        if not rows:
            return None
        rows.sort(key=lambda row: (row.get("filed", ""), row.get("end", "")), reverse=True)
        return rows[0]

    def fetch(self, symbol: CanonicalSymbol, as_of: str) -> dict:
        cik = self._cik(symbol.ticker)
        facts = self._get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json")
        cutoff = pd.Timestamp(as_of)
        metrics = {
            name: self._latest_observation(facts, tags, cutoff)
            for name, tags in self.TAGS.items()
        }
        metrics = {name: value for name, value in metrics.items() if value is not None}
        if not metrics:
            raise ValueError("SEC Company Facts 没有选股时点之前的可用数据")
        available_at = max(value["filed"] for value in metrics.values())
        return {
            "status": "verified",
            "source": "sec_edgar",
            "market": "US",
            "symbol": symbol.ticker,
            "cik": cik,
            "available_at": available_at,
            "as_of": cutoff.date().isoformat(),
            "metrics": metrics,
        }


class YFinanceCurrentFundamentals:
    """Current snapshot fallback. Explicitly forbidden in historical backtests."""

    def fetch(self, symbol: CanonicalSymbol, as_of: str) -> dict:
        import yfinance as yf

        if pd.Timestamp(as_of).date() != pd.Timestamp.today().date():
            raise ValueError("yfinance 基本面没有可靠发布时间，禁止用于历史点时回测")
        ticker = symbol.ticker
        if symbol.market is Market.HK:
            ticker = f"{(ticker.lstrip('0') or '0').zfill(4)}.HK"
        obj = yf.Ticker(ticker)
        info = obj.info or {}
        fields = (
            "marketCap", "enterpriseValue", "trailingPE", "forwardPE", "priceToBook",
            "profitMargins", "grossMargins", "operatingMargins", "returnOnEquity",
            "revenueGrowth", "earningsGrowth", "debtToEquity", "freeCashflow",
            "operatingCashflow", "totalCash", "totalDebt",
        )
        now = datetime.now(timezone.utc).isoformat()
        return {
            "status": "live_only",
            "source": "yfinance",
            "market": symbol.market.value,
            "symbol": symbol.ticker,
            "available_at": now,
            "as_of": pd.Timestamp(as_of).date().isoformat(),
            "warning": "非点时数据，只允许用于当日研究，不得进入历史回测",
            "metrics": {key: _json_value(info.get(key)) for key in fields},
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="抓取点时基本面快照")
    parser.add_argument("symbols", nargs="+", help="例如 CN:600519 HK:00700 US:AAPL")
    parser.add_argument("--as-of", default="today")
    parser.add_argument("--config", default="config/fundamental_data.yaml")
    args = parser.parse_args()

    load_dotenv(ROOT / ".env.local")
    load_dotenv(ROOT / ".env")
    as_of = pd.Timestamp.today().date().isoformat() if args.as_of == "today" else args.as_of
    cfg = yaml.safe_load((ROOT / args.config).read_text(encoding="utf-8")) or {}
    providers = {
        "tushare": TushareFundamentals,
        "sec_edgar": SecEdgarFundamentals,
        "yfinance_current": YFinanceCurrentFundamentals,
    }
    instances = {}
    results = []
    for raw in args.symbols:
        symbol = CanonicalSymbol.parse(raw)
        errors = []
        payload = None
        for name in cfg["routes"][symbol.market.value]:
            try:
                if name not in instances:
                    instances[name] = providers[name]()
                payload = instances[name].fetch(symbol, as_of)
                break
            except Exception as exc:
                errors.append({"provider": name, "error": str(exc)[:300]})
        if payload is None:
            raise RuntimeError(f"{symbol.key} 基本面抓取失败：{errors}")
        payload["attempts"] = errors
        out = ROOT / "data" / "fundamentals" / symbol.market.value / f"{symbol.ticker}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        results.append({"symbol": symbol.key, "source": payload["source"], "path": str(out)})
    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
