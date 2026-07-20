from __future__ import annotations

import argparse
import json
from pathlib import Path

import yaml
from dotenv import load_dotenv

from pipeline.market_data import BarRequest, CanonicalSymbol, MarketDataRouter, MarketDataStore


ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    parser = argparse.ArgumentParser(description="多市场标准化行情抓取")
    parser.add_argument("symbols", nargs="+", help="例如 CN:600519 HK:00700 US:AAPL")
    parser.add_argument("--start", default="2019-01-01")
    parser.add_argument("--end", default="today")
    parser.add_argument("--config", default="config/market_data.yaml")
    parser.add_argument("--no-cross-check", action="store_true")
    args = parser.parse_args()

    load_dotenv(ROOT / ".env.local")
    load_dotenv(ROOT / ".env")
    cfg_path = Path(args.config)
    if not cfg_path.is_absolute():
        cfg_path = ROOT / cfg_path
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    end = str(args.end)
    if end.lower() == "today":
        end = __import__("datetime").date.today().isoformat()

    router = MarketDataRouter(cfg["routes"], cfg.get("provider_options"))
    store = MarketDataStore(ROOT / cfg.get("storage_root", "data/lake"), ROOT / "data/raw")
    results = []
    for raw_symbol in args.symbols:
        symbol = CanonicalSymbol.parse(raw_symbol)
        routed = router.fetch(
            BarRequest(symbol=symbol, start=args.start, end=end),
            cross_check=not args.no_cross_check,
        )
        paths = store.save(symbol, routed)
        results.append({"symbol": symbol.key, "provider": routed.provider, "paths": paths})
    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
