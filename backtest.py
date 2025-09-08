#!/usr/bin/env python3
"""
回测系统 - 评估选股策略的历史表现

功能：
1. 对指定交易日的选股结果，计算下一个交易日的收益率
2. 支持对最近2个月的交易数据进行回测
3. 将结果保存为JSON格式，按策略和日期组织
4. 计算每个策略的汇总统计信息
"""

import argparse
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional, Set

import pandas as pd
import numpy as np
from tqdm import tqdm

from Selector import (
    BBIKDJSelector,
    BBIShortLongSelector,
    BreakoutVolumeKDJSelector,
    PeakKDJSelector,
)
from select_stock import load_data, load_config, instantiate_selector

# 设置日志
# 确保日志目录存在
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_dir / "backtest.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("backtest")


class StockNameMapper:
    """股票代码与名称的映射管理"""

    def __init__(self, cache_file: str = "stock_names.json"):
        self.cache_file = Path(cache_file)
        self.name_map: Dict[str, str] = {}
        self._load_cache()

    def _load_cache(self) -> None:
        """从缓存文件加载股票名称映射"""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    self.name_map = json.load(f)
                logger.info(f"已从缓存加载 {len(self.name_map)} 个股票名称")
            except Exception as e:
                logger.warning(f"加载股票名称缓存失败: {e}")
                self.name_map = {}

    def _save_cache(self) -> None:
        """保存股票名称映射到缓存文件"""
        try:
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(self.name_map, f, ensure_ascii=False, indent=2)
            logger.info(f"已保存 {len(self.name_map)} 个股票名称到缓存")
        except Exception as e:
            logger.warning(f"保存股票名称缓存失败: {e}")

    def _fetch_stock_names(self, codes: List[str]) -> Dict[str, str]:
        """获取股票名称（简化版本，避免AKShare兼容性问题）"""
        missing_codes = [code for code in codes if code not in self.name_map]
        if not missing_codes:
            return self.name_map

        logger.info(f"为 {len(missing_codes)} 个股票生成默认名称")

        # 简单的股票名称生成规则
        for code in missing_codes:
            if code.startswith("00"):
                self.name_map[code] = f"深市股票{code}"
            elif code.startswith("60"):
                self.name_map[code] = f"沪市股票{code}"
            elif code.startswith("30"):
                self.name_map[code] = f"创业板{code}"
            elif code.startswith("68"):
                self.name_map[code] = f"科创板{code}"
            else:
                self.name_map[code] = f"股票{code}"

        # 保存更新后的缓存
        self._save_cache()

        return self.name_map

    def get_stock_name(self, code: str) -> str:
        """获取股票名称，如果缓存中没有则从AKShare获取"""
        if code not in self.name_map:
            self._fetch_stock_names([code])
        return self.name_map.get(code, code)

    def get_stock_names(self, codes: List[str]) -> Dict[str, str]:
        """批量获取股票名称"""
        missing = [code for code in codes if code not in self.name_map]
        if missing:
            self._fetch_stock_names(missing)
        return {code: self.name_map.get(code, code) for code in codes}


def get_trading_dates(start_date: str, end_date: str) -> List[str]:
    """
    获取指定日期范围内的交易日列表

    Args:
        start_date: 开始日期，格式为YYYY-MM-DD
        end_date: 结束日期，格式为YYYY-MM-DD

    Returns:
        交易日列表，格式为YYYY-MM-DD
    """
    try:
        # 使用pandas生成工作日列表（排除周末）
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)

        # 生成工作日范围
        business_days = pd.bdate_range(start=start, end=end)
        trading_dates = business_days.strftime("%Y-%m-%d").tolist()

        logger.info(f"生成交易日列表: {len(trading_dates)} 个交易日")
        return trading_dates
    except Exception as e:
        logger.error(f"生成交易日历失败: {e}")
        # 如果生成失败，返回空列表
        return []


def calculate_week_performance(
    df: pd.DataFrame,
    trade_date: pd.Timestamp,
    trading_dates: List[str],
    current_price: float,
) -> Dict[str, Any]:
    """
    计算一周内的股价表现分析

    Args:
        df: 股票数据DataFrame
        trade_date: 交易日期
        trading_dates: 交易日列表
        current_price: 当前价格

    Returns:
        一周内表现分析结果
    """
    try:
        # 获取接下来一周的交易日（最多5个交易日）
        trade_date_str = trade_date.strftime("%Y-%m-%d")

        # 找到当前交易日在列表中的位置
        current_index = -1
        for i, d in enumerate(trading_dates):
            if d == trade_date_str:
                current_index = i
                break

        if current_index == -1:
            return {"error": "未找到当前交易日"}

        # 获取接下来最多5个交易日
        next_week_dates = trading_dates[current_index + 1 : current_index + 6]

        if not next_week_dates:
            return {"error": "没有后续交易日数据"}

        # 获取这些日期的股价数据
        week_data = []
        for i, date_str in enumerate(next_week_dates):
            date_pd = pd.to_datetime(date_str)
            day_data = df[df["date"] == date_pd]

            if len(day_data) > 0:
                high_price = day_data["high"].values[0]
                low_price = day_data["low"].values[0]
                close_price = day_data["close"].values[0]

                week_data.append(
                    {
                        "day": i + 1,  # 第几天（1-5）
                        "date": date_str,
                        "high": float(high_price),
                        "low": float(low_price),
                        "close": float(close_price),
                        "high_return": float(
                            (high_price - current_price) / current_price * 100
                        ),
                        "low_return": float(
                            (low_price - current_price) / current_price * 100
                        ),
                        "close_return": float(
                            (close_price - current_price) / current_price * 100
                        ),
                    }
                )

        if not week_data:
            return {"error": "没有有效的一周数据"}

        # 找到最高价和最低价
        max_high_day = max(week_data, key=lambda x: x["high"])
        min_low_day = min(week_data, key=lambda x: x["low"])

        # 计算统计信息
        high_returns = [d["high_return"] for d in week_data]
        low_returns = [d["low_return"] for d in week_data]

        analysis = {
            "days_analyzed": len(week_data),
            "max_high": {
                "day": max_high_day["day"],
                "date": max_high_day["date"],
                "price": float(max_high_day["high"]),
                "return_pct": float(max_high_day["high_return"]),
            },
            "min_low": {
                "day": min_low_day["day"],
                "date": min_low_day["date"],
                "price": float(min_low_day["low"]),
                "return_pct": float(min_low_day["low_return"]),
            },
            "avg_high_return": float(np.mean(high_returns)),
            "avg_low_return": float(np.mean(low_returns)),
            "max_high_return": float(np.max(high_returns)),
            "min_low_return": float(np.min(low_returns)),
            "daily_data": week_data,
        }

        return analysis

    except Exception as e:
        logger.warning(f"计算一周表现失败: {e}")
        return {"error": str(e)}


def calculate_week_summary_stats(stock_results: List[Dict]) -> Dict[str, Any]:
    """
    计算一周表现的汇总统计

    Args:
        stock_results: 股票结果列表

    Returns:
        一周表现汇总统计
    """
    try:
        valid_analyses = []
        max_high_returns = []
        min_low_returns = []
        avg_high_returns = []
        avg_low_returns = []
        high_peak_days = []
        low_trough_days = []

        for stock in stock_results:
            week_analysis = stock.get("week_analysis", {})
            if "error" not in week_analysis and week_analysis:
                valid_analyses.append(week_analysis)

                # 收集各种统计数据
                if "max_high" in week_analysis:
                    max_high_returns.append(week_analysis["max_high"]["return_pct"])
                    high_peak_days.append(week_analysis["max_high"]["day"])

                if "min_low" in week_analysis:
                    min_low_returns.append(week_analysis["min_low"]["return_pct"])
                    low_trough_days.append(week_analysis["min_low"]["day"])

                if "avg_high_return" in week_analysis:
                    avg_high_returns.append(week_analysis["avg_high_return"])

                if "avg_low_return" in week_analysis:
                    avg_low_returns.append(week_analysis["avg_low_return"])

        if not valid_analyses:
            return {"error": "没有有效的一周分析数据"}

        # 计算统计信息
        stats = {
            "valid_stocks": len(valid_analyses),
            "max_high_stats": {
                "avg_return": (
                    float(np.mean(max_high_returns)) if max_high_returns else 0
                ),
                "median_return": (
                    float(np.median(max_high_returns)) if max_high_returns else 0
                ),
                "max_return": (
                    float(np.max(max_high_returns)) if max_high_returns else 0
                ),
                "min_return": (
                    float(np.min(max_high_returns)) if max_high_returns else 0
                ),
                "avg_peak_day": float(np.mean(high_peak_days)) if high_peak_days else 0,
                "peak_day_distribution": (
                    {
                        int(k): int(v)
                        for k, v in pd.Series(high_peak_days).value_counts().items()
                    }
                    if high_peak_days
                    else {}
                ),
            },
            "min_low_stats": {
                "avg_return": float(np.mean(min_low_returns)) if min_low_returns else 0,
                "median_return": (
                    float(np.median(min_low_returns)) if min_low_returns else 0
                ),
                "max_return": float(np.max(min_low_returns)) if min_low_returns else 0,
                "min_return": float(np.min(min_low_returns)) if min_low_returns else 0,
                "avg_trough_day": (
                    float(np.mean(low_trough_days)) if low_trough_days else 0
                ),
                "trough_day_distribution": (
                    {
                        int(k): int(v)
                        for k, v in pd.Series(low_trough_days).value_counts().items()
                    }
                    if low_trough_days
                    else {}
                ),
            },
            "daily_avg_stats": {
                "avg_high_return": (
                    float(np.mean(avg_high_returns)) if avg_high_returns else 0
                ),
                "avg_low_return": (
                    float(np.mean(avg_low_returns)) if avg_low_returns else 0
                ),
            },
        }

        return stats

    except Exception as e:
        logger.warning(f"计算一周汇总统计失败: {e}")
        return {"error": str(e)}


def get_next_trading_day(date: str, trading_dates: List[str]) -> Optional[str]:
    """
    获取指定日期的下一个交易日

    Args:
        date: 日期，格式为YYYY-MM-DD
        trading_dates: 交易日列表，格式为YYYY-MM-DD

    Returns:
        下一个交易日，如果没有则返回None
    """
    try:
        # 确保交易日列表已排序
        sorted_dates = sorted(trading_dates)

        # 找到当前日期在列表中的位置
        for i, d in enumerate(sorted_dates):
            if d == date:
                # 如果有下一个交易日，返回它
                if i + 1 < len(sorted_dates):
                    return sorted_dates[i + 1]
                break

        # 如果没有找到当前日期或没有下一个交易日
        return None
    except Exception as e:
        logger.error(f"获取下一个交易日失败: {e}")
        return None


def run_backtest(
    data_dir: Path, config_path: Path, start_date: str, end_date: str, output_dir: Path
) -> Dict[str, Any]:
    """
    运行回测

    Args:
        data_dir: 数据目录
        config_path: 配置文件路径
        start_date: 开始日期，格式为YYYY-MM-DD
        end_date: 结束日期，格式为YYYY-MM-DD
        output_dir: 输出目录

    Returns:
        回测结果
    """
    # 创建输出目录
    output_dir.mkdir(parents=True, exist_ok=True)

    # 获取交易日列表
    trading_dates = get_trading_dates(start_date, end_date)
    if not trading_dates:
        logger.error("获取交易日列表失败")
        return {}

    # 加载股票数据
    codes = [f.stem for f in data_dir.glob("*.csv")]
    data = load_data(data_dir, codes)
    if not data:
        logger.error("未能加载任何行情数据")
        return {}

    # 加载选股器配置
    selector_cfgs = load_config(config_path)

    # 创建股票名称映射器
    name_mapper = StockNameMapper()

    # 回测结果
    results = {}

    # 对每个交易日进行回测
    for date in tqdm(trading_dates, desc="回测进度"):
        # 获取下一个交易日
        next_date = get_next_trading_day(date, trading_dates)
        if not next_date:
            logger.warning(f"交易日 {date} 没有下一个交易日，跳过")
            continue

        # 转换日期格式
        trade_date = pd.to_datetime(date)
        next_trade_date = pd.to_datetime(next_date)

        # 对每个选股器进行回测
        for cfg in selector_cfgs:
            if cfg.get("activate", True) is False:
                continue

            try:
                alias, selector = instantiate_selector(cfg)
            except Exception as e:
                logger.error(f"跳过配置 {cfg}：{e}")
                continue

            # 选股
            picks = selector.select(trade_date, data)
            if not picks:
                logger.info(f"策略 {alias} 在 {date} 没有选出股票")
                continue

            # 获取股票名称
            stock_names = name_mapper.get_stock_names(picks)

            # 计算收益率
            stock_results = []
            for code in picks:
                if code not in data:
                    continue

                df = data[code]

                # 获取当日和下一个交易日的收盘价
                current_price = df[df["date"] == trade_date]["close"].values
                next_price = df[df["date"] == next_trade_date]["close"].values

                if len(current_price) == 0 or len(next_price) == 0:
                    continue

                current_price = current_price[0]
                next_price = next_price[0]

                # 计算收益率
                change_pct = (next_price - current_price) / current_price * 100

                # 计算一周内的表现
                week_analysis = calculate_week_performance(
                    df, trade_date, trading_dates, current_price
                )

                stock_results.append(
                    {
                        "code": code,
                        "name": stock_names.get(code, code),
                        "current_price": float(current_price),
                        "next_price": float(next_price),
                        "change_pct": float(change_pct),
                        "week_analysis": week_analysis,
                    }
                )

            # 如果没有有效结果，跳过
            if not stock_results:
                continue

            # 计算汇总统计
            change_pcts = [r["change_pct"] for r in stock_results]

            # 计算一周表现统计
            week_stats = calculate_week_summary_stats(stock_results)

            summary = {
                "avg_return": float(np.mean(change_pcts)),
                "median_return": float(np.median(change_pcts)),
                "max_return": float(np.max(change_pcts)),
                "min_return": float(np.min(change_pcts)),
                "win_rate": float(
                    sum(1 for pct in change_pcts if pct > 0) / len(change_pcts)
                ),
                "stock_count": len(stock_results),
                "week_stats": week_stats,
            }

            # 保存结果
            strategy_result = {
                "strategy": alias,
                "trade_date": date,
                "next_date": next_date,
                "stocks": stock_results,
                "summary": summary,
            }

            # 添加到结果字典
            if alias not in results:
                results[alias] = []
            results[alias].append(strategy_result)

            # 保存单个策略的结果
            strategy_dir = output_dir / alias
            strategy_dir.mkdir(exist_ok=True)

            with open(strategy_dir / f"{date}.json", "w", encoding="utf-8") as f:
                json.dump(strategy_result, f, ensure_ascii=False, indent=2)

    # 计算每个策略的总体统计
    overall_results = {}
    for strategy, strategy_results in results.items():
        all_returns = []
        win_count = 0
        total_count = 0

        for result in strategy_results:
            for stock in result["stocks"]:
                all_returns.append(stock["change_pct"])
                if stock["change_pct"] > 0:
                    win_count += 1
                total_count += 1

        if total_count > 0:
            overall_results[strategy] = {
                "avg_return": float(np.mean(all_returns)) if all_returns else 0,
                "median_return": float(np.median(all_returns)) if all_returns else 0,
                "max_return": float(np.max(all_returns)) if all_returns else 0,
                "min_return": float(np.min(all_returns)) if all_returns else 0,
                "win_rate": float(win_count / total_count) if total_count else 0,
                "stock_count": total_count,
                "trading_days": len(strategy_results),
            }

    # 保存总体结果
    with open(output_dir / "overall_results.json", "w", encoding="utf-8") as f:
        json.dump(overall_results, f, ensure_ascii=False, indent=2)

    return overall_results


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="回测选股策略")
    parser.add_argument("--data-dir", default="./data", help="CSV行情目录")
    parser.add_argument("--config", default="./configs.json", help="Selector配置文件")
    parser.add_argument("--start-date", help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end-date", help="结束日期 YYYY-MM-DD")
    parser.add_argument(
        "--days", type=int, default=60, help="回测天数（如果未指定日期范围）"
    )
    parser.add_argument("--output-dir", default="./backtest_result", help="输出目录")
    args = parser.parse_args()

    # 处理日期参数
    if args.end_date:
        end_date = args.end_date
    else:
        # 默认使用今天作为结束日期
        end_date = datetime.now().strftime("%Y-%m-%d")

    if args.start_date:
        start_date = args.start_date
    else:
        # 默认使用结束日期前N天作为开始日期
        start_date = (
            datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=args.days)
        ).strftime("%Y-%m-%d")

    logger.info(f"回测日期范围: {start_date} 至 {end_date}")

    # 运行回测
    results = run_backtest(
        data_dir=Path(args.data_dir),
        config_path=Path(args.config),
        start_date=start_date,
        end_date=end_date,
        output_dir=Path(args.output_dir),
    )

    # 输出总体结果
    logger.info("回测完成，总体结果:")
    for strategy, stats in results.items():
        logger.info(f"策略: {strategy}")
        logger.info(f"  平均收益率: {stats['avg_return']:.2f}%")
        logger.info(f"  胜率: {stats['win_rate']*100:.2f}%")
        logger.info(f"  交易日数: {stats['trading_days']}")
        logger.info(f"  股票数: {stats['stock_count']}")


if __name__ == "__main__":
    main()
