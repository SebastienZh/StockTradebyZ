#!/usr/bin/env python3
"""
回测报告生成器 - 生成详细的回测分析报告

功能：
1. 读取回测结果JSON文件
2. 生成详细的策略表现分析
3. 输出中文格式的报告
"""

import json
import argparse
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime
import pandas as pd


def load_backtest_results(result_dir: Path) -> Dict[str, Any]:
    """加载回测结果"""
    overall_file = result_dir / "overall_results.json"
    if not overall_file.exists():
        print(f"错误：找不到总体结果文件 {overall_file}")
        return {}

    with open(overall_file, "r", encoding="utf-8") as f:
        return json.load(f)


def load_detailed_results(result_dir: Path) -> Dict[str, List[Dict]]:
    """加载详细的回测结果"""
    detailed_results = {}

    # 遍历每个策略目录
    for strategy_dir in result_dir.iterdir():
        if strategy_dir.is_dir() and strategy_dir.name != "__pycache__":
            strategy_name = strategy_dir.name
            strategy_results = []

            # 加载该策略的所有日期结果
            for result_file in strategy_dir.glob("*.json"):
                try:
                    with open(result_file, "r", encoding="utf-8") as f:
                        result = json.load(f)
                        strategy_results.append(result)
                except Exception as e:
                    print(f"警告：加载文件 {result_file} 失败: {e}")

            if strategy_results:
                # 按日期排序
                strategy_results.sort(key=lambda x: x["trade_date"])
                detailed_results[strategy_name] = strategy_results

    return detailed_results


def generate_week_analysis_report(detailed_results: List[Dict]) -> str:
    """生成一周表现分析报告"""
    try:
        import numpy as np

        all_week_stats = []
        for day_result in detailed_results:
            summary = day_result.get("summary", {})
            week_stats = summary.get("week_stats", {})
            if "error" not in week_stats and week_stats:
                all_week_stats.append(week_stats)

        if not all_week_stats:
            return ""

        report = []
        report.append(f"\n📊 一周表现分析:")

        # 汇总所有一周统计数据
        all_max_high_returns = []
        all_min_low_returns = []
        all_peak_days = []
        all_trough_days = []
        all_avg_high_returns = []
        all_avg_low_returns = []

        for stats in all_week_stats:
            max_high_stats = stats.get("max_high_stats", {})
            min_low_stats = stats.get("min_low_stats", {})
            daily_avg_stats = stats.get("daily_avg_stats", {})

            if max_high_stats.get("avg_return") is not None:
                all_max_high_returns.append(max_high_stats["avg_return"])
            if max_high_stats.get("avg_peak_day") is not None:
                all_peak_days.append(max_high_stats["avg_peak_day"])

            if min_low_stats.get("avg_return") is not None:
                all_min_low_returns.append(min_low_stats["avg_return"])
            if min_low_stats.get("avg_trough_day") is not None:
                all_trough_days.append(min_low_stats["avg_trough_day"])

            if daily_avg_stats.get("avg_high_return") is not None:
                all_avg_high_returns.append(daily_avg_stats["avg_high_return"])
            if daily_avg_stats.get("avg_low_return") is not None:
                all_avg_low_returns.append(daily_avg_stats["avg_low_return"])

        # 最高价分析
        if all_max_high_returns:
            avg_max_high = np.mean(all_max_high_returns)
            report.append(f"  • 一周内最高价平均收益率: {avg_max_high:.2f}%")

        if all_peak_days:
            avg_peak_day = np.mean(all_peak_days)
            report.append(f"  • 最高价平均出现在第 {avg_peak_day:.1f} 天")

        # 最低价分析
        if all_min_low_returns:
            avg_min_low = np.mean(all_min_low_returns)
            report.append(f"  • 一周内最低价平均收益率: {avg_min_low:.2f}%")

        if all_trough_days:
            avg_trough_day = np.mean(all_trough_days)
            report.append(f"  • 最低价平均出现在第 {avg_trough_day:.1f} 天")

        # 日均表现
        if all_avg_high_returns:
            overall_avg_high = np.mean(all_avg_high_returns)
            report.append(f"  • 一周内日均最高价收益率: {overall_avg_high:.2f}%")

        if all_avg_low_returns:
            overall_avg_low = np.mean(all_avg_low_returns)
            report.append(f"  • 一周内日均最低价收益率: {overall_avg_low:.2f}%")

        return "\n".join(report)

    except Exception as e:
        return f"\n📊 一周表现分析: 生成失败 ({e})"


def generate_strategy_report(
    strategy_name: str, overall_stats: Dict, detailed_results: List[Dict]
) -> str:
    """生成单个策略的详细报告"""
    report = []
    report.append(f"\n{'='*60}")
    report.append(f"策略名称: {strategy_name}")
    report.append(f"{'='*60}")

    # 总体统计
    report.append(f"\n📊 总体表现:")
    report.append(f"  • 平均收益率: {overall_stats['avg_return']:.2f}%")
    report.append(f"  • 中位数收益率: {overall_stats['median_return']:.2f}%")
    report.append(f"  • 最大收益率: {overall_stats['max_return']:.2f}%")
    report.append(f"  • 最小收益率: {overall_stats['min_return']:.2f}%")
    report.append(f"  • 胜率: {overall_stats['win_rate']*100:.2f}%")
    report.append(f"  • 总交易日数: {overall_stats['trading_days']}")
    report.append(f"  • 总股票数: {overall_stats['stock_count']}")

    # 计算额外统计信息
    all_returns = []
    daily_returns = []
    best_day = None
    worst_day = None
    best_stock = None
    worst_stock = None

    for day_result in detailed_results:
        day_returns = [stock["change_pct"] for stock in day_result["stocks"]]
        if day_returns:
            daily_avg = sum(day_returns) / len(day_returns)
            daily_returns.append(daily_avg)
            all_returns.extend(day_returns)

            # 找到最好和最差的交易日
            if best_day is None or daily_avg > best_day["avg_return"]:
                best_day = {
                    "date": day_result["trade_date"],
                    "avg_return": daily_avg,
                    "stock_count": len(day_returns),
                }

            if worst_day is None or daily_avg < worst_day["avg_return"]:
                worst_day = {
                    "date": day_result["trade_date"],
                    "avg_return": daily_avg,
                    "stock_count": len(day_returns),
                }

            # 找到最好和最差的股票
            for stock in day_result["stocks"]:
                if best_stock is None or stock["change_pct"] > best_stock["change_pct"]:
                    best_stock = {**stock, "date": day_result["trade_date"]}

                if (
                    worst_stock is None
                    or stock["change_pct"] < worst_stock["change_pct"]
                ):
                    worst_stock = {**stock, "date": day_result["trade_date"]}

    # 风险指标
    if daily_returns:
        import numpy as np

        volatility = np.std(daily_returns)
        sharpe_ratio = np.mean(daily_returns) / volatility if volatility > 0 else 0

        report.append(f"\n📈 风险指标:")
        report.append(f"  • 日收益率波动率: {volatility:.2f}%")
        report.append(f"  • 夏普比率: {sharpe_ratio:.2f}")

    # 一周表现分析
    week_analysis_report = generate_week_analysis_report(detailed_results)
    if week_analysis_report:
        report.append(week_analysis_report)

    # 最佳表现
    if best_day:
        report.append(f"\n🏆 最佳表现:")
        report.append(
            f"  • 最佳交易日: {best_day['date']} (平均收益: {best_day['avg_return']:.2f}%, 股票数: {best_day['stock_count']})"
        )

    if best_stock:
        report.append(
            f"  • 最佳股票: {best_stock['name']} ({best_stock['code']}) 在 {best_stock['date']}"
        )
        report.append(
            f"    收益率: {best_stock['change_pct']:.2f}% ({best_stock['current_price']:.2f} → {best_stock['next_price']:.2f})"
        )

    # 最差表现
    if worst_day:
        report.append(f"\n📉 最差表现:")
        report.append(
            f"  • 最差交易日: {worst_day['date']} (平均收益: {worst_day['avg_return']:.2f}%, 股票数: {worst_day['stock_count']})"
        )

    if worst_stock:
        report.append(
            f"  • 最差股票: {worst_stock['name']} ({worst_stock['code']}) 在 {worst_stock['date']}"
        )
        report.append(
            f"    收益率: {worst_stock['change_pct']:.2f}% ({worst_stock['current_price']:.2f} → {worst_stock['next_price']:.2f})"
        )

    # 收益率分布
    if all_returns:
        positive_count = sum(1 for r in all_returns if r > 0)
        negative_count = sum(1 for r in all_returns if r < 0)
        zero_count = sum(1 for r in all_returns if r == 0)

        report.append(f"\n📊 收益率分布:")
        report.append(
            f"  • 盈利股票: {positive_count} ({positive_count/len(all_returns)*100:.1f}%)"
        )
        report.append(
            f"  • 亏损股票: {negative_count} ({negative_count/len(all_returns)*100:.1f}%)"
        )
        report.append(
            f"  • 平盘股票: {zero_count} ({zero_count/len(all_returns)*100:.1f}%)"
        )

        # 收益率区间分布
        ranges = [
            (-float("inf"), -5),
            (-5, -2),
            (-2, 0),
            (0, 2),
            (2, 5),
            (5, float("inf")),
        ]
        range_names = ["< -5%", "-5% ~ -2%", "-2% ~ 0%", "0% ~ 2%", "2% ~ 5%", "> 5%"]

        report.append(f"\n📈 收益率区间分布:")
        for i, (low, high) in enumerate(ranges):
            count = sum(1 for r in all_returns if low < r <= high)
            if count > 0:
                report.append(
                    f"  • {range_names[i]}: {count} ({count/len(all_returns)*100:.1f}%)"
                )

    return "\n".join(report)


def generate_comparison_report(overall_results: Dict[str, Any]) -> str:
    """生成策略对比报告"""
    report = []
    report.append(f"\n{'='*60}")
    report.append(f"策略对比分析")
    report.append(f"{'='*60}")

    if not overall_results:
        report.append("没有找到回测结果数据")
        return "\n".join(report)

    # 按平均收益率排序
    sorted_strategies = sorted(
        overall_results.items(), key=lambda x: x[1]["avg_return"], reverse=True
    )

    report.append(f"\n🏆 策略排名 (按平均收益率):")
    for i, (strategy, stats) in enumerate(sorted_strategies, 1):
        report.append(
            f"  {i}. {strategy}: {stats['avg_return']:.2f}% (胜率: {stats['win_rate']*100:.1f}%)"
        )

    # 按胜率排序
    sorted_by_winrate = sorted(
        overall_results.items(), key=lambda x: x[1]["win_rate"], reverse=True
    )

    report.append(f"\n🎯 策略排名 (按胜率):")
    for i, (strategy, stats) in enumerate(sorted_by_winrate, 1):
        report.append(
            f"  {i}. {strategy}: {stats['win_rate']*100:.2f}% (平均收益: {stats['avg_return']:.2f}%)"
        )

    # 综合评分 (平均收益率 * 胜率)
    scored_strategies = [
        (name, stats["avg_return"] * stats["win_rate"])
        for name, stats in overall_results.items()
    ]
    scored_strategies.sort(key=lambda x: x[1], reverse=True)

    report.append(f"\n⭐ 综合评分排名 (收益率 × 胜率):")
    for i, (strategy, score) in enumerate(scored_strategies, 1):
        report.append(f"  {i}. {strategy}: {score:.4f}")

    return "\n".join(report)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="生成回测分析报告")
    parser.add_argument(
        "--result-dir", default="./backtest_result", help="回测结果目录"
    )
    parser.add_argument("--output", default="backtest_report.txt", help="输出报告文件")
    args = parser.parse_args()

    result_dir = Path(args.result_dir)
    if not result_dir.exists():
        print(f"错误：回测结果目录 {result_dir} 不存在")
        return

    # 加载回测结果
    overall_results = load_backtest_results(result_dir)
    detailed_results = load_detailed_results(result_dir)

    if not overall_results:
        print("错误：没有找到回测结果")
        return

    # 生成报告
    report_lines = []

    # 报告头部
    report_lines.append("股票策略回测分析报告")
    report_lines.append("=" * 60)
    report_lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"回测结果目录: {result_dir}")

    # 策略对比
    report_lines.append(generate_comparison_report(overall_results))

    # 各策略详细报告
    for strategy_name, stats in overall_results.items():
        if strategy_name in detailed_results:
            strategy_report = generate_strategy_report(
                strategy_name, stats, detailed_results[strategy_name]
            )
            report_lines.append(strategy_report)

    # 保存报告
    report_content = "\n".join(report_lines)

    with open(args.output, "w", encoding="utf-8") as f:
        f.write(report_content)

    print(f"回测分析报告已生成: {args.output}")

    # 同时输出到控制台
    print("\n" + "=" * 60)
    print("回测分析报告预览:")
    print("=" * 60)
    print(
        report_content[:2000] + "..." if len(report_content) > 2000 else report_content
    )


if __name__ == "__main__":
    main()
