#!/usr/bin/env python3
"""
自动切换数据源功能使用示例

当抓取当前日期数据时，如果获得的总数据行数<50条，
则自动设置数据源为akshare，从19700101开始抓取完整历史数据。
"""

import subprocess
import sys
from pathlib import Path


def run_fetch_with_auto_switch():
    """运行fetch_kline.py，演示自动切换数据源功能"""

    print("=" * 80)
    print("自动切换数据源功能演示")
    print("=" * 80)
    print()
    print("功能说明：")
    print("- 预检查本地文件状态，如果文件不存在或数据行数 < 50条")
    print("- 自动切换到akshare数据源，从1970年1月1日开始抓取")
    print("- 确保每只股票都有足够的历史数据用于分析")
    print("- 增量模式下跳过预检查，使用原始配置")
    print()

    # 创建测试输出目录
    output_dir = Path("./data_auto_switch_test")
    output_dir.mkdir(exist_ok=True)

    # 构建命令
    cmd = [
        sys.executable,
        "fetch_kline.py",
        "--datasource",
        "mootdx",  # 初始数据源
        "--start",
        "today",  # 从今天开始（可能数据不足）
        "--end",
        "today",  # 到今天结束
        "--out",
        str(output_dir),
        "--workers",
        "1",  # 单线程便于观察
        "--min-rows-threshold",
        "50",  # 设置50行阈值
        "--min-mktcap",
        "1e9",  # 降低市值要求，获得更多股票
        "--max-mktcap",
        "5e9",  # 限制市值范围，减少股票数量
    ]

    print("执行命令：")
    print(" ".join(cmd))
    print()
    print("预期行为：")
    print("1. 预检查本地文件状态（文件不存在或数据不足）")
    print("2. 自动切换到akshare数据源")
    print("3. 从1970年开始抓取完整历史数据")
    print("4. 跳过原始数据源的抓取步骤")
    print()
    print("开始执行...")
    print("-" * 80)

    try:
        # 执行命令
        result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")

        print("标准输出：")
        print(result.stdout)

        if result.stderr:
            print("错误输出：")
            print(result.stderr)

        print("-" * 80)
        print(f"命令执行完成，返回码: {result.returncode}")

        # 检查输出文件
        csv_files = list(output_dir.glob("*.csv"))
        if csv_files:
            print(f"\n生成的数据文件 ({len(csv_files)} 个):")
            for csv_file in csv_files[:5]:  # 只显示前5个
                try:
                    import pandas as pd

                    df = pd.read_csv(csv_file, parse_dates=["date"])
                    print(
                        f"  {csv_file.name}: {len(df)} 行数据, "
                        f"日期范围 {df['date'].min().date()} 到 {df['date'].max().date()}"
                    )
                except Exception as e:
                    print(f"  {csv_file.name}: 读取失败 - {e}")

            if len(csv_files) > 5:
                print(f"  ... 还有 {len(csv_files) - 5} 个文件")
        else:
            print("\n未生成数据文件")

    except Exception as e:
        print(f"执行失败: {e}")


def show_parameter_help():
    """显示新参数的帮助信息"""
    print("\n" + "=" * 80)
    print("新增参数说明")
    print("=" * 80)
    print()
    print("--min-rows-threshold INT")
    print("  最小数据行数阈值，默认50行")
    print("  当获取的数据行数少于此阈值时，自动切换到akshare数据源")
    print("  并从1970年1月1日开始重新抓取完整历史数据")
    print()
    print("使用示例：")
    print("  # 设置阈值为100行")
    print("  python fetch_kline.py --min-rows-threshold 100")
    print()
    print("  # 设置阈值为30行")
    print("  python fetch_kline.py --min-rows-threshold 30")
    print()
    print("  # 禁用自动切换（设置为0）")
    print("  python fetch_kline.py --min-rows-threshold 0")
    print()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--help-params":
        show_parameter_help()
    else:
        run_fetch_with_auto_switch()
        show_parameter_help()
