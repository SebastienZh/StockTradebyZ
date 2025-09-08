#!/usr/bin/env python3
"""
测试选股结果功能
"""

import sys
from pathlib import Path

import pandas as pd


def test_selection_result():
    """测试选股结果文件的格式和内容"""

    # 检查结果目录是否存在
    result_dir = Path("selection_result")
    if not result_dir.exists():
        print("❌ selection_result 目录不存在")
        return False

    # 查找结果文件
    result_files = list(result_dir.glob("selection_result_*.csv"))
    if not result_files:
        print("❌ 未找到选股结果文件")
        return False

    print(f"✅ 找到 {len(result_files)} 个结果文件")

    # 测试最新的结果文件
    latest_file = max(result_files, key=lambda x: x.name)
    print(f"📄 测试文件: {latest_file.name}")

    try:
        # 读取CSV时指定股票代码列为字符串类型
        df = pd.read_csv(latest_file, dtype={"股票代码": str})

        # 检查列名
        expected_columns = ["股票名称", "股票代码", "选择器", "盈亏比", "市值(亿元)"]
        if list(df.columns) != expected_columns:
            print(f"❌ 列名不正确，期望: {expected_columns}, 实际: {list(df.columns)}")
            return False
        print("✅ 列名正确")

        # 检查数据类型
        if not pd.api.types.is_string_dtype(df["股票名称"]):
            print("❌ 股票名称列应为字符串类型")
            return False

        if not pd.api.types.is_string_dtype(df["股票代码"]):
            print("❌ 股票代码列应为字符串类型")
            return False

        if not pd.api.types.is_string_dtype(df["选择器"]):
            print("❌ 选择器列应为字符串类型")
            return False

        if not pd.api.types.is_numeric_dtype(df["盈亏比"]):
            print("❌ 盈亏比列应为数值类型")
            return False

        if not pd.api.types.is_numeric_dtype(df["市值(亿元)"]):
            print("❌ 市值列应为数值类型")
            return False

        print("✅ 数据类型正确")

        # 检查排序（盈亏比应该是降序）
        if not df["盈亏比"].is_monotonic_decreasing:
            print("❌ 盈亏比未按降序排列")
            return False
        print("✅ 盈亏比按降序排列")

        # 检查数据完整性
        if df.isnull().any().any():
            print("⚠️  数据中存在空值")

        # 显示统计信息
        print(f"📊 统计信息:")
        print(f"   - 股票数量: {len(df)}")
        print(f"   - 盈亏比范围: {df['盈亏比'].min():.3f} ~ {df['盈亏比'].max():.3f}")
        print(
            f"   - 市值范围: {df['市值(亿元)'].min():.2f} ~ {df['市值(亿元)'].max():.2f} 亿元"
        )

        # 显示前5只股票
        print(f"\n🏆 前5只股票:")
        for i, row in df.head(5).iterrows():
            print(
                f"   {i+1}. {row['股票名称']}({row['股票代码']}) - 选择器: {row['选择器']} - 盈亏比: {row['盈亏比']:.3f}, 市值: {row['市值(亿元)']:.2f}亿元"
            )

        return True

    except Exception as e:
        print(f"❌ 读取文件失败: {e}")
        return False


if __name__ == "__main__":
    print("🧪 测试选股结果功能...")
    success = test_selection_result()

    if success:
        print("\n✅ 所有测试通过！")
        sys.exit(0)
    else:
        print("\n❌ 测试失败！")
        sys.exit(1)
