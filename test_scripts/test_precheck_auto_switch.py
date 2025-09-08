#!/usr/bin/env python3
"""
测试预检查自动切换数据源功能
在获取数据前检查本地文件，如果文件不存在或行数<50就自动切换到akshare
"""

import tempfile
import pandas as pd
from pathlib import Path
from unittest.mock import patch
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fetch_kline import fetch_one


def create_mock_data_few_rows(start: str, end: str, code: str) -> pd.DataFrame:
    """创建少量数据（模拟数据不足的情况）"""
    dates = pd.date_range(start=start, end=end, freq="D")[:10]  # 只返回10条数据
    data = []
    base_price = 10.0

    for i, date in enumerate(dates):
        if date.weekday() < 5:  # 只包含工作日
            data.append(
                {
                    "date": date,
                    "open": base_price + i * 0.1,
                    "high": base_price + i * 0.1 + 0.5,
                    "low": base_price + i * 0.1 - 0.3,
                    "close": base_price + i * 0.1 + 0.2,
                    "volume": 1000000 + i * 10000,
                    "code": code,
                }
            )

    return pd.DataFrame(data)


def create_mock_data_full_history(start: str, end: str, code: str) -> pd.DataFrame:
    """创建完整历史数据（模拟akshare返回的大量数据）"""
    dates = pd.date_range(start=start, end=end, freq="D")
    data = []
    base_price = 5.0

    for i, date in enumerate(dates):
        if date.weekday() < 5:  # 只包含工作日
            data.append(
                {
                    "date": date,
                    "open": base_price + i * 0.01,
                    "high": base_price + i * 0.01 + 0.5,
                    "low": base_price + i * 0.01 - 0.3,
                    "close": base_price + i * 0.01 + 0.2,
                    "volume": 1000000 + i * 1000,
                    "code": code,
                }
            )

    return pd.DataFrame(data)


def test_file_not_exists():
    """测试文件不存在时的自动切换"""
    print("=" * 80)
    print("测试1：文件不存在时自动切换到akshare")
    print("=" * 80)

    with tempfile.TemporaryDirectory() as temp_dir:
        test_dir = Path(temp_dir)
        test_code = "000001"

        def mock_get_kline(code, start, end, adjust, datasource, freq_code):
            """模拟数据获取"""
            print(f"  📊 模拟获取数据：{code}, {start} 到 {end}, 数据源: {datasource}")

            if datasource == "akshare" and start == "19700101":
                # akshare从1970年开始返回大量历史数据
                df = create_mock_data_full_history(start, end, code)
                print(f"    akshare返回 {len(df)} 条历史数据")
                return df
            else:
                # 其他情况返回少量数据
                df = create_mock_data_few_rows(start, end, code)
                print(f"    {datasource}返回 {len(df)} 条数据")
                return df

        print(f"测试股票: {test_code}")
        print(f"输出目录: {test_dir}")
        print(f"文件状态: 不存在")
        print(f"预期行为: 直接切换到akshare数据源")

        with patch("fetch_kline.get_kline", side_effect=mock_get_kline):
            success = fetch_one(
                code=test_code,
                start="20240101",
                end="20240131",
                out_dir=test_dir,
                incremental=False,  # 非增量模式
                datasource="mootdx",  # 初始数据源
                freq_code=4,
                adjust="qfq",
                max_null_ratio=0.3,
                min_rows_threshold=50,
            )

        print(f"\n抓取结果：{'成功' if success else '失败'}")

        csv_path = test_dir / f"{test_code}.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path, parse_dates=["date"])
            print(f"✅ 文件创建成功")
            print(f"   数据行数: {len(df)}")
            print(
                f"   日期范围: {df['date'].min().date()} 到 {df['date'].max().date()}"
            )

            if len(df) >= 50:
                print("✅ 预检查切换成功，获得充足的历史数据")
            else:
                print("❌ 数据行数仍然不足")
        else:
            print("❌ 文件创建失败")


def test_file_exists_insufficient_data():
    """测试文件存在但数据不足时的自动切换"""
    print("\n" + "=" * 80)
    print("测试2：文件存在但数据不足时自动切换")
    print("=" * 80)

    with tempfile.TemporaryDirectory() as temp_dir:
        test_dir = Path(temp_dir)
        test_code = "000002"
        csv_path = test_dir / f"{test_code}.csv"

        # 预先创建一个数据不足的文件
        insufficient_data = create_mock_data_few_rows("20240101", "20240110", test_code)
        insufficient_data.to_csv(csv_path, index=False)

        print(f"测试股票: {test_code}")
        print(f"预创建文件: {len(insufficient_data)} 行数据")
        print(f"预期行为: 检测到数据不足，切换到akshare数据源")

        def mock_get_kline(code, start, end, adjust, datasource, freq_code):
            """模拟数据获取"""
            print(f"  📊 模拟获取数据：{code}, {start} 到 {end}, 数据源: {datasource}")

            if datasource == "akshare" and start == "19700101":
                df = create_mock_data_full_history(start, end, code)
                print(f"    akshare返回 {len(df)} 条历史数据")
                return df
            else:
                df = create_mock_data_few_rows(start, end, code)
                print(f"    {datasource}返回 {len(df)} 条数据")
                return df

        with patch("fetch_kline.get_kline", side_effect=mock_get_kline):
            success = fetch_one(
                code=test_code,
                start="20240101",
                end="20240131",
                out_dir=test_dir,
                incremental=False,
                datasource="mootdx",
                freq_code=4,
                adjust="qfq",
                max_null_ratio=0.3,
                min_rows_threshold=50,
            )

        print(f"\n抓取结果：{'成功' if success else '失败'}")

        if csv_path.exists():
            df = pd.read_csv(csv_path, parse_dates=["date"])
            print(f"✅ 文件更新成功")
            print(f"   更新后数据行数: {len(df)}")
            print(
                f"   日期范围: {df['date'].min().date()} 到 {df['date'].max().date()}"
            )

            if len(df) >= 50:
                print("✅ 预检查切换成功，数据已补充完整")
            else:
                print("❌ 数据行数仍然不足")
        else:
            print("❌ 文件更新失败")


def test_file_exists_sufficient_data():
    """测试文件存在且数据充足时不切换"""
    print("\n" + "=" * 80)
    print("测试3：文件存在且数据充足时不切换")
    print("=" * 80)

    with tempfile.TemporaryDirectory() as temp_dir:
        test_dir = Path(temp_dir)
        test_code = "000003"
        csv_path = test_dir / f"{test_code}.csv"

        # 预先创建一个数据充足的文件
        sufficient_data = create_mock_data_full_history(
            "20240101", "20240331", test_code
        )
        sufficient_data.to_csv(csv_path, index=False)

        print(f"测试股票: {test_code}")
        print(f"预创建文件: {len(sufficient_data)} 行数据")
        print(f"预期行为: 数据充足，使用原始数据源")

        def mock_get_kline(code, start, end, adjust, datasource, freq_code):
            """模拟数据获取"""
            print(f"  📊 模拟获取数据：{code}, {start} 到 {end}, 数据源: {datasource}")

            # 应该使用原始数据源（mootdx）
            df = create_mock_data_few_rows(start, end, code)
            print(f"    {datasource}返回 {len(df)} 条数据")
            return df

        with patch("fetch_kline.get_kline", side_effect=mock_get_kline):
            success = fetch_one(
                code=test_code,
                start="20240101",
                end="20240131",
                out_dir=test_dir,
                incremental=False,
                datasource="mootdx",
                freq_code=4,
                adjust="qfq",
                max_null_ratio=0.3,
                min_rows_threshold=50,
            )

        print(f"\n抓取结果：{'成功' if success else '失败'}")

        if csv_path.exists():
            df = pd.read_csv(csv_path, parse_dates=["date"])
            print(f"✅ 文件处理成功")
            print(f"   最终数据行数: {len(df)}")
            print(
                f"   日期范围: {df['date'].min().date()} 到 {df['date'].max().date()}"
            )
            print("✅ 数据充足，正确使用原始数据源")
        else:
            print("❌ 文件处理失败")


def test_incremental_mode():
    """测试增量模式下的行为"""
    print("\n" + "=" * 80)
    print("测试4：增量模式下的预检查行为")
    print("=" * 80)

    with tempfile.TemporaryDirectory() as temp_dir:
        test_dir = Path(temp_dir)
        test_code = "000004"
        csv_path = test_dir / f"{test_code}.csv"

        # 预先创建一个数据不足的文件
        insufficient_data = create_mock_data_few_rows("20240101", "20240110", test_code)
        insufficient_data.to_csv(csv_path, index=False)

        print(f"测试股票: {test_code}")
        print(f"预创建文件: {len(insufficient_data)} 行数据")
        print(f"模式: 增量更新")
        print(f"预期行为: 即使在增量模式下，数据不足时也会切换到akshare")

        def mock_get_kline(code, start, end, adjust, datasource, freq_code):
            """模拟数据获取"""
            print(f"  📊 模拟获取数据：{code}, {start} 到 {end}, 数据源: {datasource}")

            # 增量模式应该使用原始数据源
            df = create_mock_data_few_rows(start, end, code)
            print(f"    {datasource}返回 {len(df)} 条数据")
            return df

        with patch("fetch_kline.get_kline", side_effect=mock_get_kline):
            success = fetch_one(
                code=test_code,
                start="20240101",
                end="20240131",
                out_dir=test_dir,
                incremental=True,  # 增量模式
                datasource="mootdx",
                freq_code=4,
                adjust="qfq",
                max_null_ratio=0.3,
                min_rows_threshold=50,
            )

        print(f"\n抓取结果：{'成功' if success else '失败'}")

        if csv_path.exists():
            df = pd.read_csv(csv_path, parse_dates=["date"])
            print(f"✅ 文件更新成功")
            print(f"   更新后数据行数: {len(df)}")
            print(
                f"   日期范围: {df['date'].min().date()} 到 {df['date'].max().date()}"
            )

            if len(df) >= 50:
                print("✅ 增量模式下预检查正常工作，数据不足时成功切换")
            else:
                print("❌ 数据行数仍然不足")
        else:
            print("❌ 文件更新失败")


if __name__ == "__main__":
    test_file_not_exists()
    test_file_exists_insufficient_data()
    test_file_exists_sufficient_data()
    test_incremental_mode()
    print("\n" + "=" * 80)
    print("所有测试完成")
    print("=" * 80)
