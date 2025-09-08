#!/usr/bin/env python3
"""
测试XD股票检测功能
"""

import tempfile
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock
import sys

# 添加当前目录到Python路径
sys.path.insert(0, ".")

from fetch_kline import fetch_one


def create_mock_stock_info(stock_name: str):
    """创建模拟的股票信息DataFrame"""
    return pd.DataFrame(
        {
            "item": ["股票简称", "股票代码", "所属市场"],
            "value": [stock_name, "000001", "深圳主板"],
        }
    )


def create_mock_data(start_date: str, end_date: str, code: str, rows: int = 100):
    """创建模拟的K线数据"""
    dates = pd.date_range(start_date, periods=rows, freq="D")
    return pd.DataFrame(
        {
            "date": dates,
            "open": [10.0 + i * 0.1 for i in range(rows)],
            "high": [10.5 + i * 0.1 for i in range(rows)],
            "low": [9.5 + i * 0.1 for i in range(rows)],
            "close": [10.2 + i * 0.1 for i in range(rows)],
            "volume": [1000000] * rows,
            "code": [code] * rows,
        }
    )


def test_xd_stock_detection():
    """测试XD股票检测功能"""
    print("=" * 80)
    print("测试XD股票检测功能")
    print("=" * 80)

    with tempfile.TemporaryDirectory() as temp_dir:
        test_dir = Path(temp_dir)
        test_code = "000001"

        # 测试场景1：正常股票（不以XD开头），且已有充足数据
        print("\n场景1：正常股票（不以XD开头），且已有充足数据")
        print("-" * 40)

        # 预先创建一个数据充足的文件
        csv_path = test_dir / f"{test_code}.csv"
        existing_data = create_mock_data("2023-01-01", "2024-01-01", test_code, 100)
        existing_data.to_csv(csv_path, index=False)
        print(f"预创建文件: {len(existing_data)} 行数据（数据充足）")

        def mock_stock_info_normal(symbol):
            return create_mock_stock_info("平安银行")

        def mock_get_kline_normal(code, start, end, adjust, datasource, freq_code):
            print(f"  📊 模拟获取数据：{code}, {start} 到 {end}, 数据源: {datasource}")
            if datasource == "akshare" and start == "19700101":
                return create_mock_data("1970-01-01", end, code, 14110)
            else:
                return create_mock_data(start, end, code, 30)

        with patch(
            "akshare.stock_individual_info_em", side_effect=mock_stock_info_normal
        ):
            with patch("fetch_kline.get_kline", side_effect=mock_get_kline_normal):
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

        print(f"抓取结果：{'成功' if success else '失败'}")

        # 检查结果文件
        if csv_path.exists():
            df = pd.read_csv(csv_path, parse_dates=["date"])
            print(f"✅ 文件处理成功")
            print(f"   最终数据行数: {len(df)}")
            print(
                f"   预期行为: 正常股票且数据充足，应使用原始数据源(mootdx)，获得30行数据"
            )
            if len(df) == 30:
                print("✅ 正常股票处理正确")
            else:
                print("❌ 正常股票处理异常，可能被错误切换了数据源")

        # 清理文件
        if csv_path.exists():
            csv_path.unlink()

        # 测试场景2：XD股票（以XD开头）
        print("\n场景2：XD股票（以XD开头）")
        print("-" * 40)

        def mock_stock_info_xd(symbol):
            return create_mock_stock_info("XD平安银行")

        def mock_get_kline_xd(code, start, end, adjust, datasource, freq_code):
            print(f"  📊 模拟获取数据：{code}, {start} 到 {end}, 数据源: {datasource}")
            if datasource == "akshare" and start == "19700101":
                return create_mock_data("1970-01-01", end, code, 14110)
            else:
                return create_mock_data(start, end, code, 30)

        with patch("akshare.stock_individual_info_em", side_effect=mock_stock_info_xd):
            with patch("fetch_kline.get_kline", side_effect=mock_get_kline_xd):
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

        print(f"抓取结果：{'成功' if success else '失败'}")

        # 检查结果文件
        if csv_path.exists():
            df = pd.read_csv(csv_path, parse_dates=["date"])
            print(f"✅ 文件创建成功")
            print(f"   数据行数: {len(df)}")
            print(
                f"   日期范围: {df['date'].min().date()} 到 {df['date'].max().date()}"
            )
            print(
                f"   预期行为: XD股票应自动切换到akshare数据源，从1970年开始获得14110行数据"
            )
            if len(df) == 14110 and df["date"].min().year == 1970:
                print("✅ XD股票自动切换成功")
            else:
                print("❌ XD股票自动切换失败")
        else:
            print("❌ 文件创建失败")


def test_xd_stock_with_existing_file():
    """测试XD股票在已有文件情况下的处理"""
    print("\n" + "=" * 80)
    print("测试XD股票在已有文件情况下的处理")
    print("=" * 80)

    with tempfile.TemporaryDirectory() as temp_dir:
        test_dir = Path(temp_dir)
        test_code = "000002"
        csv_path = test_dir / f"{test_code}.csv"

        # 预先创建一个数据充足的文件
        existing_data = create_mock_data("2023-01-01", "2024-01-01", test_code, 200)
        existing_data.to_csv(csv_path, index=False)

        print(f"预创建文件: {len(existing_data)} 行数据（数据充足）")
        print(f"预期行为: 即使文件数据充足，XD股票也应该重新从1970年开始抓取")

        def mock_stock_info_xd(symbol):
            return create_mock_stock_info("XD万科A")

        def mock_get_kline_xd(code, start, end, adjust, datasource, freq_code):
            print(f"  📊 模拟获取数据：{code}, {start} 到 {end}, 数据源: {datasource}")
            if datasource == "akshare" and start == "19700101":
                return create_mock_data("1970-01-01", end, code, 14110)
            else:
                return create_mock_data(start, end, code, 30)

        with patch("akshare.stock_individual_info_em", side_effect=mock_stock_info_xd):
            with patch("fetch_kline.get_kline", side_effect=mock_get_kline_xd):
                success = fetch_one(
                    code=test_code,
                    start="20240101",
                    end="20240131",
                    out_dir=test_dir,
                    incremental=True,  # 使用增量模式
                    datasource="mootdx",
                    freq_code=4,
                    adjust="qfq",
                    max_null_ratio=0.3,
                    min_rows_threshold=50,
                )

        print(f"抓取结果：{'成功' if success else '失败'}")

        # 检查结果文件
        if csv_path.exists():
            df = pd.read_csv(csv_path, parse_dates=["date"])
            print(f"✅ 文件更新成功")
            print(f"   更新后数据行数: {len(df)}")
            print(
                f"   日期范围: {df['date'].min().date()} 到 {df['date'].max().date()}"
            )

            if len(df) == 14110 and df["date"].min().year == 1970:
                print(
                    "✅ XD股票优先级检查成功，即使文件数据充足也重新抓取了完整历史数据"
                )
            else:
                print("❌ XD股票优先级检查失败")
        else:
            print("❌ 文件更新失败")


def test_akshare_api_error():
    """测试akshare API调用失败的情况"""
    print("\n" + "=" * 80)
    print("测试akshare API调用失败的情况")
    print("=" * 80)

    with tempfile.TemporaryDirectory() as temp_dir:
        test_dir = Path(temp_dir)
        test_code = "000003"

        def mock_stock_info_error(symbol):
            raise Exception("网络连接失败")

        def mock_get_kline_normal(code, start, end, adjust, datasource, freq_code):
            print(f"  📊 模拟获取数据：{code}, {start} 到 {end}, 数据源: {datasource}")
            return create_mock_data(start, end, code, 30)

        print("预期行为: API调用失败时，应该按正常流程处理（不切换数据源）")

        with patch(
            "akshare.stock_individual_info_em", side_effect=mock_stock_info_error
        ):
            with patch("fetch_kline.get_kline", side_effect=mock_get_kline_normal):
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

        print(f"抓取结果：{'成功' if success else '失败'}")

        # 检查结果文件
        csv_path = test_dir / f"{test_code}.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path, parse_dates=["date"])
            print(f"✅ 文件创建成功")
            print(f"   数据行数: {len(df)}")
            if len(df) == 30:
                print("✅ API失败时正确回退到正常流程")
            else:
                print("❌ API失败时处理异常")
        else:
            print("❌ 文件创建失败")


if __name__ == "__main__":
    print("开始XD股票检测功能测试...")
    test_xd_stock_detection()
    test_xd_stock_with_existing_file()
    test_akshare_api_error()
    print("\n" + "=" * 80)
    print("XD股票检测功能测试完成")
    print("=" * 80)
