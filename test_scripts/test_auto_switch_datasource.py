#!/usr/bin/env python3
"""
测试自动切换数据源功能
当数据行数不足50条时，自动切换到akshare数据源从1970年开始抓取
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
    dates = pd.date_range(start=start, end=end, freq='D')[:10]  # 只返回10条数据
    data = []
    base_price = 10.0
    
    for i, date in enumerate(dates):
        if date.weekday() < 5:  # 只包含工作日
            data.append({
                'date': date,
                'open': base_price + i * 0.1,
                'high': base_price + i * 0.1 + 0.5,
                'low': base_price + i * 0.1 - 0.3,
                'close': base_price + i * 0.1 + 0.2,
                'volume': 1000000 + i * 10000,
                'code': code
            })
    
    return pd.DataFrame(data)


def create_mock_data_full_history(start: str, end: str, code: str) -> pd.DataFrame:
    """创建完整历史数据（模拟akshare返回的大量数据）"""
    dates = pd.date_range(start=start, end=end, freq='D')
    data = []
    base_price = 5.0
    
    for i, date in enumerate(dates):
        if date.weekday() < 5:  # 只包含工作日
            data.append({
                'date': date,
                'open': base_price + i * 0.01,
                'high': base_price + i * 0.01 + 0.5,
                'low': base_price + i * 0.01 - 0.3,
                'close': base_price + i * 0.01 + 0.2,
                'volume': 1000000 + i * 1000,
                'code': code
            })
    
    return pd.DataFrame(data)


def test_auto_switch_datasource():
    """测试自动切换数据源功能"""
    print("=" * 80)
    print("测试自动切换数据源功能")
    print("=" * 80)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        test_dir = Path(temp_dir)
        test_code = "000001"
        
        def mock_get_kline(code, start, end, adjust, datasource, freq_code):
            """模拟数据获取，根据数据源和日期范围返回不同数据"""
            print(f"  📊 模拟获取数据：{code}, {start} 到 {end}, 数据源: {datasource}")
            
            if datasource == "mootdx":
                # mootdx返回少量数据（触发切换条件）
                df = create_mock_data_few_rows(start, end, code)
                print(f"    mootdx返回 {len(df)} 条数据")
                return df
            elif datasource == "akshare" and start == "19700101":
                # akshare从1970年开始返回大量历史数据
                df = create_mock_data_full_history(start, end, code)
                print(f"    akshare返回 {len(df)} 条历史数据")
                return df
            else:
                # 其他情况返回空数据
                return pd.DataFrame()
        
        print(f"测试股票: {test_code}")
        print(f"输出目录: {test_dir}")
        print(f"最小行数阈值: 50")
        
        # 使用mock替换真实的数据获取函数
        with patch('fetch_kline.get_kline', side_effect=mock_get_kline):
            success = fetch_one(
                code=test_code,
                start="20240101",
                end="20240131",
                out_dir=test_dir,
                incremental=False,
                datasource="mootdx",  # 初始使用mootdx
                freq_code=4,
                adjust="qfq",
                max_null_ratio=0.3,
                min_rows_threshold=50  # 设置50行阈值
            )
        
        print(f"\n抓取结果：{'成功' if success else '失败'}")
        
        # 检查结果文件
        csv_path = test_dir / f"{test_code}.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path, parse_dates=['date'])
            print(f"✅ 文件创建成功")
            print(f"   数据行数: {len(df)}")
            print(f"   日期范围: {df['date'].min().date()} 到 {df['date'].max().date()}")
            print(f"   价格范围: {df['close'].min():.2f} 到 {df['close'].max():.2f}")
            
            # 验证是否获得了足够的历史数据
            if len(df) >= 50:
                print("✅ 自动切换数据源成功，获得足够的历史数据")
            else:
                print("❌ 数据行数仍然不足")
                
            # 显示前几行和后几行数据
            print("\n前5行数据:")
            print(df.head().to_string(index=False))
            print("\n后5行数据:")
            print(df.tail().to_string(index=False))
        else:
            print("❌ 文件创建失败")


def test_normal_case_no_switch():
    """测试正常情况下不切换数据源"""
    print("\n" + "=" * 80)
    print("测试正常情况（数据充足，不切换数据源）")
    print("=" * 80)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        test_dir = Path(temp_dir)
        test_code = "000002"
        
        def mock_get_kline_sufficient(code, start, end, adjust, datasource, freq_code):
            """模拟返回充足数据的情况"""
            print(f"  📊 模拟获取数据：{code}, {start} 到 {end}, 数据源: {datasource}")
            
            # 返回充足的数据（超过50行）
            df = create_mock_data_full_history(start, end, code)
            print(f"    {datasource}返回 {len(df)} 条数据")
            return df
        
        print(f"测试股票: {test_code}")
        print(f"最小行数阈值: 50")
        
        with patch('fetch_kline.get_kline', side_effect=mock_get_kline_sufficient):
            success = fetch_one(
                code=test_code,
                start="20240101",
                end="20240331",  # 3个月数据，应该足够
                out_dir=test_dir,
                incremental=False,
                datasource="mootdx",
                freq_code=4,
                adjust="qfq",
                max_null_ratio=0.3,
                min_rows_threshold=50
            )
        
        print(f"\n抓取结果：{'成功' if success else '失败'}")
        
        csv_path = test_dir / f"{test_code}.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path, parse_dates=['date'])
            print(f"✅ 文件创建成功")
            print(f"   数据行数: {len(df)}")
            print(f"   日期范围: {df['date'].min().date()} 到 {df['date'].max().date()}")
            
            if len(df) >= 50:
                print("✅ 数据充足，无需切换数据源")
            else:
                print("❌ 数据不足，但应该有足够数据")
        else:
            print("❌ 文件创建失败")


if __name__ == "__main__":
    test_auto_switch_datasource()
    test_normal_case_no_switch()
    print("\n" + "=" * 80)
    print("测试完成")
    print("=" * 80)
