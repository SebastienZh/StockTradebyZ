#!/usr/bin/env python3
"""
测试 fetch_kline.py 的前一日数据覆盖功能
"""

import os
import sys
import pandas as pd
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch

# 添加当前目录到路径
sys.path.insert(0, '.')

from fetch_kline import fetch_one, get_kline


def create_mock_data(start_date: str, end_date: str, code: str) -> pd.DataFrame:
    """创建模拟的K线数据"""
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    # 过滤掉周末
    dates = dates[dates.weekday < 5]
    
    data = []
    base_price = 10.0
    for i, date in enumerate(dates):
        price = base_price + i * 0.1  # 简单的价格递增
        data.append({
            'date': date,
            'open': price,
            'high': price + 0.2,
            'low': price - 0.1,
            'close': price + 0.1,
            'volume': 1000000 + i * 10000,
            'amount': (price + 0.1) * (1000000 + i * 10000),
            'turnover': 1.5 + i * 0.01
        })
    
    return pd.DataFrame(data)


def test_overlap_coverage():
    """测试前一日数据覆盖功能"""
    print("=" * 60)
    print("测试前一日数据覆盖功能")
    print("=" * 60)
    
    # 创建临时目录
    with tempfile.TemporaryDirectory() as temp_dir:
        test_dir = Path(temp_dir)
        test_code = "000001"
        csv_path = test_dir / f"{test_code}.csv"
        
        # 步骤1：创建初始数据（2024-01-01 到 2024-01-10）
        print("步骤1：创建初始数据（2024-01-01 到 2024-01-10）")
        initial_data = create_mock_data("2024-01-01", "2024-01-10", test_code)
        initial_data.to_csv(csv_path, index=False)
        print(f"  初始数据：{len(initial_data)} 条记录")
        print(f"  日期范围：{initial_data['date'].min().date()} 到 {initial_data['date'].max().date()}")
        print(f"  最后一天收盘价：{initial_data.iloc[-1]['close']:.2f}")
        
        # 步骤2：模拟增量更新（应该从2024-01-09开始，覆盖前一日）
        print("\n步骤2：模拟增量更新（从前一日开始覆盖）")
        
        def mock_get_kline(code, start, end, adjust, datasource, freq_code):
            """模拟获取K线数据，返回从start到end的数据"""
            print(f"  模拟获取数据：{code}, {start} 到 {end}")
            return create_mock_data(start, end, code)
        
        # 使用mock替换真实的数据获取函数
        with patch('fetch_kline.get_kline', side_effect=mock_get_kline):
            success = fetch_one(
                code=test_code,
                start="20240101",  # 这个会被增量逻辑覆盖
                end="20240112",    # 新的结束日期
                out_dir=test_dir,
                incremental=True,  # 启用增量更新
                datasource="mootdx",
                freq_code=4,
                adjust="qfq"
            )
        
        print(f"  抓取结果：{'成功' if success else '失败'}")
        
        # 步骤3：验证结果
        print("\n步骤3：验证结果")
        if csv_path.exists():
            final_data = pd.read_csv(csv_path, parse_dates=['date'])
            print(f"  最终数据：{len(final_data)} 条记录")
            print(f"  日期范围：{final_data['date'].min().date()} 到 {final_data['date'].max().date()}")
            
            # 检查是否有重复日期
            duplicate_dates = final_data['date'].duplicated().sum()
            print(f"  重复日期数量：{duplicate_dates}")
            
            # 检查数据连续性
            final_data = final_data.sort_values('date')
            date_gaps = []
            for i in range(1, len(final_data)):
                prev_date = final_data.iloc[i-1]['date']
                curr_date = final_data.iloc[i]['date']
                gap_days = (curr_date - prev_date).days
                if gap_days > 3:  # 考虑周末，超过3天算有间隙
                    date_gaps.append((prev_date.date(), curr_date.date(), gap_days))
            
            if date_gaps:
                print(f"  发现日期间隙：{len(date_gaps)} 个")
                for gap in date_gaps[:3]:  # 只显示前3个
                    print(f"    {gap[0]} -> {gap[1]} ({gap[2]}天)")
            else:
                print("  ✅ 日期连续性良好")
            
            # 检查最后几天的数据
            print("\n  最后5天的数据：")
            last_5 = final_data.tail(5)[['date', 'close']].copy()
            last_5['date'] = last_5['date'].dt.date
            for _, row in last_5.iterrows():
                print(f"    {row['date']}: 收盘价 {row['close']:.2f}")
                
        else:
            print("  ❌ 未找到输出文件")


def test_edge_cases():
    """测试边界情况"""
    print("\n" + "=" * 60)
    print("测试边界情况")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        test_dir = Path(temp_dir)
        
        # 测试1：文件不存在的情况
        print("测试1：文件不存在时的行为")
        def mock_get_kline_empty(code, start, end, adjust, datasource, freq_code):
            return create_mock_data(start, end, code)
        
        with patch('fetch_kline.get_kline', side_effect=mock_get_kline_empty):
            success = fetch_one(
                code="000002",
                start="20240101",
                end="20240105",
                out_dir=test_dir,
                incremental=True,
                datasource="mootdx",
                freq_code=4,
                adjust="qfq"
            )
        
        csv_path = test_dir / "000002.csv"
        if csv_path.exists():
            data = pd.read_csv(csv_path, parse_dates=['date'])
            print(f"  ✅ 新文件创建成功，{len(data)} 条记录")
        else:
            print("  ❌ 新文件创建失败")


if __name__ == "__main__":
    test_overlap_coverage()
    test_edge_cases()
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)
