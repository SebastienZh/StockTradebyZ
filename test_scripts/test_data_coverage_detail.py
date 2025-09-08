#!/usr/bin/env python3
"""
详细测试前一日数据覆盖功能，验证数据确实被覆盖
"""

import os
import sys
import pandas as pd
import tempfile
from pathlib import Path
from unittest.mock import patch

# 添加当前目录到路径
sys.path.insert(0, '.')

from fetch_kline import fetch_one


def create_mock_data_with_marker(start_date: str, end_date: str, code: str, marker: str) -> pd.DataFrame:
    """创建带有标记的模拟K线数据，用于区分不同批次的数据"""
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    # 过滤掉周末
    dates = dates[dates.weekday < 5]
    
    data = []
    base_price = 10.0
    for i, date in enumerate(dates):
        # 根据marker调整价格，用于区分数据来源
        if marker == "old":
            price = base_price + i * 0.1  # 旧数据：递增0.1
        else:  # new
            price = base_price + i * 0.2  # 新数据：递增0.2（更大的增幅）
            
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


def test_data_overwrite():
    """测试数据覆盖的详细情况"""
    print("=" * 70)
    print("详细测试前一日数据覆盖功能")
    print("=" * 70)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        test_dir = Path(temp_dir)
        test_code = "000001"
        csv_path = test_dir / f"{test_code}.csv"
        
        # 步骤1：创建初始数据（旧数据，价格递增0.1）
        print("步骤1：创建初始数据（2024-01-01 到 2024-01-10）")
        initial_data = create_mock_data_with_marker("2024-01-01", "2024-01-10", test_code, "old")
        initial_data.to_csv(csv_path, index=False)
        
        print(f"  初始数据：{len(initial_data)} 条记录")
        print("  关键日期的收盘价（旧数据）：")
        key_dates = ['2024-01-08', '2024-01-09', '2024-01-10']
        for date_str in key_dates:
            row = initial_data[initial_data['date'] == date_str]
            if not row.empty:
                print(f"    {date_str}: {row.iloc[0]['close']:.2f}")
        
        # 步骤2：模拟增量更新（新数据，价格递增0.2）
        print("\n步骤2：增量更新（应该从2024-01-09开始覆盖）")
        
        def mock_get_kline(code, start, end, adjust, datasource, freq_code):
            """模拟获取新的K线数据"""
            print(f"  📊 模拟获取数据：{code}, {start} 到 {end}")
            return create_mock_data_with_marker(start, end, code, "new")
        
        with patch('fetch_kline.get_kline', side_effect=mock_get_kline):
            success = fetch_one(
                code=test_code,
                start="20240101",  # 这个会被增量逻辑调整
                end="20240115",    # 扩展到更晚的日期
                out_dir=test_dir,
                incremental=True,
                datasource="mootdx",
                freq_code=4,
                adjust="qfq"
            )
        
        print(f"  抓取结果：{'成功' if success else '失败'}")
        
        # 步骤3：详细验证覆盖结果
        print("\n步骤3：验证数据覆盖结果")
        if csv_path.exists():
            final_data = pd.read_csv(csv_path, parse_dates=['date'])
            final_data = final_data.sort_values('date')
            
            print(f"  最终数据：{len(final_data)} 条记录")
            print(f"  日期范围：{final_data['date'].min().date()} 到 {final_data['date'].max().date()}")
            
            # 检查关键日期的数据是否被正确覆盖
            print("\n  关键日期的收盘价对比：")
            print("  日期        旧数据   新数据   实际数据  状态")
            print("  " + "-" * 50)
            
            test_dates = ['2024-01-08', '2024-01-09', '2024-01-10', '2024-01-11', '2024-01-12']
            for date_str in test_dates:
                # 计算预期的旧数据价格
                date_obj = pd.to_datetime(date_str)
                days_from_start = (date_obj - pd.to_datetime('2024-01-01')).days
                old_expected = 10.0 + days_from_start * 0.1 + 0.1  # 旧数据公式
                
                # 计算预期的新数据价格（从2024-01-09开始）
                if date_obj >= pd.to_datetime('2024-01-09'):
                    days_from_new_start = (date_obj - pd.to_datetime('2024-01-09')).days
                    new_expected = 10.0 + days_from_new_start * 0.2 + 0.1  # 新数据公式
                else:
                    new_expected = None
                
                # 获取实际数据
                actual_row = final_data[final_data['date'].dt.date == date_obj.date()]
                if not actual_row.empty:
                    actual_price = actual_row.iloc[0]['close']
                    
                    # 判断状态
                    if date_obj < pd.to_datetime('2024-01-09'):
                        status = "保留旧数据" if abs(actual_price - old_expected) < 0.01 else "异常"
                    else:
                        status = "覆盖成功" if abs(actual_price - new_expected) < 0.01 else "覆盖失败"
                    
                    print(f"  {date_str}   {old_expected:6.2f}   {new_expected or 'N/A':>6}   {actual_price:8.2f}  {status}")
                else:
                    print(f"  {date_str}   {old_expected:6.2f}   {new_expected or 'N/A':>6}      缺失    数据缺失")
            
            # 检查数据完整性
            print(f"\n  数据完整性检查：")
            duplicate_dates = final_data['date'].duplicated().sum()
            print(f"    重复日期：{duplicate_dates} 个")
            
            # 检查价格趋势是否符合预期
            print(f"    价格趋势检查：")
            old_part = final_data[final_data['date'] < '2024-01-09']
            new_part = final_data[final_data['date'] >= '2024-01-09']
            
            if len(old_part) > 1:
                old_trend = (old_part.iloc[-1]['close'] - old_part.iloc[0]['close']) / len(old_part)
                print(f"      旧数据部分平均涨幅：{old_trend:.3f}/天")
            
            if len(new_part) > 1:
                new_trend = (new_part.iloc[-1]['close'] - new_part.iloc[0]['close']) / len(new_part)
                print(f"      新数据部分平均涨幅：{new_trend:.3f}/天")
                
        else:
            print("  ❌ 未找到输出文件")


if __name__ == "__main__":
    test_data_overwrite()
    print("\n" + "=" * 70)
    print("详细测试完成")
    print("=" * 70)
