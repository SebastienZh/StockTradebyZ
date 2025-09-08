#!/usr/bin/env python3
"""
XD股票检测功能演示
展示系统如何自动识别近期除权股票并切换数据源
"""

import akshare as ak
import pandas as pd
from pathlib import Path
import sys

# 添加当前目录到Python路径
sys.path.insert(0, '.')

def demo_xd_stock_detection():
    """演示XD股票检测功能"""
    print("=" * 80)
    print("XD股票检测功能演示")
    print("=" * 80)
    print()
    
    print("功能说明：")
    print("- 自动检测股票名称是否以'XD'开头（近期除权标识）")
    print("- XD股票会优先切换到akshare数据源，从1970年开始抓取")
    print("- 确保除权股票获得正确的前复权历史数据")
    print("- 无论本地文件状态如何，XD股票都会重新抓取完整数据")
    print()
    
    # 获取当前市场上的XD股票示例
    print("正在获取当前市场上的XD股票...")
    try:
        # 获取A股实时数据
        stock_data = ak.stock_zh_a_spot_em()
        
        # 筛选出XD开头的股票
        xd_stocks = stock_data[stock_data['名称'].str.startswith('XD', na=False)]
        
        if not xd_stocks.empty:
            print(f"✅ 发现 {len(xd_stocks)} 只XD股票：")
            print()
            
            # 显示前5只XD股票
            for idx, (_, row) in enumerate(xd_stocks.head(5).iterrows()):
                code = row['代码']
                name = row['名称']
                price = row['最新价']
                change = row['涨跌幅']
                
                print(f"  {idx+1}. {code} - {name}")
                print(f"     最新价: {price}元, 涨跌幅: {change}%")
                print(f"     📊 此股票会自动切换到akshare数据源")
                print()
            
            if len(xd_stocks) > 5:
                print(f"  ... 还有 {len(xd_stocks) - 5} 只XD股票")
                print()
            
            # 演示检测逻辑
            print("检测逻辑演示：")
            print("-" * 40)
            
            sample_xd = xd_stocks.iloc[0]
            sample_code = sample_xd['代码']
            sample_name = sample_xd['名称']
            
            print(f"示例股票: {sample_code} - {sample_name}")
            print(f"检测结果: 名称以'XD'开头 → 触发自动切换")
            print(f"处理方式: 切换到akshare数据源，从1970年开始抓取")
            print(f"数据模式: 强制非增量模式，确保数据完整性")
            
        else:
            print("❌ 当前市场上没有发现XD股票")
            print("这是正常现象，XD标识通常只在除权除息日前后短期出现")
            print()
            
            # 演示检测逻辑（模拟）
            print("检测逻辑演示（模拟）：")
            print("-" * 40)
            print("假设股票: 000001 - XD平安银行")
            print("检测结果: 名称以'XD'开头 → 触发自动切换")
            print("处理方式: 切换到akshare数据源，从1970年开始抓取")
            print("数据模式: 强制非增量模式，确保数据完整性")
            
    except Exception as e:
        print(f"❌ 获取股票数据失败: {e}")
        print("这可能是网络问题或API限制，不影响XD检测功能的正常工作")
        print()
        
        # 演示检测逻辑（模拟）
        print("检测逻辑演示（模拟）：")
        print("-" * 40)
        print("假设股票: 000001 - XD平安银行")
        print("检测结果: 名称以'XD'开头 → 触发自动切换")
        print("处理方式: 切换到akshare数据源，从1970年开始抓取")
        print("数据模式: 强制非增量模式，确保数据完整性")

def demo_priority_logic():
    """演示优先级逻辑"""
    print("\n" + "=" * 80)
    print("自动切换优先级逻辑演示")
    print("=" * 80)
    print()
    
    scenarios = [
        {
            "name": "XD股票 + 文件不存在",
            "xd": True,
            "file_exists": False,
            "rows": 0,
            "result": "XD检测优先，切换到akshare"
        },
        {
            "name": "XD股票 + 文件存在且数据充足",
            "xd": True,
            "file_exists": True,
            "rows": 1000,
            "result": "XD检测优先，切换到akshare（忽略文件状态）"
        },
        {
            "name": "普通股票 + 文件不存在",
            "xd": False,
            "file_exists": False,
            "rows": 0,
            "result": "文件不存在，切换到akshare"
        },
        {
            "name": "普通股票 + 文件存在但数据不足",
            "xd": False,
            "file_exists": True,
            "rows": 30,
            "result": "数据不足（< 50行），切换到akshare"
        },
        {
            "name": "普通股票 + 文件存在且数据充足",
            "xd": False,
            "file_exists": True,
            "rows": 100,
            "result": "数据充足，使用原始数据源"
        }
    ]
    
    print("优先级检查顺序：")
    print("1. XD股票检测（最高优先级）")
    print("2. 文件存在性检查")
    print("3. 数据行数检查")
    print()
    
    print("场景分析：")
    print("-" * 60)
    
    for i, scenario in enumerate(scenarios, 1):
        print(f"{i}. {scenario['name']}")
        print(f"   XD股票: {'是' if scenario['xd'] else '否'}")
        print(f"   文件存在: {'是' if scenario['file_exists'] else '否'}")
        if scenario['file_exists']:
            print(f"   数据行数: {scenario['rows']}行")
        print(f"   处理结果: {scenario['result']}")
        print()

def demo_usage_examples():
    """演示使用示例"""
    print("=" * 80)
    print("使用示例")
    print("=" * 80)
    print()
    
    print("命令行使用：")
    print("-" * 40)
    print("# 正常抓取（包含XD股票自动检测）")
    print("python fetch_kline.py --datasource mootdx")
    print()
    print("# 设置较低的行数阈值")
    print("python fetch_kline.py --datasource mootdx --min-rows-threshold 30")
    print()
    print("# 禁用行数检查（但XD检测仍然有效）")
    print("python fetch_kline.py --datasource mootdx --min-rows-threshold 0")
    print()
    
    print("日志输出示例：")
    print("-" * 40)
    print("2025-07-31 17:36:23,234 [INFO] 000001 检测到近期除权股票: XD平安银行")
    print("2025-07-31 17:36:23,236 [WARNING] 000001 检测到近期除权股票，切换到akshare数据源从1970年开始抓取")
    print("  📊 模拟获取数据：000001, 19700101 到 20240131, 数据源: akshare")
    print()
    
    print("技术要点：")
    print("-" * 40)
    print("✅ XD检测具有最高优先级，会覆盖其他所有检查")
    print("✅ 自动调用akshare API获取股票名称进行检测")
    print("✅ API调用失败时会优雅降级，不影响正常流程")
    print("✅ XD股票强制使用非增量模式，确保数据完整性")
    print("✅ 从1970年开始抓取，获得最完整的前复权历史数据")

if __name__ == "__main__":
    print("开始XD股票检测功能演示...")
    demo_xd_stock_detection()
    demo_priority_logic()
    demo_usage_examples()
    print("\n" + "=" * 80)
    print("XD股票检测功能演示完成")
    print("=" * 80)
