#!/usr/bin/env python3
"""
测试 fetch_kline.py 的改进功能
"""

import os
import sys
import subprocess
from pathlib import Path

def test_incremental_update():
    """测试增量更新功能"""
    print("=" * 50)
    print("测试增量更新功能")
    print("=" * 50)
    
    # 运行一次正常抓取（少量股票）
    cmd = [
        sys.executable, "fetch_kline.py",
        "--datasource", "mootdx",
        "--min-mktcap", "100000000000",  # 1000亿，只抓取少量大盘股
        "--max-mktcap", "500000000000",  # 5000亿
        "--start", "20240101",
        "--end", "20240131",
        "--workers", "2"
    ]
    
    print("第一次运行（抓取少量大盘股）...")
    result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8')
    print("返回码:", result.returncode)
    if result.stdout:
        print("输出:", result.stdout[-500:])  # 只显示最后500字符
    if result.stderr:
        print("错误:", result.stderr[-500:])
    
    # 再次运行相同参数，应该显示"已是最新"
    print("\n第二次运行（应该显示增量检查）...")
    result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8')
    print("返回码:", result.returncode)
    if result.stdout:
        print("输出:", result.stdout[-500:])
    if result.stderr:
        print("错误:", result.stderr[-500:])


def test_failed_list_management():
    """测试失败列表管理功能"""
    print("\n" + "=" * 50)
    print("测试失败列表管理功能")
    print("=" * 50)
    
    # 创建一个测试失败列表
    failed_file = Path("data/failed_stocks.txt")
    failed_file.parent.mkdir(exist_ok=True)
    
    with open(failed_file, "w", encoding="utf-8") as f:
        f.write("# 测试失败列表\n")
        f.write("000001\n")
        f.write("000002\n")
        f.write("600000\n")
    
    print(f"创建测试失败列表: {failed_file}")
    
    # 使用 --retry-failed-only 参数
    cmd = [
        sys.executable, "fetch_kline.py",
        "--retry-failed-only",
        "--datasource", "mootdx",
        "--start", "20240101",
        "--end", "20240110",
        "--workers", "1"
    ]
    
    print("运行失败重试模式...")
    result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8')
    print("返回码:", result.returncode)
    if result.stdout:
        print("输出:", result.stdout[-500:])
    if result.stderr:
        print("错误:", result.stderr[-500:])
    
    # 检查失败列表是否更新
    if failed_file.exists():
        print(f"\n失败列表内容:")
        with open(failed_file, "r", encoding="utf-8") as f:
            print(f.read())


def test_data_quality_check():
    """测试数据质量检查功能"""
    print("\n" + "=" * 50)
    print("测试数据质量检查功能")
    print("=" * 50)
    
    # 运行一个可能有数据质量问题的测试
    cmd = [
        sys.executable, "fetch_kline.py",
        "--datasource", "mootdx",
        "--min-mktcap", "50000000000",  # 500亿
        "--max-mktcap", "100000000000", # 1000亿
        "--start", "20240101",
        "--end", "20240105",  # 很短的时间范围
        "--max-null-ratio", "0.1",  # 较严格的空值检查
        "--workers", "1"
    ]
    
    print("运行数据质量检查测试...")
    result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8')
    print("返回码:", result.returncode)
    if result.stdout:
        print("输出:", result.stdout[-800:])
    if result.stderr:
        print("错误:", result.stderr[-500:])


def main():
    """主测试函数"""
    print("开始测试 fetch_kline.py 的改进功能")
    
    # 确保在正确的目录
    if not Path("fetch_kline.py").exists():
        print("错误: 找不到 fetch_kline.py 文件")
        sys.exit(1)
    
    try:
        # 测试增量更新
        test_incremental_update()
        
        # 测试失败列表管理
        test_failed_list_management()
        
        # 测试数据质量检查
        test_data_quality_check()
        
        print("\n" + "=" * 50)
        print("所有测试完成")
        print("=" * 50)
        
        # 显示数据目录内容
        data_dir = Path("data")
        if data_dir.exists():
            print(f"\n数据目录内容 ({data_dir}):")
            for file in sorted(data_dir.glob("*")):
                size = file.stat().st_size if file.is_file() else 0
                print(f"  {file.name} ({size} bytes)")
        
    except KeyboardInterrupt:
        print("\n测试被用户中断")
    except Exception as e:
        print(f"\n测试过程中出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
