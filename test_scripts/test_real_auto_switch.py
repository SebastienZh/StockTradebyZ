#!/usr/bin/env python3
"""
真实环境测试自动切换数据源功能
创建一个数据不足的文件，然后运行fetch_kline看是否会自动切换
"""

import pandas as pd
from pathlib import Path
import tempfile
import subprocess
import sys
import os

def create_insufficient_data_file(output_dir: Path, code: str):
    """创建一个数据不足的CSV文件"""
    # 创建只有几行数据的文件
    data = {
        'date': pd.date_range('2024-01-01', periods=8, freq='D'),
        'open': [10.0, 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7],
        'high': [10.5, 10.6, 10.7, 10.8, 10.9, 11.0, 11.1, 11.2],
        'low': [9.5, 9.6, 9.7, 9.8, 9.9, 10.0, 10.1, 10.2],
        'close': [10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8, 10.9],
        'volume': [1000000] * 8,
        'code': [code] * 8
    }
    
    df = pd.DataFrame(data)
    csv_path = output_dir / f"{code}.csv"
    df.to_csv(csv_path, index=False)
    
    print(f"创建测试文件: {csv_path}")
    print(f"数据行数: {len(df)} (少于50行阈值)")
    return csv_path

def test_auto_switch_in_real_environment():
    """在真实环境中测试自动切换功能"""
    print("=" * 80)
    print("真实环境自动切换数据源测试")
    print("=" * 80)
    
    # 创建临时目录
    with tempfile.TemporaryDirectory() as temp_dir:
        test_dir = Path(temp_dir)
        test_code = "000001"  # 平安银行
        
        # 创建数据不足的文件
        csv_path = create_insufficient_data_file(test_dir, test_code)
        
        print(f"\n测试配置:")
        print(f"  股票代码: {test_code}")
        print(f"  输出目录: {test_dir}")
        print(f"  预期行为: 检测到数据不足，自动切换到akshare数据源")
        
        # 构建命令
        script_path = Path(__file__).parent / "fetch_kline.py"
        cmd = [
            sys.executable, str(script_path),
            "--datasource", "mootdx",  # 初始数据源
            "--min-rows-threshold", "50",  # 设置阈值
            "--min-mktcap", "1e9",  # 降低市值要求确保包含测试股票
            "--max-mktcap", "1e12",  # 设置上限
        ]
        
        print(f"\n执行命令:")
        print(f"  {' '.join(cmd)}")
        
        # 设置环境变量，指定输出目录
        env = os.environ.copy()
        env['PYTHONPATH'] = str(Path(__file__).parent)
        
        try:
            # 运行命令
            print(f"\n开始执行...")
            result = subprocess.run(
                cmd,
                cwd=test_dir,
                capture_output=True,
                text=True,
                timeout=300,  # 5分钟超时
                env=env
            )
            
            print(f"\n执行结果:")
            print(f"  返回码: {result.returncode}")
            
            if result.stdout:
                print(f"\n标准输出:")
                # 只显示关键的日志行
                for line in result.stdout.split('\n'):
                    if any(keyword in line for keyword in [
                        '数据行数不足', '切换到akshare', '本地文件不存在', 
                        '开始抓取', '下载完成', 'ERROR', 'WARNING'
                    ]):
                        print(f"    {line}")
            
            if result.stderr:
                print(f"\n标准错误:")
                print(result.stderr)
            
            # 检查结果文件
            print(f"\n检查结果文件:")
            if csv_path.exists():
                df = pd.read_csv(csv_path, parse_dates=['date'])
                print(f"  ✅ 文件存在")
                print(f"  📊 数据行数: {len(df)}")
                print(f"  📅 日期范围: {df['date'].min().date()} 到 {df['date'].max().date()}")
                
                if len(df) >= 50:
                    print(f"  ✅ 自动切换成功！数据已从 8 行增加到 {len(df)} 行")
                else:
                    print(f"  ❌ 自动切换失败，数据行数仍然不足")
            else:
                print(f"  ❌ 文件不存在")
                
        except subprocess.TimeoutExpired:
            print(f"\n❌ 执行超时（5分钟）")
        except Exception as e:
            print(f"\n❌ 执行出错: {e}")

def test_file_not_exists():
    """测试文件不存在的情况"""
    print("\n" + "=" * 80)
    print("测试文件不存在的自动切换")
    print("=" * 80)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        test_dir = Path(temp_dir)
        test_code = "000002"  # 万科A
        
        print(f"测试配置:")
        print(f"  股票代码: {test_code}")
        print(f"  输出目录: {test_dir}")
        print(f"  文件状态: 不存在")
        print(f"  预期行为: 直接切换到akshare数据源")
        
        # 构建命令，只抓取一只股票
        script_path = Path(__file__).parent / "fetch_kline.py"
        cmd = [
            sys.executable, str(script_path),
            "--datasource", "mootdx",
            "--min-rows-threshold", "50",
            "--min-mktcap", "1e9",
            "--max-mktcap", "1e12",
        ]
        
        env = os.environ.copy()
        env['PYTHONPATH'] = str(Path(__file__).parent)
        
        try:
            print(f"\n开始执行...")
            result = subprocess.run(
                cmd,
                cwd=test_dir,
                capture_output=True,
                text=True,
                timeout=300,
                env=env
            )
            
            print(f"\n执行结果:")
            print(f"  返回码: {result.returncode}")
            
            # 显示关键日志
            if result.stdout:
                for line in result.stdout.split('\n'):
                    if any(keyword in line for keyword in [
                        '本地文件不存在', '切换到akshare', '开始抓取', 'ERROR', 'WARNING'
                    ]):
                        print(f"    {line}")
            
            # 检查生成的文件
            csv_files = list(test_dir.glob("*.csv"))
            if csv_files:
                print(f"\n✅ 生成了 {len(csv_files)} 个文件")
                for csv_file in csv_files[:3]:  # 只检查前3个文件
                    df = pd.read_csv(csv_file, parse_dates=['date'])
                    print(f"  📄 {csv_file.name}: {len(df)} 行数据")
                    if len(df) >= 50:
                        print(f"    ✅ 数据充足")
                    else:
                        print(f"    ❌ 数据不足")
            else:
                print(f"\n❌ 没有生成任何文件")
                
        except Exception as e:
            print(f"\n❌ 执行出错: {e}")

if __name__ == "__main__":
    print("开始真实环境测试...")
    test_auto_switch_in_real_environment()
    test_file_not_exists()
    print("\n" + "=" * 80)
    print("真实环境测试完成")
    print("=" * 80)
