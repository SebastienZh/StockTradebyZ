#!/usr/bin/env python3
"""
快速清理明显有问题的CSV文件

专门处理以下问题：
1. 所有价格数据为空的文件
2. 文件大小异常小的文件
3. 行数过少的文件
4. 无法读取的损坏文件
"""

import argparse
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import pandas as pd
from tqdm import tqdm


def quick_check_file(csv_path: Path) -> Tuple[bool, str]:
    """快速检查文件是否有明显问题"""
    try:
        # 检查文件大小（小于1KB可能有问题）
        file_size = csv_path.stat().st_size
        if file_size < 1024:
            return False, f"文件过小({file_size}字节)"
        
        # 尝试读取文件
        df = pd.read_csv(csv_path)
        
        # 检查是否为空
        if df.empty:
            return False, "文件为空"
        
        # 检查行数
        if len(df) < 10:
            return False, f"数据行数过少({len(df)}行)"
        
        # 检查必要列
        required_columns = ["date", "open", "high", "low", "close", "volume"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return False, f"缺少必要列: {missing_columns}"
        
        # 检查价格数据是否全为空
        price_columns = ["open", "high", "low", "close"]
        price_data_exists = any(
            not df[col].isna().all() 
            for col in price_columns 
            if col in df.columns
        )
        
        if not price_data_exists:
            return False, "所有价格数据为空"
        
        # 检查空值比例
        total_rows = len(df)
        for col in price_columns:
            if col in df.columns:
                null_ratio = df[col].isna().sum() / total_rows
                if null_ratio > 0.8:  # 超过80%为空
                    return False, f"{col}列空值比例过高({null_ratio:.1%})"
        
        return True, "数据正常"
        
    except Exception as e:
        return False, f"读取失败: {str(e)}"


def scan_and_clean(data_dir: Path, backup_dir: Path = None, dry_run: bool = False) -> None:
    """扫描并清理问题文件"""
    
    if not data_dir.exists():
        print(f"❌ 数据目录不存在: {data_dir}")
        return
    
    # 获取所有CSV文件（排除市值文件）
    csv_files = [f for f in data_dir.glob("*.csv") if not f.name.startswith("mktcap_")]
    
    if not csv_files:
        print("📁 未找到CSV文件")
        return
    
    print(f"📊 发现 {len(csv_files)} 个CSV文件")
    
    # 创建备份目录
    if backup_dir and not dry_run:
        backup_dir.mkdir(parents=True, exist_ok=True)
    
    # 统计信息
    stats = {
        "total": len(csv_files),
        "problematic": 0,
        "deleted": 0,
        "backed_up": 0,
        "errors": 0
    }
    
    problematic_files = []
    
    # 扫描文件
    print("\n🔍 扫描文件质量...")
    for csv_path in tqdm(csv_files, desc="扫描进度"):
        is_good, reason = quick_check_file(csv_path)
        
        if not is_good:
            stats["problematic"] += 1
            problematic_files.append((csv_path, reason))
    
    if not problematic_files:
        print("✅ 所有文件都正常，无需清理")
        return
    
    print(f"\n⚠️  发现 {len(problematic_files)} 个问题文件:")
    for csv_path, reason in problematic_files:
        print(f"  - {csv_path.name}: {reason}")
    
    # 清理文件
    if dry_run:
        print(f"\n🔍 [模拟模式] 将删除 {len(problematic_files)} 个文件")
        for csv_path, reason in problematic_files:
            print(f"  [模拟删除] {csv_path.name}: {reason}")
    else:
        print(f"\n🗑️  开始清理 {len(problematic_files)} 个问题文件...")
        
        for csv_path, reason in tqdm(problematic_files, desc="清理进度"):
            try:
                # 备份文件
                if backup_dir:
                    backup_path = backup_dir / csv_path.name
                    shutil.copy2(csv_path, backup_path)
                    stats["backed_up"] += 1
                
                # 删除文件
                csv_path.unlink()
                stats["deleted"] += 1
                
            except Exception as e:
                print(f"❌ 处理 {csv_path.name} 失败: {e}")
                stats["errors"] += 1
    
    # 生成报告
    print(f"\n📋 清理完成统计:")
    print(f"  总文件数: {stats['total']}")
    print(f"  问题文件数: {stats['problematic']}")
    print(f"  删除文件数: {stats['deleted']}")
    if backup_dir:
        print(f"  备份文件数: {stats['backed_up']}")
        print(f"  备份位置: {backup_dir}")
    print(f"  错误数: {stats['errors']}")
    
    # 保存详细报告
    if not dry_run and problematic_files:
        report_path = data_dir / f"quick_clean_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(f"快速清理报告 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"数据目录: {data_dir}\n")
            f.write(f"备份目录: {backup_dir}\n\n")
            f.write("统计信息:\n")
            for key, value in stats.items():
                f.write(f"  {key}: {value}\n")
            f.write("\n问题文件列表:\n")
            for csv_path, reason in problematic_files:
                f.write(f"  {csv_path.name}: {reason}\n")
        
        print(f"📄 详细报告已保存: {report_path}")


def main():
    parser = argparse.ArgumentParser(description="快速清理明显有问题的CSV文件")
    parser.add_argument("--data-dir", default="./data", help="数据目录路径")
    parser.add_argument("--backup-dir", help="备份目录路径（默认为data_backup）")
    parser.add_argument("--dry-run", action="store_true", help="模拟运行，不实际删除文件")
    parser.add_argument("--no-backup", action="store_true", help="删除时不备份文件")
    
    args = parser.parse_args()
    
    data_dir = Path(args.data_dir)
    backup_dir = None
    
    if not args.no_backup:
        if args.backup_dir:
            backup_dir = Path(args.backup_dir)
        else:
            backup_dir = data_dir.parent / "data_backup"
    
    print("🧹 快速数据清理工具")
    print("=" * 30)
    print(f"数据目录: {data_dir}")
    if backup_dir:
        print(f"备份目录: {backup_dir}")
    print(f"模式: {'模拟运行' if args.dry_run else '实际清理'}")
    print()
    
    try:
        scan_and_clean(data_dir, backup_dir, args.dry_run)
    except KeyboardInterrupt:
        print("\n⏹️  用户中断操作")
    except Exception as e:
        print(f"❌ 清理过程出错: {e}")


if __name__ == "__main__":
    main()
