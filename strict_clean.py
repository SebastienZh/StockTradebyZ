#!/usr/bin/env python3
"""
严格清理数据残缺的CSV文件

更严格的清理标准：
1. 价格数据空值比例超过30%
2. 文件大小小于2KB
3. 数据行数少于100行
4. 所有价格数据为空
"""

import argparse
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import pandas as pd
from tqdm import tqdm


def strict_check_file(csv_path: Path, max_null_ratio: float = 0.3, min_rows: int = 100, min_size: int = 2048) -> Tuple[bool, str]:
    """严格检查文件是否有问题"""
    try:
        # 检查文件大小
        file_size = csv_path.stat().st_size
        if file_size < min_size:
            return False, f"文件过小({file_size}字节，小于{min_size}字节)"
        
        # 尝试读取文件
        df = pd.read_csv(csv_path)
        
        # 检查是否为空
        if df.empty:
            return False, "文件为空"
        
        # 检查行数
        if len(df) < min_rows:
            return False, f"数据行数过少({len(df)}行，少于{min_rows}行)"
        
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
        
        # 检查空值比例（更严格）
        total_rows = len(df)
        for col in price_columns:
            if col in df.columns:
                null_ratio = df[col].isna().sum() / total_rows
                if null_ratio > max_null_ratio:
                    return False, f"{col}列空值比例过高({null_ratio:.1%}，超过{max_null_ratio:.1%})"
        
        # 检查是否有有效的价格数据
        valid_price_rows = 0
        for _, row in df.iterrows():
            if any(pd.notna(row[col]) and row[col] > 0 for col in price_columns if col in df.columns):
                valid_price_rows += 1
        
        valid_ratio = valid_price_rows / total_rows
        if valid_ratio < (1 - max_null_ratio):
            return False, f"有效价格数据比例过低({valid_ratio:.1%}，低于{1-max_null_ratio:.1%})"
        
        return True, "数据质量良好"
        
    except Exception as e:
        return False, f"读取失败: {str(e)}"


def strict_scan_and_clean(data_dir: Path, backup_dir: Path = None, dry_run: bool = False, 
                         max_null_ratio: float = 0.3, min_rows: int = 100, min_size: int = 2048) -> None:
    """严格扫描并清理问题文件"""
    
    if not data_dir.exists():
        print(f"❌ 数据目录不存在: {data_dir}")
        return
    
    # 获取所有CSV文件（排除市值文件）
    csv_files = [f for f in data_dir.glob("*.csv") if not f.name.startswith("mktcap_")]
    
    if not csv_files:
        print("📁 未找到CSV文件")
        return
    
    print(f"📊 发现 {len(csv_files)} 个CSV文件")
    print(f"🔍 清理标准:")
    print(f"  - 价格数据空值比例超过 {max_null_ratio:.1%}")
    print(f"  - 文件大小小于 {min_size} 字节")
    print(f"  - 数据行数少于 {min_rows} 行")
    print(f"  - 所有价格数据为空")
    
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
    print("\n🔍 严格扫描文件质量...")
    for csv_path in tqdm(csv_files, desc="扫描进度"):
        is_good, reason = strict_check_file(csv_path, max_null_ratio, min_rows, min_size)
        
        if not is_good:
            stats["problematic"] += 1
            problematic_files.append((csv_path, reason))
    
    if not problematic_files:
        print("✅ 所有文件都符合严格标准，无需清理")
        return
    
    print(f"\n⚠️  发现 {len(problematic_files)} 个问题文件:")
    for csv_path, reason in problematic_files[:10]:  # 只显示前10个
        print(f"  - {csv_path.name}: {reason}")
    if len(problematic_files) > 10:
        print(f"  ... 还有 {len(problematic_files) - 10} 个文件")
    
    # 清理文件
    if dry_run:
        print(f"\n🔍 [模拟模式] 将删除 {len(problematic_files)} 个文件")
    else:
        print(f"\n🗑️  开始严格清理 {len(problematic_files)} 个问题文件...")
        
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
    print(f"\n📋 严格清理完成统计:")
    print(f"  总文件数: {stats['total']}")
    print(f"  问题文件数: {stats['problematic']}")
    print(f"  删除文件数: {stats['deleted']}")
    if backup_dir:
        print(f"  备份文件数: {stats['backed_up']}")
        print(f"  备份位置: {backup_dir}")
    print(f"  错误数: {stats['errors']}")
    
    # 保存详细报告
    if not dry_run and problematic_files:
        report_path = data_dir / f"strict_clean_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(f"严格清理报告 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"数据目录: {data_dir}\n")
            f.write(f"备份目录: {backup_dir}\n")
            f.write(f"清理标准:\n")
            f.write(f"  - 价格数据空值比例超过 {max_null_ratio:.1%}\n")
            f.write(f"  - 文件大小小于 {min_size} 字节\n")
            f.write(f"  - 数据行数少于 {min_rows} 行\n")
            f.write(f"  - 所有价格数据为空\n\n")
            f.write("统计信息:\n")
            for key, value in stats.items():
                f.write(f"  {key}: {value}\n")
            f.write("\n问题文件列表:\n")
            for csv_path, reason in problematic_files:
                f.write(f"  {csv_path.name}: {reason}\n")
        
        print(f"📄 详细报告已保存: {report_path}")


def main():
    parser = argparse.ArgumentParser(description="严格清理数据残缺的CSV文件")
    parser.add_argument("--data-dir", default="./data", help="数据目录路径")
    parser.add_argument("--backup-dir", help="备份目录路径（默认为data_backup_strict）")
    parser.add_argument("--dry-run", action="store_true", help="模拟运行，不实际删除文件")
    parser.add_argument("--no-backup", action="store_true", help="删除时不备份文件")
    parser.add_argument("--max-null-ratio", type=float, default=0.3, help="最大空值比例阈值（默认30%）")
    parser.add_argument("--min-rows", type=int, default=100, help="最小行数要求（默认100行）")
    parser.add_argument("--min-size", type=int, default=2048, help="最小文件大小要求（默认2048字节）")
    
    args = parser.parse_args()
    
    data_dir = Path(args.data_dir)
    backup_dir = None
    
    if not args.no_backup:
        if args.backup_dir:
            backup_dir = Path(args.backup_dir)
        else:
            backup_dir = data_dir.parent / "data_backup_strict"
    
    print("🧹 严格数据清理工具")
    print("=" * 30)
    print(f"数据目录: {data_dir}")
    if backup_dir:
        print(f"备份目录: {backup_dir}")
    print(f"模式: {'模拟运行' if args.dry_run else '实际清理'}")
    print()
    
    try:
        strict_scan_and_clean(
            data_dir, backup_dir, args.dry_run, 
            args.max_null_ratio, args.min_rows, args.min_size
        )
    except KeyboardInterrupt:
        print("\n⏹️  用户中断操作")
    except Exception as e:
        print(f"❌ 清理过程出错: {e}")


if __name__ == "__main__":
    main()
