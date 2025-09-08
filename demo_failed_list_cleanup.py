#!/usr/bin/env python3
"""
演示失败列表清理功能
展示 failed_stocks.txt 在拉取成功后的实时清理效果
"""

import os
import sys
from pathlib import Path
import time
import datetime as dt
from typing import List

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# 简化版本的函数，避免导入依赖
def save_failed_list(failed_codes: List[str], out_dir: Path, reason: str = ""):
    """立即保存失败股票列表到文件"""
    if not failed_codes:
        return

    failed_file = out_dir / "failed_stocks.txt"
    timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 读取现有失败列表（如果存在）
    existing_failed = set()
    if failed_file.exists():
        try:
            with open(failed_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        existing_failed.add(line)
        except Exception as e:
            print(f"读取现有失败列表失败: {e}")

    # 合并新的失败股票
    all_failed = sorted(existing_failed | set(failed_codes))

    # 重写文件
    with open(failed_file, "w", encoding="utf-8") as f:
        f.write(f"# 抓取失败的股票列表 - 更新时间: {timestamp}\n")
        if reason:
            f.write(f"# 失败原因: {reason}\n")
        f.write(f"# 总计 {len(all_failed)} 只股票\n")
        f.write("# 格式: 每行一个股票代码\n")
        f.write("#" + "=" * 50 + "\n")
        for code in all_failed:
            f.write(f"{code}\n")

    print(f"失败股票列表已更新至: {failed_file} (共{len(all_failed)}只)")


def remove_from_failed_list(successful_codes: List[str], out_dir: Path):
    """立即从失败列表中移除成功的股票"""
    if not successful_codes:
        return

    failed_file = out_dir / "failed_stocks.txt"
    if not failed_file.exists():
        return

    try:
        remaining_failed = []
        with open(failed_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and line not in successful_codes:
                    remaining_failed.append(line)

        # 重写失败文件，只保留真正失败的
        if remaining_failed:
            # 直接重写文件，不使用 save_failed_list 避免合并
            timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(failed_file, "w", encoding="utf-8") as f:
                f.write(f"# 抓取失败的股票列表 - 更新时间: {timestamp}\n")
                f.write("# 失败原因: 移除成功股票后的失败列表\n")
                f.write(f"# 总计 {len(remaining_failed)} 只股票\n")
                f.write("# 格式: 每行一个股票代码\n")
                f.write("#" + "=" * 50 + "\n")
                for code in remaining_failed:
                    f.write(f"{code}\n")
            print(
                f"已从失败列表中移除 {len(successful_codes)} 只成功股票，剩余 {len(remaining_failed)} 只失败股票"
            )
        else:
            # 如果没有失败的股票，删除失败文件
            failed_file.unlink()
            print("所有失败股票已成功处理，已删除失败列表文件")
    except Exception as e:
        print(f"从失败列表移除成功股票时出错: {e}")


def show_failed_list_content(data_dir: Path):
    """显示失败列表文件内容"""
    failed_file = data_dir / "failed_stocks.txt"
    if failed_file.exists():
        print(f"\n📄 {failed_file} 内容:")
        print("-" * 50)
        with open(failed_file, "r", encoding="utf-8") as f:
            content = f.read()
            print(content)
        print("-" * 50)
    else:
        print(f"\n✅ {failed_file} 不存在（已被清理）")


def main():
    """演示失败列表清理功能"""
    print("=" * 70)
    print("🚀 演示失败列表清理功能")
    print("=" * 70)

    data_dir = Path("./data")
    data_dir.mkdir(exist_ok=True)

    print("\n📋 演示场景:")
    print("1. 模拟一些股票抓取失败，观察失败列表")
    print("2. 模拟部分股票重试成功，观察失败列表的实时清理")
    print("3. 模拟所有股票最终成功，观察失败列表被完全清理")

    # 使用简化版本的函数，避免导入依赖
    print("💡 使用内置的简化版本函数进行演示")

    # 场景1: 初始失败
    print("\n" + "=" * 50)
    print("📍 场景1: 模拟初始抓取，部分股票失败")
    print("=" * 50)

    initial_failed = ["000001", "000002", "600000", "600001", "300001"]
    save_failed_list(initial_failed, data_dir, "初始抓取失败")

    print(f"❌ 模拟 {len(initial_failed)} 只股票抓取失败")
    show_failed_list_content(data_dir)

    time.sleep(2)

    # 场景2: 第一轮重试，部分成功
    print("\n" + "=" * 50)
    print("📍 场景2: 第一轮重试，部分股票成功")
    print("=" * 50)

    first_retry_success = ["000001", "600000"]  # 假设这两只成功了
    print(f"✅ 模拟 {len(first_retry_success)} 只股票重试成功: {first_retry_success}")

    remove_from_failed_list(first_retry_success, data_dir)
    show_failed_list_content(data_dir)

    time.sleep(2)

    # 场景3: 第二轮重试，更多成功
    print("\n" + "=" * 50)
    print("📍 场景3: 第二轮重试，更多股票成功")
    print("=" * 50)

    second_retry_success = ["000002", "300001"]  # 又有两只成功了
    print(f"✅ 模拟 {len(second_retry_success)} 只股票重试成功: {second_retry_success}")

    remove_from_failed_list(second_retry_success, data_dir)
    show_failed_list_content(data_dir)

    time.sleep(2)

    # 场景4: 最后一轮，全部成功
    print("\n" + "=" * 50)
    print("📍 场景4: 最后一轮重试，剩余股票全部成功")
    print("=" * 50)

    final_success = ["600001"]  # 最后一只也成功了
    print(f"✅ 模拟最后 {len(final_success)} 只股票重试成功: {final_success}")

    remove_from_failed_list(final_success, data_dir)
    show_failed_list_content(data_dir)

    # 场景5: 新的抓取任务
    print("\n" + "=" * 50)
    print("📍 场景5: 新的抓取任务，又有股票失败")
    print("=" * 50)

    new_failed = ["002001", "002002"]
    save_failed_list(new_failed, data_dir, "新任务抓取失败")

    print(f"❌ 模拟新任务中 {len(new_failed)} 只股票失败: {new_failed}")
    show_failed_list_content(data_dir)

    time.sleep(2)

    # 场景6: 新任务全部成功
    print("\n" + "=" * 50)
    print("📍 场景6: 新任务重试，全部成功")
    print("=" * 50)

    print(f"✅ 模拟新任务重试，{len(new_failed)} 只股票全部成功: {new_failed}")
    remove_from_failed_list(new_failed, data_dir)
    show_failed_list_content(data_dir)

    print("\n" + "=" * 70)
    print("🎉 演示完成！")
    print("=" * 70)
    print("\n📝 总结:")
    print("✅ 失败股票会立即保存到 failed_stocks.txt")
    print("✅ 成功股票会立即从 failed_stocks.txt 中移除")
    print("✅ 当所有股票都成功时，failed_stocks.txt 会被自动删除")
    print("✅ 失败列表始终保持最新状态，只包含真正失败的股票")
    print("\n💡 这样可以避免重复处理已成功的股票，提高效率！")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⏹️  演示被用户中断")
    except Exception as e:
        print(f"\n❌ 演示过程中出错: {e}")
        sys.exit(1)
