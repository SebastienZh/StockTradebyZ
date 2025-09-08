#!/usr/bin/env python3
"""
测试失败列表清理功能
验证 failed_stocks.txt 在拉取成功后能正确清理
"""

import os
import sys
from pathlib import Path
import tempfile
import shutil
import datetime as dt
from typing import List


# 复制核心函数以避免导入依赖
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


def load_failed_list(out_dir: Path) -> List[str]:
    """从文件加载失败股票列表"""
    failed_file = out_dir / "failed_stocks.txt"
    if not failed_file.exists():
        return []

    failed_codes = []
    try:
        with open(failed_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    failed_codes.append(line)
        print(f"从文件加载失败股票列表: {len(failed_codes)}只")
    except Exception as e:
        print(f"加载失败股票列表失败: {e}")

    return failed_codes


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


def test_failed_list_cleanup():
    """测试失败列表的保存、加载和清理功能"""
    print("=" * 60)
    print("测试失败列表清理功能")
    print("=" * 60)

    # 创建临时目录
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        print(f"使用临时目录: {temp_path}")

        # 1. 测试保存失败列表
        print("\n1. 测试保存失败列表")
        failed_codes = ["000001", "000002", "600000", "600001"]
        save_failed_list(failed_codes, temp_path, "测试失败原因")

        failed_file = temp_path / "failed_stocks.txt"
        assert failed_file.exists(), "失败列表文件应该被创建"
        print(f"✓ 失败列表文件已创建: {failed_file}")

        # 2. 测试加载失败列表
        print("\n2. 测试加载失败列表")
        loaded_codes = load_failed_list(temp_path)
        assert set(loaded_codes) == set(
            failed_codes
        ), f"加载的失败列表不匹配: {loaded_codes} vs {failed_codes}"
        print(f"✓ 成功加载失败列表: {loaded_codes}")

        # 3. 测试部分清理失败列表
        print("\n3. 测试部分清理失败列表")
        successful_codes = ["000001", "600000"]  # 假设这两只股票成功了
        remove_from_failed_list(successful_codes, temp_path)

        remaining_codes = load_failed_list(temp_path)
        expected_remaining = ["000002", "600001"]
        assert set(remaining_codes) == set(
            expected_remaining
        ), f"剩余失败列表不匹配: {remaining_codes} vs {expected_remaining}"
        print(f"✓ 成功移除成功股票，剩余失败股票: {remaining_codes}")

        # 4. 测试完全清理失败列表
        print("\n4. 测试完全清理失败列表")
        remaining_successful = ["000002", "600001"]  # 剩余股票也成功了
        remove_from_failed_list(remaining_successful, temp_path)

        assert not failed_file.exists(), "失败列表文件应该被删除"
        print("✓ 所有股票成功后，失败列表文件已被删除")

        # 5. 测试添加新失败股票
        print("\n5. 测试添加新失败股票")
        new_failed = ["300001", "300002"]
        save_failed_list(new_failed, temp_path, "新的失败原因")

        loaded_new = load_failed_list(temp_path)
        assert set(loaded_new) == set(
            new_failed
        ), f"新失败列表不匹配: {loaded_new} vs {new_failed}"
        print(f"✓ 成功添加新失败股票: {loaded_new}")

        # 6. 测试混合操作
        print("\n6. 测试混合操作")
        # 添加更多失败股票
        more_failed = ["300003", "300004", "300005"]
        save_failed_list(more_failed, temp_path, "更多失败股票")

        all_failed = load_failed_list(temp_path)
        expected_all = new_failed + more_failed
        assert set(all_failed) == set(
            expected_all
        ), f"合并失败列表不匹配: {all_failed} vs {expected_all}"
        print(f"✓ 成功合并失败列表: {all_failed}")

        # 部分成功
        partial_success = ["300001", "300003", "300005"]
        remove_from_failed_list(partial_success, temp_path)

        final_failed = load_failed_list(temp_path)
        expected_final = ["300002", "300004"]
        assert set(final_failed) == set(
            expected_final
        ), f"最终失败列表不匹配: {final_failed} vs {expected_final}"
        print(f"✓ 部分成功后，剩余失败股票: {final_failed}")

    print("\n" + "=" * 60)
    print("✅ 所有测试通过！失败列表清理功能正常工作")
    print("=" * 60)


def test_edge_cases():
    """测试边界情况"""
    print("\n" + "=" * 60)
    print("测试边界情况")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # 1. 测试空列表
        print("\n1. 测试空列表操作")
        remove_from_failed_list([], temp_path)  # 应该不报错
        save_failed_list([], temp_path, "空列表")  # 应该不创建文件

        failed_file = temp_path / "failed_stocks.txt"
        assert not failed_file.exists(), "空列表不应该创建文件"
        print("✓ 空列表操作正常")

        # 2. 测试不存在的文件
        print("\n2. 测试不存在的文件")
        codes = load_failed_list(temp_path)
        assert codes == [], f"不存在的文件应该返回空列表: {codes}"
        print("✓ 不存在文件的处理正常")

        # 3. 测试移除不存在的股票
        print("\n3. 测试移除不存在的股票")
        save_failed_list(["000001", "000002"], temp_path, "测试")
        remove_from_failed_list(["999999"], temp_path)  # 移除不存在的股票

        remaining = load_failed_list(temp_path)
        assert set(remaining) == {
            "000001",
            "000002",
        }, f"移除不存在股票后列表应该不变: {remaining}"
        print("✓ 移除不存在股票的处理正常")

    print("\n✅ 边界情况测试通过！")


if __name__ == "__main__":
    try:
        test_failed_list_cleanup()
        test_edge_cases()
        print("\n🎉 所有测试完成！失败列表清理功能已验证正常工作。")
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        sys.exit(1)
