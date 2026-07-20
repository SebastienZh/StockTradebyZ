"""
run_all.py
~~~~~~~~~~
一键运行完整交易选股流程：

  步骤 1  pipeline/fetch_kline.py   — 拉取最新 K 线数据
  步骤 2  pipeline/cli.py preselect — 量化初选，生成候选列表
  步骤 3  dashboard/export_kline_charts.py — 导出候选股 K 线图
  步骤 4  agent/glm_review.py       — GLM/Kimi 等双周期证据分析
  步骤 5  打印通过复评的研究候选

用法：
    python run_all.py
    python run_all.py --skip-fetch     # 跳过行情下载（已有最新数据时）
    python run_all.py --start-from 3   # 从第 3 步开始（跳过前两步）
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable  # 与当前进程同一个 Python 解释器


def _run(step_name: str, cmd: list[str]) -> None:
    """运行子进程，失败时终止整个流程。"""
    print(f"\n{'='*60}")
    print(f"[步骤] {step_name}")
    print(f"  命令: {' '.join(cmd)}")
    print(f"{'='*60}")
    result = subprocess.run(cmd, cwd=str(ROOT))
    if result.returncode != 0:
        print(f"\n[ERROR] 步骤「{step_name}」返回非零退出码 {result.returncode}，流程已中止。")
        sys.exit(result.returncode)


def _print_recommendations() -> None:
    """读取最新 suggestion.json，打印通过复评的研究候选。"""
    candidates_file = ROOT / "data" / "candidates" / "candidates_latest.json"
    if not candidates_file.exists():
        print("[ERROR] 找不到 candidates_latest.json，无法定位 suggestion.json。")
        return

    with open(candidates_file, encoding="utf-8") as f:
        pick_date: str = json.load(f).get("pick_date", "")

    if not pick_date:
        print("[ERROR] candidates_latest.json 中未设置 pick_date。")
        return

    suggestion_file = ROOT / "data" / "review" / pick_date / "suggestion.json"
    if not suggestion_file.exists():
        print(f"[ERROR] 找不到评分汇总文件：{suggestion_file}")
        return

    with open(suggestion_file, encoding="utf-8") as f:
        suggestion: dict = json.load(f)

    recommendations: list[dict] = suggestion.get("recommendations", [])
    min_score: float = suggestion.get("min_score_threshold", 0)
    total: int = suggestion.get("total_reviewed", 0)

    print(f"\n{'='*60}")
    print(f"  选股日期：{pick_date}")
    print(f"  评审总数：{total} 只   推荐门槛：score ≥ {min_score}")
    print(f"{'='*60}")

    if not recommendations:
        print("  暂无达标推荐股票。")
        return

    header = f"{'排名':>4}  {'代码':>8}  {'总分':>6}  {'信号':>10}  {'研判':>6}  备注"
    print(header)
    print("-" * len(header))
    for r in recommendations:
        rank        = r.get("rank",        "?")
        code        = r.get("code",        "?")
        score       = r.get("total_score", "?")
        signal_type = r.get("signal_type", "")
        verdict     = r.get("verdict",     "")
        comment     = r.get("comment",     "")
        score_str   = f"{score:.1f}" if isinstance(score, (int, float)) else str(score)
        print(f"{rank:>4}  {code:>8}  {score_str:>6}  {signal_type:>10}  {verdict:>6}  {comment}")

    print(f"\n✅ 通过复评的研究候选 {len(recommendations)} 只（详见 {suggestion_file}）")


def main() -> None:
    parser = argparse.ArgumentParser(description="AgentTrader 全流程自动运行脚本")
    parser.add_argument(
        "--skip-fetch", action="store_true",
        help="跳过步骤 1（行情下载），直接从初选开始",
    )
    parser.add_argument(
        "--start-from", type=int, default=1, metavar="N",
        help="从第 N 步开始执行（1~4），跳过前面的步骤",
    )
    parser.add_argument(
        "--reviewer", choices=("glm", "kimi", "openai", "gemini"), default="glm",
        help="AI 复评后端（默认国产 glm；kimi/openai/gemini 可作对照）",
    )
    args = parser.parse_args()

    start = args.start_from
    
    if args.skip_fetch and start == 1:
        start = 2

    # ── 步骤 1：拉取 K 线数据 ─────────────────────────────────────────
    if start <= 1:
        _run(
            "1/4  拉取 K 线数据（fetch_kline）",
            [PYTHON, "-m", "pipeline.fetch_kline"],
        )

    # ── 步骤 2：量化初选 ─────────────────────────────────────────────
    if start <= 2:
        _run(
            "2/4  量化初选（cli preselect）",
            [PYTHON, "-m", "pipeline.cli", "preselect"],
        )

    # ── 步骤 3：导出 K 线图 ──────────────────────────────────────────
    if start <= 3:
        _run(
            "3/4  导出 K 线图（export_kline_charts）",
            [PYTHON, str(ROOT / "dashboard" / "export_kline_charts.py")],
        )

    # ── 步骤 4：多模型图表分析 ──────────────────────────────────────
    if start <= 4:
        reviewer_script = {
            "glm": "glm_review.py",
            "kimi": "kimi_review.py",
            "openai": "openai_review.py",
            "gemini": "gemini_review.py",
        }[args.reviewer]
        _run(
            f"4/4  {args.reviewer} 双周期证据复评",
            [PYTHON, str(ROOT / "agent" / reviewer_script)],
        )

    # ── 步骤 5：打印推荐结果 ─────────────────────────────────────────
    print(f"\n{'='*60}")
    print("[步骤] 5/5  通过复评的研究候选")
    _print_recommendations()


if __name__ == "__main__":
    main()
