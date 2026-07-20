"""
scripts/export_kline_charts.py
AgentTrader · 批量导出候选股票 K线图（日线 + 周线）

用法：
    python scripts/export_kline_charts.py [--date YYYY-MM-DD] [--bars 120] [--weekly-bars 60]

输出目录：
    data/kline/<date>/<code>_day.jpg
    data/kline/<date>/<code>_week.jpg

依赖：
    pip install kaleido   （Plotly 静态图导出必需）
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

# ── 路径设置 ──────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "dashboard"))

from components.charts import make_daily_chart, make_weekly_chart  # noqa: E402


# ── 数据加载 ──────────────────────────────────────────────────────────────────

def _load_candidates(candidates_path: Path) -> tuple[list[str], str]:
    """从 candidates JSON 文件中读取股票代码列表及 pick_date。

    Returns:
        (codes, pick_date)  pick_date 为空字符串时表示 JSON 中无该字段。
    """
    if not candidates_path.exists():
        print(f"[ERROR] 候选文件不存在：{candidates_path}")
        sys.exit(1)
    with open(candidates_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    codes = [c["code"] for c in data.get("candidates", [])]
    pick_date = data.get("pick_date", "")
    print(f"[INFO] 候选股票数量：{len(codes)}  pick_date：{pick_date or '(未设置)'}  来源：{candidates_path.name}")
    return codes, pick_date


def _load_raw(code: str, raw_dir: Path) -> pd.DataFrame:
    """加载单只股票日线 CSV。"""
    csv = raw_dir / f"{code}.csv"
    if not csv.exists():
        return pd.DataFrame()
    df = pd.read_csv(csv)
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


# ── 导出单张图 ────────────────────────────────────────────────────────────────

def _export_fig(fig, out_path: Path, width: int, height: int) -> None:
    """将 Plotly Figure 导出为 JPEG。"""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(
        str(out_path),
        format="jpg",
        width=width,
        height=height,
        scale=1,        # 模型输入保留清晰度，同时避免无意义的 4 倍像素开销
    )


# ── 主流程 ────────────────────────────────────────────────────────────────────

# 配置字典（直接修改此处）
CONFIG = {
    "candidates": str(_ROOT / "data" / "candidates" / "candidates_latest.json"),
    "raw_dir":    str(_ROOT / "data" / "raw"),
    "out_dir":    str(_ROOT / "data" / "kline"),
    "bars":       120,   # 日线显示 K 线数量（0 = 全部）
    "weekly_bars": 60,   # 周线显示 K 线数量（0 = 全部）
    "day_width":  1400,
    "day_height": 700,
    "week_width": 1400,
    "week_height": 700,
}


def main() -> None:
    candidates_path = Path(CONFIG["candidates"])
    raw_dir         = Path(CONFIG["raw_dir"])

    codes, pick_date = _load_candidates(candidates_path)

    # 导出日期直接读取 candidates.json 的 pick_date
    export_date = pick_date
    if not export_date:
        print("[ERROR] candidates.json 中未设置 pick_date，无法确定导出日期。")
        sys.exit(1)
    print(f"[INFO] 导出日期：{export_date}")

    out_root = Path(CONFIG["out_dir"]) / export_date

    ok_count    = 0
    skip_count  = 0

    for code in codes:
        df_raw = _load_raw(code, raw_dir)
        if df_raw.empty:
            print(f"[SKIP] {code}  — 无日线数据")
            skip_count += 1
            continue

        # ── 日线图 ────────────────────────────────────────────────────
        day_path = out_root / f"{code}_day.jpg"
        try:
            fig_day = make_daily_chart(
                df_raw, code,
                bars=CONFIG["bars"],
                height=CONFIG["day_height"],
            )
            _export_fig(fig_day, day_path, CONFIG["day_width"], CONFIG["day_height"])
        except Exception as e:
            print(f"[ERROR] {code} 日线导出失败：{e}")
            skip_count += 1
            continue

        # ── 周线图 ────────────────────────────────────────────────────
        week_path = out_root / f"{code}_week.jpg"
        try:
            fig_week = make_weekly_chart(
                df_raw, code,
                bars=CONFIG["weekly_bars"],
                height=CONFIG["week_height"],
            )
            _export_fig(fig_week, week_path, CONFIG["week_width"], CONFIG["week_height"])
        except Exception as e:
            print(f"[ERROR] {code} 周线导出失败：{e}")
            skip_count += 1
            continue

        print(f"[OK]   {code}  → {day_path.name}, {week_path.name}")
        ok_count += 1

    print(
        f"\n导出完成：成功 {ok_count} 只，跳过 {skip_count} 只。"
        f"\n输出目录：{out_root}"
    )


if __name__ == "__main__":
    main()
