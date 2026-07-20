"""
base_reviewer.py
~~~~~~~~~~~~~~~~
提供 LLM 图表分析的基础架构：
- 加载配置和 prompt
- 读取候选股票列表
- 查找本地 K 线图
- 遍历调用子类实现的单股评分模型
- 结果汇总和输出
"""

import json
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


class BaseReviewer:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.prompt = self.load_prompt(Path(config["prompt_path"]))
        self.kline_dir = Path(config["kline_dir"])
        self.output_dir = Path(config["output_dir"])

    @staticmethod
    def load_prompt(prompt_path: Path) -> str:
        return prompt_path.read_text(encoding="utf-8")

    @staticmethod
    def load_candidates(path: Path) -> dict:
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    def find_chart_images(self, pick_date: str, code: str) -> tuple[Optional[Path], Optional[Path]]:
        date_dir = self.kline_dir / pick_date
        day_chart = date_dir / f"{code}_day.jpg"
        if not day_chart.exists():
            day_chart_png = date_dir / f"{code}_day.png"
            day_chart = day_chart_png if day_chart_png.exists() else None
        week_chart = date_dir / f"{code}_week.jpg"
        if not week_chart.exists():
            week_chart_png = date_dir / f"{code}_week.png"
            week_chart = week_chart_png if week_chart_png.exists() else None
        return day_chart, week_chart

    @staticmethod
    def extract_json(text: str) -> dict:
        code_block = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
        if code_block:
            text = code_block.group(1)
        start = text.find("{")
        end = text.rfind("}") + 1
        if start == -1 or end == 0:
            raise ValueError(f"未能在模型输出中找到 JSON 对象:\n{text}")
        return json.loads(text[start:end])

    def review_stock(
        self,
        code: str,
        day_chart: Path,
        week_chart: Path,
        candidate: dict,
        pick_date: str,
        prompt: str,
    ) -> dict:
        """子类需实现此方法，调用具体的 LLM 进行打分，并返回 JSON 解析字典。"""
        raise NotImplementedError("子类必须实现 review_stock 方法")

    def generate_suggestion(self, pick_date: str, all_results: List[dict], min_score: float) -> dict:
        passed = [
            r for r in all_results
            if r.get("total_score", 0) >= min_score and r.get("verdict") == "PASS"
        ]
        excluded = [r["code"] for r in all_results if r not in passed]

        passed.sort(key=lambda r: r.get("total_score", 0), reverse=True)

        recommendations = [
            {
                "rank": i + 1,
                "code": r["code"],
                "verdict": r.get("verdict", ""),
                "total_score": r.get("total_score", 0),
                "signal_type": r.get("signal_type", ""),
                "comment": r.get("comment", ""),
            }
            for i, r in enumerate(passed)
        ]

        return {
            "date": pick_date,
            "min_score_threshold": min_score,
            "total_reviewed": len(all_results),
            "recommendations": recommendations,
            "excluded": excluded,
        }

    def run(self):
        candidates_data = self.load_candidates(Path(self.config["candidates"]))
        pick_date: str = candidates_data["pick_date"]
        candidates: List[dict] = candidates_data["candidates"]
        print(f"[INFO] pick_date={pick_date}，候选股票数={len(candidates)}")

        out_dir = self.output_dir / pick_date
        out_dir.mkdir(parents=True, exist_ok=True)

        all_results: List[dict] = []
        failed_codes: List[str] = []

        for i, candidate in enumerate(candidates, 1):
            code: str = candidate["code"]
            out_file = out_dir / f"{code}.json"

            if self.config.get("skip_existing", False) and out_file.exists():
                print(f"[{i}/{len(candidates)}] {code} — 已存在，跳过。")
                with open(out_file, encoding="utf-8") as f:
                    result = json.load(f)
                all_results.append(result)
                continue

            day_chart, week_chart = self.find_chart_images(pick_date, code)
            if day_chart is None or week_chart is None:
                print(f"[{i}/{len(candidates)}] {code} — 缺少日线图或周线图，跳过。")
                failed_codes.append(code)
                continue

            print(f"[{i}/{len(candidates)}] {code} — 正在分析 ...", end=" ", flush=True)

            try:
                result = self.review_stock(
                    code=code,
                    day_chart=day_chart,
                    week_chart=week_chart,
                    candidate=candidate,
                    pick_date=pick_date,
                    prompt=self.prompt,
                )
                with open(out_file, "w", encoding="utf-8") as f:
                    json.dump(result, f, ensure_ascii=False, indent=2)
                all_results.append(result)
                verdict = result.get("verdict", "?")
                score = result.get("total_score", "?")
                print(f"完成 — verdict={verdict}, score={score}")
            except Exception as e:
                print(f"失败 — {e}")
                failed_codes.append(code)

            if i < len(candidates):
                time.sleep(self.config.get("request_delay", 5))

        print(f"\n[INFO] 评分完成：成功 {len(all_results)} 支，失败/跳过 {len(failed_codes)} 支")
        if failed_codes:
            print(f"[WARN] 未处理股票：{failed_codes}")

        if not all_results:
            print("[ERROR] 没有可用的评分结果，跳过汇总。")
            return

        print("\n[INFO] 正在生成汇总推荐建议 ...")
        min_score = self.config.get("suggest_min_score", 4.0)
        suggestion = self.generate_suggestion(
            pick_date=pick_date,
            all_results=all_results,
            min_score=min_score,
        )
        suggestion_file = out_dir / "suggestion.json"
        with open(suggestion_file, "w", encoding="utf-8") as f:
            json.dump(suggestion, f, ensure_ascii=False, indent=2)
        print(f"[INFO] 汇总推荐已写入: {suggestion_file}")
        print(f"       推荐股票数（score≥{min_score}）: {len(suggestion['recommendations'])}")

        print("\n✅ 全部完成。")
        print(f"   输出目录: {out_dir}")
