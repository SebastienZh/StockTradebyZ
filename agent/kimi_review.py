from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from openai import OpenAI

from base_reviewer import BaseReviewer
from evidence import build_evidence_packet
from review_schema import ReviewModelOutput, finalize_review


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = ROOT / "config" / "kimi_review.yaml"


def _path(value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else ROOT / path


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    for key in ("candidates", "kline_dir", "output_dir", "prompt_path", "raw_dir"):
        raw[key] = _path(raw[key])
    return raw


def _data_url(path: Path) -> str:
    mime = "image/png" if path.suffix.lower() == ".png" else "image/jpeg"
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"data:{mime};base64,{encoded}"


def parse_model_output(text: str) -> ReviewModelOutput:
    """Kimi 返回 JSON 后仍由本地 Pydantic 做最终强类型校验。"""
    return ReviewModelOutput.model_validate(BaseReviewer.extract_json(text))


def request_options(config: dict[str, Any]) -> dict[str, Any]:
    """构造 K3 专属参数；官方不建议手动设置 temperature。"""
    return {
        "reasoning_effort": str(config.get("reasoning_effort", "high")),
        "max_tokens": int(config.get("max_tokens", 16384)),
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "stock_review",
                "strict": True,
                "schema": ReviewModelOutput.model_json_schema(),
            },
        },
    }


class KimiReviewer(BaseReviewer):
    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        load_dotenv(ROOT / ".env.local")
        load_dotenv(ROOT / ".env")
        api_key = os.getenv("MOONSHOT_API_KEY") or os.getenv("KIMI_API_KEY")
        if not api_key:
            print("[ERROR] 未找到 MOONSHOT_API_KEY（Kimi API 开放平台密钥）", file=sys.stderr)
            raise SystemExit(1)
        self.client = OpenAI(
            api_key=api_key,
            base_url=str(config.get("base_url", "https://api.moonshot.cn/v1")),
            timeout=float(config.get("timeout", 240)),
            max_retries=int(config.get("max_retries", 2)),
        )

    def review_stock(
        self,
        code: str,
        day_chart: Path,
        week_chart: Path,
        candidate: dict,
        pick_date: str,
        prompt: str,
    ) -> dict:
        packet = build_evidence_packet(ROOT, candidate, pick_date)
        system_prompt = (
            prompt
            + "\n\n只返回符合给定 JSON Schema 的最终答案；不要输出或保存推理过程。"
        )
        content = [
            {
                "type": "text",
                "text": "请审阅以下点时证据包。数值证据优先于视觉估计：\n"
                + json.dumps(packet, ensure_ascii=False, separators=(",", ":")),
            },
            {"type": "text", "text": "【日线图】"},
            {"type": "image_url", "image_url": {"url": _data_url(day_chart)}},
            {"type": "text", "text": "【周线图】"},
            {"type": "image_url", "image_url": {"url": _data_url(week_chart)}},
        ]
        response = self.client.chat.completions.create(
            model=str(self.config.get("model", "kimi-k3")),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": content},
            ],
            **request_options(self.config),
        )
        text = response.choices[0].message.content
        if not text:
            raise RuntimeError("Kimi K3 返回空响应")
        parsed = parse_model_output(text)
        usage = getattr(response, "usage", None)
        metadata = {
            "provider": "moonshot_kimi",
            "model": self.config.get("model", "kimi-k3"),
            "response_id": getattr(response, "id", None),
            "reviewed_at": datetime.now(timezone.utc).isoformat(),
            "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
            "evidence_as_of": packet["technical"]["as_of"],
            "reasoning_effort": self.config.get("reasoning_effort", "high"),
            "usage": usage.model_dump() if hasattr(usage, "model_dump") else None,
        }
        return finalize_review(parsed, code=code, metadata=metadata)


def main() -> None:
    parser = argparse.ArgumentParser(description="Kimi K3 双周期证据复评")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    args = parser.parse_args()
    KimiReviewer(load_config(Path(args.config))).run()


if __name__ == "__main__":
    main()
