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
DEFAULT_CONFIG_PATH = ROOT / "config" / "openai_review.yaml"


def _path(value: str | Path) -> Path:
    p = Path(value)
    return p if p.is_absolute() else ROOT / p


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    for key in ("candidates", "kline_dir", "output_dir", "prompt_path", "raw_dir"):
        raw[key] = _path(raw[key])
    return raw


def _data_url(path: Path) -> str:
    mime = "image/png" if path.suffix.lower() == ".png" else "image/jpeg"
    return f"data:{mime};base64,{base64.b64encode(path.read_bytes()).decode('ascii')}"


class OpenAIReviewer(BaseReviewer):
    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        load_dotenv(ROOT / ".env.local")
        load_dotenv(ROOT / ".env")
        if not os.getenv("OPENAI_API_KEY"):
            print("[ERROR] 未找到 OPENAI_API_KEY", file=sys.stderr)
            raise SystemExit(1)
        self.client = OpenAI(timeout=float(config.get("timeout", 180)), max_retries=2)

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
        detail = str(self.config.get("image_detail", "high"))
        content = [
            {
                "type": "input_text",
                "text": "请审阅以下点时证据包。数值字段优先于视觉估计；图片用于确认形态和上下文。\n"
                + json.dumps(packet, ensure_ascii=False, separators=(",", ":")),
            },
            {"type": "input_text", "text": "【日线图】"},
            {"type": "input_image", "image_url": _data_url(day_chart), "detail": detail},
            {"type": "input_text", "text": "【周线图】"},
            {"type": "input_image", "image_url": _data_url(week_chart), "detail": detail},
        ]
        reasoning: dict[str, str] = {"effort": str(self.config.get("reasoning_effort", "high"))}
        if self.config.get("reasoning_mode"):
            reasoning["mode"] = str(self.config["reasoning_mode"])

        response = self.client.responses.parse(
            model=str(self.config.get("model", "gpt-5.6-sol")),
            instructions=prompt,
            input=[{"role": "user", "content": content}],
            reasoning=reasoning,
            text_format=ReviewModelOutput,
            store=False,
        )
        parsed = response.output_parsed
        if parsed is None:
            raise RuntimeError(f"OpenAI 未返回可解析结果：{code}")
        metadata = {
            "provider": "openai",
            "model": self.config.get("model", "gpt-5.6-sol"),
            "response_id": response.id,
            "reviewed_at": datetime.now(timezone.utc).isoformat(),
            "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
            "evidence_as_of": packet["technical"]["as_of"],
            "image_detail": detail,
        }
        return finalize_review(parsed, code=code, metadata=metadata)


def main() -> None:
    parser = argparse.ArgumentParser(description="OpenAI 双周期证据复评")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    args = parser.parse_args()
    config = load_config(Path(args.config))
    OpenAIReviewer(config).run()


if __name__ == "__main__":
    main()
