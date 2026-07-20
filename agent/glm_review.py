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
DEFAULT_CONFIG_PATH = ROOT / "config" / "glm_review.yaml"


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
    """JSON 提取后再强类型校验，模型不能绕过评分范围和必填字段。"""
    return ReviewModelOutput.model_validate(BaseReviewer.extract_json(text))


class GLMReviewer(BaseReviewer):
    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        load_dotenv(ROOT / ".env.local")
        load_dotenv(ROOT / ".env")
        api_key = os.getenv("ZAI_API_KEY") or os.getenv("ZHIPUAI_API_KEY")
        if not api_key:
            print("[ERROR] 未找到 ZAI_API_KEY（智谱开放平台 API Key）", file=sys.stderr)
            raise SystemExit(1)
        self.client = OpenAI(
            api_key=api_key,
            base_url=str(config.get("base_url", "https://open.bigmodel.cn/api/paas/v4/")),
            timeout=float(config.get("timeout", 180)),
            max_retries=int(config.get("max_retries", 2)),
        )

    def _call_model(self, model: str, messages: list[dict]) -> tuple[ReviewModelOutput, Any]:
        extra_body = None
        if self.config.get("thinking", True):
            extra_body = {"thinking": {"type": "enabled"}}
        response = self.client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=float(self.config.get("temperature", 0.2)),
            max_tokens=int(self.config.get("max_tokens", 4096)),
            response_format={"type": "json_object"},
            extra_body=extra_body,
        )
        text = response.choices[0].message.content
        if not text:
            raise RuntimeError(f"{model} 返回空响应")
        return parse_model_output(text), response

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
        schema = ReviewModelOutput.model_json_schema()
        system_prompt = (
            prompt
            + "\n\n只返回一个JSON对象，不要使用Markdown代码块。必须严格符合以下JSON Schema：\n"
            + json.dumps(schema, ensure_ascii=False, separators=(",", ":"))
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
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": content},
        ]

        primary = str(self.config.get("model", "glm-5v-turbo"))
        fallback = str(self.config.get("fallback_model", "glm-4.6v-flash"))
        models = list(dict.fromkeys(model for model in (primary, fallback) if model))
        errors: list[str] = []
        for model in models:
            try:
                parsed, response = self._call_model(model, messages)
                usage = getattr(response, "usage", None)
                metadata = {
                    "provider": "zhipu_glm",
                    "model": model,
                    "primary_model": primary,
                    "used_fallback": model != primary,
                    "response_id": getattr(response, "id", None),
                    "reviewed_at": datetime.now(timezone.utc).isoformat(),
                    "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
                    "evidence_as_of": packet["technical"]["as_of"],
                    "usage": usage.model_dump() if hasattr(usage, "model_dump") else None,
                }
                return finalize_review(parsed, code=code, metadata=metadata)
            except Exception as exc:
                errors.append(f"{model}: {type(exc).__name__}: {str(exc)[:300]}")
        raise RuntimeError("；".join(errors))


def main() -> None:
    parser = argparse.ArgumentParser(description="智谱 GLM 双周期证据复评")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    args = parser.parse_args()
    GLMReviewer(load_config(Path(args.config))).run()


if __name__ == "__main__":
    main()
