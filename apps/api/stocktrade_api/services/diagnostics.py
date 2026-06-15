from __future__ import annotations

from typing import Any


def build_failure_payload(exc: BaseException, *, mode: str) -> dict[str, Any]:
    message = str(exc) or type(exc).__name__
    diagnostic = _market_data_diagnostic(exc, message) if mode == "market_data" else _generic_diagnostic(mode)
    return {
        "type": type(exc).__name__,
        "message": message,
        "diagnostic": diagnostic,
    }


def format_failure_event(error: dict[str, Any]) -> str:
    message = str(error.get("message") or "workflow failed")
    diagnostic = error.get("diagnostic")
    if not isinstance(diagnostic, dict):
        return message
    code = str(diagnostic.get("code") or "runtime_failure")
    actions = diagnostic.get("next_actions")
    first_action = ""
    if isinstance(actions, list) and actions:
        first_action = str(actions[0])
    if first_action:
        return f"[{code}] {message}；建议：{first_action}"
    return f"[{code}] {message}"


def _generic_diagnostic(mode: str) -> dict[str, Any]:
    return {
        "code": f"{mode}_runtime_failure",
        "title": "运行失败",
        "explanation": "该运行在产品任务执行期间失败，错误详情已写入运行摘要、步骤错误和事件流。",
        "next_actions": [
            "先查看运行控制台中的 error 事件和关联产物。",
            "确认本次输入、配置路径和本机环境变量后再重试。",
            "如果同一错误可稳定复现，用运行 ID、事件和 artifact 记录开 issue。",
        ],
        "retryable": True,
    }


def _market_data_diagnostic(exc: BaseException, message: str) -> dict[str, Any]:
    normalized = message.lower()
    type_name = type(exc).__name__
    if "tushare_token" in normalized:
        return {
            "code": "market_data_missing_tushare_token",
            "title": "缺少 Tushare Token",
            "explanation": "后端进程环境中没有可用的 TUSHARE_TOKEN，因此不会发起真实行情下载。",
            "next_actions": [
                "在启动产品服务的 shell 中设置 export TUSHARE_TOKEN=你的token。",
                "重新启动 ./start_product，让 FastAPI 进程继承新的环境变量。",
                "不要把 token 写入仓库文件、PR、日志或验收记录。",
            ],
            "retryable": True,
            "docs": ["docs/product-usage-manual.md#tushare-live-acceptance"],
        }
    if "找不到配置文件" in message or "no such file" in normalized:
        return {
            "code": "market_data_config_not_found",
            "title": "抓取配置文件不存在",
            "explanation": "market-data 运行无法读取 fetch_kline YAML 配置。",
            "next_actions": [
                "确认运行中心里的 Fetch config path 是否存在。",
                "留空配置路径时会使用 config/fetch_kline.yaml。",
                "如果使用自定义路径，优先使用仓库相对路径或绝对路径。",
            ],
            "retryable": True,
        }
    if type_name == "MarketDataDownloadValidationError" or "must be yyyymmdd" in normalized:
        return {
            "code": "market_data_invalid_request",
            "title": "行情下载参数无效",
            "explanation": "请求参数或 YAML 内容未通过产品侧校验。",
            "next_actions": [
                "确认开始/结束日期为 YYYY-MM-DD、YYYYMMDD 或 today。",
                "确认 workers 在 1 到 32 之间。",
                "确认 stocklist CSV 包含 ts_code 和 symbol 列。",
            ],
            "retryable": True,
        }
    if _looks_like_rate_limit(normalized):
        return {
            "code": "market_data_tushare_rate_limited",
            "title": "Tushare 限流或封禁",
            "explanation": "下载过程疑似触发 Tushare 频率限制或网络侧封禁。",
            "next_actions": [
                "降低 Workers 并缩小日期/股票范围后重试。",
                "等待冷却时间结束，不要连续立即重试。",
                "保留本次 log artifact，用于判断是频率限制还是网络错误。",
            ],
            "retryable": True,
        }
    if _looks_like_network_failure(normalized):
        return {
            "code": "market_data_network_failure",
            "title": "Tushare 网络访问失败",
            "explanation": "后端调用 Tushare 时遇到连接、超时、代理或 DNS 类错误。",
            "next_actions": [
                "确认当前网络可以访问 api.waditu.com。",
                "检查代理/NO_PROXY 设置是否影响 Tushare 请求。",
                "查看 log artifact 中的完整异常后再重试。",
            ],
            "retryable": True,
        }
    if "exited with status" in normalized or type_name == "MarketDataDownloadError":
        return {
            "code": "market_data_fetch_failed",
            "title": "行情下载脚本失败",
            "explanation": "fetch_kline 执行失败，产品运行已经保留摘要、事件和可用日志产物。",
            "next_actions": [
                "打开本次运行的 log artifact 查看 fetch_kline 原始日志。",
                "确认 stocklist、输出目录权限和 Tushare 账号权限。",
                "修复配置或环境后从运行中心重新发起下载。",
            ],
            "retryable": True,
        }
    return {
        "code": "market_data_unknown_failure",
        "title": "每日行情下载失败",
        "explanation": "market-data 运行失败，但错误未匹配到已知诊断类型。",
        "next_actions": [
            "查看运行控制台中的 error 事件。",
            "打开本次运行的 config/log artifact 对照有效配置和原始日志。",
            "如果同一错误重复出现，把运行 ID、事件和日志摘要记录到 issue。",
        ],
        "retryable": True,
    }


def _looks_like_rate_limit(text: str) -> bool:
    return any(
        pattern in text
        for pattern in (
            "访问频繁",
            "请稍后",
            "超过频率",
            "too many requests",
            "rate limit",
            "429",
            "forbidden",
            "403",
        )
    )


def _looks_like_network_failure(text: str) -> bool:
    return any(
        pattern in text
        for pattern in (
            "connection",
            "timeout",
            "timed out",
            "proxy",
            "dns",
            "network",
            "max retries exceeded",
            "connectionreset",
            "connection refused",
        )
    )
