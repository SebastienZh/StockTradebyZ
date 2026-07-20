from __future__ import annotations

import datetime as dt
import logging
import sys
import threading
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, List, Optional
import os

import pandas as pd
import yaml
from tqdm import tqdm

warnings.filterwarnings("ignore")

# --------------------------- pandas 兼容补丁 --------------------------- #
# tushare 内部使用了 fillna(method='ffill'/'bfill')，在 pandas 2.2+ 中已移除该参数。
# 此补丁将旧式调用自动转发到 ffill()/bfill()，无需降级 pandas。
import pandas as _pd

_orig_fillna = _pd.DataFrame.fillna

def _patched_fillna(self, value=None, *, method=None, axis=None, inplace=False, limit=None, **kwargs):
    if method is not None:
        if method == "ffill":
            result = self.ffill(axis=axis, inplace=inplace, limit=limit)
        elif method == "bfill":
            result = self.bfill(axis=axis, inplace=inplace, limit=limit)
        else:
            raise ValueError(f"Unsupported fillna method: {method}")
        return result
    return _orig_fillna(self, value, axis=axis, inplace=inplace, limit=limit, **kwargs)

_pd.DataFrame.fillna = _patched_fillna  # type: ignore[method-assign]

_orig_series_fillna = _pd.Series.fillna

def _patched_series_fillna(self, value=None, *, method=None, axis=None, inplace=False, limit=None, **kwargs):
    if method is not None:
        if method == "ffill":
            result = self.ffill(axis=axis, inplace=inplace, limit=limit)
        elif method == "bfill":
            result = self.bfill(axis=axis, inplace=inplace, limit=limit)
        else:
            raise ValueError(f"Unsupported fillna method: {method}")
        return result
    return _orig_series_fillna(self, value, axis=axis, inplace=inplace, limit=limit, **kwargs)

_pd.Series.fillna = _patched_series_fillna  # type: ignore[method-assign]

# --------------------------- 全局日志配置 --------------------------- #
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_LOG_DIR = _PROJECT_ROOT / "data" / "logs"

def _resolve_cfg_path(path_like: str | Path, base_dir: Path = _PROJECT_ROOT) -> Path:
    """将配置中的路径统一解析为绝对路径：相对路径基于项目根目录。"""
    p = Path(path_like)
    return p if p.is_absolute() else (base_dir / p)

def _default_log_path() -> Path:
    today = dt.date.today().strftime("%Y-%m-%d")
    return _DEFAULT_LOG_DIR / f"fetch_{today}.log"

def setup_logging(log_path: Optional[Path] = None) -> None:
    """初始化日志：同时输出到 stdout 和指定文件。"""
    if log_path is None:
        log_path = _default_log_path()
    log_path = Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_path, mode="a", encoding="utf-8"),
        ],
    )

logger = logging.getLogger("fetch_from_stocklist")

# --------------------------- 限流/封禁处理配置 --------------------------- #
BAN_PATTERNS = (
    "访问频繁", "请稍后", "超过频率", "频繁访问",
    "too many requests", "429",
    "forbidden", "403",
)

def _looks_like_ip_ban(exc: Exception) -> bool:
    msg = (str(exc) or "").lower()
    return any(pat in msg for pat in BAN_PATTERNS)

NETWORK_PATTERNS = (
    "remote end closed", "connection aborted", "connection reset",
    "max retries exceeded", "proxyerror", "connecttimeout", "readtimeout",
    "could not connect", "name resolution", "timed out",
)


def _is_systemic_provider_error(exc: Exception) -> bool:
    """判断应触发数据源熔断的网络/限流错误，而非个股数据错误。"""
    msg = (str(exc) or "").lower()
    return _looks_like_ip_ban(exc) or any(pattern in msg for pattern in NETWORK_PATTERNS)


class ProviderCircuitBreaker:
    """线程安全的运行期熔断器；避免数千只股票重复请求已失效的上游。"""

    def __init__(self) -> None:
        self._disabled: dict[str, str] = {}
        self._lock = threading.Lock()

    def available(self, provider: str) -> bool:
        with self._lock:
            return provider not in self._disabled

    def disable(self, provider: str, reason: Exception) -> bool:
        with self._lock:
            first = provider not in self._disabled
            self._disabled.setdefault(provider, str(reason)[:300])
            return first

    def reason(self, provider: str) -> str | None:
        with self._lock:
            return self._disabled.get(provider)


_PROVIDER_BREAKER = ProviderCircuitBreaker()
_BAOSTOCK_LOCK = threading.Lock()

# --------------------------- 历史K线数据源 --------------------------- #
# 默认使用无需 API Key 的 AKShare。Tushare 仅在配置明确选择时延迟导入，
# 因此默认流程不要求安装凭证，也不会在导入本模块时初始化 Tushare。
pro: Optional[Any] = None  # 可选的 Tushare 模块级会话

def set_api(session) -> None:
    """由外部(比如GUI)注入已创建好的 ts.pro_api() 会话"""
    global pro
    pro = session
    

def _to_ts_code(code: str) -> str:
    """把6位code映射到标准 ts_code 后缀。"""
    code = str(code).zfill(6)
    if code.startswith(("60", "68", "9")):
        return f"{code}.SH"
    elif code.startswith(("4", "8")):
        return f"{code}.BJ"
    else:
        return f"{code}.SZ"

def _get_kline_tushare(code: str, start: str, end: str) -> pd.DataFrame:
    import tushare as ts

    ts_code = _to_ts_code(code)
    try:
        df = ts.pro_bar(
            ts_code=ts_code,
            adj="qfq",
            start_date=start,
            end_date=end,
            freq="D",
            api=pro
        )
    except Exception:
        raise

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.rename(columns={"trade_date": "date", "vol": "volume"})[
        ["date", "open", "close", "high", "low", "volume"]
    ].copy()
    df["date"] = pd.to_datetime(df["date"])
    for c in ["open", "close", "high", "low", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values("date").reset_index(drop=True)


_AKSHARE_COLUMNS = {
    "日期": "date",
    "开盘": "open",
    "收盘": "close",
    "最高": "high",
    "最低": "low",
    "成交量": "volume",
}


def _normalize_akshare_kline(raw: pd.DataFrame) -> pd.DataFrame:
    """将 AKShare A股历史行情标准化为现有选择器需要的 CSV schema。"""
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["date", "open", "close", "high", "low", "volume"])
    df = raw.rename(columns=_AKSHARE_COLUMNS).copy()
    required = ["date", "open", "close", "high", "low", "volume"]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"AKShare 返回字段缺失：{missing}；实际字段：{list(raw.columns)}")
    df = df[required]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for column in required[1:]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df = df.dropna(subset=required)
    return df.sort_values("date").reset_index(drop=True)


def _get_kline_akshare(
    code: str,
    start: str,
    end: str,
    *,
    adjust: str = "qfq",
    timeout: float = 20,
) -> pd.DataFrame:
    import akshare as ak

    try:
        raw = ak.stock_zh_a_hist(
            symbol=str(code).zfill(6),
            period="daily",
            start_date=start,
            end_date=end,
            adjust=adjust,
            timeout=timeout,
        )
    except Exception:
        raise
    return _normalize_akshare_kline(raw)


def _get_kline_sina(
    code: str,
    start: str,
    end: str,
    *,
    adjust: str = "qfq",
    timeout: float = 20,
) -> pd.DataFrame:
    """通过 AKShare 的新浪行情接口获取 A 股日线。"""
    import akshare as ak

    del timeout  # 新浪接口当前不暴露 timeout 参数
    prefix = "sh" if str(code).zfill(6).startswith(("60", "68", "9")) else "sz"
    raw = ak.stock_zh_a_daily(
        symbol=f"{prefix}{str(code).zfill(6)}",
        start_date=start,
        end_date=end,
        adjust=adjust,
    )
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["date", "open", "close", "high", "low", "volume"])
    required = ["date", "open", "close", "high", "low", "volume"]
    missing = [column for column in required if column not in raw.columns]
    if missing:
        raise ValueError(f"新浪行情返回字段缺失：{missing}；实际字段：{list(raw.columns)}")
    df = raw[required].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for column in required[1:]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    return df.dropna(subset=required).sort_values("date").reset_index(drop=True)


def _get_kline_baostock(code: str, start: str, end: str, *, adjust: str = "qfq") -> pd.DataFrame:
    """使用无需 API Key 的 BaoStock；adjustflag=2 对应前复权。"""
    import baostock as bs

    code = str(code).zfill(6)
    prefix = "sh" if code.startswith(("60", "68", "9")) else "sz"
    with _BAOSTOCK_LOCK:
        login = bs.login()
        if login.error_code != "0":
            raise RuntimeError(f"BaoStock 登录失败：{login.error_msg}")
        try:
            adjustflag = {"qfq": "2", "hfq": "1", "": "3"}.get(adjust)
            if adjustflag is None:
                raise ValueError(f"BaoStock 不支持复权方式：{adjust}")
            rs = bs.query_history_k_data_plus(
                f"{prefix}.{code}",
                "date,open,high,low,close,volume",
                start_date=pd.Timestamp(start).strftime("%Y-%m-%d"),
                end_date=pd.Timestamp(end).strftime("%Y-%m-%d"),
                frequency="d",
                adjustflag=adjustflag,
            )
            rows: list[list[str]] = []
            while rs.error_code == "0" and rs.next():
                rows.append(rs.get_row_data())
            if rs.error_code != "0":
                raise RuntimeError(f"BaoStock 查询失败：{rs.error_msg}")
            raw = pd.DataFrame(rows, columns=rs.fields)
            if raw.empty:
                return pd.DataFrame(columns=["date", "open", "close", "high", "low", "volume"])
            return _get_normalized_baostock(raw)
        finally:
            bs.logout()


def _get_normalized_baostock(raw: pd.DataFrame) -> pd.DataFrame:
    required = ["date", "open", "close", "high", "low", "volume"]
    df = raw[required].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for column in required[1:]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    return df.dropna(subset=required).sort_values("date").reset_index(drop=True)


def _get_kline(
    provider: str,
    code: str,
    start: str,
    end: str,
    *,
    adjust: str,
    timeout: float,
) -> pd.DataFrame:
    if provider == "akshare":
        return _get_kline_akshare(code, start, end, adjust=adjust, timeout=timeout)
    if provider == "sina":
        return _get_kline_sina(code, start, end, adjust=adjust, timeout=timeout)
    if provider == "baostock":
        return _get_kline_baostock(code, start, end, adjust=adjust)
    if provider == "tushare":
        return _get_kline_tushare(code, start, end)
    raise ValueError(f"不支持的数据源：{provider}")


def _get_kline_with_fallback(
    providers: list[str], code: str, start: str, end: str, *, adjust: str, timeout: float
) -> tuple[pd.DataFrame, str]:
    errors: list[str] = []
    for provider in providers:
        if not _PROVIDER_BREAKER.available(provider):
            errors.append(f"{provider}=已熔断")
            continue
        try:
            frame = _get_kline(provider, code, start, end, adjust=adjust, timeout=timeout)
            if frame is None or frame.empty:
                errors.append(f"{provider}=返回空数据")
                logger.info("%s 使用 %s 返回空数据，尝试下一数据源", code, provider)
                continue
            return frame, provider
        except Exception as exc:
            errors.append(f"{provider}={str(exc)[:160]}")
            if _is_systemic_provider_error(exc) and _PROVIDER_BREAKER.disable(provider, exc):
                logger.warning("数据源 %s 发生系统性故障，本次运行全局熔断并自动降级：%s", provider, exc)
            else:
                logger.info("%s 使用 %s 失败，尝试下一数据源：%s", code, provider, exc)
    raise RuntimeError("全部数据源失败：" + " | ".join(errors))

def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.drop_duplicates(subset="date").sort_values("date").reset_index(drop=True)
    if df["date"].isna().any():
        raise ValueError("存在缺失日期！")
    if (df["date"] > pd.Timestamp.today()).any():
        raise ValueError("数据包含未来日期，可能抓取错误！")
    return df

# --------------------------- 读取 stocklist.csv & 过滤板块 --------------------------- #

def _filter_by_boards_stocklist(df: pd.DataFrame, exclude_boards: set[str]) -> pd.DataFrame:
    ts = df["ts_code"].astype(str).str.upper()
    num = ts.str.extract(r"(\d{6})", expand=False).str.zfill(6)
    mask = pd.Series(True, index=df.index)

    if "gem" in exclude_boards:
        mask &= ~((ts.str.endswith(".SZ")) & num.str.startswith(("300", "301")))
    if "star" in exclude_boards:
        mask &= ~((ts.str.endswith(".SH")) & num.str.startswith(("688",)))
    if "bj" in exclude_boards:
        mask &= ~((ts.str.endswith(".BJ")) | num.str.startswith(("4", "8")))

    return df[mask].copy()


def load_codes_from_stocklist(stocklist_csv: Path, exclude_boards: set[str]) -> List[str]:
    df = pd.read_csv(stocklist_csv)    
    df = _filter_by_boards_stocklist(df, exclude_boards)
    codes = df["symbol"].astype(str).str.zfill(6).tolist()
    codes = list(dict.fromkeys(codes))  # 去重保持顺序
    logger.info("从 %s 读取到 %d 只股票（排除板块：%s）",
                stocklist_csv, len(codes), ",".join(sorted(exclude_boards)) or "无")
    return codes

# --------------------------- 单只抓取（全量覆盖保存） --------------------------- #
def fetch_one(
    code: str,
    start: str,
    end: str,
    out_dir: Path,
    provider: str | list[str] = "akshare",
    adjust: str = "qfq",
    timeout: float = 20,
    retry_wait_seconds: int = 5,
):
    csv_path = out_dir / f"{code}.csv"

    providers = [provider] if isinstance(provider, str) else provider
    for attempt in range(1, 3):
        try:
            new_df, selected_provider = _get_kline_with_fallback(
                providers, code, start, end, adjust=adjust, timeout=timeout
            )
            if new_df.empty:
                logger.debug("%s 无数据，生成空表。", code)
                new_df = pd.DataFrame(columns=["date", "open", "close", "high", "low", "volume"])
            new_df = validate(new_df)
            new_df.to_csv(csv_path, index=False)  # 直接覆盖保存
            logger.info("%s 抓取成功 | source=%s | adjust=%s | rows=%d", code, selected_provider, adjust or "raw", len(new_df))
            break
        except Exception as e:
            silent_seconds = retry_wait_seconds * attempt
            if attempt < 2:
                logger.info(f"{code} 第 {attempt} 次抓取失败，{silent_seconds} 秒后重试：{e}")
                time.sleep(silent_seconds)
            else:
                logger.error("%s 第 %d 次抓取失败：%s", code, attempt, e)
    else:
        logger.error("%s 两次抓取均失败，已跳过！", code)



# --------------------------- 配置加载 --------------------------- #
_CONFIG_PATH = Path(__file__).parent.parent / "config" / "fetch_kline.yaml"

def _load_config(config_path: Path = _CONFIG_PATH) -> dict:
    if not config_path.exists():
        raise FileNotFoundError(f"找不到配置文件：{config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    logger.info("已加载配置文件：%s", config_path.resolve())
    return cfg


# --------------------------- 主入口 --------------------------- #
def main(log_path: Optional[Path] = None):
    # ---------- 读取 YAML 配置 ---------- #
    cfg = _load_config()

    # ---------- 日志路径（优先参数，其次 YAML，最后默认值） ---------- #
    if log_path is None:
        cfg_log = cfg.get("log")
        log_path = _resolve_cfg_path(cfg_log) if cfg_log else _default_log_path()
    setup_logging(log_path)
    logger.info("日志文件：%s", Path(log_path).resolve())

    # ---------- 数据源初始化 ---------- #
    provider = str(cfg.get("provider", "auto")).strip().lower()
    provider_chain = [str(item).strip().lower() for item in cfg.get("providers", [])]
    if provider == "auto":
        providers = provider_chain or ["akshare", "sina", "baostock"]
    else:
        providers = [provider]
    supported = {"akshare", "sina", "baostock", "tushare"}
    if not providers or any(item not in supported for item in providers):
        raise ValueError(f"数据源仅支持 {sorted(supported)}，或 provider: auto")
    if "tushare" in providers:
        import tushare as ts

        os.environ["NO_PROXY"] = "api.waditu.com,.waditu.com,waditu.com"
        os.environ["no_proxy"] = os.environ["NO_PROXY"]
        ts_token = os.environ.get("TUSHARE_TOKEN")
        if not ts_token:
            raise ValueError("选择 Tushare 时必须设置 TUSHARE_TOKEN")
        ts.set_token(ts_token)
        global pro
        pro = ts.pro_api()

    # ---------- 日期解析 ---------- #
    raw_start = str(cfg.get("start", "20190101"))
    raw_end   = str(cfg.get("end",   "today"))
    start = dt.date.today().strftime("%Y%m%d") if raw_start.lower() == "today" else raw_start
    end   = dt.date.today().strftime("%Y%m%d") if raw_end.lower()   == "today" else raw_end

    out_dir = _resolve_cfg_path(cfg.get("out", "./data"))
    out_dir.mkdir(parents=True, exist_ok=True)

    # ---------- 从 stocklist.csv 读取股票池 ---------- #
    stocklist_path = _resolve_cfg_path(cfg.get("stocklist", "./pipeline/stocklist.csv"))
    exclude_boards = set(cfg.get("exclude_boards") or [])
    codes = load_codes_from_stocklist(stocklist_path, exclude_boards)

    if not codes:
        logger.error("stocklist 为空或被过滤后无代码，请检查。")
        sys.exit(1)

    logger.info(
        "开始抓取 %d 支股票 | 数据源:%s(日线,%s) | 日期:%s → %s | 排除:%s",
        len(codes), "→".join(providers), cfg.get("adjust", "qfq"), start, end,
        ",".join(sorted(exclude_boards)) or "无",
    )

    # ---------- 多线程抓取（全量覆盖） ---------- #
    workers = int(cfg.get("workers", 2))
    adjust = str(cfg.get("adjust", "qfq"))
    timeout = float(cfg.get("timeout", 20))
    retry_wait_seconds = int(cfg.get("retry_wait_seconds", 5))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                fetch_one,
                code,
                start,
                end,
                out_dir,
                providers,
                adjust,
                timeout,
                retry_wait_seconds,
            )
            for code in codes
        ]
        for _ in tqdm(as_completed(futures), total=len(futures), desc="下载进度"):
            pass

    logger.info("全部任务完成，数据已保存至 %s", out_dir.resolve())

if __name__ == "__main__":
    main()
