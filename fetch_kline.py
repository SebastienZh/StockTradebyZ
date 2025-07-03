"""
Batch-style historical K‑line downloader for A‑shares.

Changes compared with the original version
------------------------------------------
1. **Batch download for Tushare** – instead of requesting one stock per call we now
   fetch up to `--batch-size` stocks at once via `pro.daily`, which natively accepts
   a comma‑separated ticker list.
2. **Longer back‑off after failures** – each retry now sleeps a **random 10‑20 s**
   (multiplied by the attempt number) to mitigate the server‑side rate limit.

Only the Tushare path is affected; AKShare and mootdx fetching logic is left
untouched so the script remains a drop‑in replacement for the original one.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import random
import sys
import time
import warnings
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional

import akshare as ak
import pandas as pd
import tushare as ts
from mootdx.quotes import Quotes
from tqdm import tqdm
from dotenv import load_dotenv

warnings.filterwarnings("ignore")

# 加载.env文件
load_dotenv()

# --------------------------- 全局日志配置 --------------------------- #
LOG_FILE = Path("fetch.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger("fetch_mktcap")

# 屏蔽第三方库多余 INFO 日志
for noisy in ("httpx", "urllib3", "_client", "akshare"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

# --------------------------- 常量 --------------------------- #
_SLEEP_MIN, _SLEEP_MAX = 10, 20  # seconds for retry back‑off (Tushare only)

# --------------------------- 市值快照 --------------------------- #

def _get_mktcap_ak() -> pd.DataFrame:
    """实时快照，返回列：code, mktcap（单位：元）"""
    for attempt in range(1, 4):
        try:
            df = ak.stock_zh_a_spot_em()
            break
        except Exception as e:
            logger.warning("AKShare 获取市值快照失败(%d/3): %s", attempt, e)
            time.sleep(random.uniform(1, 3) * attempt)
    else:
        raise RuntimeError("AKShare 连续三次拉取市值快照失败！")

    df = df[["代码", "总市值"]].rename(columns={"代码": "code", "总市值": "mktcap"})
    df["mktcap"] = pd.to_numeric(df["mktcap"], errors="coerce")
    return df

# --------------------------- 股票池筛选 --------------------------- #

def get_constituents(
    min_cap: float,
    max_cap: float,
    small_player: bool,
    mktcap_df: Optional[pd.DataFrame] = None,
) -> List[str]:
    df = mktcap_df if mktcap_df is not None else _get_mktcap_ak()

    cond = (df["mktcap"] >= min_cap) & (df["mktcap"] <= max_cap)
    if small_player:
        cond &= ~df["code"].str.startswith(("300", "301", "688", "8", "4"))

    codes = df.loc[cond, "code"].str.zfill(6).tolist()

    # 附加股票池 appendix.json
    try:
        with open("appendix.json", "r", encoding="utf-8") as f:
            appendix_codes = json.load(f)["data"]
    except FileNotFoundError:
        appendix_codes = []
    codes = list(dict.fromkeys(appendix_codes + codes))  # 去重保持顺序

    logger.info("筛选得到 %d 只股票", len(codes))
    return codes

# --------------------------- 历史 K 线抓取（AKShare / mootdx 与原脚本一致） --------------------------- #
COLUMN_MAP_HIST_AK = {
    "日期": "date",
    "开盘": "open",
    "收盘": "close",
    "最高": "high",
    "最低": "low",
    "成交量": "volume",
    "成交额": "amount",
    "换手率": "turnover",
}

_FREQ_MAP = {
    0: "5m",
    1: "15m",
    2: "30m",
    3: "1h",
    4: "day",
    5: "week",
    6: "mon",
    7: "1m",
    8: "1m",
    9: "day",
    10: "3mon",
    11: "year",
}

# ---------- Tushare 工具函数（批量版） ---------- #

def _to_ts_code(code: str) -> str:
    """Converts 6‑digit ticker to Tushare format."""
    return f"{code.zfill(6)}.SH" if code.startswith(("60", "68", "9")) else f"{code.zfill(6)}.SZ"


def _fetch_batch_tushare(
    codes: List[str],
    per_code_start: Dict[str, str],
    end: str,
) -> pd.DataFrame:
    """Pull daily bars for *multiple* stocks via pro.daily.

    Parameters
    ----------
    codes            list of 6‑digit tickers (without exchange suffix)
    per_code_start   mapping code -> YYYYMMDD start date
    end              YYYYMMDD end date (common to all)

    Returns
    -------
    DataFrame with columns [ts_code, trade_date, open, high, low, close, vol]
    """
    ts_codes = [_to_ts_code(code) for code in codes]
    # Use the earliest required start among the batch – cheaper single query
    start = min(per_code_start.values())

    for attempt in range(1, 4):
        try:
            df = pro.daily(ts_code=",".join(ts_codes), start_date=start, end_date=end)
            break
        except Exception as e:
            logger.warning("Tushare 批量拉取失败(%d/3): %s", attempt, e)
            time.sleep(random.uniform(_SLEEP_MIN, _SLEEP_MAX) * attempt)
    else:
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    # 统一字段名 & 类型
    df = df.rename(columns={"trade_date": "date", "ts_code": "ts_code", "vol": "volume"})
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
    numeric_cols = [c for c in df.columns if c not in ("date", "ts_code")]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    return df.sort_values(["ts_code", "date"]).reset_index(drop=True)

# ---------- AKShare 工具函数 ---------- #

def _get_kline_akshare(code: str, start: str, end: str, adjust: str) -> pd.DataFrame:
    for attempt in range(1, 4):
        try:
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start,
                end_date=end,
                adjust=adjust,
            )
            break
        except Exception as e:
            logger.warning("AKShare 拉取 %s 失败(%d/3): %s", code, attempt, e)
            time.sleep(random.uniform(1, 2) * attempt)
    else:
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    df = (
        df[list(COLUMN_MAP_HIST_AK)]
        .rename(columns=COLUMN_MAP_HIST_AK)
        .assign(date=lambda x: pd.to_datetime(x["date"]))
    )
    df[[c for c in df.columns if c != "date"]] = df[[c for c in df.columns if c != "date"]].apply(
        pd.to_numeric, errors="coerce"
    )
    df = df[["date", "open", "close", "high", "low", "volume"]]
    return df.sort_values("date").reset_index(drop=True)

# ---------- Mootdx 工具函数 ---------- #

def _get_kline_mootdx(code: str, start: str, end: str, adjust: str, freq_code: int) -> pd.DataFrame:
    symbol = code.zfill(6)
    freq = _FREQ_MAP.get(freq_code, "day")
    client = Quotes.factory(market="std")
    try:
        df = client.bars(symbol=symbol, frequency=freq, adjust=adjust or None)
    except Exception as e:
        logger.warning("Mootdx 拉取 %s 失败: %s", code, e)
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.rename(
        columns={"datetime": "date", "open": "open", "high": "high", "low": "low", "close": "close", "vol": "volume"}
    )
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    start_ts = pd.to_datetime(start, format="%Y%m%d")
    end_ts = pd.to_datetime(end, format="%Y%m%d")
    df = df[(df["date"].dt.date >= start_ts.date()) & (df["date"].dt.date <= end_ts.date())].copy()
    df = df.sort_values("date").reset_index(drop=True)
    return df[["date", "open", "close", "high", "low", "volume"]]

# --------------------------- 数据校验 & 公共工具 --------------------------- #

def validate(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset="date").sort_values("date").reset_index(drop=True)
    if df["date"].isna().any():
        raise ValueError("存在缺失日期！")
    if (df["date"] > pd.Timestamp.today()).any():
        raise ValueError("数据包含未来日期，可能抓取错误！")
    return df


def drop_dup_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, ~df.columns.duplicated()]

# --------------------------- 批量抓取逻辑（Tushare 专用） --------------------------- #

def _persist_code_dataframe(code: str, new_df: pd.DataFrame, out_dir: Path, incremental: bool):
    """Merge with local CSV if needed and write to disk."""
    csv_path = out_dir / f"{code}.csv"

    if incremental and csv_path.exists():
        old_df = pd.read_csv(csv_path, parse_dates=["date"])
        old_df = drop_dup_columns(old_df)
        new_df = drop_dup_columns(new_df)
        new_df = (
            pd.concat([old_df, new_df], ignore_index=True)
            .drop_duplicates(subset="date")
            .sort_values("date")
        )
    new_df.to_csv(csv_path, index=False)


def fetch_batch_tushare(
    codes: List[str],
    start: str,
    end: str,
    out_dir: Path,
    incremental: bool,
):
    """Fetch a *batch* of stocks via Tushare and persist each to its own CSV."""
    # For incremental mode we need per‑code start dates.
    per_code_start = {code: start for code in codes}
    if incremental:
        for code in codes:
            csv_path = out_dir / f"{code}.csv"
            if csv_path.exists():
                try:
                    existing = pd.read_csv(csv_path, parse_dates=["date"])
                    last_date = existing["date"].max()
                    if last_date.date() >= pd.to_datetime(end, format="%Y%m%d").date():
                        # already up‑to‑date; skip later
                        per_code_start[code] = None
                    else:
                        per_code_start[code] = (last_date + pd.Timedelta(days=1)).strftime("%Y%m%d")
                except Exception:
                    logger.exception("读取 %s 失败，将重新下载", csv_path)
    # Remove codes that do not need refresh
    codes_to_download = [c for c in codes if per_code_start.get(c)]
    if not codes_to_download:
        return  # nothing to do

    df_all = _fetch_batch_tushare(codes_to_download, per_code_start, end)
    if df_all.empty:
        logger.debug("Batch %s 无数据", codes_to_download)
        return

    for code in codes_to_download:
        ts_code = _to_ts_code(code)
        sub = df_all[df_all["ts_code"] == ts_code].copy()
        if sub.empty:
            continue
        sub = sub.rename(columns={"open": "open", "close": "close", "high": "high", "low": "low", "volume": "volume"})[
            ["date", "open", "close", "high", "low", "volume"]
        ]
        sub = validate(sub)
        _persist_code_dataframe(code, sub, out_dir, incremental)

# --------------------------- 单只股票抓取 (AKShare / mootdx) --------------------------- #

def fetch_one(
    code: str,
    start: str,
    end: str,
    out_dir: Path,
    incremental: bool,
    datasource: str,
    freq_code: int,
):
    """Original per‑stock fetcher for non‑Tushare sources (unchanged)."""
    csv_path = out_dir / f"{code}.csv"

    # 增量更新：若本地已有数据则从最后一天开始
    if incremental and csv_path.exists():
        try:
            existing = pd.read_csv(csv_path, parse_dates=["date"])
            last_date = existing["date"].max()
            if last_date.date() >= pd.to_datetime(end, format="%Y%m%d").date():
                logger.debug("%s 已是最新，无需更新", code)
                return
            start = (last_date + pd.Timedelta(days=1)).strftime("%Y%m%d")
        except Exception:
            logger.exception("读取 %s 失败，将重新下载", csv_path)

    for attempt in range(1, 4):
        try:
            if datasource == "akshare":
                new_df = _get_kline_akshare(code, start, end, "qfq")
            else:  # mootdx
                new_df = _get_kline_mootdx(code, start, end, "qfq", freq_code)
            if new_df.empty:
                logger.debug("%s 无新数据", code)
                break
            new_df = validate(new_df)
            _persist_code_dataframe(code, new_df, out_dir, incremental)
            break
        except Exception:
            logger.exception("%s 第 %d 次抓取失败", code, attempt)
            time.sleep(random.uniform(1, 3) * attempt)
    else:
        logger.error("%s 三次抓取均失败，已跳过！", code)

# --------------------------- 主入口 --------------------------- #

def main():
    parser = argparse.ArgumentParser(description="按市值筛选 A 股并抓取历史 K 线 – 支持批量模式 (Tushare)")
    parser.add_argument("--datasource", choices=["tushare", "akshare", "mootdx"], default="tushare", help="历史 K 线数据源")
    parser.add_argument("--frequency", type=int, choices=list(_FREQ_MAP.keys()), default=4, help="K线频率编码，参见说明 (仅 mootdx 有效)")
    parser.add_argument("--exclude-gem", default=True, help="True则排除创业板/科创板/北交所")
    parser.add_argument("--min-mktcap", type=float, default=5e9, help="最小总市值（含），单位：元")
    parser.add_argument("--max-mktcap", type=float, default=float("+inf"), help="最大总市值（含），单位：元，默认无限制")
    parser.add_argument("--start", default="20190101", help="起始日期 YYYYMMDD 或 'today'")
    parser.add_argument("--end", default="today", help="结束日期 YYYYMMDD 或 'today'")
    parser.add_argument("--out", default="./data", help="输出目录")
    parser.add_argument("--workers", type=int, default=3, help="并发线程数")    
    args = parser.parse_args()

    # ---------- Token 处理 ---------- #
    if args.datasource == "tushare":
        # 从环境变量获取token
        ts_token = os.getenv("TUSHARE_TOKEN")
        if not ts_token:
            logger.error("未找到TUSHARE_TOKEN环境变量，请在.env文件中设置")
            sys.exit(1)
        ts.set_token(ts_token)
        global pro
        pro = ts.pro_api()

    # ---------- 日期解析 ---------- #
    start = dt.date.today().strftime("%Y%m%d") if args.start.lower() == "today" else args.start
    end = dt.date.today().strftime("%Y%m%d") if args.end.lower() == "today" else args.end

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ---------- 市值快照 & 股票池 ---------- #
    mktcap_df = _get_mktcap_ak()

    codes_from_filter = get_constituents(
        args.min_mktcap,
        args.max_mktcap,
        args.exclude_gem,
        mktcap_df=mktcap_df,
    )
    # 加上本地已有的股票，确保旧数据也能更新
    local_codes = [p.stem for p in out_dir.glob("*.csv")]
    codes = sorted(set(codes_from_filter) | set(local_codes))

    if not codes:
        logger.error("筛选结果为空，请调整参数！")
        sys.exit(1)

    logger.info(
        "开始抓取 %d 支股票 | 数据源:%s | 频率:%s | 日期:%s → %s",
        len(codes),
        args.datasource,
        _FREQ_MAP.get(args.frequency, "day"),
        start,
        end     
    )
    
    # ---------- 多线程抓取 ---------- #
    if args.datasource == "tushare":
        # 按 batch_size 分组
        batch_size = 20
        batches = [codes[i : i + batch_size] for i in range(0, len(codes), batch_size)]
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [
                executor.submit(fetch_batch_tushare, batch, start, end, out_dir, True)
                for batch in batches
            ]
            for _ in tqdm(as_completed(futures), total=len(futures), desc="下载进度"):
                pass
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [
                executor.submit(
                    fetch_one,
                    code,
                    start,
                    end,
                    out_dir,
                    True,
                    args.datasource,
                    args.frequency,
                )
                for code in codes
            ]
            for _ in tqdm(as_completed(futures), total=len(futures), desc="下载进度"):
                pass

    logger.info("全部任务完成，数据已保存至 %s", out_dir.resolve())


if __name__ == "__main__":
    main()
