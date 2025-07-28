from __future__ import annotations

import argparse
import datetime as dt
import logging
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional

import akshare as ak
import numpy as np
import pandas as pd
import tushare as ts
from mootdx.reader import Reader
from tqdm import tqdm

# ---------- 日志 ---------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("fetch.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("fetch")

# ---------- 全局变量 ---------- #
pro = None  # Tushare Pro API 实例
_external_ts_token = None  # 外部设置的 Tushare Token

def set_tushare_token(token: str):
    """设置外部 Tushare Token"""
    global _external_ts_token
    _external_ts_token = token

# ---------- 频率映射 ---------- #
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

# ---------- 数据验证 ---------- #
def validate(df: pd.DataFrame) -> pd.DataFrame:
    """验证并清理数据"""
    if df.empty:
        return df
    
    # 确保必要的列存在
    required_cols = ["date", "open", "close", "high", "low", "volume"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"缺少必要列: {col}")
    
    # 移除无效数据
    df = df.dropna(subset=["open", "close", "high", "low"])
    df = df[df["volume"] >= 0]
    
    # 确保价格数据为正数
    price_cols = ["open", "close", "high", "low"]
    for col in price_cols:
        df = df[df[col] > 0]
    
    return df.reset_index(drop=True)

# ---------- 市值筛选 ---------- #
def _get_mktcap_ak() -> pd.DataFrame:
    """通过 AkShare 获取实时市值快照"""
    logger.info("正在获取实时市值数据...")
    try:
        df = ak.stock_zh_a_spot_em()
        df = df.rename(columns={"代码": "code", "总市值": "total_mv"})
        df["total_mv"] = pd.to_numeric(df["total_mv"], errors="coerce") * 1e8  # 转换为元
        return df[["code", "total_mv"]].dropna()
    except Exception:
        logger.exception("获取市值数据失败")
        return pd.DataFrame()

def get_constituents(
    min_mktcap: float,
    max_mktcap: float,
    exclude_gem: bool,
    mktcap_df: Optional[pd.DataFrame] = None,
) -> List[str]:
    """根据市值和板块筛选股票"""
    if mktcap_df is None or mktcap_df.empty:
        logger.warning("市值数据为空，返回空列表")
        return []
    
    # 市值筛选
    filtered = mktcap_df[
        (mktcap_df["total_mv"] >= min_mktcap) & 
        (mktcap_df["total_mv"] <= max_mktcap)
    ]
    
    # 排除特定板块
    if exclude_gem:
        filtered = filtered[
            ~filtered["code"].str.startswith(("300", "301", "688", "689", "430", "831", "832", "833", "834", "835", "836", "837", "838", "839"))
        ]
    
    codes = filtered["code"].tolist()
    logger.info("市值筛选完成，共 %d 支股票", len(codes))
    return codes

# ---------- Tushare 相关 ---------- #
def _to_ts_code(code: str) -> str:
    """转换为 Tushare 格式的股票代码"""
    if code.startswith(("000", "001", "002", "003")):
        return f"{code}.SZ"
    elif code.startswith("6"):
        return f"{code}.SH"
    elif code.startswith(("300", "301")):
        return f"{code}.SZ"
    else:
        return f"{code}.SZ"  # 默认深圳

def _fetch_batch_tushare(codes: List[str], per_code_start: Dict[str, str], end: str) -> pd.DataFrame:
    """批量获取 Tushare 数据"""
    if not codes:
        return pd.DataFrame()
    
    ts_codes = [_to_ts_code(code) for code in codes]
    
    try:
        # 获取最早的开始日期
        start_dates = [per_code_start[code] for code in codes if per_code_start[code]]
        if not start_dates:
            return pd.DataFrame()
        
        earliest_start = min(start_dates)
        
        # 批量获取数据
        df_list = []
        for ts_code in ts_codes:
            try:
                df = pro.daily(ts_code=ts_code, start_date=earliest_start, end_date=end)
                if not df.empty:
                    df["date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
                    df_list.append(df)
                time.sleep(0.1)  # 避免频率限制
            except Exception as e:
                logger.warning("获取 %s 数据失败: %s", ts_code, e)
                continue
        
        if df_list:
            return pd.concat(df_list, ignore_index=True)
        else:
            return pd.DataFrame()
            
    except Exception as e:
        logger.error("批量获取 Tushare 数据失败: %s", e)
        return pd.DataFrame()

# ---------- AkShare 相关 ---------- #
def _get_kline_akshare(code: str, start: str, end: str, adjust: str) -> pd.DataFrame:
    """通过 AkShare 获取 K 线数据"""
    try:
        df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start, end_date=end, adjust=adjust)
        if df.empty:
            return df
        
        df = df.rename(columns={
            "日期": "date",
            "开盘": "open", 
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume"
        })
        df["date"] = pd.to_datetime(df["date"])
        return df[["date", "open", "close", "high", "low", "volume"]]
    except Exception as e:
        logger.error("AkShare 获取 %s 数据失败: %s", code, e)
        return pd.DataFrame()

# ---------- Mootdx 相关 ---------- #
def _get_kline_mootdx(code: str, start: str, end: str, adjust: str, freq_code: int) -> pd.DataFrame:
    """通过 Mootdx 获取 K 线数据"""
    try:
        reader = Reader.factory(market="std", multithread=True, heartbeat=True)
        
        # 确定市场
        market = 1 if code.startswith("6") else 0
        
        # 获取数据
        df = reader.daily(symbol=code, market=market)
        if df.empty:
            return df
        
        # 日期筛选
        df["date"] = pd.to_datetime(df["date"])
        start_date = pd.to_datetime(start, format="%Y%m%d")
        end_date = pd.to_datetime(end, format="%Y%m%d")
        df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
        
        return df[["date", "open", "close", "high", "low", "volume"]]
    except Exception as e:
        logger.error("Mootdx 获取 %s 数据失败: %s", code, e)
        return pd.DataFrame()

# ---------- 数据处理 ---------- #
def drop_dup_columns(df: pd.DataFrame) -> pd.DataFrame:
    """删除重复列"""
    return df.loc[:, ~df.columns.duplicated()]

def _persist_code_dataframe(code: str, new_df: pd.DataFrame, out_dir: Path, incremental: bool):
    """合并本地 CSV 数据并写入磁盘"""
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
    """批量获取 Tushare 数据并保存"""
    # 增量模式下计算每个股票的起始日期
    per_code_start = {code: start for code in codes}
    if incremental:
        for code in codes:
            csv_path = out_dir / f"{code}.csv"
            if csv_path.exists():
                try:
                    existing = pd.read_csv(csv_path, parse_dates=["date"])
                    last_date = existing["date"].max()
                    if last_date.date() >= pd.to_datetime(end, format="%Y%m%d").date():
                        # 已是最新，跳过
                        per_code_start[code] = None
                    else:
                        per_code_start[code] = (last_date + pd.Timedelta(days=1)).strftime("%Y%m%d")
                except Exception:
                    logger.exception("读取 %s 失败，将重新下载", csv_path)
    
    # 移除不需要更新的股票
    codes_to_download = [c for c in codes if per_code_start.get(c)]
    if not codes_to_download:
        return  # 无需下载

    df_all = _fetch_batch_tushare(codes_to_download, per_code_start, end)
    if df_all.empty:
        logger.debug("批次 %s 无数据", codes_to_download)
        return

    for code in codes_to_download:
        ts_code = _to_ts_code(code)
        sub = df_all[df_all["ts_code"] == ts_code].copy()
        if sub.empty:
            continue
        sub = sub.rename(columns={"open": "open", "close": "close", "high": "high", "low": "low", "vol": "volume"})[
            ["date", "open", "close", "high", "low", "volume"]
        ]
        sub = validate(sub)
        _persist_code_dataframe(code, sub, out_dir, incremental)

def fetch_one(
    code: str,
    start: str,
    end: str,
    out_dir: Path,
    incremental: bool,
    datasource: str,
    freq_code: int,
):
    """单只股票数据获取（AKShare / mootdx）"""
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

# ---------- 主入口 ---------- #
def main():
    parser = argparse.ArgumentParser(description="按市值筛选 A 股并抓取历史 K 线")
    parser.add_argument("--datasource", choices=["tushare", "akshare", "mootdx"], default="tushare", help="历史 K 线数据源")
    parser.add_argument("--frequency", type=int, choices=list(_FREQ_MAP.keys()), default=4, help="K线频率编码")
    parser.add_argument("--exclude-gem", action="store_true", help="排除创业板/科创板/北交所")
    parser.add_argument("--min-mktcap", type=float, default=5e9, help="最小总市值（元）")
    parser.add_argument("--max-mktcap", type=float, default=float("+inf"), help="最大总市值（元）")
    parser.add_argument("--start", default="20190101", help="起始日期 YYYYMMDD 或 'today'")
    parser.add_argument("--end", default="today", help="结束日期 YYYYMMDD 或 'today'")
    parser.add_argument("--out", default="./data", help="输出目录")
    parser.add_argument("--workers", type=int, default=3, help="并发线程数")    
    args = parser.parse_args()

    # ---------- Token 处理 ---------- #
    if args.datasource == "tushare":
        # 优先使用外部设置的token，然后是环境变量，最后是硬编码
        import os
        ts_token = None
        
        if _external_ts_token:
            ts_token = _external_ts_token
        elif os.getenv('TUSHARE_TOKEN'):
            ts_token = os.getenv('TUSHARE_TOKEN')
        else:
            ts_token = ""  # 硬编码token
        
        if not ts_token:
            logger.error("使用 Tushare 数据源时必须提供 Token")
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
