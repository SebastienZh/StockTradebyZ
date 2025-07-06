from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import random
import sys
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional

import akshare as ak
import pandas as pd
import tushare as ts
from mootdx.quotes import Quotes
from tqdm import tqdm
import yfinance as yf
import threading

warnings.filterwarnings("ignore")

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
for noisy in ("httpx", "urllib3", "_client", "akshare", "yfinance"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

# --------------------------- 市值快照 --------------------------- #

def _get_mktcap_ak() -> pd.DataFrame:
    """实时快照，返回列：code, mktcap（单位：元）"""
    for attempt in range(1, 4):
        try:
            df = ak.stock_zh_a_spot_em()
            break
        except Exception as e:
            logger.warning("AKShare 获取市值快照失败(%d/3): %s", attempt, e)
            time.sleep(backoff := random.uniform(1, 3) * attempt)
    else:
        raise RuntimeError("AKShare 连续三次拉取市值快照失败！")

    df = df[["代码", "总市值"]].rename(columns={"代码": "code", "总市值": "mktcap"})
    df["mktcap"] = pd.to_numeric(df["mktcap"], errors="coerce")
    return df

# --------------------------- 增加港股通所有股票 --------------------------- #

def _get_hk_codes_ak() -> pd.DataFrame:
    for attempt in range(1, 4):
        try:
            df = ak.stock_hk_ggt_components_em()
            break
        except Exception as e:
            logger.warning("AKShare 获取港股通股票失败(%d/3): %s", attempt, e)
            time.sleep(backoff := random.uniform(1, 3) * attempt)
    else:
        raise RuntimeError("AKShare 连续三次拉取港股通股票失败！")
    
    df = df[["代码", "名称"]].rename(columns={"代码": "code", "名称": "name"})
    df["code"] = df["code"].astype(str) + ".HK"
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
    cond &= ~df["code"].str.startswith(("8", "4", "9"))
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

    with open("logs/selected_codes.json", "w", encoding="utf-8") as f:
        json.dump(codes, f, ensure_ascii=False, indent=4)
    logger.info("筛选得到 %d 只股票", len(codes))
    return codes

def get_constituents_hk() -> List[str]:
    hk_codes = _get_hk_codes_ak()
    codes = list(dict.fromkeys(hk_codes["code"].tolist()))
    with open("logs/selected_codes_hk.json", "w", encoding="utf-8") as f:
        json.dump(codes, f, ensure_ascii=False, indent=4)
    logger.info("筛选得到 %d 只股票", len(codes))
    return codes
    
# --------------------------- 历史 K 线抓取 --------------------------- #
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

# ---------- Tushare 工具函数 ---------- #

def _to_ts_code(code: str) -> str:
    return f"{code.zfill(6)}.SH" if code.startswith(("60", "68", "9")) else f"{code.zfill(6)}.SZ"


def _get_kline_tushare(code: str, start: str, end: str, adjust: str) -> pd.DataFrame:
    ts_code = _to_ts_code(code)
    adj_flag = None if adjust == "" else adjust
    for attempt in range(1, 4):
        try:
            df = ts.pro_bar(
                ts_code=ts_code,
                adj=adj_flag,
                start_date=start,
                end_date=end,
                freq="D",
            )
            break
        except Exception as e:
            logger.warning("Tushare 拉取 %s 失败(%d/3): %s", code, attempt, e)
            time.sleep(random.uniform(1, 2) * attempt)
    else:
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.rename(columns={"trade_date": "date", "vol": "volume"})[
        ["date", "open", "close", "high", "low", "volume"]
    ].copy()
    df["date"] = pd.to_datetime(df["date"])
    df[[c for c in df.columns if c != "date"]] = df[[c for c in df.columns if c != "date"]].apply(
        pd.to_numeric, errors="coerce"
    )    
    return df.sort_values("date").reset_index(drop=True)

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

# ---------- yfinance 工具函数 ---------- #

def _convert_to_yfinance_symbol(symbol: str) -> str:
    """
    将股票代码转换为yfinance格式
    
    Args:
        symbol (str): 原始股票代码
        
    Returns:
        str: yfinance格式的股票代码
    """
    if symbol.endswith('.HK'):
        return symbol[1:]  # 港股减掉第1个数字
    elif symbol.startswith('6'):
        return f"{symbol}.SS"
    elif symbol.startswith('8') or symbol.startswith('4'):
        return f"{symbol}.BJ"
    elif symbol.startswith('0') or symbol.startswith('3'):
        return f"{symbol}.SZ"
    else:
        return symbol

yf_lock = threading.Lock()
        
def _get_kline_yfinance(code: str, start: str, end: str, adjust: str, freq_code: int) -> pd.DataFrame:    
    symbol = _convert_to_yfinance_symbol(code)
    start_ts = pd.to_datetime(start, format="%Y%m%d")
    end_ts = pd.to_datetime(end, format="%Y%m%d")
    
    try:
        adj_start_date = start_ts.strftime('%Y-%m-%d')
        adj_end_date = end_ts.strftime('%Y-%m-%d')

        with yf_lock:
            df = yf.download(symbol, start=adj_start_date, end=adj_end_date, interval="1d", group_by="ticker", auto_adjust=True, progress=False)
            
    except Exception as e:
        logger.warning("yfinance 拉取 %s 失败: %s", code, e)
        return pd.DataFrame()
        
    if df is None or df.empty:
        return pd.DataFrame()
    
    # 处理 MultiIndex 列名，去掉 symbol 层级
    if isinstance(df.columns, pd.MultiIndex):
        # 如果列名是 MultiIndex，只保留 OHLCV 数据，去掉 symbol 层级
        df.columns = df.columns.get_level_values(1)  # 获取第二层列名（OHLCV）
    
    df = df.reset_index()
    df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None)

    df = df.rename(
        columns={"Date": "date", "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}
    )
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    
    df = df[(df["date"].dt.date >= start_ts.date()) & (df["date"].dt.date <= end_ts.date())].copy()
    df = df.sort_values("date").reset_index(drop=True)    
    return df[["date", "open", "close", "high", "low", "volume"]]

def _get_kline_batch_yfinance(codes: List[str], start: str, end: str, adjust: str, freq_code: int) -> dict[str, pd.DataFrame]:
    """
    批量获取多个股票代码的yfinance数据
    
    Args:
        codes (List[str]): 股票代码列表
        start (str): 开始日期，格式：YYYYMMDD
        end (str): 结束日期，格式：YYYYMMDD
        adjust (str): 复权类型
        freq_code (int): 频率代码
        
    Returns:
        dict[str, pd.DataFrame]: 以股票代码为键，DataFrame为值的字典
    """
    symbols = [_convert_to_yfinance_symbol(code) for code in codes]
    start_ts = pd.to_datetime(start, format="%Y%m%d")
    end_ts = pd.to_datetime(end, format="%Y%m%d")
    
    try:
        adj_start_date = start_ts.strftime('%Y-%m-%d')
        adj_end_date = end_ts.strftime('%Y-%m-%d')

        with yf_lock:
            df = yf.download(symbols, start=adj_start_date, end=adj_end_date, interval="1d", group_by="ticker", auto_adjust=True, progress=False)
            
    except Exception as e:
        logger.warning("yfinance 批量拉取失败: %s", e)
        return {code: pd.DataFrame() for code in codes}
        
    if df is None or df.empty:
        return {code: pd.DataFrame() for code in codes}
    
    dfs = {}
    
    # 处理 MultiIndex 列名
    if isinstance(df.columns, pd.MultiIndex):
        # 获取所有symbol名称
        symbols_in_df = df.columns.get_level_values(0).unique()
        
        for symbol in symbols_in_df:
            # 找到对应的原始代码
            original_code = None
            for code in codes:
                if _convert_to_yfinance_symbol(code) == symbol:
                    original_code = code
                    break
            
            if original_code is None:
                continue
                
            # 提取该symbol的数据
            symbol_df = df[symbol].copy()
            symbol_df = symbol_df.reset_index()
            symbol_df['Date'] = pd.to_datetime(symbol_df['Date']).dt.tz_localize(None)
            
            symbol_df = symbol_df.rename(
                columns={"Date": "date", "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}
            )
            symbol_df["date"] = pd.to_datetime(symbol_df["date"]).dt.normalize()
            
            symbol_df = symbol_df[(symbol_df["date"].dt.date >= start_ts.date()) & (symbol_df["date"].dt.date <= end_ts.date())].copy()
            symbol_df = symbol_df.sort_values("date").reset_index(drop=True)
            
            dfs[original_code] = symbol_df[["date", "open", "close", "high", "low", "volume"]]
    else:
        # 单个股票的情况，处理方式与单个函数相同
        for code in codes:
            symbol = _convert_to_yfinance_symbol(code)
            if symbol in df.columns.get_level_values(0) if isinstance(df.columns, pd.MultiIndex) else [df.name]:
                symbol_df = df.copy()
                symbol_df = symbol_df.reset_index()
                symbol_df['Date'] = pd.to_datetime(symbol_df['Date']).dt.tz_localize(None)
                
                symbol_df = symbol_df.rename(
                    columns={"Date": "date", "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}
                )
                symbol_df["date"] = pd.to_datetime(symbol_df["date"]).dt.normalize()
                
                symbol_df = symbol_df[(symbol_df["date"].dt.date >= start_ts.date()) & (symbol_df["date"].dt.date <= end_ts.date())].copy()
                symbol_df = symbol_df.sort_values("date").reset_index(drop=True)
                
                dfs[code] = symbol_df[["date", "open", "close", "high", "low", "volume"]]
            else:
                dfs[code] = pd.DataFrame()
    
    # 确保所有请求的代码都有返回值，即使为空DataFrame
    for code in codes:
        if code not in dfs:
            dfs[code] = pd.DataFrame()
    
    return dfs

# ---------- 通用接口 ---------- #

def get_kline(
    code: str,
    start: str,
    end: str,
    adjust: str,
    datasource: str,
    freq_code: int = 4,
) -> pd.DataFrame:
    if datasource == "tushare":
        return _get_kline_tushare(code, start, end, adjust)
    elif datasource == "akshare":
        return _get_kline_akshare(code, start, end, adjust)
    elif datasource == "mootdx":        
        return _get_kline_mootdx(code, start, end, adjust, freq_code)
    elif datasource == "yfinance":
        return _get_kline_yfinance(code, start, end, adjust, freq_code)
    else:
        raise ValueError("datasource 仅支持 'tushare', 'akshare', 'mootdx' 或 'yfinance'")

# ---------- 数据校验 ---------- #

def validate(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset="date").sort_values("date").reset_index(drop=True)
    if df["date"].isna().any():
        raise ValueError("存在缺失日期！")
    if (df["date"] > pd.Timestamp.today()).any():
        raise ValueError("数据包含未来日期，可能抓取错误！")
    return df

def drop_dup_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, ~df.columns.duplicated()]
# ---------- 单只股票抓取 ---------- #
def fetch_one(
    code: str,
    start: str,
    end: str,
    out_dir: Path,
    incremental: bool,
    datasource: str,
    freq_code: int,
):    
    csv_path = out_dir / f"{code}.csv"

    # 增量更新：若本地已有数据则从最后一天开始
    if incremental and csv_path.exists():
        try:
            existing = pd.read_csv(csv_path, parse_dates=["date"])
            last_date = existing["date"].max()
            if last_date.date() > pd.to_datetime(end, format="%Y%m%d").date():
                logger.debug("%s 已是最新，无需更新", code)
                return
            start = last_date.strftime("%Y%m%d")
        except Exception:
            logger.exception("读取 %s 失败，将重新下载", csv_path)

    for attempt in range(1, 4):
        try:            
            new_df = get_kline(code, start, end, "qfq", datasource, freq_code)
            if new_df.empty:
                logger.debug("%s 无新数据", code)
                break
            new_df = validate(new_df)
            if csv_path.exists() and incremental:
                old_df = pd.read_csv(
                    csv_path,
                    parse_dates=["date"],
                    index_col=False
                )
                old_df = drop_dup_columns(old_df)
                new_df = drop_dup_columns(new_df)
                new_df = (
                    pd.concat([old_df, new_df], ignore_index=True)
                    .drop_duplicates(subset="date")
                    .sort_values("date")
                )
            new_df.to_csv(csv_path, index=False)
            break
        except Exception:
            logger.exception("%s 第 %d 次抓取失败", code, attempt)
            time.sleep(random.uniform(1, 3) * attempt)  # 指数退避
    else:
        logger.error("%s 三次抓取均失败，已跳过！", code)

# ---------- 批量股票抓取（yfinance） ---------- #
def fetch_batch_yfinance(
    codes: List[str],
    start: str,
    end: str,
    out_dir: Path,
    incremental: bool,
    freq_code: int,
):
    """
    批量获取多个股票代码的yfinance数据并保存为CSV文件
    
    Args:
        codes (List[str]): 股票代码列表
        start (str): 开始日期，格式：YYYYMMDD
        end (str): 结束日期，格式：YYYYMMDD
        out_dir (Path): 输出目录
        incremental (bool): 是否增量更新
        freq_code (int): 频率代码
    """
    # 处理增量更新：收集需要更新的代码和对应的开始日期
    codes_to_fetch = []
    start_dates = {}
    
    for code in codes:
        csv_path = out_dir / f"{code}.csv"
        
        if incremental and csv_path.exists():
            try:
                existing = pd.read_csv(csv_path, parse_dates=["date"])
                last_date = existing["date"].max()
                if last_date.date() > pd.to_datetime(end, format="%Y%m%d").date():
                    logger.debug("%s 已是最新，无需更新", code)
                    continue
                start_dates[code] = last_date.strftime("%Y%m%d")
            except Exception:
                logger.exception("读取 %s 失败，将重新下载", csv_path)
                start_dates[code] = start
        else:
            start_dates[code] = start
        
        codes_to_fetch.append(code)
    
    if not codes_to_fetch:
        logger.info("所有股票都已是最新，无需更新")
        return
    
    # 按开始日期分组，相同开始日期的代码可以批量获取
    start_groups = {}
    for code in codes_to_fetch:
        start_date = start_dates[code]
        if start_date not in start_groups:
            start_groups[start_date] = []
        start_groups[start_date].append(code)
    
    # 对每个开始日期组进行批量获取
    for start_date, group_codes in start_groups.items():
        for attempt in range(1, 4):
            try:
                logger.info("批量获取 %d 只股票，开始日期: %s", len(group_codes), start_date)
                batch_data = _get_kline_batch_yfinance(group_codes, start_date, end, "qfq", freq_code)
                
                # 处理每个股票的数据
                for code in group_codes:
                    csv_path = out_dir / f"{code}.csv"
                    new_df = batch_data.get(code, pd.DataFrame())
                    
                    if new_df.empty:
                        logger.debug("%s 无新数据", code)
                        continue
                    
                    new_df = validate(new_df)
                    
                    # 处理增量更新
                    if csv_path.exists() and incremental and start_date != start:
                        try:
                            old_df = pd.read_csv(
                                csv_path,
                                parse_dates=["date"],
                                index_col=False
                            )
                            old_df = drop_dup_columns(old_df)
                            new_df = drop_dup_columns(new_df)
                            new_df = (
                                pd.concat([old_df, new_df], ignore_index=True)
                                .drop_duplicates(subset="date")
                                .sort_values("date")
                            )
                        except Exception:
                            logger.exception("合并 %s 数据失败，将覆盖", code)
                    
                    new_df.to_csv(csv_path, index=False)
                    logger.debug("%s 数据已保存", code)
                
                break  # 成功获取，跳出重试循环
                
            except Exception:
                logger.exception("批量获取第 %d 次失败", attempt)
                if attempt < 3:
                    time.sleep(random.uniform(1, 3) * attempt)  # 指数退避
        else:
            logger.error("批量获取三次均失败，已跳过 %d 只股票", len(group_codes))

# ---------- 主入口 ---------- #

def main():
    parser = argparse.ArgumentParser(description="按市值筛选 A 股并抓取历史 K 线")
    parser.add_argument("--datasource", choices=["tushare", "akshare", "mootdx", "yfinance"], default="yfinance", help="历史 K 线数据源")
    parser.add_argument("--frequency", type=int, choices=list(_FREQ_MAP.keys()), default=4, help="K线频率编码，参见说明")
    parser.add_argument("--market", default="a", choices=["a", "hk"], help="市场，a: A股，hk: 港股")
    parser.add_argument("--symbol", type=str, help="指定股票代码，使用该参数会跳过股票池筛选参数")
    parser.add_argument("--exclude-gem", default=False, help="True则排除创业板/科创板/北交所")
    parser.add_argument("--min-mktcap", type=float, default=5e9, help="最小总市值（含），单位：元")
    parser.add_argument("--max-mktcap", type=float, default=float("+inf"), help="最大总市值（含），单位：元，默认无限制")
    parser.add_argument("--start", default="20190101", help="起始日期 YYYYMMDD 或 'today'")
    parser.add_argument("--end", default="today", help="结束日期 YYYYMMDD 或 'today'")
    parser.add_argument("--out", default="./data", help="输出目录")
    parser.add_argument("--workers", type=int, default=3, help="并发线程数")
    args = parser.parse_args()

    # ---------- Token 处理 ---------- #
    if args.datasource == "tushare":
        ts_token = " "  # 在这里补充token
        ts.set_token(ts_token)
        global pro
        pro = ts.pro_api()

    # ---------- 日期解析 ---------- #
    start = dt.date.today().strftime("%Y%m%d") if args.start.lower() == "today" else args.start
    end = dt.date.today().strftime("%Y%m%d") if args.end.lower() == "today" else args.end

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ---------- 指定股票代码 & 市值快照 & 股票池 ---------- #
    if args.symbol:
        codes = [args.symbol]
    elif args.market == "hk":
        codes = get_constituents_hk()
    else:
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
        _FREQ_MAP[args.frequency],
        start,
        end,
    )

    # ---------- 数据抓取 ---------- #
    if args.datasource == "yfinance":
        # yfinance 使用批量获取，不使用多线程
        batch_size = 500  # 每次最多获取500个股票
        for i in range(0, len(codes), batch_size):
            batch_codes = codes[i:i + batch_size]
            logger.info("处理第 %d-%d 批股票，共 %d 只", i + 1, min(i + batch_size, len(codes)), len(batch_codes))
            fetch_batch_yfinance(
                batch_codes,
                start,
                end,
                out_dir,
                True,
                args.frequency,
            )
            logger.info("已完成 %d/%d 批次", i // batch_size + 1, (len(codes) + batch_size - 1) // batch_size)
    else:
        # 其他数据源使用多线程
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
