from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
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

warnings.filterwarnings("ignore")

# --------------------------- 全局日志配置 --------------------------- #
# 确保日志目录存在
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

LOG_FILE = log_dir / "fetch.log"
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

# --------------------------- 市值快照 --------------------------- #


def _get_mktcap_ak() -> pd.DataFrame:
    """实时快照，返回列：code, mktcap（单位：元）"""
    for attempt in range(1, 4):
        try:
            df = ak.stock_zh_a_spot_em()
            break
        except Exception as e:
            logger.warning("AKShare 获取市值快照失败(%d/3): %s", attempt, e)
            backoff = random.uniform(1, 3) * attempt
            time.sleep(backoff)
    else:
        raise RuntimeError("AKShare 连续三次拉取市值快照失败！")

    df = df[["代码", "总市值", "名称"]].rename(
        columns={"代码": "code", "总市值": "mktcap", "名称": "name"}
    )
    df["mktcap"] = pd.to_numeric(df["mktcap"], errors="coerce")
    return df


# --------------------------- 股票池筛选 --------------------------- #


def get_constituents(
    min_cap: float,
    max_cap: float,
    small_player: bool,
    mktcap_df: Optional[pd.DataFrame] = None,
) -> List[str]:
    today = dt.date.today().strftime("%Y%m%d")
    mktcap_file = f"data/mktcap_{today}.csv"

    # 检查当日市值文件是否已存在
    if os.path.exists(mktcap_file):
        print(f"当日市值文件已存在: {mktcap_file}，跳过重复抓取")
        try:
            df = pd.read_csv(mktcap_file)
            print(f"从本地文件加载市值数据，共 {len(df)} 条记录")

            # 验证文件数据完整性
            required_columns = ["code", "mktcap"]
            if not all(col in df.columns for col in required_columns):
                raise ValueError(f"文件缺少必要列: {required_columns}")

            if df.empty:
                raise ValueError("文件为空")

            # 确保数据类型正确
            df["code"] = df["code"].astype(str)
            df["mktcap"] = pd.to_numeric(df["mktcap"], errors="coerce")

            # 检查是否有无效的市值数据
            if df["mktcap"].isna().all():
                raise ValueError("市值数据全部无效")

        except Exception as e:
            print(f"读取本地市值文件失败: {e}，重新抓取数据")
            df = mktcap_df if mktcap_df is not None else _get_mktcap_ak()
            df.to_csv(mktcap_file, index=False)
            print(f"市值已保存到 {mktcap_file}")
    else:
        df = mktcap_df if mktcap_df is not None else _get_mktcap_ak()
        df.to_csv(mktcap_file, index=False)
        print(f"市值已保存到 {mktcap_file}")

    cond = (df["mktcap"] >= min_cap) & (df["mktcap"] <= max_cap)
    print("is_small_player:", small_player)
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

_FREQ_MAP: dict[int, str] = {
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
    return (
        f"{code.zfill(6)}.SH"
        if code.startswith(("60", "68", "9"))
        else f"{code.zfill(6)}.SZ"
    )


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
    df[[c for c in df.columns if c != "date"]] = df[
        [c for c in df.columns if c != "date"]
    ].apply(pd.to_numeric, errors="coerce")
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
    df[[c for c in df.columns if c != "date"]] = df[
        [c for c in df.columns if c != "date"]
    ].apply(pd.to_numeric, errors="coerce")
    df = df[["date", "open", "close", "high", "low", "volume"]]
    return df.sort_values("date").reset_index(drop=True)


# ---------- Mootdx 工具函数 ---------- #


def _adjust_before_mootdx(
    bfq_data: pd.DataFrame, xdxr_data: pd.DataFrame
) -> pd.DataFrame:
    """
    前复权调整算法（用于mootdx数据）

    Args:
        bfq_data: 不复权的原始数据
        xdxr_data: 除权除息数据

    Returns:
        前复权调整后的数据
    """
    if xdxr_data is None or xdxr_data.empty:
        logger.debug("无除权除息数据，返回原始数据")
        return bfq_data.copy()

    # 复制数据避免修改原始数据
    result = bfq_data.copy().reset_index(drop=True)

    # 处理日期列 - mootdx返回的数据有year, month, day列
    if "datetime" in result.columns:
        result["date"] = pd.to_datetime(result["datetime"])
    elif all(col in result.columns for col in ["year", "month", "day"]):
        result["date"] = pd.to_datetime(result[["year", "month", "day"]])

    # 处理除权除息数据的日期
    xdxr_copy = xdxr_data.copy().reset_index(drop=True)
    if all(col in xdxr_copy.columns for col in ["year", "month", "day"]):
        xdxr_copy["date"] = pd.to_datetime(xdxr_copy[["year", "month", "day"]])

    # 按日期排序
    result = result.sort_values("date")
    xdxr_copy = xdxr_copy.sort_values("date")

    logger.debug(f"除权除息数据: {len(xdxr_copy)} 条记录")

    # 计算复权因子
    try:
        for _, xdxr_row in xdxr_copy.iterrows():
            xdxr_date = xdxr_row["date"]

            # 获取除权除息信息，安全处理None值
            def safe_float(value, default=0):
                """安全转换为float，处理None值"""
                if value is None or pd.isna(value):
                    return default
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return default

            fenhong = safe_float(xdxr_row.get("fenhong", 0))  # 分红
            peigujia = safe_float(xdxr_row.get("peigujia", 0))  # 配股价
            songzhuangu = safe_float(xdxr_row.get("songzhuangu", 0))  # 送转股
            peigu = safe_float(xdxr_row.get("peigu", 0))  # 配股

            # 计算复权因子
            # 前复权公式: 复权价 = (原价 - 分红 + 配股价*配股比例) / (1 + 送转股比例 + 配股比例)
            factor = 1 + songzhuangu / 10 + peigu / 10

            # 对除权日之前的数据进行调整
            mask = result["date"] < xdxr_date

            if mask.any():
                # 调整价格相关字段
                for col in ["open", "high", "low", "close"]:
                    if col in result.columns:
                        result.loc[mask, col] = (
                            pd.to_numeric(result.loc[mask, col], errors="coerce")
                            - fenhong / 10
                            + peigujia * peigu / 10
                        ) / factor

                logger.debug(
                    f"{xdxr_date.date()}: 调整了 {mask.sum()} 条记录 (因子: {factor:.4f})"
                )
    except Exception as e:
        logger.warning(f"复权调整过程中出错: {e}，返回原始数据")

    return result


# 全局客户端实例，用于批量查询时复用连接
_mootdx_client = None


def _get_mootdx_client():
    """获取或创建mootdx客户端实例"""
    global _mootdx_client
    if _mootdx_client is None:
        _mootdx_client = Quotes.factory(market="std")
    return _mootdx_client


def _get_kline_mootdx_batch(
    codes: List[str], start: str, end: str, adjust: str, freq_code: int
) -> dict:
    """
    批量获取mootdx K线数据

    Args:
        codes: 股票代码列表
        start: 开始日期
        end: 结束日期
        adjust: 复权类型
        freq_code: 频率代码

    Returns:
        dict: {code: DataFrame} 格式的结果字典
    """
    # 将频率映射转换为mootdx需要的整数格式
    freq_map_to_int = {
        "5m": 0,
        "15m": 1,
        "30m": 2,
        "1h": 3,
        "day": 9,
        "week": 7,
        "mon": 8,
        "1m": 0,
        "3mon": 8,
        "year": 8,
    }
    freq_str = _FREQ_MAP.get(freq_code, "day")
    freq_int = freq_map_to_int.get(freq_str, 9)  # 默认日线

    client = _get_mootdx_client()
    results = {}

    logger.debug(f"批量获取 {len(codes)} 只股票数据: {codes}")

    for code in codes:
        symbol = code.zfill(6)
        try:
            # 根据adjust参数决定是否进行前复权处理
            if adjust == "qfq":
                # 获取不复权数据和除权除息数据，然后手动进行前复权调整
                logger.debug(f"获取 {code} 的原始数据和除权除息数据进行前复权调整")
                bfq_data = client.bars(symbol=symbol, frequency=freq_int)
                xdxr_data = client.xdxr(symbol=symbol)

                if bfq_data is None or bfq_data.empty:
                    logger.warning("Mootdx 获取 %s 原始数据失败", code)
                    results[code] = pd.DataFrame()
                    continue

                # 添加股票代码列
                bfq_data["code"] = symbol

                # 进行前复权调整
                if xdxr_data is not None and not xdxr_data.empty:
                    logger.debug(
                        f"对 {code} 进行前复权调整，除权除息记录: {len(xdxr_data)} 条"
                    )
                    df = _adjust_before_mootdx(bfq_data, xdxr_data)
                else:
                    logger.debug(f"{code} 无除权除息数据，使用原始数据")
                    df = bfq_data
            else:
                # 其他情况直接获取数据（不复权或后复权）
                df = client.bars(
                    symbol=symbol, frequency=freq_int, adjust=adjust or None
                )

        except Exception as e:
            logger.warning("Mootdx 拉取 %s 失败: %s", code, e)
            results[code] = pd.DataFrame()
            continue

        if df is None or df.empty:
            results[code] = pd.DataFrame()
            continue

        # 重命名列以保持一致性
        df = df.rename(
            columns={
                "datetime": "date",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "vol": "volume",
            }
        )

        # 去除重复列
        df = df.loc[:, ~df.columns.duplicated()]

        # 处理日期列
        if "date" not in df.columns:
            if all(col in df.columns for col in ["year", "month", "day"]):
                df["date"] = pd.to_datetime(df[["year", "month", "day"]])
            elif "datetime" in df.columns:
                df["date"] = pd.to_datetime(df["datetime"])

        df["date"] = pd.to_datetime(df["date"]).dt.normalize()

        # 过滤日期范围
        start_ts = pd.to_datetime(start, format="%Y%m%d")
        end_ts = pd.to_datetime(end, format="%Y%m%d")
        df = df[
            (df["date"].dt.date >= start_ts.date())
            & (df["date"].dt.date <= end_ts.date())
        ].copy()

        df = df.sort_values("date").reset_index(drop=True)
        results[code] = df[["date", "open", "close", "high", "low", "volume"]]

    return results


def _get_kline_mootdx(
    code: str, start: str, end: str, adjust: str, freq_code: int
) -> pd.DataFrame:
    """
    单只股票mootdx K线数据获取（保持向后兼容）
    """
    results = _get_kline_mootdx_batch([code], start, end, adjust, freq_code)
    return results.get(code, pd.DataFrame())


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
    else:
        raise ValueError("datasource 仅支持 'tushare', 'akshare' 或 'mootdx'")


# ---------- 数据校验 ---------- #


def check_data_quality(
    df: pd.DataFrame, code: str, max_null_ratio: float = 0.3
) -> tuple[bool, str]:
    """
    检查数据质量，判断是否需要重新抓取

    Args:
        df: 数据DataFrame
        code: 股票代码
        max_null_ratio: 最大空值比例阈值，超过此比例认为数据质量差

    Returns:
        tuple[bool, str]: (是否通过质量检查, 问题描述)
    """
    if df.empty:
        return False, "数据为空"

    total_rows = len(df)
    if total_rows == 0:
        return False, "无数据行"

    # 检查关键列的空值情况
    key_columns = ["open", "high", "low", "close", "volume"]
    quality_issues = []

    for col in key_columns:
        if col not in df.columns:
            quality_issues.append(f"缺少{col}列")
            continue

        null_count = df[col].isna().sum()
        null_ratio = null_count / total_rows

        if null_ratio > max_null_ratio:
            quality_issues.append(
                f"{col}列空值比例{null_ratio:.1%}(>{max_null_ratio:.1%})"
            )

    # 检查价格数据的合理性
    if "close" in df.columns:
        close_data = df["close"].dropna()
        if len(close_data) > 0:
            #     # 检查是否有异常的零值（负值在前复权中是正常的，不检查）
            #     zero_values = (close_data == 0).sum()
            #     if zero_values > 0:
            #         quality_issues.append(f"收盘价有{zero_values}个零值")

            # 检查是否有异常的重复值（超过50%的数据相同）
            if len(close_data) > 1:
                most_common_ratio = close_data.value_counts().iloc[0] / len(close_data)
                if most_common_ratio > 0.5:
                    quality_issues.append(
                        f"收盘价重复值比例{most_common_ratio:.1%}(>50%)"
                    )

    # 检查日期连续性（工作日）
    if "date" in df.columns and len(df) > 1:
        df_sorted = df.sort_values("date")
        date_gaps = []
        for i in range(1, len(df_sorted)):
            prev_date = df_sorted.iloc[i - 1]["date"]
            curr_date = df_sorted.iloc[i]["date"]
            if isinstance(prev_date, str):
                prev_date = pd.to_datetime(prev_date)
            if isinstance(curr_date, str):
                curr_date = pd.to_datetime(curr_date)

            # 计算工作日差距（排除周末）
            business_days = pd.bdate_range(prev_date, curr_date, freq="B")
            if len(business_days) > 8:  # 超过8个工作日认为有较大间隔
                date_gaps.append(f"{prev_date.date()}到{curr_date.date()}")

        if len(date_gaps) > len(df) * 0.1:  # 如果超过10%的数据有大间隔
            quality_issues.append(f"日期间隔异常: {len(date_gaps)}处大间隔")

    if quality_issues:
        return False, "; ".join(quality_issues)

    return True, "数据质量良好"


def validate(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset="date").sort_values("date").reset_index(drop=True)
    if df["date"].isna().any():
        raise ValueError("存在缺失日期！")
    if (df["date"] > pd.Timestamp.today()).any():
        raise ValueError("数据包含未来日期，可能抓取错误！")
    return df


def drop_dup_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, ~df.columns.duplicated()]


def check_xd_stock(stock_code: str) -> bool:
    """检查股票是否为近期除权股票（名称以XD开头）"""
    try:
        # 尝试从akshare获取股票名称
        import akshare as ak

        stock_info = ak.stock_individual_info_em(symbol=stock_code)
        if not stock_info.empty:
            stock_name = stock_info[stock_info["item"] == "股票简称"]["value"].iloc[0]
            if stock_name.startswith("XD"):
                logger.info("%s 检测到近期除权股票: %s", stock_code, stock_name)
                return True
    except Exception as e:
        logger.debug("%s 获取股票名称失败: %s", stock_code, e)
    return False


# ---------- 单只股票抓取 ---------- #
def fetch_batch_mootdx(
    codes: List[str],
    start: str,
    end: str,
    out_dir: Path,
    incremental: bool,
    freq_code: int,
    adjust: str = "qfq",
    max_null_ratio: float = 0.3,
    min_rows_threshold: int = 50,
) -> List[str]:
    """
    批量获取mootdx数据并保存到文件

    Args:
        codes: 股票代码列表
        start: 开始日期
        end: 结束日期
        out_dir: 输出目录
        incremental: 是否增量更新
        freq_code: 频率代码
        adjust: 复权类型
        max_null_ratio: 最大空值比例
        min_rows_threshold: 最小行数阈值

    Returns:
        List[str]: 失败的股票代码列表
    """
    failed_codes = []

    # 预处理：检查哪些股票需要特殊处理（XD股票或数据不足）
    codes_need_akshare = []
    codes_use_mootdx = []

    for code in codes:
        csv_path = out_dir / f"{code}.csv"

        # 检查是否是近期除权股票
        if check_xd_stock(code):
            logger.warning(
                "%s 检测到近期除权股票，需要切换到akshare数据源从1970年开始抓取", code
            )
            codes_need_akshare.append(code)
            continue

        # 检查本地文件状态
        if csv_path.exists():
            try:
                existing_df = pd.read_csv(csv_path, parse_dates=["date"])
                if len(existing_df) < min_rows_threshold:
                    logger.warning(
                        "%s 本地文件数据行数不足 (%d < %d)，需要切换到akshare数据源从1970年开始抓取",
                        code,
                        len(existing_df),
                        min_rows_threshold,
                    )
                    codes_need_akshare.append(code)
                    continue
                else:
                    logger.debug(
                        "%s 本地文件数据充足 (%d 行)，使用mootdx批量获取",
                        code,
                        len(existing_df),
                    )
            except Exception as e:
                logger.warning(
                    "%s 读取本地文件失败: %s，需要切换到akshare数据源", code, e
                )
                codes_need_akshare.append(code)
                continue
        else:
            # 文件不存在，需要切换到akshare获取完整历史数据
            logger.info(
                "%s 本地文件不存在，需要切换到akshare数据源从1970年开始抓取", code
            )
            codes_need_akshare.append(code)
            continue

        # 可以使用mootdx批量获取
        codes_use_mootdx.append(code)

    # 处理需要akshare的股票（逐个处理）
    for code in codes_need_akshare:
        try:
            success = fetch_one(
                code=code,
                start="19700101",  # 从1970年开始
                end=end,
                out_dir=out_dir,
                incremental=False,  # 强制非增量模式
                datasource="akshare",
                freq_code=freq_code,
                adjust=adjust,
                max_null_ratio=max_null_ratio,
                min_rows_threshold=min_rows_threshold,
            )
            if not success:
                failed_codes.append(code)
        except Exception as e:
            logger.error(f"使用akshare处理 {code} 时发生异常: {e}")
            failed_codes.append(code)

    # 处理可以使用mootdx批量获取的股票
    if codes_use_mootdx:
        logger.debug(f"mootdx批量获取 {len(codes_use_mootdx)} 只股票数据")
        batch_results = _get_kline_mootdx_batch(
            codes_use_mootdx, start, end, adjust, freq_code
        )

        # 逐个处理和保存
        for code in codes_use_mootdx:
            try:
                df = batch_results.get(code, pd.DataFrame())

                if df.empty:
                    logger.warning(f"批量获取 {code} 数据为空")
                    failed_codes.append(code)
                    continue

                # 使用现有的保存和处理逻辑
                success = _process_and_save_stock_data(
                    code,
                    df,
                    out_dir,
                    incremental,
                    start,
                    end,
                    "mootdx",
                    freq_code,
                    adjust,
                    max_null_ratio,
                    min_rows_threshold,
                )

                if not success:
                    failed_codes.append(code)

            except Exception as e:
                logger.error(f"处理 {code} 时发生异常: {e}")
                failed_codes.append(code)

    return failed_codes


def _process_and_save_stock_data(
    code: str,
    df: pd.DataFrame,
    out_dir: Path,
    incremental: bool,
    start: str,
    end: str,
    datasource: str,
    freq_code: int,
    adjust: str,
    max_null_ratio: float,
    min_rows_threshold: int,
) -> bool:
    """
    处理和保存单只股票数据的通用逻辑

    Args:
        code: 股票代码
        df: 股票数据DataFrame
        out_dir: 输出目录
        incremental: 是否增量更新
        start: 开始日期
        end: 结束日期
        datasource: 数据源
        freq_code: 频率代码
        adjust: 复权类型
        max_null_ratio: 最大空值比例
        min_rows_threshold: 最小行数阈值

    Returns:
        bool: 是否成功
    """
    csv_path = out_dir / f"{code}.csv"

    try:
        if df.empty:
            logger.debug("%s 无新数据", code)
            return True  # 无新数据也算成功

        logger.debug(
            "%s 获得 %d 条数据 (数据源: %s, 日期范围: %s-%s)",
            code,
            len(df),
            datasource,
            start,
            end,
        )

        # 数据质量检查
        quality_ok, quality_msg = check_data_quality(df, code, max_null_ratio)
        if not quality_ok:
            logger.warning("%s 数据质量不佳: %s", code, quality_msg)
            return False

        new_df = validate(df)
        if csv_path.exists() and incremental:
            old_df = pd.read_csv(csv_path, parse_dates=["date"], index_col=False)
            old_df = drop_dup_columns(old_df)
            new_df = drop_dup_columns(new_df)

            # 获取新数据的日期范围
            if not new_df.empty:
                new_start_date = new_df["date"].min()
                # 保留旧数据中早于新数据开始日期的部分
                old_df_filtered = old_df[old_df["date"] < new_start_date]
                # 合并：旧数据（过滤后）+ 新数据，新数据会覆盖重叠部分
                new_df = pd.concat(
                    [old_df_filtered, new_df], ignore_index=True
                ).sort_values("date")
                logger.debug(
                    "%s 合并数据：保留 %d 条旧数据，添加 %d 条新数据",
                    code,
                    len(old_df_filtered),
                    len(new_df) - len(old_df_filtered),
                )
            else:
                # 如果新数据为空，保持原有数据
                new_df = old_df

        # 最终数据质量检查（合并后）
        final_quality_ok, final_quality_msg = check_data_quality(
            new_df, code, max_null_ratio
        )
        if final_quality_ok:
            logger.debug("%s 数据质量检查通过: %s", code, final_quality_msg)
        else:
            logger.warning("%s 最终数据仍有质量问题: %s", code, final_quality_msg)

        new_df.to_csv(csv_path, index=False)
        return True  # 成功

    except Exception as e:
        logger.error("%s 处理和保存数据失败: %s", code, e)
        return False


def fetch_one(
    code: str,
    start: str,
    end: str,
    out_dir: Path,
    incremental: bool,
    datasource: str,
    freq_code: int,
    adjust: str = "qfq",
    max_null_ratio: float = 0.3,
    min_rows_threshold: int = 50,
) -> bool:

    csv_path = out_dir / f"{code}.csv"

    # 增量更新：若本地已有数据则从前一天开始（覆盖前一日数据）
    # 对于近两日的数据，无论CSV是否存在当天数据都进行更新
    if incremental and csv_path.exists():
        try:
            existing = pd.read_csv(csv_path, parse_dates=["date"])
            last_date = existing["date"].max()
            end_date = pd.to_datetime(end, format="%Y%m%d").date()
            today = dt.date.today()
            yesterday = today - dt.timedelta(days=1)

            # 检查是否需要强制更新近两日数据
            if end_date >= yesterday:
                # 如果结束日期是近两日，强制从两天前开始更新
                two_days_ago = today - dt.timedelta(days=2)
                start = two_days_ago.strftime("%Y%m%d")
                logger.debug(
                    "%s 近两日数据强制更新，从 %s 开始（覆盖近两日数据）", code, start
                )
            elif last_date.date() > end_date:
                logger.debug("%s 已是最新，无需更新", code)
                return True
            else:
                # 从前一天开始抓取，覆盖前一日数据
                previous_date = last_date - pd.Timedelta(days=1)
                start = previous_date.strftime("%Y%m%d")
                logger.debug("%s 从前一日 %s 开始更新（覆盖前一日数据）", code, start)
        except Exception:
            logger.exception("读取 %s 失败，将重新下载", csv_path)

    # 预检查：检查本地文件是否存在且数据行数是否足够
    current_datasource = datasource
    current_start = start

    # 检查是否为近期除权股票（XD开头），如果是则直接切换到akshare
    if check_xd_stock(code):
        logger.warning(
            "%s 检测到近期除权股票，切换到akshare数据源从1970年开始抓取", code
        )
        current_datasource = "akshare"
        current_start = "19700101"
        incremental = False  # 强制使用非增量模式，重新获取完整数据
    # 如果不是XD股票，则检查本地文件状态（无论是否增量模式都进行预检查）
    elif csv_path.exists():
        try:
            existing_df = pd.read_csv(csv_path, parse_dates=["date"])
            if len(existing_df) < min_rows_threshold:
                logger.warning(
                    "%s 本地文件数据行数不足 (%d < %d)，切换到akshare数据源从1970年开始抓取",
                    code,
                    len(existing_df),
                    min_rows_threshold,
                )
                current_datasource = "akshare"
                current_start = "19700101"
                # 数据不足时强制使用非增量模式，重新获取完整数据
                incremental = False
            else:
                logger.debug(
                    "%s 本地文件数据充足 (%d 行)，使用原始配置", code, len(existing_df)
                )
        except Exception as e:
            logger.warning("%s 读取本地文件失败: %s，切换到akshare数据源", code, e)
            current_datasource = "akshare"
            current_start = "19700101"
            # 文件读取失败时强制使用非增量模式，重新获取完整数据
            incremental = False
    else:
        # 文件不存在，直接切换到akshare获取完整历史数据
        logger.info("%s 本地文件不存在，切换到akshare数据源从1970年开始抓取", code)
        current_datasource = "akshare"
        current_start = "19700101"
        # 文件不存在时强制使用非增量模式
        incremental = False

    for attempt in range(1, 4):
        try:
            new_df = get_kline(
                code, current_start, end, adjust, current_datasource, freq_code
            )
            if new_df.empty:
                logger.debug("%s 无新数据", code)
                return True  # 无新数据也算成功

            logger.debug(
                "%s 获得 %d 条数据 (数据源: %s, 日期范围: %s-%s)",
                code,
                len(new_df),
                current_datasource,
                current_start,
                end,
            )

            # 数据质量检查
            quality_ok, quality_msg = check_data_quality(new_df, code, max_null_ratio)
            if not quality_ok:
                logger.warning(
                    "%s 第 %d 次数据质量不佳: %s", code, attempt, quality_msg
                )
                if attempt < 4:  # 不是最后一次尝试
                    wait_time = random.uniform(4, 10) * attempt**3  # 增加等待时间
                    logger.info("%s 等待 %.1f 秒后重新抓取", code, wait_time)
                    time.sleep(wait_time)
                    continue
                else:
                    logger.warning(
                        "%s 最后一次尝试仍有质量问题，但继续保存: %s", code, quality_msg
                    )

            new_df = validate(new_df)
            if csv_path.exists() and incremental:
                old_df = pd.read_csv(csv_path, parse_dates=["date"], index_col=False)
                old_df = drop_dup_columns(old_df)
                new_df = drop_dup_columns(new_df)

                # 获取新数据的日期范围
                if not new_df.empty:
                    new_start_date = new_df["date"].min()
                    # 保留旧数据中早于新数据开始日期的部分
                    old_df_filtered = old_df[old_df["date"] < new_start_date]
                    # 合并：旧数据（过滤后）+ 新数据，新数据会覆盖重叠部分
                    new_df = pd.concat(
                        [old_df_filtered, new_df], ignore_index=True
                    ).sort_values("date")
                    logger.debug(
                        "%s 合并数据：保留 %d 条旧数据，添加 %d 条新数据",
                        code,
                        len(old_df_filtered),
                        len(new_df) - len(old_df_filtered),
                    )
                else:
                    # 如果新数据为空，保持原有数据
                    new_df = old_df

            # 最终数据质量检查（合并后）
            final_quality_ok, final_quality_msg = check_data_quality(
                new_df, code, max_null_ratio
            )
            if final_quality_ok:
                logger.debug("%s 数据质量检查通过: %s", code, final_quality_msg)
            else:
                logger.warning("%s 最终数据仍有质量问题: %s", code, final_quality_msg)

            new_df.to_csv(csv_path, index=False)
            return True  # 成功
        except Exception:
            logger.exception("%s 第 %d 次抓取失败", code, attempt)
            time.sleep(random.uniform(1, 3) * attempt)  # 指数退避

    logger.error("%s 三次抓取均失败！", code)
    return False  # 失败


# ---------- 失败列表管理 ---------- #


def save_failed_list(failed_codes: List[str], out_dir: Path, reason: str = ""):
    """立即保存失败股票列表到文件"""
    if not failed_codes:
        return

    failed_file = out_dir / "failed_stocks.txt"
    timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 读取现有失败列表（如果存在）
    existing_failed = set()
    if failed_file.exists():
        try:
            with open(failed_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        existing_failed.add(line)
        except Exception as e:
            logger.warning("读取现有失败列表失败: %s", e)

    # 合并新的失败股票
    all_failed = sorted(existing_failed | set(failed_codes))

    # 重写文件
    with open(failed_file, "w", encoding="utf-8") as f:
        f.write(f"# 抓取失败的股票列表 - 更新时间: {timestamp}\n")
        if reason:
            f.write(f"# 失败原因: {reason}\n")
        f.write(f"# 总计 {len(all_failed)} 只股票\n")
        f.write("# 格式: 每行一个股票代码\n")
        f.write("#" + "=" * 50 + "\n")
        for code in all_failed:
            f.write(f"{code}\n")

    logger.info("失败股票列表已更新至: %s (共%d只)", failed_file, len(all_failed))


def load_failed_list(out_dir: Path) -> List[str]:
    """从文件加载失败股票列表"""
    failed_file = out_dir / "failed_stocks.txt"
    if not failed_file.exists():
        return []

    failed_codes = []
    try:
        with open(failed_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    failed_codes.append(line)
        logger.info("从文件加载失败股票列表: %d只", len(failed_codes))
    except Exception as e:
        logger.warning("加载失败股票列表失败: %s", e)

    return failed_codes


def remove_from_failed_list(successful_codes: List[str], out_dir: Path):
    """立即从失败列表中移除成功的股票"""
    if not successful_codes:
        return

    failed_file = out_dir / "failed_stocks.txt"
    if not failed_file.exists():
        return

    try:
        remaining_failed = []
        with open(failed_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and line not in successful_codes:
                    remaining_failed.append(line)

        # 重写失败文件，只保留真正失败的
        if remaining_failed:
            # 直接重写文件，不使用 save_failed_list 避免合并
            timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(failed_file, "w", encoding="utf-8") as f:
                f.write(f"# 抓取失败的股票列表 - 更新时间: {timestamp}\n")
                f.write("# 失败原因: 移除成功股票后的失败列表\n")
                f.write(f"# 总计 {len(remaining_failed)} 只股票\n")
                f.write("# 格式: 每行一个股票代码\n")
                f.write("#" + "=" * 50 + "\n")
                for code in remaining_failed:
                    f.write(f"{code}\n")
            logger.debug(
                "已从失败列表中移除 %d 只成功股票，剩余 %d 只失败股票",
                len(successful_codes),
                len(remaining_failed),
            )
        else:
            # 如果没有失败的股票，删除失败文件
            failed_file.unlink()
            logger.info("所有失败股票已成功处理，已删除失败列表文件")
    except Exception as e:
        logger.warning("从失败列表移除成功股票时出错: %s", e)


def check_existing_data(
    codes: List[str], out_dir: Path, start: str, end: str
) -> tuple[List[str], List[str]]:
    """检查已有数据，返回需要下载和已完成的股票列表"""
    need_download = []
    already_done = []

    start_date = pd.to_datetime(start, format="%Y%m%d").date()
    end_date = pd.to_datetime(end, format="%Y%m%d").date()

    for code in codes:
        csv_path = out_dir / f"{code}.csv"

        if not csv_path.exists():
            need_download.append(code)
            continue

        try:
            # 检查现有数据的日期范围
            df = pd.read_csv(csv_path, parse_dates=["date"])
            if df.empty:
                need_download.append(code)
                continue

            data_start = df["date"].min().date()
            data_end = df["date"].max().date()

            # 检查数据是否覆盖所需日期范围
            if data_start <= start_date and data_end >= end_date:
                already_done.append(code)
                logger.debug("%s 数据已完整 (%s 至 %s)", code, data_start, data_end)
            else:
                need_download.append(code)
                logger.debug(
                    "%s 需要更新 (现有: %s 至 %s, 需要: %s 至 %s)",
                    code,
                    data_start,
                    data_end,
                    start_date,
                    end_date,
                )

        except Exception as e:
            logger.warning("检查 %s 现有数据失败: %s，将重新下载", code, e)
            need_download.append(code)

    logger.info(
        "数据检查完成: 需下载 %d 只，已完成 %d 只",
        len(need_download),
        len(already_done),
    )
    return need_download, already_done


# ---------- 主入口 ---------- #


def main():
    parser = argparse.ArgumentParser(description="按市值筛选 A 股并抓取历史 K 线")
    parser.add_argument(
        "--datasource",
        choices=["tushare", "akshare", "mootdx"],
        default="mootdx",
        help="历史 K 线数据源",
    )
    parser.add_argument(
        "--frequency",
        type=int,
        choices=list(_FREQ_MAP.keys()),
        default=4,
        help="K线频率编码，参见说明",
    )
    parser.add_argument(
        "--adjust",
        choices=["qfq", "hfq", "none"],
        default="qfq",
        help="复权类型：qfq=前复权(推荐), hfq=后复权, none=不复权",
    )
    parser.add_argument(
        "--max-null-ratio",
        type=float,
        default=0.3,
        help="最大空值比例阈值，超过此比例将重新抓取数据（默认30%%）",
    )
    parser.add_argument(
        "--min-rows-threshold",
        type=int,
        default=50,
        help="最小数据行数阈值，少于此行数将切换到akshare数据源从1970年开始抓取（默认50行）",
    )
    parser.add_argument(
        "--exclude-gem", action="store_true", help="排除创业板/科创板/北交所"
    )
    parser.add_argument(
        "--min-mktcap", type=float, default=5e9, help="最小总市值（含），单位：元"
    )
    parser.add_argument(
        "--max-mktcap",
        type=float,
        default=float("+inf"),
        help="最大总市值（含），单位：元，默认无限制",
    )
    parser.add_argument(
        "--start", default="20190101", help="起始日期 YYYYMMDD 或 'today'"
    )
    parser.add_argument("--end", default="today", help="结束日期 YYYYMMDD 或 'today'")
    parser.add_argument("--out", default="./data", help="输出目录")
    parser.add_argument("--workers", type=int, default=3, help="并发线程数")
    parser.add_argument(
        "--retry-failed-only",
        action="store_true",
        help="只处理失败列表中的股票，不进行市值筛选",
    )
    args = parser.parse_args()

    # ---------- Token 处理 ---------- #
    if args.datasource == "tushare":
        ts_token = " "  # 在这里补充token
        ts.set_token(ts_token)
        global pro
        pro = ts.pro_api()

    # ---------- 日期解析 ---------- #
    start = (
        dt.date.today().strftime("%Y%m%d")
        if args.start.lower() == "today"
        else args.start
    )
    end = (
        dt.date.today().strftime("%Y%m%d") if args.end.lower() == "today" else args.end
    )

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ---------- 市值快照 & 股票池 ---------- #
    if args.retry_failed_only:
        # 只处理失败列表中的股票
        logger.info("只处理失败列表模式")
        failed_codes = load_failed_list(out_dir)
        if not failed_codes:
            logger.info("没有失败的股票需要处理")
            return
        all_codes = failed_codes
        logger.info("从失败列表加载 %d 只股票", len(all_codes))
    else:
        # 正常模式：市值筛选 + 本地已有股票
        codes_from_filter = get_constituents(
            args.min_mktcap,
            args.max_mktcap,
            args.exclude_gem,
            mktcap_df=None,  # 让 get_constituents 自己决定是否需要抓取数据
        )
        print(len(codes_from_filter))
        # 加上本地已有的股票，确保旧数据也能更新
        local_codes = [p.stem for p in out_dir.glob("*.csv")]
        all_codes = sorted(set(codes_from_filter) | set(local_codes))

    if not all_codes:
        logger.error("筛选结果为空，请调整参数！")
        sys.exit(1)

    # 检查已有数据，实现增量更新
    logger.info("检查已有数据，实现增量更新...")
    codes_to_download, already_completed = check_existing_data(
        all_codes, out_dir, start, end
    )

    # 加载之前失败的股票列表，优先处理
    previous_failed = load_failed_list(out_dir)
    if previous_failed:
        logger.info("发现之前失败的股票 %d 只，将优先处理", len(previous_failed))
        # 将失败的股票放在前面，但只处理在当前股票池中的
        failed_in_pool = [code for code in previous_failed if code in all_codes]
        other_codes = [code for code in codes_to_download if code not in failed_in_pool]
        codes_to_download = failed_in_pool + other_codes
        logger.info("其中 %d 只在当前股票池中，将优先处理", len(failed_in_pool))

    if not codes_to_download:
        logger.info("所有股票数据已是最新，无需下载")
        return

    logger.info(
        "开始抓取 %d 支股票 (总计%d支，已完成%d支) | 数据源:%s | 频率:%s | 复权:%s | 空值阈值:%.1f%% | 最小行数:%d | 日期:%s → %s",
        len(codes_to_download),
        len(all_codes),
        len(already_completed),
        args.datasource,
        _FREQ_MAP[args.frequency],
        args.adjust,
        args.max_null_ratio * 100,
        args.min_rows_threshold,
        start,
        end,
    )

    # ---------- 批量抓取（带失败重试） ---------- #
    failed_codes = []
    retry_round = 1
    max_retry_rounds = 3
    codes_to_process = codes_to_download.copy()

    while codes_to_process and retry_round <= max_retry_rounds:
        if retry_round > 1:
            logger.info(
                "第 %d 轮重试，处理 %d 只失败股票", retry_round, len(codes_to_process)
            )

        current_failed = []

        if args.datasource == "mootdx":
            # 使用批量处理模式
            batch_size = 20  # 每批处理20只股票
            logger.info(f"使用mootdx批量模式，批量大小: {batch_size}")

            # 分批处理
            for i in range(0, len(codes_to_process), batch_size):
                batch_codes = codes_to_process[i : i + batch_size]
                logger.debug(f"处理批次 {i//batch_size + 1}: {len(batch_codes)} 只股票")

                try:
                    batch_failed = fetch_batch_mootdx(
                        batch_codes,
                        start,
                        end,
                        out_dir,
                        True,  # incremental
                        args.frequency,
                        args.adjust,
                        args.max_null_ratio,
                        args.min_rows_threshold,
                    )
                    current_failed.extend(batch_failed)

                    # 立即保存失败的股票
                    if batch_failed:
                        save_failed_list(
                            batch_failed, out_dir, f"第{retry_round}轮批量抓取失败"
                        )

                    # 立即从失败列表中移除成功的股票
                    batch_successful = [
                        code for code in batch_codes if code not in batch_failed
                    ]
                    if batch_successful:
                        remove_from_failed_list(batch_successful, out_dir)

                except Exception as e:
                    logger.error("批次处理失败: %s", e)
                    current_failed.extend(batch_codes)
                    save_failed_list(
                        batch_codes,
                        out_dir,
                        f"第{retry_round}轮批次异常: {str(e)[:50]}",
                    )
        else:
            # 使用原有的多线程处理模式（非mootdx数据源）
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                # 提交任务
                future_to_code = {
                    executor.submit(
                        fetch_one,
                        code,
                        start,
                        end,
                        out_dir,
                        True,
                        args.datasource,
                        args.frequency,
                        args.adjust,
                        args.max_null_ratio,
                        args.min_rows_threshold,
                    ): code
                    for code in codes_to_process
                }

                # 收集结果
                desc = f"第{retry_round}轮下载" if retry_round > 1 else "下载进度"
                batch_successful = []  # 收集本轮成功的股票

                for future in tqdm(
                    as_completed(future_to_code), total=len(future_to_code), desc=desc
                ):
                    code = future_to_code[future]
                    try:
                        success = future.result()
                        if not success:
                            current_failed.append(code)
                            # 立即保存失败的股票
                            save_failed_list(
                                [code], out_dir, f"第{retry_round}轮抓取失败"
                            )
                        else:
                            # 收集成功的股票
                            batch_successful.append(code)
                    except Exception as e:
                        logger.error("处理 %s 时发生异常: %s", code, e)
                        current_failed.append(code)
                        # 立即保存失败的股票
                        save_failed_list(
                            [code], out_dir, f"第{retry_round}轮异常: {str(e)[:50]}"
                        )

                # 立即从失败列表中移除成功的股票
                if batch_successful:
                    remove_from_failed_list(batch_successful, out_dir)

        # 更新失败列表和下一轮要处理的股票
        failed_codes.extend(current_failed)
        codes_to_process = current_failed
        retry_round += 1

        if current_failed:
            logger.warning(
                "第 %d 轮完成，%d 只股票失败", retry_round - 1, len(current_failed)
            )
            # 批量更新失败列表（确保所有失败都被记录）
            save_failed_list(current_failed, out_dir, f"第{retry_round-1}轮批量失败")

            if codes_to_process and retry_round <= max_retry_rounds:
                wait_time = 5 * (retry_round - 1)  # 轮次间等待时间递增
                if wait_time > 0:
                    logger.info("等待 %d 秒后开始下一轮重试...", wait_time)
                    time.sleep(wait_time)
        else:
            logger.info("第 %d 轮完成，所有股票处理成功", retry_round - 1)
            break

    # 最终统计
    total_codes = len(codes_to_download)
    success_codes = total_codes - len(set(failed_codes))  # 去重后的失败数量
    final_failed = list(set(failed_codes))  # 最终失败的股票（去重）

    # 清理成功的股票从失败列表中移除
    successful_codes = [code for code in codes_to_download if code not in final_failed]

    # 只要有成功的股票或者没有失败的股票，就需要清理失败列表
    if successful_codes or not final_failed:
        failed_file = out_dir / "failed_stocks.txt"
        if failed_file.exists():
            try:
                remaining_failed = []
                with open(failed_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if (
                            line
                            and not line.startswith("#")
                            and line not in successful_codes
                        ):
                            remaining_failed.append(line)

                # 重写失败文件，只保留真正失败的
                if remaining_failed:
                    save_failed_list(remaining_failed, out_dir, "最终失败列表")
                    logger.info(
                        "已从失败列表中移除 %d 只成功股票，剩余 %d 只失败股票",
                        len(successful_codes),
                        len(remaining_failed),
                    )
                else:
                    # 如果没有失败的股票，删除失败文件
                    failed_file.unlink()
                    logger.info("所有股票处理成功，已删除失败列表文件")
            except Exception as e:
                logger.warning("清理失败列表时出错: %s", e)

    logger.info("=" * 60)
    logger.info("抓取完成统计:")
    logger.info("本次处理股票数: %d", total_codes)
    logger.info("总股票池大小: %d", len(all_codes))
    logger.info("已完成股票数: %d", len(already_completed))
    logger.info("本次成功股票数: %d", success_codes)
    logger.info("本次失败股票数: %d", len(final_failed))
    logger.info(
        "本次成功率: %.1f%%",
        (success_codes / total_codes) * 100 if total_codes > 0 else 0,
    )
    logger.info(
        "总体完成率: %.1f%%",
        (
            ((len(already_completed) + success_codes) / len(all_codes)) * 100
            if len(all_codes) > 0
            else 0
        ),
    )

    if final_failed:
        logger.warning("最终失败的股票: %s", ", ".join(final_failed))
        logger.info("失败股票列表已保存至: %s", out_dir / "failed_stocks.txt")
    else:
        logger.info("所有股票处理成功！")

    logger.info("全部任务完成，数据已保存至 %s", out_dir.resolve())


if __name__ == "__main__":
    main()
