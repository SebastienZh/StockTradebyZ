import pandas as pd
import argparse
import importlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List
from datetime import datetime, timedelta


def load_data(data_dir: Path, codes: Iterable[str]) -> Dict[str, pd.DataFrame]:
    frames: Dict[str, pd.DataFrame] = {}
    for code in codes:
        fp = data_dir / f"{code}.csv"
        if not fp.exists():
            logger.warning("%s 不存在，跳过", fp.name)
            continue
        df = pd.read_csv(fp, parse_dates=["date"]).sort_values("date")
        frames[code] = df
    return frames


def get_kline_data(csv_data, date, n):
    """
    从CSV数据中获取从指定日期开始的n天K线数据

    参数:
    csv_data (str): CSV格式的股票数据
    date (str): 起始日期，格式为'YYYY-MM-DD'
    n (int): 需要获取的天数

    返回:
    DataFrame: 包含n天K线数据的DataFrame
    """
    # 将CSV数据读取为DataFrame
    from io import StringIO
    df = pd.read_csv(StringIO(csv_data))

    # 确保日期列是datetime类型
    df['date'] = pd.to_datetime(df['date'])

    # 将输入日期转换为datetime对象
    target_date = pd.to_datetime(date)

    # 找到起始日期的索引位置
    try:
        start_idx = df[df['date'] == target_date].index[0]
    except IndexError:
        raise ValueError(f"日期 {date} 不在数据中")

    # 计算结束索引
    end_idx = start_idx + n

    # 检查是否超出数据范围
    if end_idx > len(df):
        raise ValueError(f"请求的天数 {n} 超出数据范围")

    # 返回指定范围内的数据
    return df.iloc[start_idx:end_idx].reset_index(drop=True)

def main():
    p = argparse.ArgumentParser(description="Run selectors defined in configs.json")
    p.add_argument("--data-dir", default="./data", help="CSV 行情目录")
    p.add_argument("--date", help="交易日 YYYY-MM-DD；缺省=数据最新日期")
    p.add_argument("--tickers", default="all", help="'all' 或逗号分隔股票代码列表")
    args = p.parse_args()

    # --- 加载行情 ---
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        logger.error("数据目录 %s 不存在", data_dir)
        sys.exit(1)

    codes = (
        [f.stem for f in data_dir.glob("*.csv")]
        if args.tickers.lower() == "all"
        else [c.strip() for c in args.tickers.split(",") if c.strip()]
    )
    if not codes:
        logger.error("股票池为空！")
        sys.exit(1)

    data = load_data(data_dir, codes)
    if not data:
        logger.error("未能加载任何行情数据")
        sys.exit(1)

    trade_date = (
        pd.to_datetime(args.date)
        if args.date
        else max(df["date"].max() for df in data.values())
    )
    if not args.date:
        logger.info("未指定 --date，使用最近日期 %s", trade_date.date())