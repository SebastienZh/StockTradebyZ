from __future__ import annotations

import argparse
import importlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd

# ---------- 日志 ----------
# 确保日志目录存在
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        # 将日志写入文件
        logging.FileHandler(log_dir / "select_results.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("select")


# ---------- 工具 ----------


def load_stock_names(data_dir: Path) -> Dict[str, str]:
    """从市值数据文件中加载股票名称映射"""
    name_map = {}

    # 查找最新的市值数据文件
    mktcap_files = list(data_dir.glob("mktcap_*.csv"))
    if not mktcap_files:
        logger.warning("未找到市值数据文件，将使用默认股票名称")
        return name_map

    # 使用最新的市值文件
    latest_mktcap_file = max(mktcap_files, key=lambda x: x.name)
    logger.info("使用市值数据文件: %s", latest_mktcap_file.name)

    try:
        df = pd.read_csv(latest_mktcap_file)
        if "code" in df.columns and "name" in df.columns:
            # 将代码转换为6位字符串格式
            df["code"] = df["code"].astype(str).str.zfill(6)
            name_map = dict(zip(df["code"], df["name"]))
            logger.info("成功加载 %d 个股票名称", len(name_map))
        else:
            logger.warning("市值数据文件格式不正确，缺少 code 或 name 列")
    except Exception as e:
        logger.error("读取市值数据文件失败: %s", e)

    return name_map


def get_stock_display_name(code: str, name_map: Dict[str, str]) -> str:
    """获取股票显示名称，格式为：名称(代码)"""
    name = name_map.get(code, f"股票{code}")
    return f"{name}({code})"


def load_market_cap_data(data_dir: Path) -> Dict[str, float]:
    """从市值数据文件中加载市值映射，单位转换为亿元"""
    mktcap_map = {}

    # 查找最新的市值数据文件
    mktcap_files = list(data_dir.glob("mktcap_*.csv"))
    if not mktcap_files:
        logger.warning("未找到市值数据文件")
        return mktcap_map

    # 使用最新的市值文件
    latest_mktcap_file = max(mktcap_files, key=lambda x: x.name)
    logger.info("使用市值数据文件: %s", latest_mktcap_file.name)

    try:
        df = pd.read_csv(latest_mktcap_file)
        if "code" in df.columns and "mktcap" in df.columns:
            # 将代码转换为6位字符串格式，市值转换为亿元
            df["code"] = df["code"].astype(str).str.zfill(6)
            df["mktcap_yi"] = df["mktcap"] / 100000000  # 转换为亿元
            mktcap_map = dict(zip(df["code"], df["mktcap_yi"]))
            logger.info("成功加载 %d 个股票市值数据", len(mktcap_map))
        else:
            logger.warning("市值数据文件格式不正确，缺少 code 或 mktcap 列")
    except Exception as e:
        logger.error("读取市值数据文件失败: %s", e)

    return mktcap_map


def calculate_profit_loss_ratio(df: pd.DataFrame, trade_date: pd.Timestamp) -> float:
    """
    计算盈亏比: (最近90天的最高价-当前价格)/(当前价格-最近60天最低价格)
    """
    try:
        # 获取交易日当天或之前的最新数据
        current_data = df[df["date"] <= trade_date]
        if current_data.empty:
            return 0.0

        current_price = current_data.iloc[-1]["close"]

        # 计算最近90天的最高价(前高)
        lasthigh_period = 90
        lasthigh_date = trade_date - pd.Timedelta(days=lasthigh_period)
        recent_90_data = current_data[current_data["date"] >= lasthigh_date]
        if recent_90_data.empty:
            return 0.0
        max_price_90 = recent_90_data["high"].max()

        # 计算最近30天的最低价（前期支撑/止损价）
        lastsupport_period = 30
        days_60_ago = trade_date - pd.Timedelta(days=lastsupport_period)
        recent_60_data = current_data[current_data["date"] >= days_60_ago]
        if recent_60_data.empty:
            return 0.0
        min_price_60 = recent_60_data["low"].min()

        # 计算盈亏比
        if current_price <= min_price_60:
            return 0.0  # 避免分母为0或负数

        profit_loss_ratio = (max_price_90 - current_price) / (
            current_price - min_price_60
        )
        return profit_loss_ratio

    except Exception as e:
        logger.warning("计算盈亏比失败: %s", e)
        return 0.0


def create_selection_result(
    all_selected_stocks: Dict[
        str, List[str]
    ],  # 改为字典，key为股票代码，value为选择器别名列表
    data: Dict[str, pd.DataFrame],
    stock_names: Dict[str, str],
    mktcap_data: Dict[str, float],
    trade_date: pd.Timestamp,
    output_dir: Path,
) -> None:
    """创建选股结果文件，包含排序后的股票信息"""

    if not all_selected_stocks:
        logger.info("没有选出任何股票，跳过结果文件生成")
        return

    # 确保输出目录存在
    output_dir.mkdir(exist_ok=True)

    # 计算每只股票的指标
    stock_results = []
    for code, selector_aliases in all_selected_stocks.items():
        if code not in data:
            continue

        df = data[code]
        stock_name = stock_names.get(code, f"股票{code}")
        mktcap = mktcap_data.get(code, 0.0)
        profit_loss_ratio = calculate_profit_loss_ratio(df, trade_date)

        # 将选择器别名列表转换为字符串，用逗号分隔
        selector_names = "|".join(selector_aliases)

        stock_results.append(
            {
                "股票名称": stock_name,
                "股票代码": code,  # 确保代码保持为字符串格式
                "选择器": selector_names,
                "盈亏比": profit_loss_ratio,
                "市值(亿元)": mktcap,
            }
        )

    # 按盈亏比降序排序
    stock_results.sort(key=lambda x: x["盈亏比"], reverse=True)

    # 创建DataFrame并保存
    result_df = pd.DataFrame(stock_results)

    # 确保股票代码列为字符串类型
    result_df["股票代码"] = result_df["股票代码"].astype(str)

    # 生成文件名，包含日期
    date_str = trade_date.strftime("%Y%m%d")
    output_file = output_dir / f"selection_result_{date_str}.csv"

    result_df.to_csv(output_file, index=False, encoding="utf-8-sig")
    logger.info("选股结果已保存到: %s", output_file)
    logger.info("共选出 %d 只股票", len(stock_results))

    # 显示前10只股票的信息
    if len(stock_results) > 0:
        logger.info("排序后前10只股票:")
        for i, stock in enumerate(stock_results[:10], 1):
            logger.info(
                "%d. %s(%s) - 盈亏比: %.3f, 市值: %.2f亿元",
                i,
                stock["股票名称"],
                stock["股票代码"],
                stock["盈亏比"],
                stock["市值(亿元)"],
            )


def load_data(data_dir: Path, codes: Iterable[str]) -> Dict[str, pd.DataFrame]:
    frames: Dict[str, pd.DataFrame] = {}
    for code in codes:
        fp = data_dir / f"{code}.csv"
        if not fp.exists():
            logger.warning("%s 不存在，跳过", fp.name)
            continue
        try:
            # Read CSV and handle duplicate columns
            df = pd.read_csv(fp)

            # Remove duplicate columns (keep first occurrence)
            df = df.loc[:, ~df.columns.duplicated()]

            # Parse date column if it exists
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"])
                df = df.sort_values("date")
            else:
                logger.warning("%s 缺少 date 列，跳过", fp.name)
                continue

            frames[code] = df
        except Exception as e:
            logger.error("读取 %s 失败: %s，跳过", fp.name, e)
            continue
    return frames


def load_config(cfg_path: Path) -> List[Dict[str, Any]]:
    if not cfg_path.exists():
        logger.error("配置文件 %s 不存在", cfg_path)
        sys.exit(1)
    with cfg_path.open(encoding="utf-8") as f:
        cfg_raw = json.load(f)

    # 兼容三种结构：单对象、对象数组、或带 selectors 键
    if isinstance(cfg_raw, list):
        cfgs = cfg_raw
    elif isinstance(cfg_raw, dict) and "selectors" in cfg_raw:
        cfgs = cfg_raw["selectors"]
    else:
        cfgs = [cfg_raw]

    if not cfgs:
        logger.error("configs.json 未定义任何 Selector")
        sys.exit(1)

    return cfgs


def instantiate_selector(cfg: Dict[str, Any]):
    """动态加载 Selector 类并实例化"""
    cls_name = cfg.get("class")
    if not cls_name:
        raise ValueError("缺少 class 字段")

    try:
        module = importlib.import_module("Selector")
        cls = getattr(module, cls_name)
    except (ModuleNotFoundError, AttributeError) as e:
        raise ImportError(f"无法加载 Selector.{cls_name}: {e}") from e

    params = cfg.get("params", {})
    return cfg.get("alias", cls_name), cls(**params)


# ---------- 主函数 ----------


def main():
    p = argparse.ArgumentParser(description="Run selectors defined in configs.json")
    p.add_argument("--data-dir", default="./data", help="CSV 行情目录")
    p.add_argument("--config", default="./configs.json", help="Selector 配置文件")
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

    # --- 加载股票名称映射 ---
    stock_names = load_stock_names(data_dir)

    # --- 加载市值数据 ---
    mktcap_data = load_market_cap_data(data_dir)

    # --- 加载 Selector 配置 ---
    selector_cfgs = load_config(Path(args.config))

    # --- 逐个 Selector 运行 ---
    all_selected_stocks = (
        {}
    )  # 用于收集所有选出的股票，key为股票代码，value为选择器别名列表

    for cfg in selector_cfgs:
        if cfg.get("activate", True) is False:
            continue
        try:
            alias, selector = instantiate_selector(cfg)
        except Exception as e:
            logger.error("跳过配置 %s：%s", cfg, e)
            continue

        picks = selector.select(trade_date, data)

        # 将结果写入日志，同时输出到控制台
        logger.info("")
        logger.info("============== 选股结果 [%s] ==============", alias)
        logger.info("交易日: %s", trade_date.date())
        logger.info("符合条件股票数: %d", len(picks))

        if picks:
            # 格式化股票列表，显示名称(代码)
            formatted_picks = [
                get_stock_display_name(code, stock_names) for code in picks
            ]
            logger.info("%s", ", ".join(formatted_picks))
            # 收集选出的股票，记录选择器别名
            for code in picks:
                if code not in all_selected_stocks:
                    all_selected_stocks[code] = []
                all_selected_stocks[code].append(alias)
        else:
            logger.info("无符合条件股票")

    # --- 生成排序后的选股结果文件 ---
    if all_selected_stocks:
        logger.info("")
        logger.info("============== 汇总选股结果 ==============")
        logger.info("总共选出 %d 只不重复股票", len(all_selected_stocks))

        # 创建选股结果文件
        output_dir = Path("selection_result")
        create_selection_result(
            all_selected_stocks,  # 直接传递字典
            data,
            stock_names,
            mktcap_data,
            trade_date,
            output_dir,
        )
    else:
        logger.info("")
        logger.info("============== 汇总选股结果 ==============")
        logger.info("所有选择器均未选出股票")


if __name__ == "__main__":
    main()
