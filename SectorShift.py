# backend/j_industry_service.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Union
import re
import pandas as pd
from datetime import datetime

from Selector import compute_kdj
from select_stock import load_data


def _list_codes_from_data_dir(data_dir: Union[str, Path]) -> List[str]:
    """扫描 data_dir 下的行情文件，提取 6 位代码"""
    data_dir = Path(data_dir)
    patterns = ["*.csv", "*.feather", "*.parquet", "*.pkl"]
    files = []
    for pat in patterns:
        files.extend(data_dir.rglob(pat))

    codes: List[str] = []
    for f in files:
        m = re.search(r"(\d{6})", f.stem)
        if m:
            codes.append(m.group(1))
    return sorted(set(codes))


def _load_industry_from_stocklist(
    stocklist_path: Union[str, Path],
    codes: List[str],
) -> pd.DataFrame:
    """从本地 stocklist.csv 读取行业，返回 ['代码','行业']"""
    stocklist_path = Path(stocklist_path)
    if not stocklist_path.exists():
        raise FileNotFoundError(f"stocklist.csv 不存在：{stocklist_path}")

    sl = pd.read_csv(stocklist_path, dtype=str)
    if sl.empty:
        raise ValueError("stocklist.csv 为空。")

    # 提取 6 位代码
    code_col = next((c for c in ["symbol", "ts_code", "code"] if c in sl.columns), None)
    if code_col:
        sl["代码"] = sl[code_col].astype(str).str.extract(r"(\d{6})", expand=False)
    else:
        fallback = None
        for c in sl.columns:
            m = sl[c].astype(str).str.extract(r"(\d{6})", expand=False)
            if m.notna().any():
                fallback = m
                break
        if fallback is None:
            raise ValueError("stocklist.csv 中没有可解析为 6 位证券代码的列。")
        sl["代码"] = fallback

    # 行业列
    industry_col = next((c for c in ["industry", "行业"] if c in sl.columns), None)
    if industry_col is None:
        raise ValueError("stocklist.csv 缺少行业列（需要 'industry' 或 '行业'）。")

    industry_df = (
        sl.loc[sl["代码"].notna(), ["代码", industry_col]]
        .rename(columns={industry_col: "行业"})
        .drop_duplicates(subset=["代码"])
    )
    industry_df = industry_df[industry_df["代码"].isin(codes)]
    industry_df["行业"] = industry_df["行业"].fillna("未知")
    return industry_df


def compute_j_industry_distribution(
    *,
    data_dir: Union[str, Path],
    stocklist_path: Union[str, Path] = "stocklist.csv",
    j_threshold: float = 15.0,
    export_excel_path: Optional[Union[str, Path]] = None,
    trade_date: Optional[Union[str, datetime]] = None,   # ← 新增
) -> Dict:
    """
    计算“指定交易日(或其之前最近一日)的日线J值 < 阈值”的行业分布，返回汇总JSON（无明细）。

    Parameters
    ----------
    trade_date : str | datetime | None
        指定交易日 (YYYYMMDD / YYYY-MM-DD)。为 None 时使用各股票数据中的最近一行。
    """
    # 0) 解析 trade_date（可空）
    trade_dt: Optional[pd.Timestamp] = None
    if trade_date:
        if isinstance(trade_date, datetime):
            trade_dt = pd.Timestamp(trade_date.date())
        else:
            s = str(trade_date).strip()
            # 支持 YYYYMMDD / YYYY-MM-DD
            if re.match(r"^\d{8}$", s):
                trade_dt = pd.to_datetime(s, format="%Y%m%d", errors="coerce")
            else:
                trade_dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(trade_dt):
            raise ValueError(f"无法解析 trade_date: {trade_date}")

    # 1) 扫描代码
    codes = _list_codes_from_data_dir(data_dir)
    if not codes:
        return {
            "meta": {"total_codes": 0, "selected_count": 0, "j_threshold": j_threshold,
                     "trade_date": str(trade_dt.date()) if trade_dt is not None else None},
            "industry_counts": [],
        }

    # 2) 读历史数据
    frames = load_data(Path(data_dir), codes)

    # 3) 计算 指定日（或之前最近一日） 的 J 值
    j_map: Dict[str, float] = {}
    for code, df_code in frames.items():
        if df_code is None or df_code.empty:
            j_map[code] = float("nan")
            continue

        df = df_code.copy()
        if "date" not in df.columns:
            j_map[code] = float("nan")
            continue

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date")

        if trade_dt is not None:
            # 取 <= 指定日 的最后一行；若没有，视为无数据
            df = df[df["date"] <= trade_dt]
            if df.empty:
                j_map[code] = float("nan")
                continue

        kd = compute_kdj(df)
        j_map[code] = float(kd.iloc[-1]["J"]) if (kd is not None and not kd.empty) else float("nan")

    df_j_all = pd.DataFrame({"代码": list(j_map.keys()), "J(日)": list(j_map.values())})

    # 4) 行业
    industry_df = _load_industry_from_stocklist(stocklist_path, codes)

    # 5) 筛选 + 统计
    selected_all = df_j_all[df_j_all["J(日)"] < j_threshold].merge(
        industry_df, on="代码", how="left"
    )
    selected_all["行业"] = selected_all["行业"].fillna("未知")

    industry_counts = (
        selected_all["行业"].value_counts().rename_axis("行业").reset_index(name="股票数")
    )

    # 6) 可选导出
    if export_excel_path:
        export_excel_path = Path(export_excel_path)
        with pd.ExcelWriter(export_excel_path) as writer:
            industry_counts.to_excel(writer, sheet_name="行业分布", index=False)

    return {
        "meta": {
            "total_codes": int(len(codes)),
            "selected_count": int(len(selected_all)),
            "j_threshold": float(j_threshold),
            "trade_date": str(trade_dt.date()) if trade_dt is not None else None,
        },
        "industry_counts": industry_counts.to_dict(orient="records"),
    }
    
if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="计算 J<阈值 股票行业分布")
    parser.add_argument("--data_dir", type=str, default="./data", help="历史数据目录")
    parser.add_argument("--stocklist", type=str, default="stocklist.csv", help="stocklist.csv 路径")
    parser.add_argument("--j_threshold", type=float, default=15.0, help="J(日) 阈值")
    parser.add_argument("--trade_date", type=str, default=None, help="交易日 (YYYYMMDD / YYYY-MM-DD，可选)")
    args = parser.parse_args()

    result = compute_j_industry_distribution(
        data_dir=args.data_dir,
        stocklist_path=args.stocklist,
        j_threshold=args.j_threshold,
        trade_date=args.trade_date,
    )

    # 只打印行业分布
    print(json.dumps(result["industry_counts"], ensure_ascii=False, indent=2))