from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd

from .models import REQUIRED_BAR_COLUMNS


@dataclass(slots=True)
class QualityReport:
    rows: int
    first_date: str | None
    last_date: str | None
    duplicate_dates: int
    null_ohlc: int
    invalid_ohlc: int
    negative_volume: int
    return_outliers: int
    passed: bool

    def to_dict(self) -> dict:
        return asdict(self)


def normalize_bars(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        raise ValueError("数据源返回空行情")
    missing = [c for c in REQUIRED_BAR_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"标准行情缺少字段：{missing}")

    out = df.loc[:, REQUIRED_BAR_COLUMNS].copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.tz_localize(None)
    for col in ("open", "high", "low", "close", "volume", "turnover", "adj_close"):
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["date"]).sort_values("date")
    out = out.drop_duplicates("date", keep="last").reset_index(drop=True)
    return out


def validate_bars(df: pd.DataFrame, *, strict: bool = True) -> QualityReport:
    out = normalize_bars(df)
    ohlc = out[["open", "high", "low", "close"]]
    invalid = (
        (out["high"] < ohlc[["open", "close"]].max(axis=1))
        | (out["low"] > ohlc[["open", "close"]].min(axis=1))
        | (out["low"] > out["high"])
        | (ohlc <= 0).any(axis=1)
    )
    returns = out["adj_close"].pct_change(fill_method=None)
    report = QualityReport(
        rows=len(out),
        first_date=out["date"].min().date().isoformat() if len(out) else None,
        last_date=out["date"].max().date().isoformat() if len(out) else None,
        duplicate_dates=int(out["date"].duplicated().sum()),
        null_ohlc=int(ohlc.isna().any(axis=1).sum()),
        invalid_ohlc=int(invalid.sum()),
        negative_volume=int((out["volume"] < 0).sum()),
        return_outliers=int((returns.abs() > 0.60).sum()),
        passed=True,
    )
    report.passed = (
        report.rows > 0
        and report.duplicate_dates == 0
        and report.null_ohlc == 0
        and report.invalid_ohlc == 0
        and report.negative_volume == 0
    )
    if strict and not report.passed:
        raise ValueError(f"行情质量校验失败：{report.to_dict()}")
    return report


def compare_sources(primary: pd.DataFrame, secondary: pd.DataFrame, window: int = 60) -> dict:
    """Compare adjusted returns so differently anchored adjustment factors remain comparable."""
    a = normalize_bars(primary)[["date", "adj_close"]].rename(columns={"adj_close": "a"})
    b = normalize_bars(secondary)[["date", "adj_close"]].rename(columns={"adj_close": "b"})
    merged = a.merge(b, on="date", how="inner").tail(window).copy()
    if len(merged) < 5:
        return {"overlap_rows": len(merged), "passed": False, "reason": "重叠交易日不足"}
    merged["ra"] = merged["a"].pct_change(fill_method=None)
    merged["rb"] = merged["b"].pct_change(fill_method=None)
    diff_bps = (merged["ra"] - merged["rb"]).abs() * 10_000
    p95 = float(np.nanpercentile(diff_bps, 95))
    return {
        "overlap_rows": len(merged),
        "median_return_diff_bps": round(float(np.nanmedian(diff_bps)), 4),
        "p95_return_diff_bps": round(p95, 4),
        "passed": p95 <= 25.0,
    }
