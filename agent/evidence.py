from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


def _safe(value, digits: int = 4):
    if value is None or pd.isna(value) or np.isinf(value):
        return None
    return round(float(value), digits)


def _return(series: pd.Series, bars: int) -> float | None:
    if len(series) <= bars or not series.iloc[-bars - 1]:
        return None
    return _safe(series.iloc[-1] / series.iloc[-bars - 1] - 1)


def build_technical_evidence(raw_path: Path, pick_date: str) -> dict:
    df = pd.read_csv(raw_path)
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"] <= pd.Timestamp(pick_date)].sort_values("date").dropna(subset=["close"])
    if len(df) < 20:
        raise ValueError(f"结构化证据所需历史不足：{raw_path.name}")

    close = pd.to_numeric(df["close"], errors="coerce")
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    volume = pd.to_numeric(df["volume"], errors="coerce")
    ma = {n: close.rolling(n).mean() for n in (5, 10, 20, 60, 120)}
    true_range = pd.concat(
        [(high - low), (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1
    ).max(axis=1)
    atr14 = true_range.rolling(14).mean()
    high_60 = high.shift(1).rolling(60).max()
    high_250 = high.rolling(250, min_periods=min(120, len(df))).max()
    low_250 = low.rolling(250, min_periods=min(120, len(df))).min()
    range_250 = high_250.iloc[-1] - low_250.iloc[-1]
    position_250 = (close.iloc[-1] - low_250.iloc[-1]) / range_250 if range_250 else None
    rolling_peak = close.cummax()
    drawdown = close / rolling_peak - 1

    weekly = (
        df.set_index("date")
        .resample("W-FRI")
        .agg(open=("open", "first"), high=("high", "max"), low=("low", "min"), close=("close", "last"), volume=("volume", "sum"))
        .dropna(subset=["close"])
    )
    wclose = weekly["close"]
    wma = {n: wclose.rolling(n).mean() for n in (5, 10, 20, 60)}

    return {
        "as_of": df["date"].iloc[-1].date().isoformat(),
        "bars": len(df),
        "last_close": _safe(close.iloc[-1]),
        "returns": {f"d{n}": _return(close, n) for n in (5, 20, 60, 120)},
        "moving_averages": {
            f"ma{n}": _safe(series.iloc[-1]) for n, series in ma.items()
        },
        "distance_to_ma": {
            f"ma{n}": _safe(close.iloc[-1] / series.iloc[-1] - 1) if not pd.isna(series.iloc[-1]) else None
            for n, series in ma.items()
        },
        "volume": {
            "last": _safe(volume.iloc[-1], 2),
            "ratio_5d_to_20d": _safe(volume.tail(5).mean() / volume.tail(20).mean()),
            "last_to_20d": _safe(volume.iloc[-1] / volume.tail(20).mean()),
        },
        "risk_position": {
            "atr14_pct": _safe(atr14.iloc[-1] / close.iloc[-1]),
            "position_in_250d_range": _safe(position_250),
            "drawdown_from_running_peak": _safe(drawdown.iloc[-1]),
            "breakout_above_prior_60d_high": bool(close.iloc[-1] > high_60.iloc[-1]) if not pd.isna(high_60.iloc[-1]) else None,
        },
        "weekly": {
            "bars": len(weekly),
            "return_4w": _return(wclose, 4),
            "return_12w": _return(wclose, 12),
            "moving_averages": {f"ma{n}": _safe(series.iloc[-1]) for n, series in wma.items()},
            "close_above_ma20": bool(wclose.iloc[-1] > wma[20].iloc[-1]) if len(wclose) >= 20 else None,
        },
    }


def load_fundamental_evidence(root: Path, market: str, code: str, pick_date: str) -> dict:
    path = root / "data" / "fundamentals" / market / f"{code}.json"
    if not path.exists():
        return {"status": "missing", "reason": "尚未接入该标的的点时基本面快照"}
    payload = json.loads(path.read_text(encoding="utf-8"))
    available_at = payload.get("available_at") or payload.get("published_at")
    if not available_at:
        return {"status": "stale", "reason": "基本面数据缺少 available_at，禁止用于回测"}
    if pd.Timestamp(available_at) > pd.Timestamp(pick_date) + pd.Timedelta(days=1):
        return {"status": "stale", "reason": "基本面发布时间晚于选股时点，已阻止未来数据泄漏"}
    return {"status": "verified", **payload}


def build_evidence_packet(root: Path, candidate: dict, pick_date: str) -> dict:
    code = str(candidate["code"])
    market = str(candidate.get("market") or "CN").upper()
    raw_path = root / "data" / "raw" / f"{code}.csv"
    return {
        "instrument": {"market": market, "code": code, "pick_date": pick_date},
        "candidate": candidate,
        "technical": build_technical_evidence(raw_path, pick_date),
        "fundamentals": load_fundamental_evidence(root, market, code, pick_date),
    }
