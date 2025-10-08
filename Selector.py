from typing import Dict, List, Optional, Any

from scipy.signal import find_peaks
import numpy as np
import pandas as pd


# --------------------------- 通用指标 --------------------------- #

def compute_kdj(df: pd.DataFrame, n: int = 9) -> pd.DataFrame:
    if df.empty:
        return df.assign(K=np.nan, D=np.nan, J=np.nan)

    low_n = df["low"].rolling(window=n, min_periods=1).min()
    high_n = df["high"].rolling(window=n, min_periods=1).max()
    rsv = (df["close"] - low_n) / (high_n - low_n + 1e-9) * 100

    K = np.zeros_like(rsv, dtype=float)
    D = np.zeros_like(rsv, dtype=float)
    for i in range(len(df)):
        if i == 0:
            K[i] = D[i] = 50.0
        else:
            K[i] = 2 / 3 * K[i - 1] + 1 / 3 * rsv.iloc[i]
            D[i] = 2 / 3 * D[i - 1] + 1 / 3 * K[i]
    J = 3 * K - 2 * D
    return df.assign(K=K, D=D, J=J)


def compute_bbi(df: pd.DataFrame) -> pd.Series:
    ma3 = df["close"].rolling(3).mean()
    ma6 = df["close"].rolling(6).mean()
    ma12 = df["close"].rolling(12).mean()
    ma24 = df["close"].rolling(24).mean()
    return (ma3 + ma6 + ma12 + ma24) / 4


def compute_rsv(
        df: pd.DataFrame,
        n: int,
) -> pd.Series:
    """
    按公式：RSV(N) = 100 × (C - LLV(L,N)) ÷ (HHV(C,N) - LLV(L,N))
    - C 用收盘价最高值 (HHV of close)
    - L 用最低价最低值 (LLV of low)
    """
    low_n = df["low"].rolling(window=n, min_periods=1).min()
    high_close_n = df["close"].rolling(window=n, min_periods=1).max()
    rsv = (df["close"] - low_n) / (high_close_n - low_n + 1e-9) * 100.0
    return rsv


def compute_dif(df: pd.DataFrame, fast: int = 12, slow: int = 26) -> pd.Series:
    """计算 MACD 指标中的 DIF (EMA fast - EMA slow)。"""
    ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
    ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
    return ema_fast - ema_slow


def bbi_deriv_uptrend(
        bbi: pd.Series,
        *,
        min_window: int,
        max_window: int | None = None,
        q_threshold: float = 0.0,
) -> bool:
    """
    判断 BBI 是否“整体上升”。

    令最新交易日为 T，在区间 [T-w+1, T]（w 自适应，w ≥ min_window 且 ≤ max_window）
    内，先将 BBI 归一化：BBI_norm(t) = BBI(t) / BBI(T-w+1)。

    再计算一阶差分 Δ(t) = BBI_norm(t) - BBI_norm(t-1)。  
    若 Δ(t) 的前 q_threshold 分位数 ≥ 0，则认为该窗口通过；只要存在
    **最长** 满足条件的窗口即可返回 True。q_threshold=0 时退化为
    “全程单调不降”（旧版行为）。

    Parameters
    ----------
    bbi : pd.Series
        BBI 序列（最新值在最后一位）。
    min_window : int
        检测窗口的最小长度。
    max_window : int | None
        检测窗口的最大长度；None 表示不设上限。
    q_threshold : float, default 0.0
        允许一阶差分为负的比例（0 ≤ q_threshold ≤ 1）。
    """
    if not 0.0 <= q_threshold <= 1.0:
        raise ValueError("q_threshold 必须位于 [0, 1] 区间内")

    bbi = bbi.dropna()
    if len(bbi) < min_window:
        return False

    longest = min(len(bbi), max_window or len(bbi))

    # 自最长窗口向下搜索，找到任一满足条件的区间即通过
    for w in range(longest, min_window - 1, -1):
        seg = bbi.iloc[-w:]  # 区间 [T-w+1, T]
        norm = seg / seg.iloc[0]  # 归一化
        diffs = np.diff(norm.values)  # 一阶差分
        if np.quantile(diffs, q_threshold) >= 0:
            return True
    return False


def _find_peaks(
        df: pd.DataFrame,
        *,
        column: str = "high",
        distance: Optional[int] = None,
        prominence: Optional[float] = None,
        height: Optional[float] = None,
        width: Optional[float] = None,
        rel_height: float = 0.5,
        **kwargs: Any,
) -> pd.DataFrame:
    if column not in df.columns:
        raise KeyError(f"'{column}' not found in DataFrame columns: {list(df.columns)}")

    y = df[column].to_numpy()

    indices, props = find_peaks(
        y,
        distance=distance,
        prominence=prominence,
        height=height,
        width=width,
        rel_height=rel_height,
        **kwargs,
    )

    peaks_df = df.iloc[indices].copy()
    peaks_df["is_peak"] = True

    # Flatten SciPy arrays into columns (only those with same length as indices)
    for key, arr in props.items():
        if isinstance(arr, (list, np.ndarray)) and len(arr) == len(indices):
            peaks_df[f"peak_{key}"] = arr

    return peaks_df


def last_valid_ma_cross_up(
        close: pd.Series,
        ma: pd.Series,
        lookback_n: int | None = None,
) -> Optional[int]:
    """
    查找“有效上穿 MA”的最后一个交易日 T（close[T-1] < ma[T-1] 且 close[T] ≥ ma[T]）。
    - 返回的是 **整数位置**（iloc 用）。
    - lookback_n: 仅在最近 N 根内查找；None 则全历史。
    """
    n = len(close)
    start = 1  # 至少要从 1 起，因为要看 T-1
    if lookback_n is not None:
        start = max(start, n - lookback_n)

    # 自后向前找最后一次有效上穿
    for i in range(n - 1, start - 1, -1):
        if i - 1 < 0:
            continue
        c_prev, c_now = close.iloc[i - 1], close.iloc[i]
        m_prev, m_now = ma.iloc[i - 1], ma.iloc[i]
        if pd.notna(c_prev) and pd.notna(c_now) and pd.notna(m_prev) and pd.notna(m_now):
            if c_prev < m_prev and c_now >= m_now:
                return i
    return None


def compute_zx_lines(
        df: pd.DataFrame,
        m1: int = 14, m2: int = 28, m3: int = 57, m4: int = 114
) -> tuple[pd.Series, pd.Series]:
    """返回 (ZXDQ, ZXDKX)
    ZXDQ = EMA(EMA(C,10),10)
    ZXDKX = (MA(C,14)+MA(C,28)+MA(C,57)+MA(C,114))/4
    """
    close = df["close"].astype(float)
    zxdq = close.ewm(span=10, adjust=False).mean().ewm(span=10, adjust=False).mean()

    ma1 = close.rolling(window=m1, min_periods=m1).mean()
    ma2 = close.rolling(window=m2, min_periods=m2).mean()
    ma3 = close.rolling(window=m3, min_periods=m3).mean()
    ma4 = close.rolling(window=m4, min_periods=m4).mean()
    zxdkx = (ma1 + ma2 + ma3 + ma4) / 4.0
    return zxdq, zxdkx


# 计算收盘价的10日指数移动平均的10日指数移动平均
def compute_ema10_of_ema10_close(df: pd.DataFrame) -> pd.Series:
    ema10 = df['close'].ewm(span=10, adjust=False).mean()
    ema10_of_ema10 = ema10.ewm(span=10, adjust=False).mean()
    return ema10_of_ema10


# 知行多空线:(收盘价的M1日简单移动平均+收盘价的M2日简单移动平均+收盘价的M3日简单移动平均+收盘价的M4日简单移动平均)/4
def compute_ma_average(df: pd.DataFrame, m1: int, m2: int, m3: int, m4: int) -> pd.Series:
    """
    计算四个不同周期简单移动平均的算术平均值。

    参数:
        df: 包含 'close' 列的 DataFrame
        m1, m2, m3, m4: 四个移动平均的周期

    返回:
        pd.Series: 计算结果序列
    """
    ma1 = df['close'].rolling(window=m1).mean()
    ma2 = df['close'].rolling(window=m2).mean()
    ma3 = df['close'].rolling(window=m3).mean()
    ma4 = df['close'].rolling(window=m4).mean()

    # 计算四个MA的平均值
    result = (ma1 + ma2 + ma3 + ma4) / 4

    return result


def passes_day_constraints_today(df: pd.DataFrame, pct_limit: float = 0.02, amp_limit: float = 0.07) -> bool:
    """
    所有战法的统一当日过滤：
    1) 当前交易日相较于前一日涨跌幅 < pct_limit（绝对值）
    2) 当日振幅（High-Low 相对 Low） < amp_limit
    """
    if len(df) < 2:
        return False
    last = df.iloc[-1]
    prev = df.iloc[-2]
    close_today = float(last["close"])
    close_yest = float(prev["close"])
    high_today = float(last["high"])
    low_today = float(last["low"])
    if close_yest <= 0 or low_today <= 0:
        return False
    pct_chg = abs(close_today / close_yest - 1.0)
    amplitude = (high_today - low_today) / low_today
    return (pct_chg < pct_limit) and (amplitude < amp_limit)


def zx_condition_at_positions(
        df: pd.DataFrame,
        *,
        require_close_gt_long: bool = True,
        require_short_gt_long: bool = True,
        pos: int | None = None,
) -> bool:
    """
    在指定位置 pos（iloc 位置；None 表示当日）检查知行条件：
      - 收盘 > 长期线（可选）
      - 短期线 > 长期线（可选）
    注：长期线需满样本；若为 NaN 直接返回 False。
    """
    if df.empty:
        return False
    zxdq, zxdkx = compute_zx_lines(df)
    if pos is None:
        pos = len(df) - 1

    if pos < 0 or pos >= len(df):
        return False

    s = float(zxdq.iloc[pos])
    l = float(zxdkx.iloc[pos]) if pd.notna(zxdkx.iloc[pos]) else float("nan")
    c = float(df["close"].iloc[pos])

    if not np.isfinite(l) or not np.isfinite(s):
        return False

    if require_close_gt_long and not (c > l):
        return False
    if require_short_gt_long and not (s > l):
        return False
    return True


def zx_condition_between_two_line(
        df: pd.DataFrame,
        *,
        require_close_gt_long: bool = True,
        require_close_lt_short: bool = True,
        pos: int | None = None,
) -> bool:
    """
        在指定位置 pos（iloc 位置；None 表示当日）检查知行条件：
          - 收盘 > 长期线（可选）
          - 收盘 < 短期期线（可选）
        注：长期线需满样本；若为 NaN 直接返回 False。
        """
    if df.empty:
        return False
    zxdq, zxdkx = compute_zx_lines(df)
    if pos is None:
        pos = len(df) - 1

    if pos < 0 or pos >= len(df):
        return False

    s = float(zxdq.iloc[pos])
    l = float(zxdkx.iloc[pos]) if pd.notna(zxdkx.iloc[pos]) else float("nan")
    c = float(df["close"].iloc[pos])

    if not np.isfinite(l) or not np.isfinite(s):
        return False

    if require_close_gt_long and (c < l):
        return False
    if require_close_lt_short and (c > s):
        return False
    return True


# --------------------------- Selector 类 --------------------------- #
# 少妇战法
class BBIKDJSelector:
    """
    自适应 *BBI(导数)* + *KDJ* 选股器
        • BBI: 允许 bbi_q_threshold 比例的回撤
        • KDJ: J < threshold ；或位于历史 J 的 j_q_threshold 分位及以下
        • MACD: DIF > 0
        • 收盘价波动幅度 ≤ price_range_pct
    """

    def __init__(
            self,
            j_threshold: float = -5,
            bbi_min_window: int = 90,
            max_window: int = 90,
            price_range_pct: float = 100.0,
            bbi_q_threshold: float = 0.05,
            j_q_threshold: float = 0.10,
    ) -> None:
        self.j_threshold = j_threshold
        self.bbi_min_window = bbi_min_window
        self.max_window = max_window
        self.price_range_pct = price_range_pct
        self.bbi_q_threshold = bbi_q_threshold  # ← 原 q_threshold
        self.j_q_threshold = j_q_threshold  # ← 新增

    # ---------- 单支股票过滤 ---------- #
    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        hist = hist.copy()
        # hist["BBI"] = compute_bbi(hist)

        if not passes_day_constraints_today(hist):
            return False

        # 0. 收盘价波动幅度约束（最近 max_window 根 K 线）
        # win = hist.tail(self.max_window)
        # high, low = win["close"].max(), win["close"].min()
        # if low <= 0 or (high / low - 1) > self.price_range_pct:
        #     return False

        # 1. BBI 上升（允许部分回撤）
        # if not bbi_deriv_uptrend(
        #     hist["BBI"],
        #     min_window=self.bbi_min_window,
        #     max_window=self.max_window,
        #     q_threshold=self.bbi_q_threshold,
        # ):
        #     return False

        # 2. KDJ 过滤 —— 双重条件
        kdj = compute_kdj(hist)
        j_today = float(kdj.iloc[-1]["J"])

        # 最近 max_window 根 K 线的 J 分位
        j_window = kdj["J"].tail(self.max_window).dropna()
        if j_window.empty:
            return False
        j_quantile = float(j_window.quantile(self.j_q_threshold))

        if not (j_today < self.j_threshold or j_today <= j_quantile):
            return False

        # —— 2.5 知行多空线条件（黄线）
        # hist["MaAvg"] = compute_ma_average(hist, 14, 28, 57, 114)

        # 当前必须在 知行多空线条件 上方（保持原条件）
        # if hist["close"].iloc[-1] < hist["MaAvg"].iloc[-1]:
        #     return False

        # 寻找最近一次“有效上穿 MaAvg”的 T（使用 max_window 作为回看长度，避免过旧）
        # t_pos = last_valid_ma_cross_up(hist["close"], hist["MaAvg"], lookback_n=self.max_window)
        # if t_pos is None:
        #     return False

        # 3. MACD：DIF > 0
        # hist["DIF"] = compute_dif(hist)
        # if hist["DIF"].iloc[-1] <= 0:
        #     return False

        # 4. 当日：收盘>长期线 且 短期线>长期线
        if not zx_condition_at_positions(hist, require_close_gt_long=True, require_short_gt_long=True, pos=None):
            return False

        return True

    # ---------- 多股票批量 ---------- #
    def select(
            self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]
    ) -> List[str]:
        picks: List[str] = []
        for code, df in data.items():
            hist = df[df["date"] <= date]
            if hist.empty:
                continue
            # 额外预留 20 根 K 线缓冲
            hist = hist.tail(self.max_window + 20)
            if self._passes_filters(hist):
                picks.append(code)
        return picks


# --------------------------- TwoLineSelector 类 --------------------------- #
# 双线战法
class TwoLineSelector:
    """

    """

    def __init__(
            self,
            j_threshold: float = -5,
            max_window: int = 90,
            j_q_threshold: float = 0.10,
    ) -> None:
        self.j_threshold = j_threshold
        self.max_window = max_window
        self.j_q_threshold = j_q_threshold  # ← 新增

    # ---------- 单支股票过滤 ---------- #
    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        hist = hist.copy()
        # hist["BBI"] = compute_bbi(hist)

        if not passes_day_constraints_today(hist):
            return False

        kdj = compute_kdj(hist)
        j_today = float(kdj.iloc[-1]["J"])

        # 最近 max_window 根 K 线的 J 分位
        j_window = kdj["J"].tail(self.max_window).dropna()
        if j_window.empty:
            return False
        j_quantile = float(j_window.quantile(self.j_q_threshold))

        if not (j_today < self.j_threshold or j_today <= j_quantile):
            return False

        # 当日：收盘>长期线 且 短期线>长期线
        if not zx_condition_between_two_line(hist, require_close_gt_long=True, require_close_lt_short=True, pos=None):
            return False

        return True

    # ---------- 多股票批量 ---------- #
    def select(
            self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]
    ) -> List[str]:
        picks: List[str] = []
        for code, df in data.items():
            hist = df[df["date"] <= date]
            if hist.empty:
                continue
            # 额外预留 20 根 K 线缓冲
            hist = hist.tail(self.max_window + 20)
            if self._passes_filters(hist):
                picks.append(code)
        return picks


#
# B2 战法
class B2Selector:
    """B2 战法


    """

    def __init__(
            self,
            j_threshold: float = 55,
            up_volume_ratio: float = 0.04,
            max_window: int = 90,
            b1_j_threshold: float = 20,
            j_q_threshold: float = 0.10,
    ) -> None:
        self.j_threshold = j_threshold
        self.up_volume_ratio = up_volume_ratio
        self.max_window = max_window
        self.b1_j_threshold = b1_j_threshold
        self.j_q_threshold = j_q_threshold

    # ---------- 单支股票过滤 ---------- #
    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        hist = hist.copy()
        # 当日：收盘>长期线 且 短期线>长期线
        if not zx_condition_at_positions(hist, require_close_gt_long=True, require_short_gt_long=True, pos=None):
            return False

        # 当日涨幅大于4%
        if len(hist) < 2:
            return False
        last = hist.iloc[-1]
        prev = hist.iloc[-2]
        close_today = float(last["close"])
        close_yest = float(prev["close"])
        if close_yest <= 0:
            return False
        pct_chg = (close_today / close_yest - 1.0)
        if pct_chg < self.up_volume_ratio:
            return False

        # 成交量比前一日放大
        if len(hist) < 2:
            return False
        last = hist.iloc[-1]
        prev = hist.iloc[-2]
        vol_today = float(last["volume"])
        vol_yest = float(prev["volume"])
        if vol_yest <= 0:
            return False
        vol_ratio = (vol_today / vol_yest)
        if vol_ratio < 1.0:
            return False

        # j 小于 55
        kdj = compute_kdj(hist)
        j_today = float(kdj.iloc[-1]["J"])

        if not (j_today <= self.j_threshold):
            return False

        # 昨日收盘价满足少妇战法
        if len(hist) < 2:
            return False
        if not passes_day_constraints_today(hist.iloc[:-1]):
            return False
        lastdayHist = hist.iloc[:-1]
        if hist.empty:
            return False
        # hist["BBI"] = compute_bbi(hist)
        kdj = compute_kdj(lastdayHist)
        j_today = float(kdj.iloc[-1]["J"])

        # 最近 max_window 根 K 线的 J 分位
        j_window = kdj["J"].tail(self.max_window).dropna()
        if j_window.empty:
            return False
        j_quantile = float(j_window.quantile(self.j_q_threshold))

        if not (j_today < self.b1_j_threshold or j_today <= j_quantile):
            return False


        return True

    # ---------- 多股票批量 ---------- #
    def select(
            self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]
    ) -> List[str]:
        picks: List[str] = []
        for code, df in data.items():
            hist = df[df["date"] <= date]
            if hist.empty:
                continue
            # 额外预留 20 根 K 线缓冲
            hist = hist.tail(self.max_window + 20)
            if self._passes_filters(hist):
                picks.append(code)
        return picks

# --------------------------- SuperB1Selector 类 --------------------------- #
# 超级 B1 战法
class SuperB1Selector:
    """SuperB1 选股器

    过滤逻辑概览
    ----------------
    1. **历史匹配 (t_m)** — 在 *lookback_n* 个交易日窗口内，至少存在一日
       满足 :class:`BBIKDJSelector`。

    2. **盘整区间** — 区间 ``[t_m, date-1]`` 收盘价波动率不超过 ``close_vol_pct``。

    3. **当日下跌** — ``(close_{date-1} - close_date) / close_{date-1}``
       ≥ ``price_drop_pct``。

    4. **J 值极低** — ``J < j_threshold`` *或* 位于历史 ``j_q_threshold`` 分位。
    """

    # ---------------------------------------------------------------------
    # 构造函数
    # ---------------------------------------------------------------------
    def __init__(
            self,
            *,
            lookback_n: int = 60,
            close_vol_pct: float = 0.05,
            price_drop_pct: float = 0.03,
            j_threshold: float = -5,
            j_q_threshold: float = 0.10,
            # ↓↓↓ 新增：嵌套 BBIKDJSelector 配置
            B1_params: Optional[Dict[str, Any]] = None
    ) -> None:
        # ---------- 参数合法性检查 ----------
        if lookback_n < 2:
            raise ValueError("lookback_n 应 ≥ 2")
        if not (0 < close_vol_pct < 1):
            raise ValueError("close_vol_pct 应位于 (0, 1) 区间")
        if not (0 < price_drop_pct < 1):
            raise ValueError("price_drop_pct 应位于 (0, 1) 区间")
        if not (0 <= j_q_threshold <= 1):
            raise ValueError("j_q_threshold 应位于 [0, 1] 区间")
        if B1_params is None:
            raise ValueError("bbi_params没有给出")

        # ---------- 基本参数 ----------
        self.lookback_n = lookback_n
        self.close_vol_pct = close_vol_pct
        self.price_drop_pct = price_drop_pct
        self.j_threshold = j_threshold
        self.j_q_threshold = j_q_threshold

        # ---------- 内部 BBIKDJSelector ----------
        self.bbi_selector = BBIKDJSelector(**(B1_params or {}))

        # 为保证给 BBIKDJSelector 提供足够历史，预留额外缓冲
        self._extra_for_bbi = self.bbi_selector.max_window + 20

    # 单支股票过滤核心
    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        if len(hist) < 2:
            return False

        # —— 新增：所有战法统一当日过滤
        if not passes_day_constraints_today(hist):
            return False

        # ---------- Step-0: 数据量判断 ----------
        if len(hist) < self.lookback_n + self._extra_for_bbi:
            return False

        # ---------- Step-1: 搜索满足 BBIKDJ 的 t_m ----------
        lb_hist = hist.tail(self.lookback_n + 1)  # +1 以排除自身
        tm_idx: int | None = None
        for idx in lb_hist.index[:-1]:
            if self.bbi_selector._passes_filters(hist.loc[:idx]):
                tm_idx = idx
                stable_seg = hist.loc[tm_idx: hist.index[-2], "close"]
                if len(stable_seg) < 3:
                    tm_idx = None
                    break
                high, low = stable_seg.max(), stable_seg.min()
                if low <= 0 or (high / low - 1) > self.close_vol_pct:
                    tm_idx = None
                    continue
                else:
                    break
        if tm_idx is None:
            return False

        # —— 新增：在 t_m 当日检查【收盘>长期线 且 短期线>长期线】
        tm_pos = hist.index.get_loc(tm_idx)
        if not zx_condition_at_positions(hist, require_close_gt_long=True, require_short_gt_long=True, pos=tm_pos):
            return False

        # ---------- Step-3: 当日相对前一日跌幅 ----------
        close_today, close_prev = hist["close"].iloc[-1], hist["close"].iloc[-2]
        if close_prev <= 0 or (close_prev - close_today) / close_prev < self.price_drop_pct:
            return False

        # ---------- Step-4: J 值极低 ----------
        kdj = compute_kdj(hist)
        j_today = float(kdj["J"].iloc[-1])
        j_window = kdj["J"].iloc[-self.lookback_n:].dropna()
        j_q_val = float(j_window.quantile(self.j_q_threshold)) if not j_window.empty else np.nan
        if not (j_today < self.j_threshold or j_today <= j_q_val):
            return False

        # —— 当日仅要求【短期线>长期线】
        if not zx_condition_at_positions(hist, require_close_gt_long=False, require_short_gt_long=True, pos=None):
            return False

        return True

    # 批量选股接口
    def select(self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]) -> List[str]:
        picks: List[str] = []
        min_len = self.lookback_n + self._extra_for_bbi

        for code, df in data.items():
            hist = df[df["date"] <= date].tail(min_len)
            if len(hist) < min_len:
                continue
            if self._passes_filters(hist):
                picks.append(code)

        return picks


class PeakKDJSelector:
    """
    Peaks + KDJ 选股器    
    """

    def __init__(
            self,
            j_threshold: float = -5,
            max_window: int = 90,
            fluc_threshold: float = 0.03,
            gap_threshold: float = 0.02,
            j_q_threshold: float = 0.10,
    ) -> None:
        self.j_threshold = j_threshold
        self.max_window = max_window
        self.fluc_threshold = fluc_threshold  # 当日↔peak_(t-n) 波动率上限
        self.gap_threshold = gap_threshold  # oc_prev 必须高于区间最低收盘价的比例
        self.j_q_threshold = j_q_threshold

    # ---------- 单支股票过滤 ---------- #
    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        if hist.empty:
            return False

        if not passes_day_constraints_today(hist):
            return False

        hist = hist.copy().sort_values("date")
        hist["oc_max"] = hist[["open", "close"]].max(axis=1)

        # 1. 提取 peaks
        peaks_df = _find_peaks(
            hist,
            column="oc_max",
            distance=6,
            prominence=0.5,
        )

        # 至少两个峰      
        date_today = hist.iloc[-1]["date"]
        peaks_df = peaks_df[peaks_df["date"] < date_today]
        if len(peaks_df) < 2:
            return False

        peak_t = peaks_df.iloc[-1]  # 最新一个峰
        peaks_list = peaks_df.reset_index(drop=True)
        oc_t = peak_t.oc_max
        total_peaks = len(peaks_list)

        # 2. 回溯寻找 peak_(t-n)
        target_peak = None
        for idx in range(total_peaks - 2, -1, -1):
            peak_prev = peaks_list.loc[idx]
            oc_prev = peak_prev.oc_max
            if oc_t <= oc_prev:  # 要求 peak_t > peak_(t-n)
                continue

            # 只有当“总峰数 ≥ 3”时才检查区间内其他峰 oc_max
            if total_peaks >= 3 and idx < total_peaks - 2:
                inter_oc = peaks_list.loc[idx + 1: total_peaks - 2, "oc_max"]
                if not (inter_oc < oc_prev).all():
                    continue

            # 新增： oc_prev 高于区间最低收盘价 gap_threshold
            date_prev = peak_prev.date
            mask = (hist["date"] > date_prev) & (hist["date"] < peak_t.date)
            min_close = hist.loc[mask, "close"].min()
            if pd.isna(min_close):
                continue  # 区间无数据
            if oc_prev <= min_close * (1 + self.gap_threshold):
                continue

            target_peak = peak_prev

            break

        if target_peak is None:
            return False

        # 3. 当日收盘价波动率
        close_today = hist.iloc[-1]["close"]
        fluc_pct = abs(close_today - target_peak.close) / target_peak.close
        if fluc_pct > self.fluc_threshold:
            return False

        # 4. KDJ 过滤
        kdj = compute_kdj(hist)
        j_today = float(kdj.iloc[-1]["J"])
        j_window = kdj["J"].tail(self.max_window).dropna()
        if j_window.empty:
            return False
        j_quantile = float(j_window.quantile(self.j_q_threshold))
        if not (j_today < self.j_threshold or j_today <= j_quantile):
            return False

        if not zx_condition_at_positions(hist, require_close_gt_long=True, require_short_gt_long=True, pos=None):
            return False

        return True

    # ---------- 多股票批量 ---------- #
    def select(
            self,
            date: pd.Timestamp,
            data: Dict[str, pd.DataFrame],
    ) -> List[str]:
        picks: List[str] = []
        for code, df in data.items():
            hist = df[df["date"] <= date]
            if hist.empty:
                continue
            hist = hist.tail(self.max_window + 20)  # 额外缓冲
            if self._passes_filters(hist):
                picks.append(code)
        return picks


# --------------------------- BBIShortLongSelector 类 --------------------------- #
# 补票战法
class BBIShortLongSelector:
    """
    BBI 上升 + 短/长期 RSV 条件 + DIF > 0 选股器
    """

    def __init__(
            self,
            n_short: int = 3,
            n_long: int = 21,
            m: int = 3,
            bbi_min_window: int = 90,
            max_window: int = 150,
            bbi_q_threshold: float = 0.05,
            upper_rsv_threshold: float = 75,
            lower_rsv_threshold: float = 25
    ) -> None:
        if m < 2:
            raise ValueError("m 必须 ≥ 2")
        self.n_short = n_short
        self.n_long = n_long
        self.m = m
        self.bbi_min_window = bbi_min_window
        self.max_window = max_window
        self.bbi_q_threshold = bbi_q_threshold
        self.upper_rsv_threshold = upper_rsv_threshold
        self.lower_rsv_threshold = lower_rsv_threshold

    # ---------- 单支股票过滤 ---------- #
    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        hist = hist.copy()
        hist["BBI"] = compute_bbi(hist)

        if not passes_day_constraints_today(hist):
            return False

            # 1. BBI 上升（允许部分回撤）
        if not bbi_deriv_uptrend(
                hist["BBI"],
                min_window=self.bbi_min_window,
                max_window=self.max_window,
                q_threshold=self.bbi_q_threshold,
        ):
            return False

        # 2. 计算短/长期 RSV -----------------
        hist["RSV_short"] = compute_rsv(hist, self.n_short)
        hist["RSV_long"] = compute_rsv(hist, self.n_long)

        if len(hist) < self.m:
            return False  # 数据不足

        win = hist.iloc[-self.m:]  # 最近 m 天
        long_ok = (win["RSV_long"] >= self.upper_rsv_threshold).all()  # 长期 RSV 全 ≥ upper_rsv_threshold

        short_series = win["RSV_short"]

        # 条件：从最近 m 天的第一天起，存在某天 i 满足 RSV_short[i] >= upper，
        # 且在该天之后（j > i）存在某天 j 满足 RSV_short[j] < lower
        mask_upper = short_series >= self.upper_rsv_threshold
        mask_lower = short_series < self.lower_rsv_threshold

        has_upper_then_lower = False
        if mask_upper.any():
            upper_indices = np.where(mask_upper.to_numpy())[0]
            for i in upper_indices:
                # 只检查 i 之后的日子
                if i + 1 < len(short_series) and mask_lower.iloc[i + 1:].any():
                    has_upper_then_lower = True
                    break

        end_ok = short_series.iloc[-1] >= self.upper_rsv_threshold

        if not (long_ok and has_upper_then_lower and end_ok):
            return False

        # 3. MACD：DIF > 0 -------------------
        hist["DIF"] = compute_dif(hist)
        if hist["DIF"].iloc[-1] <= 0:
            return False

        # 4. 新增：知行情形
        if not zx_condition_at_positions(hist, require_close_gt_long=True, require_short_gt_long=True, pos=None):
            return False

        return True

    # ---------- 多股票批量 ---------- #
    def select(
            self,
            date: pd.Timestamp,
            data: Dict[str, pd.DataFrame],
    ) -> List[str]:
        picks: List[str] = []
        for code, df in data.items():
            hist = df[df["date"] <= date]
            if hist.empty:
                continue
            # 预留足够长度：RSV 计算窗口 + BBI 检测窗口 + m
            need_len = (
                    max(self.n_short, self.n_long)
                    + self.bbi_min_window
                    + self.m
            )
            hist = hist.tail(max(need_len, self.max_window))
            if self._passes_filters(hist):
                picks.append(code)
        return picks


class MA60CrossVolumeWaveSelector:
    """
    条件：
    1) 当日 J 绝对低或相对低（J < j_threshold 或 J ≤ 近 max_window 根 J 的 j_q_threshold 分位）
    2) 最近 lookback_n 内，存在一次“有效上穿 MA60”（t-1 收盘 < MA60, t 收盘 ≥ MA60）；
       且从该上穿日 T 到今天的“上涨波段”日均成交量 ≥ 上穿前等长窗口的日均成交量 * vol_multiple
       —— 上涨波段定义为 [T, today] 间的所有交易日（不做趋势单调性强约束，稳健且可复现）
    3) 近 ma60_slope_days（默认 5）个交易日的 MA60 回归斜率 > 0
    """

    def __init__(
            self,
            *,
            lookback_n: int = 60,
            vol_multiple: float = 1.5,
            j_threshold: float = -5.0,
            j_q_threshold: float = 0.10,
            ma60_slope_days: int = 5,
            max_window: int = 120,  # 用于计算 J 分位
    ) -> None:
        if lookback_n < 2:
            raise ValueError("lookback_n 应 ≥ 2")
        if not (0.0 <= j_q_threshold <= 1.0):
            raise ValueError("j_q_threshold 应位于 [0,1]")
        if ma60_slope_days < 2:
            raise ValueError("ma60_slope_days 应 ≥ 2")
        self.lookback_n = lookback_n
        self.vol_multiple = vol_multiple
        self.j_threshold = j_threshold
        self.j_q_threshold = j_q_threshold
        self.ma60_slope_days = ma60_slope_days
        self.max_window = max_window

    @staticmethod
    def _ma_slope_positive(series: pd.Series, days: int) -> bool:
        """对最近 days 个点做一阶线性回归，斜率 > 0 判为正"""
        seg = series.dropna().tail(days)
        if len(seg) < days:
            return False
        x = np.arange(len(seg), dtype=float)
        # 线性回归（最小二乘）：斜率 k
        k, _ = np.polyfit(x, seg.values.astype(float), 1)
        return bool(k > 0)

    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        """
        hist：按日期升序，最后一行是目标交易日
        需包含列：date, open, high, low, close, volume
        """
        if hist.empty:
            return False

        hist = hist.copy().sort_values("date")
        # 至少要有 60 日用于 MA60，再加 lookback/slope 的缓冲
        min_len = max(60 + self.lookback_n + self.ma60_slope_days, self.max_window + 5)
        if len(hist) < min_len:
            return False

        if not passes_day_constraints_today(hist):
            return False

        # --- 计算指标 ---
        kdj = compute_kdj(hist)
        j_today = float(kdj["J"].iloc[-1])
        j_window = kdj["J"].tail(self.max_window).dropna()
        if j_window.empty:
            return False
        j_q_val = float(j_window.quantile(self.j_q_threshold))

        # 1) 当日 J 绝对低或相对低
        if not (j_today < self.j_threshold or j_today <= j_q_val):
            return False

        # 2) MA60 及有效上穿（使用通用函数）
        hist["MA60"] = hist["close"].rolling(window=60, min_periods=1).mean()
        if hist["close"].iloc[-1] < hist["MA60"].iloc[-1]:
            return False

        t_pos = last_valid_ma_cross_up(hist["close"], hist["MA60"], lookback_n=self.lookback_n)
        if t_pos is None:
            return False

        # === [T, today] 内以 High 最大值的交易日为 Tmax ===
        seg_T_to_today = hist.iloc[t_pos:]
        if seg_T_to_today.empty:
            return False

        # 若并列最高，默认取“第一次”出现的那天；要“最后一次”可改见注释
        tmax_label = seg_T_to_today["high"].idxmax()
        int_pos_T = t_pos
        int_pos_Tmax = hist.index.get_loc(tmax_label)

        if int_pos_Tmax < int_pos_T:
            return False

        # 上涨波段 [T, Tmax]（含端点）
        wave = hist.iloc[int_pos_T: int_pos_Tmax + 1]
        wave_len = len(wave)
        if wave_len < 3:
            return False

        # 等长前置窗口 [T - wave_len, T-1]
        pre_start_pos = max(0, int_pos_T - min(wave_len, 10))
        pre = hist.iloc[pre_start_pos:int_pos_T]
        if len(pre) < max(5, min(10, wave_len)):
            return False

        # 成交量均值对比
        wave_avg_vol = float(wave["volume"].replace(0, np.nan).dropna().mean())
        pre_avg_vol = float(pre["volume"].replace(0, np.nan).dropna().mean())
        if not (np.isfinite(wave_avg_vol) and np.isfinite(pre_avg_vol) and pre_avg_vol > 0):
            return False

        if wave_avg_vol < self.vol_multiple * pre_avg_vol:
            return False

        # 3) MA60 斜率 > 0（保留原实现）
        if not self._ma_slope_positive(hist["MA60"], self.ma60_slope_days):
            return False

        if not zx_condition_at_positions(hist, require_close_gt_long=True, require_short_gt_long=True, pos=None):
            return False

        return True

    def select(self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]) -> List[str]:
        picks: List[str] = []
        # 给足 60 日均线与量能比较的历史长度
        need_len = max(60 + self.lookback_n + self.ma60_slope_days, self.max_window + 20)
        for code, df in data.items():
            hist = df[df["date"] <= date].tail(need_len)
            if len(hist) < need_len:
                continue
            if self._passes_filters(hist):
                picks.append(code)
        return picks
