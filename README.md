# Z哥战法的 Python 实现（更新版）

> **更新时间：2025-09-10** –
>
> 1. 重构 `fetch_kline.py`：仅使用 **Tushare 日线（前复权 qfq）**、从 **`stocklist.csv`** 读取股票池、支持排除板块（创业板/科创板/北交所），抓取为**全量覆盖保存**；
> 2. `Selector.py`：删除 **TePu 战法**，新增/强化统一日内过滤与“知行短/长线”约束；
> 3. `configs.json` 已同步新参数与默认值；
> 4. 新增 **“统一当日过滤&知行约束”** 说明章节。

---

## 目录

* [项目简介](#项目简介)
* [快速上手](#快速上手)

  * [环境与依赖](#环境与依赖)
  * [准备 Tushare Token](#准备-tushare-token)
  * [准备 stocklist.csv](#准备-stocklistcsv)
  * [下载历史 K 线（qfq，日线）](#下载历史-k-线qfq日线)
  * [运行选股](#运行选股)
* [参数说明](#参数说明)

  * [`fetch_kline.py`](#fetch_klinepy)
  * [`select_stock.py`](#select_stockpy)
* [统一当日过滤 & 知行约束](#统一当日过滤--知行约束)
* [内置策略（Selector）](#内置策略selector)

  * [1. BBIKDJSelector（少妇战法）](#1-bbikdjselector少妇战法)
  * [2. SuperB1Selector（SuperB1战法）](#2-superb1selectorsuperb1战法)
  * [3. BBIShortLongSelector（补票战法）](#3-bbishortlongselector补票战法)
  * [4. PeakKDJSelector（填坑战法）](#4-peakkdjselector填坑战法)
  * [5. MA60CrossVolumeWaveSelector（上穿60放量战法）](#5-ma60crossvolumewaveselector上穿60放量战法)
* [项目结构](#项目结构)
* [常见问题](#常见问题)
* [免责声明](#免责声明)

---

## 项目简介

| 名称                    | 功能简介                                                                                                                                                                               |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`fetch_kline.py`**  | 仅使用 **Tushare** 抓取 **A 股日线（前复权 qfq）**。**股票池从 `stocklist.csv` 读取**，支持排除 **创业板/科创板/北交所**，并发抓取，**每次运行全量覆盖保存**（不做增量合并），输出 CSV 列：`date, open, close, high, low, volume`。 |
| **`select_stock.py`** | 加载 `./data` 目录内 CSV 行情与 `configs.json`，批量执行选择器（Selector）并输出结果到控制台与 `select_results.log`。                                                                                           |
| **`Selector.py`**     | 实现各类战法（选择器）。**已删除 TePu 战法**；现包含 5 个策略，统一纳入“当日过滤 & 知行约束”。                                                                                                                           |

---

## 快速上手

### 环境与依赖

```bash
# Python 3.11/3.12 均可，示例以 3.12
conda create -n stock python=3.12 -y
conda activate stock

# 进入你的项目目录
cd /path/to/your/project

# 安装依赖
pip install -r requirements.txt
```

> 关键依赖：`pandas`, `tqdm`, `tushare`, `numpy`, `scipy`。

### 准备 Tushare Token

1. 在系统环境中写入 `TUSHARE_TOKEN`：

```bash
# Windows (PowerShell)
setx TUSHARE_TOKEN "你的token"

# macOS / Linux (bash)
export TUSHARE_TOKEN=你的token
```

### 下载历史 K 线（qfq，日线）

```bash
python fetch_kline.py \
  --start 20240101 \
  --end today \
  --stocklist ./stocklist.csv \
  --exclude-boards gem star bj \
  --out ./data \
  --workers 6
```

* **数据源固定**：Tushare 日线，**前复权 qfq**。
* **保存策略**：每只股票**全量覆盖写入** `./data/XXXXXX.csv`。
* **并发抓取**：默认 6 线程；支持封禁冷却（命中「访问频繁/429/403…」将睡眠约 600s 并重试，最多 3 次）。

### 运行选股

```bash
python select_stock.py \
  --data-dir ./data \
  --config ./configs.json \
  --date 2025-09-10
```

> `--date` 可省略，默认取数据中的最后交易日。

---

## 参数说明

### `fetch_kline.py`

| 参数                 | 默认值               | 说明                                                                         |
| ------------------ | ----------------- | -------------------------------------------------------------------------- |
| `--start`          | `20190101`        | 起始日期，格式 `YYYYMMDD` 或 `today`                                               |
| `--end`            | `today`           | 结束日期，格式同上                                                                  |
| `--stocklist`      | `./stocklist.csv` | 股票清单 CSV 路径（含 `ts_code` 或 `symbol`）                                        |
| `--exclude-boards` | `[]`              | 排除板块，枚举：`gem`(创业板 300/301) / `star`(科创板 688) / `bj`(北交所 .BJ / 4/8 开头)。可多选。 |
| `--out`            | `./data`          | 输出目录（自动创建）                                                                 |
| `--workers`        | `6`               | 并发线程数                                                                      |

**输出 CSV 列**：`date, open, close, high, low, volume`（按日期升序）。

**抓取与重试**：每支股票最多 3 次尝试；疑似限流/封禁触发 **600s 冷却**；其它异常采用递进式短等候重试（15s×尝试次数）。

### `select_stock.py`

| 参数           | 默认值              | 说明       |
| ------------ | ---------------- | -------- |
| `--data-dir` | `./data`         | CSV 行情目录 |
| `--config`   | `./configs.json` | 选择器配置    |
| `--date`     | 数据最后交易日          | 选股交易日    |

---

## 内置策略（Selector）

> **提示**：文中“窗口”均指交易日数量。实际实现均已替换为最新代码逻辑。

### 1. BBIKDJSelector（少妇战法）

核心逻辑：

* **价格波动约束**：最近 `max_window` 根收盘价的波动（`high/low-1`）≤ `price_range_pct`；
* **BBI 上升**：`bbi_deriv_uptrend`，允许一阶差分在 `bbi_q_threshold` 分位内为负（容忍回撤）；
* **KDJ 低位**：当日 J 值 **< `j_threshold`** 或 **≤ 最近 `max_window` 的 `j_q_threshold` 分位**；
* **MACD**：`DIF > 0`；
* **MA60 条件**：当日 `close ≥ MA60` 且最近 `max_window` 内存在“**有效上穿 MA60**”；
* **知行当日约束**：**收盘 > 长期线** 且 **短期线 > 长期线**。

`configs.json` 预设（与示例一致）：

```json
{
  "class": "BBIKDJSelector",
  "alias": "少妇战法",
  "activate": true,
  "params": {
    "j_threshold": 15,
    "bbi_min_window": 20,
    "max_window": 120,
    "price_range_pct": 1,
    "bbi_q_threshold": 0.2,
    "j_q_threshold": 0.10
  }
}
```

### 2. SuperB1Selector（SuperB1战法）

核心逻辑：

1. 在 `lookback_n` 窗内，存在某日 `t_m` **满足 BBIKDJSelector**；
2. 区间 `[t_m, 当日前一日]` 收盘价波动率 ≤ `close_vol_pct`；
3. 当日相对前一日 **下跌 ≥ `price_drop_pct`**；
4. 当日 J **< `j_threshold`** 或 **≤ `j_q_threshold` 分位**；
5. **知行约束**：

   * 在 `t_m` 当日：**收盘 > 长期线** 且 **短期线 > 长期线**；
   * 在 **当日**：只需 **短期线 > 长期线**。

`configs.json` 预设：

```json
{
  "class": "SuperB1Selector",
  "alias": "SuperB1战法",
  "activate": true,
  "params": {
    "lookback_n": 10,
    "close_vol_pct": 0.02,
    "price_drop_pct": 0.02,
    "j_threshold": 10,
    "j_q_threshold": 0.10,
    "B1_params": {
      "j_threshold": 15,
      "bbi_min_window": 20,
      "max_window": 120,
      "price_range_pct": 1,
      "bbi_q_threshold": 0.3,
      "j_q_threshold": 0.10
    }
  }
}
```

### 3. BBIShortLongSelector（补票战法）

核心逻辑：

* **BBI 上升**（容忍回撤）；
* 最近 `m` 日内：

  * 长 RSV（`n_long`）**全 ≥ `upper_rsv_threshold`**；
  * 短 RSV（`n_short`）出现“**先 ≥ upper，再 < lower**”的序列结构；
  * 当日短 RSV **≥ upper**；
* **MACD**：`DIF > 0`；
* **知行当日约束**：**收盘 > 长期线** 且 **短期线 > 长期线**。

`configs.json` 预设：

```json
{
  "class": "BBIShortLongSelector",
  "alias": "补票战法",
  "activate": true,
  "params": {
    "n_short": 5,
    "n_long": 21,
    "m": 5,
    "bbi_min_window": 2,
    "max_window": 120,
    "bbi_q_threshold": 0.2,
    "upper_rsv_threshold": 75,
    "lower_rsv_threshold": 25
  }
}
```

### 4. PeakKDJSelector（填坑战法）

核心逻辑：

* 基于 `open/close` 的 `oc_max` 寻找峰值（`scipy.signal.find_peaks`）；
* 选择最新峰 `peak_t` 与其前方**有效参照峰** `peak_(t-n)`：要求 `oc_t > oc_(t-n)`，并确保区间内其它峰不“抬高门槛”；且 `oc_(t-n)` 必须 **高于区间最低收盘价 `gap_threshold`**；
* 当日收盘与 `peak_(t-n)` 的波动率 ≤ `fluc_threshold`；
* 当日 J **< `j_threshold`** 或 **≤ `j_q_threshold` 分位**；
* **知行当日约束**：**收盘 > 长期线** 且 **短期线 > 长期线**。

`configs.json` 预设：

```json
{
  "class": "PeakKDJSelector",
  "alias": "填坑战法",
  "activate": true,
  "params": {
    "j_threshold": 10,
    "max_window": 120,
    "fluc_threshold": 0.03,
    "j_q_threshold": 0.10,
    "gap_threshold": 0.2
  }
}
```

### 5. MA60CrossVolumeWaveSelector（上穿60放量战法）

核心逻辑：

1. 当日 J **< `j_threshold`** 或 **≤ `j_q_threshold` 分位**；
2. 最近 `lookback_n` 内存在**有效上穿 MA60**；
3. 以上穿日 `T` 到当日区间内 **High 最大日** 作为 `Tmax`，定义上涨波段 `[T, Tmax]`，其 **平均成交量 ≥ `vol_multiple` × 上穿前等长或截断窗口的平均量**；
4. `MA60` 的最近 `ma60_slope_days` 日 **回归斜率 > 0**；
5. **知行当日约束**：**收盘 > 长期线** 且 **短期线 > 长期线**。

`configs.json` 预设：

```json
{
  "class": "MA60CrossVolumeWaveSelector",
  "alias": "上穿60放量战法",
  "activate": true,
  "params": {
    "lookback_n": 25,
    "vol_multiple": 1.8,
    "j_threshold": 15,
    "j_q_threshold": 0.10,
    "ma60_slope_days": 5,
    "max_window": 120
  }
}
```

> **已移除**：`BreakoutVolumeKDJSelector（TePu 战法）`。

---

## 项目结构

```
.
├── configs.json             # 选择器参数（示例见上文）
├── fetch_kline.py           # 从 stocklist.csv 读取并抓取 Tushare 日线（qfq）
├── select_stock.py          # 批量选股入口
├── Selector.py              # 策略实现（含公共指标/过滤）
├── stocklist.csv            # 你的股票池（示例列：ts_code/symbol/...）
├── data/                    # 行情 CSV 输出目录
├── fetch.log                # 抓取日志
└── select_results.log       # 选股日志
```

---

## 常见问题

**Q1：为什么抓取会“卡住很久”？**
可能命中 Tushare 频控或网络封禁。脚本检测到典型关键字（如“访问频繁/429/403”）时，会进入\*\*长冷却（默认 600s）\*\*再重试。

**Q2：为什么不做增量合并？**
考虑采用增量更新会遇到前复权的问题，本版选择**每次全量覆盖写入**。

**Q3：创业板/科创板/北交所如何排除？**
运行时使用 `--exclude-boards gem star bj`，或按需选择其一/其二。

---

## 免责声明

* 本仓库仅供学习与技术研究之用，**不构成任何投资建议**。股市有风险，入市需谨慎。
* 数据来源与接口可能随平台策略调整而变化，请合法合规使用。
* 致谢 **@Zettaranc** 在 Bilibili 的无私分享：[https://b23.tv/JxIOaNE](https://b23.tv/JxIOaNE)
