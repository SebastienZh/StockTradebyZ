import numpy as np

def run_backtest(
    signals: list,
    kline_df: pd.DataFrame,
    initial_capital: float,
    position_size: float,
    holding_period: int,
    take_profit_pct: float = None,
    stop_loss_pct: float = None,
    commission_rate: float = 0.0003,
    stamp_duty_rate: float = 0.001 # 印花税仅卖出时收取
) -> (list, pd.DataFrame):
    """
    执行回测的核心函数。

    Args:
        signals (list): 策略信号列表，格式为 [('YYYY-MM-DD', 'code'), ...]
        kline_df (pd.DataFrame): 预处理好的K线数据 (MultiIndex: code, date)
        initial_capital (float): 初始资金
        position_size (float): 每次交易使用的资金比例
        holding_period (int): 最大持仓天数
        take_profit_pct (float, optional): 止盈百分比. Defaults to None.
        stop_loss_pct (float, optional): 止损百分比. Defaults to None.
        commission_rate (float, optional): 佣金率. Defaults to 0.0003.
        stamp_duty_rate (float, optional): 印花税率. Defaults to 0.001.

    Returns:
        tuple: (交易日志列表, 每日净值Series)
    """
    trade_log = []
    capital = initial_capital
    equity_curve = pd.Series(index=kline_df.index.get_level_values('date').unique().sort_values())
    equity_curve.iloc[0] = initial_capital

    # 按日期对信号进行排序，确保按时间顺序处理
    sorted_signals = sorted(signals, key=lambda x: x[0])

    # 获取所有交易日
    all_trading_days = kline_df.index.get_level_values('date').unique().sort_values()

    for signal_date_str, stock_code in sorted_signals:
        signal_date = pd.to_datetime(signal_date_str)

        # --- 模拟买入 ---
        # 找到信号日的下一个交易日
        buy_date_loc = all_trading_days.searchsorted(signal_date, side='right')
        if buy_date_loc >= len(all_trading_days):
            continue # 信号日之后没有交易日了

        buy_date = all_trading_days[buy_date_loc]

        try:
            # 尝试获取买入日的K线数据
            buy_day_data = kline_df.loc[(stock_code, buy_date)]
            buy_price = buy_day_data['open']

            # 检查是否能买入 (非一字涨停)
            # 简单判断：开盘价等于最低价，且高于前一日收盘价的9.8%（近似涨停）
            prev_day_loc = all_trading_days.searchsorted(buy_date) - 1
            if prev_day_loc >= 0:
                prev_day = all_trading_days[prev_day_loc]
                prev_close = kline_df.loc[(stock_code, prev_day)]['close']
                if buy_day_data['open'] == buy_day_data['low'] and buy_day_data['open'] > prev_close * 1.098:
                    # print(f"Skipping buy for {stock_code} on {buy_date.date()}: Limit up.")
                    continue

        except KeyError:
            # 当天可能停牌或数据缺失
            # print(f"Skipping buy for {stock_code} on {buy_date.date()}: Data not available.")
            continue

        # 计算交易成本
        trade_value = capital * position_size
        commission_buy = trade_value * commission_rate

        # --- 模拟持仓与卖出 ---
        sell_date, sell_price, sell_reason = (None, None, None)

        for i in range(1, holding_period + 1):
            current_day_loc = buy_date_loc + i
            if current_day_loc >= len(all_trading_days):
                break # 超出数据范围

            current_date = all_trading_days[current_day_loc]

            try:
                day_data = kline_df.loc[(stock_code, current_date)]
                daily_high = day_data['high']
                daily_low = day_data['low']

                # 优先判断止盈
                if take_profit_pct and daily_high >= buy_price * (1 + take_profit_pct):
                    sell_price = buy_price * (1 + take_profit_pct)
                    sell_reason = 'take_profit'
                    sell_date = current_date
                    break

                # 再判断止损
                if stop_loss_pct and daily_low <= buy_price * (1 - stop_loss_pct):
                    sell_price = buy_price * (1 - stop_loss_pct)
                    sell_reason = 'stop_loss'
                    sell_date = current_date
                    break

            except KeyError:
                # 持仓期间停牌，跳过当天
                continue

        # 如果循环结束仍未触发止盈止损，则在持仓期最后一天卖出
        if not sell_reason:
            final_sell_day_loc = buy_date_loc + holding_period
            if final_sell_day_loc < len(all_trading_days):
                sell_date = all_trading_days[final_sell_day_loc]
                try:
                    # 检查是否能卖出 (非一字跌停)
                    sell_day_data = kline_df.loc[(stock_code, sell_date)]
                    prev_day_loc = all_trading_days.searchsorted(sell_date) - 1
                    prev_day = all_trading_days[prev_day_loc]
                    prev_close = kline_df.loc[(stock_code, prev_day)]['close']

                    if sell_day_data['open'] == sell_day_data['high'] and sell_day_data['open'] < prev_close * 0.902:
                        # print(f"Failed to sell {stock_code} on {sell_date.date()}: Limit down. Holding.")
                        # 实际情况可能需要更复杂的顺延逻辑，这里简化为交易失败
                        pass
                    else:
                        sell_price = sell_day_data['close']
                        sell_reason = 'holding_period'
                except KeyError:
                    # 卖出日停牌，交易失败
                    pass

        # --- 记录交易日志 ---
        if sell_price is not None:
            commission_sell = trade_value * (sell_price / buy_price) * commission_rate
            stamp_duty = trade_value * (sell_price / buy_price) * stamp_duty_rate
            total_commission = commission_buy + commission_sell + stamp_duty

            net_profit = trade_value * (sell_price / buy_price - 1) - total_commission
            return_pct = net_profit / trade_value

            trade_log.append({
                'code': stock_code,
                'buy_date': buy_date.date(),
                'buy_price': buy_price,
                'sell_date': sell_date.date(),
                'sell_price': sell_price,
                'sell_reason': sell_reason,
                'return_pct': return_pct,
                'commission': total_commission,
                'holding_days': (sell_date - buy_date).days
            })

            # 更新资金 (简化模型：假设所有交易并行，不考虑资金占用)
            # 一个更真实的模型需要管理可用资金
            capital += net_profit

    # 填充每日净值曲线 (简化逻辑)
    # 严谨的净值曲线需要每日更新持仓市值，这里仅作示意
    trade_df = pd.DataFrame(trade_log).set_index('sell_date')
    trade_df.index = pd.to_datetime(trade_df.index)
    daily_returns = trade_df['return_pct'].resample('D').sum()
    cumulative_returns = (1 + daily_returns).cumprod()

    equity_curve = initial_capital * cumulative_returns
    equity_curve = equity_curve.reindex(all_trading_days, method='ffill').fillna(initial_capital)

    return trade_log, equity_curve



def main():
    parser = argparse.ArgumentParser(description="按市值筛选 A 股并抓取历史 K 线")
    parser.add_argument("--datasource", choices=["tushare", "akshare", "mootdx"], default="tushare", help="历史 K 线数据源")
    parser.add_argument("--frequency", type=int, choices=list(_FREQ_MAP.keys()), default=4, help="K线频率编码，参见说明")
    parser.add_argument("--exclude-gem", default=True, help="True则排除创业板/科创板/北交所")
    parser.add_argument("--min-mktcap", type=float, default=5e9, help="最小总市值（含），单位：元")
    parser.add_argument("--max-mktcap", type=float, default=float("+inf"), help="最大总市值（含），单位：元，默认无限制")
    parser.add_argument("--start", default="20190101", help="起始日期 YYYYMMDD 或 'today'")
    parser.add_argument("--end", default="today", help="结束日期 YYYYMMDD 或 'today'")
    parser.add_argument("--out", default="./data", help="输出目录")
    parser.add_argument("--workers", type=int, default=3, help="并发线程数")
    args = parser.parse_args()