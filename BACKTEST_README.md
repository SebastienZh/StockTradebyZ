# 股票策略回测系统

## 功能简介

本回测系统用于评估选股策略的历史表现，通过分析策略在过去一段时间内的选股结果，计算每只股票在下一个交易日的收益率，从而评估策略的有效性。

## 主要功能

1. **策略回测**: 对指定时间范围内的每个交易日运行选股策略，计算下一个交易日的收益率
2. **多策略支持**: 支持同时回测多个策略，包括少妇战法、补票战法、TePu战法、填坑战法
3. **详细统计**: 计算平均收益率、胜率、最大/最小收益率等统计指标
4. **结果保存**: 将回测结果保存为JSON格式，按策略和日期组织
5. **报告生成**: 生成详细的中文分析报告

## 文件说明

- `backtest.py`: 主回测程序
- `backtest_report.py`: 回测报告生成器
- `test_backtest.py`: 测试脚本
- `run_backtest.bat`: 运行回测的批处理文件
- `generate_report.bat`: 生成报告的批处理文件
- `run_tests.bat`: 运行测试的批处理文件

## 快速开始

### 1. 运行回测

最简单的方式是使用批处理文件：

```bash
run_backtest.bat
```

这将对最近60天的数据进行回测，结果保存到 `backtest_result` 目录。

### 2. 自定义回测参数

如果需要自定义参数，可以直接运行Python脚本：

```bash
uv run python backtest.py --data-dir ./data --config ./configs.json --days 60 --output-dir ./backtest_result
```

参数说明：
- `--data-dir`: 股票数据目录（默认: ./data）
- `--config`: 策略配置文件（默认: ./configs.json）
- `--start-date`: 开始日期，格式YYYY-MM-DD
- `--end-date`: 结束日期，格式YYYY-MM-DD
- `--days`: 回测天数，如果未指定日期范围（默认: 60）
- `--output-dir`: 输出目录（默认: ./backtest_result）

### 3. 生成分析报告

运行回测后，可以生成详细的分析报告：

```bash
generate_report.bat
```

或者：

```bash
uv run python backtest_report.py --result-dir ./backtest_result --output backtest_report.txt
```

### 4. 运行测试

验证系统是否正常工作：

```bash
run_tests.bat
```

## 输出结果说明

### 目录结构

回测完成后，`backtest_result` 目录结构如下：

```
backtest_result/
├── overall_results.json          # 总体统计结果
├── 少妇战法/                      # 策略目录
│   ├── 2025-05-15.json           # 单日回测结果
│   ├── 2025-05-16.json
│   └── ...
├── 补票战法/
│   ├── 2025-05-15.json
│   └── ...
└── ...
```

### JSON结果格式

#### overall_results.json
```json
{
  "少妇战法": {
    "avg_return": 1.23,           // 平均收益率(%)
    "median_return": 0.89,        // 中位数收益率(%)
    "max_return": 9.87,           // 最大收益率(%)
    "min_return": -5.43,          // 最小收益率(%)
    "win_rate": 0.65,             // 胜率
    "stock_count": 150,           // 总股票数
    "trading_days": 42            // 交易日数
  }
}
```

#### 单日结果文件
```json
{
  "strategy": "少妇战法",
  "trade_date": "2025-05-15",
  "next_date": "2025-05-16",
  "stocks": [
    {
      "code": "000001",
      "name": "平安银行",
      "current_price": 12.34,
      "next_price": 12.56,
      "change_pct": 1.78
    }
  ],
  "summary": {
    "avg_return": 1.78,
    "median_return": 1.78,
    "max_return": 1.78,
    "min_return": 1.78,
    "win_rate": 1.0,
    "stock_count": 1
  }
}
```

## 分析报告

生成的分析报告包含以下内容：

1. **策略对比**: 按收益率、胜率、综合评分排名
2. **详细分析**: 每个策略的详细表现分析
3. **风险指标**: 波动率、夏普比率等
4. **最佳/最差表现**: 最好和最差的交易日、股票
5. **收益率分布**: 盈亏分布和区间统计

## 注意事项

1. **数据要求**: 确保 `data` 目录中有足够的历史股票数据
2. **网络连接**: 首次运行需要联网获取股票名称和交易日历
3. **计算时间**: 回测时间取决于数据量和策略复杂度
4. **结果解读**: 回测结果仅供参考，不构成投资建议

## 系统要求

- Python 3.10+
- 依赖包: pandas, numpy, akshare, tqdm
- 网络连接（用于获取股票名称和交易日历）

## 故障排除

### 常见问题

1. **"获取交易日历失败"**: 检查网络连接，或稍后重试
2. **"未能加载任何行情数据"**: 检查data目录是否存在CSV文件
3. **"跳过配置"**: 检查configs.json文件格式是否正确

### 日志文件

系统会生成 `logs/backtest.log` 日志文件，包含详细的运行信息，可用于问题诊断。

## 扩展功能

系统设计为模块化，可以轻松扩展：

1. **新增策略**: 在Selector.py中添加新的选股器类
2. **自定义指标**: 修改统计计算逻辑
3. **报告格式**: 自定义报告生成器
4. **数据源**: 支持其他数据源接入

## 技术支持

如有问题或建议，请查看日志文件或联系开发团队。
