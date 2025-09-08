@echo off
echo 正在生成回测分析报告...
uv run python backtest_report.py --result-dir ./backtest_result --output backtest_report.txt
echo 报告生成完成！已保存到 backtest_report.txt
pause
