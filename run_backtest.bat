@echo off
echo Running Backtesting...
uv run backtest.py --data-dir ./data --config ./configs.json --days 10 --output-dir ./backtest_result
echo DONE£¡Results saved at `backtest_result`
pause
