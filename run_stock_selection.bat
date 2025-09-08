@echo off
@REM uv run python select_stock.py --data-dir ./data --config ./configs.json --date today
echo Starting Stock Selection Analysis TODAY...
uv run python select_stock.py --data-dir ./data --config ./configs.json --date today
pause
