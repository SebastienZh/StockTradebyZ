@echo off
call setup_mootdx.bat
echo Fetching latest stock data using Mootdx...
for /f %%i in ('powershell -command "(Get-Date).AddDays(-1).ToString('yyyyMMdd')"') do set YESTERDAY=%%i
uv run python fetch_kline.py --datasource mootdx --frequency 4 --adjust qfq --min-mktcap 2e9 --max-mktcap +inf --start %YESTERDAY% --end today --out ./data --workers 5
@REM uv run python fetch_kline.py --datasource mootdx --frequency 4 --adjust qfq --min-mktcap 5e9 --max-mktcap +inf --start 20250421 --end today --out ./data --workers 5
@REM uv run python fetch_kline.py --datasource akshare --frequency 4 --adjust qfq --min-mktcap 5e9 --max-mktcap +inf --start 19700101 --end today --out ./data --workers 10
echo Data fetch completed!
call run_stock_selection.bat
pause
