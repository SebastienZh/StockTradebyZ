#!/bin/bash

# 设置时区为北京时间
export TZ=Asia/Shanghai

# 创建定时任务脚本
cat > /app/daily_task.sh << 'EOF'
#!/bin/bash

# 设置时区
export TZ=Asia/Shanghai

# 获取当前日期
TODAY=$(date +%Y-%m-%d)

# 记录开始时间
echo "$(date): 开始执行每日任务" >> /app/logs/daily_task.log

# 执行fetch_kline
echo "$(date): 开始执行fetch_kline" >> /app/logs/daily_task.log
cd /app && python fetch_kline.py --datasource yfinance --frequency 4 --min-mktcap 2e9 --start 20240701 --end today --out ./data --workers 1 >> /app/logs/fetch_kline.log 2>&1
cd /app && python fetch_kline.py --datasource yfinance --frequency 4 --start 20240701 --end today --out ./data.hk --workers 1 --market hk >> /app/logs/fetch_kline.log 2>&1

# 检查fetch_kline是否成功
if [ $? -eq 0 ]; then
    echo "$(date): fetch_kline执行成功" >> /app/logs/daily_task.log
    
    # 执行select_stock
    echo "$(date): 开始执行select_stock，日期: $TODAY" >> /app/logs/daily_task.log
    cd /app && python select_stock.py --data-dir ./data --config ./configs.json --date $TODAY >> /app/logs/select_stock.log 2>&1
    cd /app && python select_stock.py --data-dir ./data.hk --config ./configs_hk.json --date $TODAY >> /app/logs/select_stock.log 2>&1
    
    if [ $? -eq 0 ]; then
        echo "$(date): select_stock执行成功" >> /app/logs/daily_task.log
    else
        echo "$(date): select_stock执行失败" >> /app/logs/daily_task.log
    fi
else
    echo "$(date): fetch_kline执行失败" >> /app/logs/daily_task.log
fi

echo "$(date): 每日任务执行完成" >> /app/logs/daily_task.log
echo "----------------------------------------" >> /app/logs/daily_task.log
EOF

# 设置执行权限
chmod +x /app/daily_task.sh

# 创建cron任务（每天下午5点执行）
echo "0 17 * * * /app/daily_task.sh" > /etc/cron.d/daily_task

# 给cron文件设置权限
chmod 0644 /etc/cron.d/daily_task

# 创建cron日志文件
touch /app/logs/cron.log

# 启动cron服务
service cron start

# 显示cron任务
echo "已设置定时任务："
crontab -l

# 显示当前时间
echo "当前时间：$(date)"

# 保持容器运行
tail -f /app/logs/cron.log 