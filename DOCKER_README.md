# Docker 部署说明

本项目已配置为Docker容器，支持定时自动执行股票数据获取和选股任务。

## 快速开始

### 1. 构建并启动容器

```bash
# 使用docker-compose启动（推荐）
docker-compose up -d

# 或者使用docker命令
docker build -t stock-trade-by-z .
docker run -d --name stock-trade-by-z \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  -v $(pwd)/configs.json:/app/configs.json \
  -e TZ=Asia/Shanghai \
  stock-trade-by-z
```

### 2. 查看容器状态

```bash
# 查看容器运行状态
docker ps

# 查看容器日志
docker logs stock-trade-by-z

# 实时查看日志
docker logs -f stock-trade-by-z
```

### 3. 查看任务日志

```bash
# 查看每日任务执行日志
tail -f logs/daily_task.log

# 查看fetch_kline执行日志
tail -f logs/fetch_kline.log

# 查看select_stock执行日志
tail -f logs/select_stock.log
```

## 定时任务配置

容器内配置了cron定时任务，每天下午5点（17:00）自动执行：

1. **fetch_kline任务**：
   ```bash
   python fetch_kline.py --datasource yfinance --frequency 4 --min-mktcap 2e9 --start 20240701 --end today --out ./data --workers 1
   ```

2. **select_stock任务**：
   ```bash
   python select_stock.py --data-dir ./data --config ./configs.json --date ${today}
   ```

## 手动执行任务

如果需要手动执行任务，可以进入容器：

```bash
# 进入容器
docker exec -it stock-trade-by-z bash

# 手动执行每日任务脚本
/app/daily_task.sh

# 或者单独执行某个任务
python fetch_kline.py --datasource yfinance --frequency 4 --min-mktcap 2e9 --start 20240701 --end today --out ./data --workers 1
python select_stock.py --data-dir ./data --config ./configs.json --date $(date +%Y-%m-%d)
```

## 数据持久化

- **数据目录**：`./data` - 存储股票K线数据
- **日志目录**：`./logs` - 存储执行日志
- **配置文件**：`./configs.json` - 选股策略配置

这些目录都通过volume挂载到宿主机，容器重启后数据不会丢失。

## 修改配置

### 修改选股策略

编辑 `configs.json` 文件，修改后重启容器：

```bash
docker-compose restart
```

### 修改定时任务时间

如果需要修改执行时间，可以编辑 `entrypoint.sh` 文件中的cron表达式：

```bash
# 当前设置为每天下午5点执行
echo "0 17 * * * /app/daily_task.sh" > /etc/cron.d/daily_task

# 例如改为每天上午9点执行
echo "0 9 * * * /app/daily_task.sh" > /etc/cron.d/daily_task
```

修改后重新构建镜像：

```bash
docker-compose down
docker-compose up -d --build
```

## 停止和清理

```bash
# 停止容器
docker-compose down

# 删除容器和镜像
docker-compose down --rmi all

# 删除数据（谨慎操作）
rm -rf data/ logs/
```

## 故障排除

### 1. 容器无法启动

检查Docker和docker-compose是否正确安装：

```bash
docker --version
docker-compose --version
```

### 2. 定时任务不执行

检查cron服务是否正常运行：

```bash
docker exec -it stock-trade-by-z service cron status
```

### 3. 查看详细日志

```bash
# 查看容器启动日志
docker logs stock-trade-by-z

# 查看cron日志
docker exec -it stock-trade-by-z tail -f /app/logs/cron.log
```

### 4. 时区问题

确保容器时区设置正确：

```bash
docker exec -it stock-trade-by-z date
```

## 注意事项

1. **数据源配置**：当前使用yfinance数据源，如需使用其他数据源，请修改 `entrypoint.sh` 中的参数
2. **网络连接**：确保容器能够访问外网获取股票数据
3. **磁盘空间**：定期检查数据目录大小，避免磁盘空间不足
4. **日志轮转**：建议定期清理日志文件，避免占用过多磁盘空间 