FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    cron \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目文件
COPY . .

# 创建数据目录
RUN mkdir -p /app/data

# 创建日志目录
RUN mkdir -p /app/logs

# 设置权限
RUN chmod +x /app/entrypoint.sh

# 暴露端口（如果需要的话）
EXPOSE 8000

# 设置入口点
ENTRYPOINT ["/app/entrypoint.sh"] 