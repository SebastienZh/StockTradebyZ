FROM python:3.12-slim

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    cron \
    && rm -rf /var/lib/apt/lists/*

# 创建cron日志目录
RUN mkdir -p /var/log/cron

# 复制依赖文件
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 只复制 python 文件
COPY *.py /app/
COPY entrypoint.sh /app/

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