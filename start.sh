#!/bin/bash

echo "=== Z哥战法 Docker 启动脚本 ==="
echo ""

# 检查Docker是否安装
if ! command -v docker &> /dev/null; then
    echo "错误: Docker 未安装，请先安装 Docker"
    exit 1
fi

# 检查docker-compose是否安装
if ! command -v docker-compose &> /dev/null; then
    echo "错误: docker-compose 未安装，请先安装 docker-compose"
    exit 1
fi

# 创建必要的目录
echo "创建数据目录..."
mkdir -p data logs

# 检查配置文件是否存在
if [ ! -f "configs.json" ]; then
    echo "错误: configs.json 文件不存在"
    exit 1
fi

# 构建并启动容器
echo "构建并启动容器..."
docker-compose up -d --build

# 检查容器是否启动成功
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ 容器启动成功！"
    echo ""
    echo "📊 查看容器状态:"
    echo "   docker ps"
    echo ""
    echo "📋 查看实时日志:"
    echo "   docker logs -f stock-trade-by-z"
    echo ""
    echo "📁 查看任务日志:"
    echo "   tail -f logs/daily_task.log"
    echo ""
    echo "⏰ 定时任务设置:"
    echo "   每天下午5点自动执行数据获取和选股"
    echo ""
    echo "🔧 手动执行任务:"
    echo "   docker exec -it stock-trade-by-z /app/daily_task.sh"
    echo ""
    echo "📖 详细说明请查看: DOCKER_README.md"
else
    echo "❌ 容器启动失败，请检查错误信息"
    exit 1
fi 