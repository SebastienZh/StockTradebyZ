# 技术架构设计文档

## 1. 整体架构概览

### 1.1 系统架构图
```
┌─────────────────────────────────────────────────────────────┐
│                    前端展示层                                │
├─────────────────────────────────────────────────────────────┤
│  React/Vue.js + TypeScript                                  │
│  ├── 仪表板页面     ├── K线图表页面    ├── 选股结果页面      │
│  ├── 系统配置页面   ├── 回检分析页面    ├── 研究分析页面      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    API网关层                                │
├─────────────────────────────────────────────────────────────┤
│  FastAPI + Uvicorn                                         │
│  ├── 认证授权      ├── 请求路由       ├── 限流控制          │
│  ├── 错误处理      ├── 日志记录       ├── 监控统计          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    业务服务层                                │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │  数据服务   │  │  分析服务   │  │  通知服务   │          │
│  │             │  │             │  │             │          │
│  │ ├股票数据   │  │ ├选股分析   │  │ ├飞书机器人 │          │
│  │ ├K线数据    │  │ ├回检分析   │  │ ├邮件通知   │          │
│  │ ├指标计算   │  │ ├LLM复盘    │  │ ├短信通知   │          │
│  │ └数据同步   │  │ └持仓检查   │  │ └推送通知   │          │
│  └─────────────┘  └─────────────┘  └─────────────┘          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    任务调度层                                │
├─────────────────────────────────────────────────────────────┤
│  APScheduler                                                │
│  ├── 数据获取任务 (每日16:00)                               │
│  ├── 选股分析任务 (数据获取后)                              │
│  ├── 复盘分析任务 (选股后)                                  │
│  └── 通知推送任务 (分析完成后)                              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    数据存储层                                │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │   SQLite    │  │   CSV文件   │  │   缓存层    │          │
│  │   数据库    │  │   存储      │  │   Redis     │          │
│  │             │  │             │  │             │          │
│  │ ├股票信息   │  │ ├历史K线    │  │ ├热点数据   │          │
│  │ ├选股结果   │  │ ├指标数据   │  │ ├计算结果   │          │
│  │ ├回测数据   │  │ └备份数据   │  │ └会话数据   │          │
│  │ └系统日志   │  │             │  │             │          │
│  └─────────────┘  └─────────────┘  └─────────────┘          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    外部接口层                                │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │  数据源API  │  │   LLM API   │  │  第三方API  │          │
│  │             │  │             │  │             │          │
│  │ ├Mootdx     │  │ ├OpenAI     │  │ ├飞书API    │          │
│  │ ├AkShare    │  │ ├Claude     │  │ ├CueCue     │          │
│  │ ├Tushare    │  │ └本地模型   │  │ └新闻API    │          │
│  │ └实时行情   │  │             │  │             │          │
│  └─────────────┘  └─────────────┘  └─────────────┘          │
└─────────────────────────────────────────────────────────────┘
```

### 1.2 技术栈选择

**后端技术栈**:
- **Web框架**: FastAPI (高性能、自动文档生成)
- **数据库**: SQLite + SQLAlchemy (轻量级、易部署)
- **任务调度**: APScheduler (Python原生、易集成)
- **缓存**: Redis (可选，用于高频数据缓存)

**前端技术栈**:
- **框架**: React 18 + TypeScript
- **状态管理**: Redux Toolkit / Zustand
- **UI组件**: Ant Design / Material-UI
- **图表库**: TradingView Lightweight Charts (专业金融图表)
- **构建工具**: Vite (快速构建)

**部署和运维**:
- **容器化**: Docker + Docker Compose
- **反向代理**: Nginx
- **监控**: Prometheus + Grafana
- **日志**: ELK Stack (可选)

## 2. 数据库设计

### 2.1 核心数据表

```sql
-- 股票基础信息表
CREATE TABLE stocks (
    code VARCHAR(10) PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    industry VARCHAR(50),
    concept_tags TEXT,
    market_cap BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- K线数据表 (主要用于快速查询)
CREATE TABLE kline_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(10,3),
    high DECIMAL(10,3),
    low DECIMAL(10,3),
    close DECIMAL(10,3),
    volume BIGINT,
    amount BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(code, date)
);

-- 选股结果表
CREATE TABLE selection_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    strategy VARCHAR(50) NOT NULL,
    code VARCHAR(10) NOT NULL,
    stock_name VARCHAR(50),
    selection_reason TEXT,
    technical_indicators JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 回测结果表
CREATE TABLE backtest_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    strategy VARCHAR(50) NOT NULL,
    code VARCHAR(10) NOT NULL,
    stock_name VARCHAR(50),
    entry_price DECIMAL(10,3),
    next_day_return DECIMAL(8,4),
    max_return_1d DECIMAL(8,4),
    min_return_1d DECIMAL(8,4),
    return_3d DECIMAL(8,4),
    return_5d DECIMAL(8,4),
    return_10d DECIMAL(8,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 持仓记录表
CREATE TABLE positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code VARCHAR(10) NOT NULL,
    stock_name VARCHAR(50),
    strategy VARCHAR(50),
    entry_date DATE NOT NULL,
    entry_price DECIMAL(10,3),
    quantity INTEGER,
    current_price DECIMAL(10,3),
    current_return DECIMAL(8,4),
    status VARCHAR(20) DEFAULT 'HOLDING', -- HOLDING, SOLD
    exit_date DATE,
    exit_price DECIMAL(10,3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 系统任务日志表
CREATE TABLE task_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_name VARCHAR(100) NOT NULL,
    task_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL, -- RUNNING, SUCCESS, FAILED
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration INTEGER, -- 执行时间(秒)
    message TEXT,
    error_details TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- LLM分析结果表
CREATE TABLE llm_analysis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    analysis_type VARCHAR(50) NOT NULL, -- DAILY_REVIEW, STOCK_RESEARCH
    content TEXT NOT NULL,
    summary TEXT,
    confidence_score DECIMAL(3,2),
    tokens_used INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 2.2 索引设计

```sql
-- 性能优化索引
CREATE INDEX idx_kline_code_date ON kline_data(code, date);
CREATE INDEX idx_selection_date_strategy ON selection_results(date, strategy);
CREATE INDEX idx_backtest_date_strategy ON backtest_results(date, strategy);
CREATE INDEX idx_positions_status ON positions(status);
CREATE INDEX idx_task_logs_date_status ON task_logs(DATE(created_at), status);
```

## 3. API接口设计

### 3.1 RESTful API规范

**基础路径**: `/api/v1`

**认证方式**: JWT Token (可选，初期可不实现)

**响应格式**:
```json
{
    "code": 200,
    "message": "success",
    "data": {},
    "timestamp": "2025-01-15T10:30:00Z"
}
```

### 3.2 核心API接口

```python
# 股票数据相关
GET    /api/v1/stocks                    # 获取股票列表
GET    /api/v1/stocks/{code}             # 获取股票详情
GET    /api/v1/stocks/{code}/kline       # 获取K线数据
GET    /api/v1/stocks/{code}/indicators  # 获取技术指标

# 选股结果相关
GET    /api/v1/selections                # 获取选股结果列表
GET    /api/v1/selections/latest         # 获取最新选股结果
GET    /api/v1/selections/{date}         # 获取指定日期选股结果
POST   /api/v1/selections/run            # 手动触发选股

# 回测分析相关
GET    /api/v1/backtest/results          # 获取回测结果
GET    /api/v1/backtest/statistics       # 获取回测统计
POST   /api/v1/backtest/run              # 运行回测分析

# 持仓管理相关
GET    /api/v1/positions                 # 获取持仓列表
POST   /api/v1/positions                 # 添加持仓
PUT    /api/v1/positions/{id}            # 更新持仓
DELETE /api/v1/positions/{id}            # 删除持仓

# 系统管理相关
GET    /api/v1/system/status             # 获取系统状态
GET    /api/v1/system/tasks              # 获取任务执行状态
POST   /api/v1/system/tasks/{task}/run   # 手动执行任务
GET    /api/v1/system/logs               # 获取系统日志

# LLM分析相关
GET    /api/v1/analysis/reviews          # 获取复盘分析
POST   /api/v1/analysis/research         # 个股研究分析
GET    /api/v1/analysis/concepts/{name}  # 概念分析
```

## 4. 前端架构设计

### 4.1 组件层次结构

```
src/
├── components/           # 通用组件
│   ├── Charts/          # 图表组件
│   │   ├── KLineChart.tsx
│   │   ├── IndicatorChart.tsx
│   │   └── PerformanceChart.tsx
│   ├── Tables/          # 表格组件
│   ├── Forms/           # 表单组件
│   └── Layout/          # 布局组件
├── pages/               # 页面组件
│   ├── Dashboard/       # 仪表板
│   ├── Charts/          # 图表页面
│   ├── Selection/       # 选股结果
│   ├── Backtest/        # 回测分析
│   ├── Position/        # 持仓管理
│   └── Settings/        # 系统设置
├── hooks/               # 自定义Hooks
├── services/            # API服务
├── store/               # 状态管理
├── utils/               # 工具函数
└── types/               # TypeScript类型定义
```

### 4.2 状态管理设计

```typescript
// 全局状态结构
interface AppState {
  user: UserState;
  stocks: StockState;
  selections: SelectionState;
  backtest: BacktestState;
  system: SystemState;
}

interface StockState {
  list: Stock[];
  current: Stock | null;
  klineData: KLineData[];
  indicators: IndicatorData[];
  loading: boolean;
}

interface SelectionState {
  results: SelectionResult[];
  latest: SelectionResult[];
  statistics: SelectionStatistics;
  loading: boolean;
}
```

## 5. 任务调度设计

### 5.1 定时任务配置

```python
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

scheduler = AsyncIOScheduler()

# 每日数据获取任务 (北京时间16:00)
scheduler.add_job(
    func=daily_data_fetch_task,
    trigger=CronTrigger(hour=16, minute=0, timezone='Asia/Shanghai'),
    id='daily_data_fetch',
    name='每日数据获取',
    max_instances=1,
    coalesce=True
)

# 选股分析任务 (数据获取完成后)
scheduler.add_job(
    func=stock_selection_task,
    trigger='interval',
    minutes=5,  # 每5分钟检查一次是否需要执行
    id='stock_selection',
    name='选股分析',
    max_instances=1
)

# LLM复盘任务 (选股完成后)
scheduler.add_job(
    func=llm_review_task,
    trigger='interval',
    minutes=10,
    id='llm_review',
    name='LLM复盘分析',
    max_instances=1
)
```

### 5.2 任务执行流程

```python
async def daily_workflow():
    """每日自动化工作流程"""
    try:
        # 1. 数据获取
        await execute_data_fetch()
        
        # 2. 数据同步到数据库
        await sync_csv_to_database()
        
        # 3. 执行选股分析
        selection_results = await execute_stock_selection()
        
        # 4. 执行回测分析
        await execute_backtest_analysis()
        
        # 5. LLM复盘分析
        review_result = await execute_llm_review(selection_results)
        
        # 6. 发送通知
        await send_daily_notification(selection_results, review_result)
        
        # 7. 更新系统状态
        await update_system_status('SUCCESS')
        
    except Exception as e:
        await handle_workflow_error(e)
        await update_system_status('FAILED', str(e))
```

## 6. 缓存策略

### 6.1 多级缓存架构

```python
# 1. 内存缓存 (应用级)
from functools import lru_cache

@lru_cache(maxsize=1000)
def get_stock_basic_info(code: str):
    """股票基础信息缓存"""
    pass

# 2. Redis缓存 (分布式)
import redis

redis_client = redis.Redis(host='localhost', port=6379, db=0)

async def get_kline_data_cached(code: str, days: int = 30):
    """K线数据缓存"""
    cache_key = f"kline:{code}:{days}"
    cached_data = redis_client.get(cache_key)
    
    if cached_data:
        return json.loads(cached_data)
    
    # 从数据库获取数据
    data = await get_kline_data_from_db(code, days)
    
    # 缓存30分钟
    redis_client.setex(cache_key, 1800, json.dumps(data))
    return data
```

### 6.2 缓存更新策略

- **实时数据**: TTL 5分钟
- **K线数据**: TTL 30分钟
- **选股结果**: TTL 1小时
- **回测统计**: TTL 4小时
- **股票基础信息**: TTL 24小时

## 7. 监控和日志

### 7.1 监控指标

```python
# 系统性能指标
- API响应时间
- 数据库查询时间
- 任务执行时间
- 内存使用率
- CPU使用率

# 业务指标
- 每日选股数量
- 数据获取成功率
- LLM API调用次数
- 通知发送成功率
```

### 7.2 日志配置

```python
import logging
from logging.handlers import RotatingFileHandler

# 配置日志格式
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler('app.log', maxBytes=10*1024*1024, backupCount=5),
        logging.StreamHandler()
    ]
)
```

## 8. 安全考虑

### 8.1 数据安全
- 敏感配置信息环境变量存储
- 数据库连接加密
- API密钥安全管理
- 定期数据备份

### 8.2 访问控制
- API访问频率限制
- 用户认证和授权 (可选)
- CORS跨域配置
- HTTPS强制使用

这个技术架构设计为系统提供了清晰的技术路线和实施指导，确保系统的可扩展性、可维护性和高性能。
