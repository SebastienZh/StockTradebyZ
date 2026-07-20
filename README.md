# AgentTrader

一个面向 A 股并正在扩展到港股、美股的半自动研究项目：

- 默认使用无需 API Key 的 AKShare 拉取 A 股日线数据
- 用量化规则做初选（目前只实现了B1选股）
- 导出候选股票日线和周线图
- 默认调用国产 GLM-5V-Turbo，对双周期图和精确指标证据进行强类型复评
- 主模型不可用时自动降级到免费的 GLM-4.6V-Flash
- 保留 OpenAI 和 Gemini 作为可选 A/B 对照后端
- 提供多市场行情路由、主备源质量核验和点时基本面接口

---

## 更新说明

- 推翻了旧版选股模式（各式各样的B1太麻烦了）
- 新加入了AI看图打分精选功能（是的，不用再自己看图了）
- 目前只支持B1选股，后续Z哥讲了砖型图10张图后，会更新砖型图精选

---

## 1. 项目流程

完整流程对应 [run_all.py](run_all.py)：

1. 下载 K 线数据（pipeline.fetch_kline）
2. 量化初选（pipeline.cli preselect）
3. 导出候选图表（dashboard/export_kline_charts.py）
4. 国产 GLM 复评（agent/glm_review.py）
5. 打印推荐结果（读取 suggestion.json）

输出主链路：

- data/raw：原始日线 CSV
- data/candidates：初选候选列表
- data/kline/日期：候选图表
- data/review/日期：AI 单股评分与汇总建议

---

## 2. 目录说明

- [pipeline](pipeline)：数据抓取与量化初选
- [dashboard](dashboard)：看盘界面与图表导出
- [agent](agent)：GLM 默认评审与 OpenAI、Gemini 对照评审
- [config](config)：行情、基本面、初选与模型复评配置
- [data](data)：运行数据与结果
- [run_all.py](run_all.py)：全流程一键入口

---

## 3. 快速开始（一键跑通）

### 3.1 Clone 项目

~~~bash
git clone https://github.com/SebastienZh/StockTradebyZ
cd StockTradebyZ
~~~

### 3.2 安装依赖

~~~bash
pip install -r requirements.txt
~~~

### 3.3 设置环境变量

Windows PowerShell（永久写入）：

~~~powershell
[Environment]::SetEnvironmentVariable("OPENAI_API_KEY", "你的OpenAIApiKey", "User")
[Environment]::SetEnvironmentVariable("ZAI_API_KEY", "你的智谱ApiKey", "User")
[Environment]::SetEnvironmentVariable("GEMINI_API_KEY", "可选的GeminiApiKey", "User")
~~~

写入后请重开终端，环境变量才会在新会话中生效。

### 3.4 运行一键脚本

在项目根目录执行：

~~~bash
python run_all.py
~~~

常用参数：

~~~bash
python run_all.py --skip-fetch
python run_all.py --start-from 3
~~~

参数说明：

- --skip-fetch：跳过数据下载，直接进入初选
- --start-from N：从第 N 步开始执行（1 到 4）

---

## 4. 分步运行攻略

### 步骤 1：拉取 K 线

~~~bash
python -m pipeline.fetch_kline
~~~

配置见 [config/fetch_kline.yaml](config/fetch_kline.yaml)：

- start、end：抓取区间
- stocklist：股票池文件
- exclude_boards：排除板块（gem、star、bj）
- out：输出目录（默认 data/raw）
- workers：并发线程数

### 步骤 2：量化初选

~~~bash
python -m pipeline.cli preselect
~~~

可选参数示例：

~~~bash
python -m pipeline.cli preselect --date 2026-03-13
python -m pipeline.cli preselect --config config/rules_preselect.yaml --data data/raw
~~~

规则配置见 [config/rules_preselect.yaml](config/rules_preselect.yaml)。

### 步骤 3：导出候选图表

~~~bash
python dashboard/export_kline_charts.py
~~~

输出到 data/kline/选股日期，图像命名为 代码_day.jpg 和 代码_week.jpg。

### 步骤 4：国产 GLM 双周期证据复评

~~~bash
python agent/glm_review.py
~~~

可选参数示例：

~~~bash
python agent/glm_review.py --config config/glm_review.yaml
python run_all.py --start-from 4 --reviewer openai
python run_all.py --start-from 4 --reviewer gemini
~~~

默认配置见 [config/glm_review.yaml](config/glm_review.yaml)。OpenAI 与 Gemini 仍保留为可选对照后端。

读取候选与图表后，输出：

- data/review/日期/代码.json
- data/review/日期/suggestion.json

---

## 5. 关键配置建议

### 5.1 AKShare 与 Tushare 怎么选

| 维度 | AKShare（当前默认） | Tushare（可选） |
|---|---|---|
| 行情凭证 | 无需注册或 API Key | 需要 Token，部分接口受积分/权限限制 |
| 接入体验 | 安装后直接调用 | 需要账号、Token 和权限配置 |
| 上游稳定性 | 聚合公开网页，上游改版或限流会影响接口 | 标准化 API，字段和调用方式通常更稳定 |
| A股历史行情 | 支持日/周/月线和前后复权 | 覆盖完整、字段规范、适合长期维护 |
| 点时财务回测 | 不建议直接假设网页快照具备历史可得时间 | 可按 `ann_date` 截断，更适合防未来数据泄漏 |
| 本项目定位 | 日常免 Key 行情主源 | 高质量基本面和交叉核验的可选升级源 |

默认运行不需要安装 Tushare，也不需要 `TUSHARE_TOKEN`。如果以后在
`config/fetch_kline.yaml` 中切回 `provider: tushare`，再执行
`pip install tushare` 并配置 Token。

### 5.2 抓取层

- AKShare 首次全量抓取建议 workers 保持 1 到 2
- 若遇到频率限制，降低并发并重试

### 5.3 初选层

- top_m 决定流动性股票池大小
- b1.enabled、brick.enabled 控制策略开关
- 可先只开一个策略做回放验证

### 5.4 复评层

在 [config/openai_review.yaml](config/openai_review.yaml) 中可调整：

- model：模型名称
- request_delay：调用间隔（防限流）
- skip_existing：是否断点续跑
- suggest_min_score：推荐分数门槛

---

## 6. 输出结果解读

### 候选文件

[data/candidates/candidates_latest.json](data/candidates/candidates_latest.json)

- pick_date：选股日期
- candidates：候选列表（含 code、strategy、close 等）

### 复评汇总

data/review/日期/suggestion.json

- recommendations：最终推荐（按分数排序）
- excluded：未达门槛代码
- min_score_threshold：推荐门槛

---

## 7. 常见问题

### Q1：fetch_kline 报 token 错误

- 默认 AKShare 不需要 Token。请检查 `config/fetch_kline.yaml` 中 `provider: akshare`。
- 只有主动切换为 `provider: tushare` 时才需要设置 TUSHARE_TOKEN。

### Q2：导出图表时报 write_image 错误

- 确认已安装 kaleido
- Kaleido 1.x 需要本机 Chrome/Chromium；可运行 `plotly_get_chrome` 安装兼容浏览器

### Q3：OpenAI 运行失败

- 检查 OPENAI_API_KEY、项目额度和账单状态
- 观察是否命中限流，可提高 request_delay

### Q4：没有候选股票

- 检查 data/raw 是否有最新数据
- 放宽初选阈值（如 B1 或 Brick 参数）
- 检查 pick_date 是否在有效交易日

### 多市场数据与基本面

~~~bash
python -m pipeline.fetch_market CN:600519 HK:00700 US:AAPL --start 2024-01-01
python -m pipeline.fetch_fundamentals CN:600519 US:AAPL --as-of 2026-07-20
~~~

完整架构、数据源优先级和上线验收方法见 [v2 升级设计](docs/UPGRADE_V2.md)。

---

## License

本项目采用 [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) 协议发布。

- 允许：学习、研究、非商业用途的使用与分发
- 禁止：任何形式的商业使用、出售或以盈利为目的的部署
- 要求：转载或引用须注明原作者与来源

Copyright © 2026 SebastienZh. All rights reserved.
