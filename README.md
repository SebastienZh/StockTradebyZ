# AgentTrader

一个面向 A 股的半自动选股项目：

- 使用 Tushare 拉取股票日线数据
- 用量化规则做初选（目前只实现了B1选股）
- 导出候选股票 K 线图
- 调用 Gemini 对图表进行 AI 复评打分

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
4. Gemini 复评（agent/gemini_review.py）
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
- [agent](agent)：LLM 评审逻辑（Gemini）
- [config](config)：抓取、初选、Gemini 复评配置
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

在项目根目录复制模板文件，填入你的真实 Key：

~~~bash
cp .env.example .env
~~~

编辑 `.env`：

~~~
TUSHARE_TOKEN=你的TushareToken
GEMINI_API_KEY=你的GeminiApiKey
~~~

项目启动时会自动加载 `.env`，无需手动设置系统环境变量，跨平台通用。`.env` 已在 `.gitignore` 中，不会被提交到版本库。

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

输出到 data/kline/选股日期，图像命名为 代码_day.jpg。

### 步骤 4：Gemini 图表复评

~~~bash
python agent/gemini_review.py
~~~

可选参数示例：

~~~bash
python agent/gemini_review.py --config config/gemini_review.yaml
~~~

配置见 [config/gemini_review.yaml](config/gemini_review.yaml)。

读取候选与图表后，输出：

- data/review/日期/代码.json
- data/review/日期/suggestion.json

---

## 5. 关键配置建议

### 6.1 抓取层

- 首次全量抓取建议 workers 设小一些（如 4 到 8）
- 若遇到频率限制，降低并发并重试

### 6.2 初选层

- top_m 决定流动性股票池大小
- b1.enabled、brick.enabled 控制策略开关
- 可先只开一个策略做回放验证

### 6.3 复评层

在 [config/gemini_review.yaml](config/gemini_review.yaml) 中可调整：

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

- 检查 TUSHARE_TOKEN 是否已设置
- 确认 token 有效且账号权限正常

### Q2：导出图表时报 write_image 错误

- 确认已安装 kaleido
- 重新安装：pip install -U kaleido

### Q3：Gemini 运行失败

- 检查 GEMINI_API_KEY 是否设置
- 观察是否命中限流，可提高 request_delay

### Q4：没有候选股票

- 检查 data/raw 是否有最新数据
- 放宽初选阈值（如 B1 或 Brick 参数）
- 检查 pick_date 是否在有效交易日

---

## License

本项目采用 [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) 协议发布。

- 允许：学习、研究、非商业用途的使用与分发
- 禁止：任何形式的商业使用、出售或以盈利为目的的部署
- 要求：转载或引用须注明原作者与来源

Copyright © 2026 SebastienZh. All rights reserved.