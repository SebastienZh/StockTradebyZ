# Post-R7 产品硬化计划

Managing issue: #72
Parent epic: #23
Status date: 2026-06-15

本文记录 R7 完成后的下一轮产品化硬化范围：Tushare 真实端到端验收、
运行日志和失败诊断增强，以及 legacy oracle/rollback 的删除计划。它不改变
R7 的完成结论，也不是删除 legacy 的授权 PR。

## 范围

本轮纳入：

- Tushare 每日行情下载的 live acceptance 路径和脱敏记录格式；
- Run Center 失败诊断：把失败原因、建议动作和可重试性写入 run summary、
  step error 和事件流，并在前端展示；
- legacy oracle/rollback destructive cleanup 的前置证明和 issue 顺序。

本轮不纳入：

- 提交 `TUSHARE_TOKEN`、真实凭证、provider 原始秘密或生成行情快照；
- 删除 `data/`、`pipeline/Selector.py`、legacy CLI、Streamlit workbench 或
  rollback flags；
- 改变选股、复评、归档等 strict-parity 业务语义；
- 把 live Tushare 可用性变成默认 CI gate。

## Tushare Live Acceptance

默认 harness 必须保持 credential-free。真实 Tushare 验收使用显式 live 脚本：

```bash
PYTHONPATH=apps/api:src python3 scripts/harness/tushare_e2e_acceptance.py \
  --start 20260601 \
  --end today \
  --workers 1
```

脚本必须走产品 API `POST /api/runs/market-data`，不能绕回 legacy workbench 或
legacy CLI。产物写入 `var/acceptance/tushare-e2e/<timestamp>/`，该目录被
`.gitignore` 忽略。可提交的证据只能是脱敏摘要：运行状态、run id、CSV 数量、
本地最新日期、artifact 元数据和事件尾部；不得提交真实 CSV 内容或 token。

验收通过标准：

- 当前进程能看到 `TUSHARE_TOKEN`，但输出中不展示 token 值；
- FastAPI market-data endpoint 返回成功；
- run 状态为 `succeeded`；
- 至少生成一个 CSV；
- run detail 中包含 config/log artifact；
- 事件流包含配置加载、抓取开始和抓取完成。

如果 token、网络或 Tushare 频率限制导致失败，脚本仍要写出
`acceptance.json` 和 `summary.md`，状态为 `failed` 或 `skipped`，用于 issue
诊断，但不应阻塞 credential-free `quick`。

当前脱敏 live 记录见
[post-r7-tushare-live-acceptance-2026-06-15.md](post-r7-tushare-live-acceptance-2026-06-15.md)。

## 运行失败诊断

产品运行失败时，后端应写入统一结构：

```json
{
  "type": "MarketDataDownloadValidationError",
  "message": "请先设置环境变量 TUSHARE_TOKEN",
  "diagnostic": {
    "code": "market_data_missing_tushare_token",
    "title": "缺少 Tushare Token",
    "explanation": "后端进程环境中没有可用的 TUSHARE_TOKEN。",
    "next_actions": ["..."],
    "retryable": true
  }
}
```

该结构必须同时出现在：

- run summary；
- failed step 的 `error_json`；
- error 级别 job event 的可读消息中。

前端 Run Detail 必须展示诊断标题、解释、诊断代码、是否可重试和建议动作。
market-data mutation 失败后必须刷新 runs，使用户可以打开刚创建的 failed run。

## Legacy Oracle/Rollback 删除计划

R7 的“退休”只表示默认产品路径下线；永久删除需要新的 destructive cleanup
issue。删除前必须满足三类证明：

- replacement proof：React/FastAPI 产品路径覆盖同一用户工作流；
- parity proof：golden master 或 contract test 证明业务行为没有变；
- rollback proof：有产品备份、迁移验证和恢复说明，且恢复不会修改 legacy
  `data/`。

| Surface | 当前保留原因 | 删除前置条件 | 建议 issue |
| --- | --- | --- | --- |
| `data/candidates`, `data/review`, `data/history`, `data/kline`, `data/runs` | 迁移输入、事故对照、rollback 证据 | 备份/恢复演练、migration verify、产品 no-read proof、用户确认保留策略 | legacy data retention policy |
| `pipeline/Selector.py` 和 legacy formula adapter | selector 行为 oracle | 产品 selector formula reference tests 覆盖全部保留公式，且无新产品路径导入 legacy factory | selector oracle removal |
| `pipeline/cli.py` | preselect rollback 入口 | 产品 preselect API proof、候选写入 parity、rollback flag 使用记录归零或用户确认 | preselect CLI deletion |
| `pipeline/archive_results.py` | archive rollback 入口 | 产品 archive API proof、history import verify、归档状态 parity | archive writer deletion |
| `dashboard/export_kline_charts.py` | chart export rollback 入口 | 产品 artifact chart proof、浏览器可查看图表、legacy chart import 不再需要 | chart exporter deletion |
| `agent/gemini_review.py` | Gemini API reviewer rollback/parity | 产品 provider evidence proof、review/recommendation parity、API reviewer helper 不再被测试依赖 | Gemini API legacy deletion |
| `agent/gemini_cli_review.py` | Gemini CLI retry/checkpoint/raw log oracle | 产品 provider retry/checkpoint/raw-evidence proof、AGY/Gemini 迁移决策稳定 | Gemini CLI legacy deletion |
| `dashboard/app.py`, `workbench/app.py`, `workbench/runner.py`, `start_workbench` | legacy UI rollback | Run Center、Candidates、Reviews、Archive、Settings 浏览器 proof 刷新，用户确认不再需要 Streamlit | workbench deletion |
| `run_all.py` | legacy 一键 orchestration rollback | Run Center/API 全流程 live/fixture proof，子 legacy flags 已有替代路径 | run_all deletion |
| `data/trading` 和 paper trading | R7 明确 out of scope | 需要单独产品范围决策；不能混入 refactor cleanup | paper trading scope decision |

建议顺序：

1. 建立 legacy usage inventory：静态引用、文档引用、rollback flag 使用方式。
2. 先处理 generated `data/` 的保留/归档策略，不直接删除用户历史资产。
3. 删除已默认停止且无测试依赖的 legacy executable wrappers。
4. 最后处理 selector oracle 和 reviewer helper，因为它们承担 parity 证明价值。
5. 每个 destructive issue 独立 PR，PR 内必须含 rollback note 和验证输出。

## 验证

本计划相关 PR 至少运行：

```bash
PYTHON=.venv/bin/python scripts/harness/check.sh post-r7-product-hardening
PYTHON=.venv/bin/python scripts/harness/check.sh product-refactor-readiness
PYTHONPATH=apps/api:src .venv/bin/python -m unittest discover -s tests -p test_market_data_run_contracts.py
```

有 token 的本地验收可追加：

```bash
PYTHONPATH=apps/api:src .venv/bin/python scripts/harness/tushare_e2e_acceptance.py --workers 1
```
