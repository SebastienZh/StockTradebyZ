# Post-R7 Tushare Live Acceptance Record

Managing issue: #72
Parent epic: #23
Run date: 2026-06-15

本记录是一次脱敏的 Tushare 真实端到端验收摘要。真实 CSV、SQLite、log artifact
和有效配置文件保留在本机 `var/acceptance/` 下，不提交到仓库。

## Command

```bash
PYTHONPATH=apps/api:src .venv/bin/python scripts/harness/tushare_e2e_acceptance.py \
  --start 20260601 \
  --end today \
  --workers 1 \
  --symbol 000001.SZ:000001:PingAnBank
```

## Result

| Field | Value |
| --- | --- |
| Status | passed |
| HTTP status | 200 |
| Product run id | `52747f550c7e4cc5af1e8c275fc69d9e` |
| Product run status | succeeded |
| Started at | 2026-06-15T09:59:08Z |
| Finished at | 2026-06-15T10:00:03Z |
| Duration | 55.054 seconds |
| Sample symbols | `000001.SZ` |
| Date range | 20260601 to today |
| Workers | 1 |
| CSV files | 1 |
| Local latest date | 2026-06-15 |
| Artifact kinds | config, log |
| Local record | `var/acceptance/tushare-e2e/20260615T095908Z/acceptance.json` |

## Event Tail

- Market data download queued.
- Market data download started.
- Market data config loaded from the temporary acceptance config.
- Market data output and log path were registered under `var/acceptance/`.
- Market data fetch started for `20260601` to `today`.
- Market data fetch finished with 1 CSV file and latest local date `2026-06-15`.
- Market data download completed with 1 CSV file.

## Scope Notes

- This validates the product FastAPI path `POST /api/runs/market-data`; it does
  not use the legacy workbench or legacy CLI.
- This is a small live acceptance sample, not a full 2019-to-present backfill.
- Token values, generated CSV contents, provider raw secrets, and local runtime
  databases are intentionally excluded from this document.
