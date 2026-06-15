# Agent Harness Index

This directory defines how agents should work in StockTradebyZ. It translates
the harness-engineering ideas from OpenAI's article into repo-specific,
verifiable rules.

## Source Inspiration

OpenAI's [harness engineering article](https://openai.com/zh-Hans-CN/index/harness-engineering/)
argues that agent effectiveness depends on the environment: clear intent,
readable systems, feedback loops, repository knowledge, mechanical architecture
constraints, and recurring cleanup. The project-specific interpretation here is:

- Give agents a map, not a giant manual.
- Store durable product and architecture knowledge in the repo.
- Make important checks executable.
- Preserve observability for local workflows.
- Turn repeated review feedback into docs or harness rules.
- Prefer boring, inspectable interfaces over hidden magic.

## Files

- [../../AGENTS.md](../../AGENTS.md): short entry map injected into agent context.
- [../../ARCHITECTURE.md](../../ARCHITECTURE.md): high-level domain and storage map.
- [harness-engineering-principles.md](harness-engineering-principles.md):
  repo-specific interpretation of the article.
- [issue-pr-governance.md](issue-pr-governance.md): issue and PR rules for all
  refactor work.
- [product-refactor-charter.md](product-refactor-charter.md): scope and target
  qualities for the full product rewrite.
- [product-refactor-status.md](product-refactor-status.md): current phase
  status, implemented evidence, architectural gaps, and next issue queue.
- [business-logic-spec.md](business-logic-spec.md): behavior parity and golden
  master rules for rewriting core business logic in a new stack.
- [refactor-execution.md](refactor-execution.md): phase model and stop
  conditions for the product-level rewrite.
- [refactor-contracts.md](refactor-contracts.md): compatibility contracts that
  must survive or be explicitly migrated during the rewrite.
- [refactor-preconditions.md](refactor-preconditions.md): confirmed decisions,
  agent-owned defaults, and cases that still need user confirmation before
  destructive implementation work.
- [uiux-quality-bar.md](uiux-quality-bar.md): measurable UI/UX acceptance bar.
- [architecture-quality-bar.md](architecture-quality-bar.md): frontend,
  backend, system, storage, maintainability, and resource quality bar.
- [target-architecture-design.md](target-architecture-design.md): Phase 2
  SQLite/DuckDB schema ownership, API contract, job runtime, migration, and
  rollback design.
- [validation-gates.md](validation-gates.md): what to run for each change type.
- [r5-ui-browser-smoke.md](r5-ui-browser-smoke.md): deterministic browser-review
  fixture and evidence checklist for #114 R5 UI/UX validation.
- [r6-storage-cutover-plan.md](r6-storage-cutover-plan.md): source-of-truth
  ownership, cutover sequence, validation, and rollback plan for #115.
- [r7-hardening-retirement-plan.md](r7-hardening-retirement-plan.md): runtime
  hardening, resource evidence, and legacy retirement plan for #152.
- [r7-dashboard-retirement.md](r7-dashboard-retirement.md): default retirement
  guard and rollback flag for the legacy Streamlit single-stock dashboard.
- [r7-final-browser-proof.md](r7-final-browser-proof.md): desktop/mobile
  browser proof for R7 product shell routes and chart artifact inspection.
- [r7-final-retirement-proof.md](r7-final-retirement-proof.md): completion
  audit packet, final-cutover validation checklist, and remaining-blocker map.
- [r7-gemini-api-review-retirement.md](r7-gemini-api-review-retirement.md):
  default retirement guard and rollback flag for the legacy Gemini API reviewer.
- [r7-gemini-cli-review-retirement.md](r7-gemini-cli-review-retirement.md):
  default retirement guard and rollback flag for the legacy Gemini CLI reviewer.
- [r7-legacy-write-freeze.md](r7-legacy-write-freeze.md): compatibility-only
  notices and product no-read guard for legacy file-system writers.
- [r7-archive-retirement.md](r7-archive-retirement.md): default retirement
  guard and rollback flag for the legacy `data/history` archive writer.
- [r7-preselect-cli-retirement.md](r7-preselect-cli-retirement.md): default
  retirement guard and rollback flag for the legacy preselect CLI writer.
- [r7-chart-export-retirement.md](r7-chart-export-retirement.md): default
  retirement, rollback override, and product replacement proof for the legacy
  chart export writer.
- [r7-product-launcher.md](r7-product-launcher.md): default local
  React/FastAPI launcher and replacement path for `start_workbench`.
- [r7-run-all-retirement.md](r7-run-all-retirement.md): default retirement
  guard and rollback flag for the legacy `run_all.py` orchestration wrapper.
- [r7-selector-adapter-retirement.md](r7-selector-adapter-retirement.md):
  product-owned selector formula factory and default retirement of legacy
  selector compatibility adapters.
- [r7-workbench-retirement.md](r7-workbench-retirement.md): default retirement
  guard and rollback flag for the legacy Streamlit workbench and runner.
- [r7-runtime-terminal-integrity.md](r7-runtime-terminal-integrity.md):
  terminal run/step immutability rules for R7 product job diagnostics.
- [r7-resource-envelope.md](r7-resource-envelope.md): credential-free runtime,
  memory, SQLite/DuckDB growth, and artifact-growth evidence for #152.
- [r7-runtime-recovery.md](r7-runtime-recovery.md): FastAPI startup recovery
  and local product job concurrency semantics for #152.
- [post-r7-product-hardening.md](post-r7-product-hardening.md): #72 Tushare
  live acceptance, runtime failure diagnostics, and legacy oracle/rollback
  destructive-cleanup plan.
- [workflows.md](workflows.md): repeatable agent workflows.
- [quality-scorecard.md](quality-scorecard.md): review scorecard for harness quality.

## Executable Entry Points

```bash
scripts/harness/check.sh quick
scripts/harness/check.sh product-refactor-readiness
scripts/harness/check.sh refactor-readiness
scripts/harness/check.sh ui-smoke-fixture
scripts/harness/check.sh storage-cutover-plan
scripts/harness/check.sh r7-retirement-plan
scripts/harness/check.sh r7-final-retirement-proof
scripts/harness/check.sh r7-dashboard-retirement
scripts/harness/check.sh r7-browser-proof
scripts/harness/check.sh r7-gemini-api-review-retirement
scripts/harness/check.sh r7-gemini-cli-review-retirement
scripts/harness/check.sh r7-legacy-write-freeze
scripts/harness/check.sh r7-archive-retirement
scripts/harness/check.sh r7-preselect-cli-retirement
scripts/harness/check.sh r7-chart-export-retirement
scripts/harness/check.sh r7-product-launcher
scripts/harness/check.sh r7-run-all-retirement
scripts/harness/check.sh r7-selector-adapter-retirement
scripts/harness/check.sh r7-workbench-retirement
scripts/harness/check.sh r7-runtime-terminal-integrity
scripts/harness/check.sh r7-resource-envelope
scripts/harness/check.sh r7-runtime-recovery
scripts/harness/check.sh post-r7-product-hardening
scripts/harness/check.sh docs
scripts/harness/check.sh contracts
scripts/harness/check.sh python
```

The harness is intentionally lightweight. It should be safe in a clean clone and
should not require Tushare, Gemini credentials, or local `data/` artifacts. The
`product-refactor-readiness` gate is the required preflight for full product
rewrites, target-stack proposals, UI/UX replacement, backend redesign, storage
redesign, and core business logic rewrites.

## Maintenance Rule

When an agent or reviewer hits the same failure pattern twice, add one of:

- a short rule in `AGENTS.md` if it is context-critical;
- a durable explanation in `docs/agent-harness/`;
- a mechanical check in `scripts/harness/`;
- a focused regression test in `tests/`.
