# Validation Gates

Use the narrowest meaningful gate first, then broaden if the touched surface
requires it.

## Gate Summary

| Gate | Command | Use When |
| --- | --- | --- |
| docs | `scripts/harness/check.sh docs` | Docs, AGENTS, architecture, harness text |
| contracts | `scripts/harness/check.sh contracts` | Candidate/review/history contracts and legacy paper-trading guardrails |
| python | `scripts/harness/check.sh python` | Python source or tests changed |
| product-refactor-readiness | `scripts/harness/check.sh product-refactor-readiness` | Product rewrite plans, target-stack proposals, UI/UX/backend/storage redesign |
| refactor-readiness | `scripts/harness/check.sh refactor-readiness` | Alias for product-refactor-readiness |
| ui-smoke-fixture | `scripts/harness/check.sh ui-smoke-fixture` | R5 React workstation browser-review fixture |
| storage-cutover-plan | `scripts/harness/check.sh storage-cutover-plan` | R6 source-of-truth and rollback planning |
| r7-retirement-plan | `scripts/harness/check.sh r7-retirement-plan` | R7 hardening, resource, and legacy retirement planning |
| r7-final-retirement-proof | `scripts/harness/check.sh r7-final-retirement-proof` | R7 completion audit packet and final-cutover closure checklist |
| r7-dashboard-retirement | `scripts/harness/check.sh r7-dashboard-retirement` | R7 default retirement guard for the legacy single-stock Streamlit dashboard |
| r7-browser-proof | `scripts/harness/check.sh r7-browser-proof` | R7 desktop/mobile React workstation proof and chart artifact inspection |
| r7-gemini-api-review-retirement | `scripts/harness/check.sh r7-gemini-api-review-retirement` | R7 default retirement guard for the legacy Gemini API reviewer |
| r7-gemini-cli-review-retirement | `scripts/harness/check.sh r7-gemini-cli-review-retirement` | R7 default retirement guard for the legacy Gemini CLI reviewer |
| r7-legacy-write-freeze | `scripts/harness/check.sh r7-legacy-write-freeze` | R7 compatibility-only notices and product no-read guard for legacy generated files |
| r7-archive-retirement | `scripts/harness/check.sh r7-archive-retirement` | R7 default retirement guard for the legacy archive writer |
| r7-preselect-cli-retirement | `scripts/harness/check.sh r7-preselect-cli-retirement` | R7 default retirement guard for the legacy preselect CLI writer |
| r7-chart-export-retirement | `scripts/harness/check.sh r7-chart-export-retirement` | R7 legacy chart exporter default retirement and rollback override |
| r7-product-launcher | `scripts/harness/check.sh r7-product-launcher` | R7 default React/FastAPI local launcher and start_workbench replacement |
| r7-run-all-retirement | `scripts/harness/check.sh r7-run-all-retirement` | R7 default retirement guard for the legacy one-command orchestration wrapper |
| r7-selector-adapter-retirement | `scripts/harness/check.sh r7-selector-adapter-retirement` | R7 product selector formula factory and legacy selector adapter retirement |
| r7-workbench-retirement | `scripts/harness/check.sh r7-workbench-retirement` | R7 default retirement guard for the legacy Streamlit workbench and runner |
| r7-runtime-terminal-integrity | `scripts/harness/check.sh r7-runtime-terminal-integrity` | R7 run/step terminal-state immutability for product job diagnostics |
| r7-resource-envelope | `scripts/harness/check.sh r7-resource-envelope` | R7 credential-free runtime, memory, storage-growth, and artifact-growth evidence |
| r7-runtime-recovery | `scripts/harness/check.sh r7-runtime-recovery` | R7 FastAPI startup recovery and local product job concurrency semantics |
| post-r7-product-hardening | `scripts/harness/check.sh post-r7-product-hardening` | #72 Tushare live acceptance, runtime failure diagnostics, and legacy cleanup planning |
| quick | `scripts/harness/check.sh quick` | Default before final response |

## Docs Gate

Checks:

- `AGENTS.md` exists and links to core docs.
- `ARCHITECTURE.md` exists.
- agent harness docs are present.
- markdown links to local files resolve.

This gate should stay credential-free and safe in a clean clone.

## Contracts Gate

Checks source-level invariants that future agents must not accidentally erase:

- candidate identity preserves strategy;
- same-date candidate merge still exists;
- archive status values remain represented;
- Gemini CLI review still exposes retry/checkpoint/raw-log concepts;
- legacy paper trading still has local-only account/plan/fill surfaces so
  refactor work does not accidentally damage the out-of-scope module.

This gate is not a full business test. It is an early warning that an agent may
have removed a critical affordance.

## Python Gate

Checks:

- Python files compile.
- lightweight unit tests run when the environment has required dependencies.

Expected command:

```bash
scripts/harness/check.sh python
```

If dependencies such as pandas or numba are unavailable, report the failure and
run `docs` and `contracts` as the minimum fallback.

## Product Refactor Readiness Gate

Checks:

- full product rewrite scope is documented;
- business logic rewrite uses golden master and behavior parity rules;
- refactor execution phases R0 through R7 are documented;
- stop conditions are explicit;
- UI/UX quality bar covers accessibility, responsive layout, loading/error
  states, charts, and not-overdesigned constraints;
- architecture quality bar covers frontend, backend, storage, system,
  maintainability, observability, and runtime resource expectations;
- golden contracts include candidate identity, latest candidates,
  same-date merge, review keys, Gemini checkpoint, history archive, and run
  state;
- blocking preconditions are visible before implementation.

Expected command:

```bash
scripts/harness/check.sh product-refactor-readiness
```

Run this before broad edits, new storage layers, backend/runtime scaffolding,
UI replacement, core business logic rewrite, or deleting legacy paths.

## UI Smoke Fixture Gate

Checks:

- `scripts/harness/seed_ui_smoke.py` and `scripts/harness/ui_smoke_app.py`
  compile;
- a temporary SQLite product database can be migrated and seeded;
- a temporary DuckDB analytics database can be seeded;
- smoke artifacts for Run Center and Archive chart evidence are written.

Expected command:

```bash
scripts/harness/check.sh ui-smoke-fixture
```

This gate is credential-free and safe in a clean clone. It does not replace the
manual/browser evidence in [r5-ui-browser-smoke.md](r5-ui-browser-smoke.md).

## Storage Cutover Plan Gate

Checks:

- the R6 source-of-truth ownership matrix exists;
- simulated/paper trading is explicitly excluded;
- candidate, review, archive/history, chart artifact, provider evidence,
  backup/restore, migration, and rollback decisions are documented;
- the next implementation target is explicit before write behavior changes.

Expected command:

```bash
scripts/harness/check.sh storage-cutover-plan
```

Run this before any PR that changes product-owned storage, legacy import,
backup/restore, or legacy write-path behavior.

## R7 Retirement Plan Gate

Checks:

- the R7 issue and scope boundary are documented;
- simulated/paper trading remains explicitly excluded;
- legacy surfaces under `pipeline/`, `agent/`, `dashboard/`, `workbench`, and
  `start_workbench` have retirement decisions;
- runtime hardening, resource envelope, final browser proof, legacy write
  freeze, retirement PRs, and rollback rules are documented.

Expected command:

```bash
scripts/harness/check.sh r7-retirement-plan
```

Run this before PRs that harden product runtime, collect resource evidence,
freeze legacy writes, or retire compatibility entrypoints.

## R7 Dashboard Retirement Gate

Checks:

- `docs/agent-harness/r7-dashboard-retirement.md` documents the decision,
  rollback flag, product replacement proof, and non-goals;
- `dashboard/app.py` stops by default before loading legacy chart components or
  reading legacy generated files;
- `STOCKTRADE_ALLOW_LEGACY_DASHBOARD=1` is the only explicit rollback flag;
- the retirement does not change selection, review, archive, storage, or
  simulated trading behavior.

Expected command:

```bash
scripts/harness/check.sh r7-dashboard-retirement
```

Run this before PRs that disable or remove the legacy single-stock Streamlit
dashboard surface.

## R7 Browser Proof Gate

Checks:

- `docs/agent-harness/r7-final-browser-proof.md` documents the reproducible
  smoke setup and desktop/mobile route matrix;
- the UI smoke fixture can be seeded mechanically;
- Overview, Run Center, Candidates, Reviews, Archive, Analytics, Settings, and
  Migrations are included in the proof matrix;
- chart artifact inspection and browser console/no-overflow evidence are
  documented;
- simulated trading remains out of scope.

Expected command:

```bash
scripts/harness/check.sh r7-browser-proof
```

This gate does not automate browser control. The browser pass is recorded in
the document and must be refreshed when UI layout, route behavior, artifact
inspection, or smoke fixture state changes.

## Post-R7 Product Hardening Gate

Checks:

- `docs/agent-harness/post-r7-product-hardening.md` names issue #72 and parent
  epic #23;
- Tushare live acceptance remains explicit and outside credential-free `quick`;
- runtime failure diagnostics require structured run summary, step error, and
  event evidence;
- legacy oracle/rollback deletion remains a future destructive-cleanup sequence
  with replacement, parity, and rollback proof.

Expected command:

```bash
scripts/harness/check.sh post-r7-product-hardening
```

Run this before PRs that change live Tushare acceptance, Run Center diagnostics,
or the legacy oracle/rollback cleanup plan.

## R7 Gemini API Review Retirement Gate

Checks:

- `docs/agent-harness/r7-gemini-api-review-retirement.md` documents the
  default retirement state, rollback flag, product provider replacement, and
  non-goals;
- `agent/gemini_review.py` exits before loading legacy review config or
  constructing `GeminiReviewer` unless
  `STOCKTRADE_ALLOW_LEGACY_GEMINI_API_REVIEW=1`;
- the product replacement is `POST /api/runs/review/provider`;
- no live Gemini call is required for validation;
- simulated trading remains out of scope.

Expected command:

```bash
scripts/harness/check.sh r7-gemini-api-review-retirement
```

Run this before PRs that disable or remove the legacy Gemini API-key reviewer.

## R7 Gemini CLI Review Retirement Gate

Checks:

- `docs/agent-harness/r7-gemini-cli-review-retirement.md` documents the
  default retirement state, rollback flag, product provider replacement, and
  non-goals;
- `agent/gemini_cli_review.py` exits before loading legacy review config,
  reading candidates/charts, or writing `data/review` unless
  `STOCKTRADE_ALLOW_LEGACY_GEMINI_CLI_REVIEW=1`;
- the product replacement is `POST /api/runs/review/provider` with
  `provider=gemini-cli`;
- product provider evidence still covers retry, checkpoint, raw logs,
  `skip_existing`, result cache, and usage without a live Gemini call;
- simulated trading remains out of scope.

Expected command:

```bash
scripts/harness/check.sh r7-gemini-cli-review-retirement
```

Run this before PRs that disable or remove the legacy Gemini CLI reviewer
entrypoint.

## R7 Legacy Write Freeze Gate

Checks:

- `docs/agent-harness/r7-legacy-write-freeze.md` documents frozen legacy
  writers, product replacements, product no-read rules, and rollback;
- legacy CLI/Streamlit entrypoints emit or display `R7 legacy write freeze`
  compatibility-only notices;
- product source under `apps/api/stocktrade_api`, `apps/web/src`, and
  `src/stocktrade` does not directly reference legacy generated paths except
  through the explicit legacy import service;
- simulated trading remains out of scope.

Expected command:

```bash
scripts/harness/check.sh r7-legacy-write-freeze
```

Run this before PRs that freeze legacy writes, add new product storage paths,
or start surface-specific retirement.

## R7 Archive Writer Retirement Gate

Checks:

- `docs/agent-harness/r7-archive-retirement.md` documents the retirement
  decision, product replacement proof, rollback override, and validation;
- `pipeline/archive_results.py` exits before reading legacy candidates,
  reviews, charts, or writing `data/history` unless
  `STOCKTRADE_ALLOW_LEGACY_ARCHIVE_RESULTS=1` is set;
- the default exit explains `R7 archive writer retirement` and `POST
  /api/runs/archive`;
- helper functions remain importable for parity and migration tests;
- simulated trading remains out of scope.

Expected command:

```bash
scripts/harness/check.sh r7-archive-retirement
```

Run this before PRs that disable or remove the legacy archive writer.

## R7 Preselect CLI Retirement Gate

Checks:

- `docs/agent-harness/r7-preselect-cli-retirement.md` documents the retirement
  decision, product replacement proof, rollback override, and validation;
- `pipeline/cli.py preselect` exits before loading preselect config, loading
  market data, or writing `data/candidates` unless
  `STOCKTRADE_ALLOW_LEGACY_PRESELECT_CLI=1` is set;
- the default exit explains `R7 preselect CLI retirement` and `POST
  /api/runs/preselect`;
- simulated trading remains out of scope.

Expected command:

```bash
scripts/harness/check.sh r7-preselect-cli-retirement
```

Run this before PRs that disable or remove the legacy preselect CLI writer.

## R7 Chart Export Retirement Gate

Checks:

- `docs/agent-harness/r7-chart-export-retirement.md` documents the retirement
  decision, product replacement proof, rollback override, and validation;
- `dashboard/export_kline_charts.py` exits before reading legacy candidate files
  unless `STOCKTRADE_ALLOW_LEGACY_CHART_EXPORT=1` is set;
- the default exit explains `R7 legacy retirement` and `POST
  /api/runs/chart-export`;
- simulated trading remains out of scope.

Expected command:

```bash
scripts/harness/check.sh r7-chart-export-retirement
```

Run this before PRs that disable or remove the legacy chart export writer.

## R7 Product Launcher Gate

Checks:

- `start_product` exists and is executable;
- the launcher starts `stocktrade_api.main:app` and the React/Vite workstation
  with local host/port defaults;
- `start_workbench` points users to `./start_product` for supported
  React/FastAPI workflows;
- simulated trading remains out of scope.

Expected command:

```bash
scripts/harness/check.sh r7-product-launcher
```

Run this before PRs that change local product startup, replacement launch docs,
or `start_workbench` retirement.

## R7 Run All Retirement Gate

Checks:

- `docs/agent-harness/r7-run-all-retirement.md` documents the retirement
  decision, product replacement path, child rollback flags, and validation;
- `run_all.py` exits before invoking legacy subprocesses or reading
  `candidates_latest.json`/`suggestion.json` unless
  `STOCKTRADE_ALLOW_LEGACY_RUN_ALL=1` is set;
- the default exit explains `R7 run_all retirement`, `./start_product`, and
  product Run Center/API replacements;
- child legacy flags remain explicit and are not enabled by the wrapper;
- simulated trading remains out of scope.

Expected command:

```bash
scripts/harness/check.sh r7-run-all-retirement
```

Run this before PRs that disable or remove the legacy one-command orchestration
wrapper.

## R7 Selector Adapter Retirement Gate

Checks:

- `docs/agent-harness/r7-selector-adapter-retirement.md` documents the product
  selector factory, legacy compatibility boundary, validation, and rollback;
- product selector classes live under `src/stocktrade/domain/selection/`;
- default `LegacyPreselectExecutionPort` wiring uses
  `ProductStrategyFormulaFactoryPort`, not `LegacyStrategyFormulaFactoryPort`;
- parity tests compare product selector preparation columns against the legacy
  selector oracle;
- simulated trading remains out of scope.

Expected command:

```bash
scripts/harness/check.sh r7-selector-adapter-retirement
```

Run this before PRs that change product preselect selector defaults, retire
legacy selector adapters, or remove legacy selector compatibility.

## R7 Workbench Retirement Gate

Checks:

- `docs/agent-harness/r7-workbench-retirement.md` documents the retirement
  decision, product replacement proof, rollback flag, and validation;
- `start_workbench` exits before token lookup, Streamlit checks, or Streamlit
  launch unless `STOCKTRADE_ALLOW_LEGACY_WORKBENCH=1` is set;
- `workbench/app.py` stops before loading legacy chart or paper-trading
  dependencies;
- `workbench/runner.py` exits before reading `run_config.json`;
- simulated trading logic remains out of scope.

Expected command:

```bash
scripts/harness/check.sh r7-workbench-retirement
```

Run this before PRs that disable or remove the legacy Streamlit workbench,
background runner, or `start_workbench` entrypoint.

## R7 Runtime Terminal Integrity Gate

Checks:

- `docs/agent-harness/r7-runtime-terminal-integrity.md` documents the terminal
  state rule and validation command;
- `RunRepository.transition_run()` rejects attempts to overwrite a terminal
  run status or summary;
- `RunRepository.transition_step()` rejects attempts to overwrite a terminal
  step status or error payload;
- late cancellation preserves the existing terminal run state;
- simulated trading remains out of scope.

Expected command:

```bash
scripts/harness/check.sh r7-runtime-terminal-integrity
```

Run this before PRs that change product runtime status transitions,
cancellation, recovery, or error-diagnostics behavior.

## R7 Resource Envelope Gate

Checks:

- `docs/agent-harness/r7-resource-envelope.md` documents the reproducible
  command, conservative guardrails, and latest credential-free evidence;
- `scripts/harness/resource_envelope.py` runs the product-owned API workflow
  from temporary fixture state;
- the workflow covers preselect, chart export, provider review, archive,
  SQLite growth, DuckDB growth, artifact growth, and memory evidence;
- simulated trading and live Tushare/Gemini calls remain out of scope.

Expected command:

```bash
scripts/harness/check.sh r7-resource-envelope
```

Run this before PRs that claim resource envelope evidence, materially change
product workflow storage writes, or prepare final R7 browser/legacy retirement
proof.

## R7 Runtime Recovery Gate

Checks:

- `docs/agent-harness/r7-runtime-recovery.md` documents FastAPI startup
  recovery, local concurrency rules, unsupported multi-process writes, and
  rollback;
- `JobRuntime` exposes recovery and serializes product workflow jobs with an
  in-process workflow lock;
- `RunRepository` can recover interrupted `queued`, `running`, and `cancelling`
  runs into terminal states with `RuntimeRecovery` diagnostics;
- targeted job runtime tests cover startup recovery, serialized workflow calls,
  and late-cancellation terminal protection;
- simulated trading remains out of scope.

Expected command:

```bash
scripts/harness/check.sh r7-runtime-recovery
```

Run this before PRs that change FastAPI job lifecycle behavior, startup
recovery, local concurrency semantics, cancellation behavior, or final runtime
hardening evidence.

## R7 Final Retirement Proof Gate

Checks:

- `docs/agent-harness/r7-final-retirement-proof.md` exists and names #152;
- the audit packet explicitly says the current verdict is `complete for the
  confirmed scope`;
- every R7 proof document and gate is named in the audit packet or status map;
- completion boundaries distinguish fixture-backed proof, compatibility-only
  retirement, unsupported multi-process writes, and deletion decisions;
- final-cutover closure packet requirements include `r7-final-retirement-proof`,
  `r7-retirement-plan`, `product-refactor-readiness`, `quick`, and
  `git diff --check`.

Expected command:

```bash
scripts/harness/check.sh r7-final-retirement-proof
```

Run this before claiming R7 completion, closing #152, or publishing a final
legacy-retirement status comment.

## Runtime Gates

Runtime gates are intentionally not part of `quick` because they can require
credentials, large local data, or paid model capacity.

Run these only when the task touches the relevant path:

```bash
STOCKTRADE_ALLOW_LEGACY_PRESELECT_CLI=1 python -m pipeline.cli preselect --config config/rules_preselect.yaml --merge-same-date
STOCKTRADE_ALLOW_LEGACY_CHART_EXPORT=1 python dashboard/export_kline_charts.py
STOCKTRADE_ALLOW_LEGACY_GEMINI_CLI_REVIEW=1 python agent/gemini_cli_review.py --config config/gemini_cli_review.yaml
STOCKTRADE_ALLOW_LEGACY_ARCHIVE_RESULTS=1 python -m pipeline.archive_results
STOCKTRADE_ALLOW_LEGACY_WORKBENCH=1 ./start_workbench
STOCKTRADE_ALLOW_LEGACY_RUN_ALL=1 STOCKTRADE_ALLOW_LEGACY_PRESELECT_CLI=1 STOCKTRADE_ALLOW_LEGACY_CHART_EXPORT=1 STOCKTRADE_ALLOW_LEGACY_GEMINI_CLI_REVIEW=1 python run_all.py --skip-fetch
```

Before invoking Gemini, confirm that the task really requires model calls and
that existing `skip_existing` output cannot satisfy the validation.

## Change-Type Matrix

| Change Type | Minimum Gate | Broaden To |
| --- | --- | --- |
| README/docs only | docs | quick if links mention code contracts |
| strategy filter logic | python | runtime preselect on fixture or local data |
| candidate persistence | contracts + python | archive fixture test |
| Gemini review prompt/config | contracts + python | one small existing candidate with `skip_existing` |
| chart export | python | chart export smoke on one date |
| workbench UI | python | local Streamlit browser smoke |
| legacy paper trading rules | python | only when user explicitly reopens that scope |
| storage migration | docs + contracts + python | import/export fixture and rollback check |
| storage cutover plan | storage-cutover-plan + product-refactor-readiness | quick before PR |
| R7 planning | r7-retirement-plan + product-refactor-readiness | quick before PR |
| R7 final retirement proof | r7-final-retirement-proof + r7-retirement-plan + product-refactor-readiness | quick before PR and final-cutover closure packet |
| R7 dashboard retirement | r7-dashboard-retirement + r7-retirement-plan | targeted harness test + quick before PR |
| R7 browser proof | r7-browser-proof + web build/lint | screenshots, console/API notes, no-overflow matrix |
| R7 Gemini API reviewer retirement | r7-gemini-api-review-retirement + r7-retirement-plan | targeted harness test + quick before PR |
| R7 Gemini CLI reviewer retirement | r7-gemini-cli-review-retirement + r7-retirement-plan | targeted harness test + quick before PR |
| R7 legacy write freeze | r7-legacy-write-freeze + r7-retirement-plan | quick before PR |
| R7 archive writer retirement | r7-archive-retirement + r7-retirement-plan | targeted harness test + quick before PR |
| R7 preselect CLI retirement | r7-preselect-cli-retirement + r7-retirement-plan | targeted harness test + quick before PR |
| R7 chart export retirement | r7-chart-export-retirement + r7-retirement-plan | quick before PR |
| R7 product launcher | r7-product-launcher + r7-retirement-plan | bash syntax + targeted harness test + quick before PR |
| R7 run_all retirement | r7-run-all-retirement + r7-retirement-plan | targeted harness test + quick before PR |
| R7 selector adapter retirement | r7-selector-adapter-retirement + r7-retirement-plan | preselect domain contracts + quick before PR |
| R7 workbench retirement | r7-workbench-retirement + r7-retirement-plan | targeted harness test + quick before PR |
| R7 runtime terminal integrity | r7-runtime-terminal-integrity + r7-retirement-plan | quick before PR |
| R7 resource evidence | r7-resource-envelope + r7-retirement-plan | quick before PR |
| R7 runtime recovery | r7-runtime-recovery + r7-retirement-plan | targeted job lifecycle tests + quick before PR |
| major product refactor | product-refactor-readiness + quick | phase-specific fixture, migration proof, rollback check |
| R5 UI browser review | ui-smoke-fixture + web build/lint | browser screenshots and no-overflow notes |

## Major Refactor Matrix

| Phase | Minimum Gate | Additional Evidence |
| --- | --- | --- |
| R0 product charter | product-refactor-readiness + quick | scope, non-goals, and decision list |
| R1 business spec | contracts + python | golden masters and behavior parity notes |
| R2 target architecture | product-refactor-readiness | stack, schema, API, resource, rollback plan, and status map |
| R3 core domain rewrite | python | golden master parity tests |
| R4 backend runtime/API | python | API contract and job lifecycle tests |
| R5 frontend UI/UX | ui-smoke-fixture + python | fixture UI smoke and screenshot/browser notes |
| R6 storage cutover | storage-cutover-plan + product-refactor-readiness + quick | migration fixture and rollback drill |
| R7 hardening/retirement | r7-retirement-plan + product-refactor-readiness + quick | r7-final-retirement-proof for completion audit, r7-gemini-api-review-retirement and r7-gemini-cli-review-retirement for reviewer retirement, r7-dashboard-retirement for dashboard surface retirement, r7-chart-export-retirement, r7-archive-retirement, and r7-preselect-cli-retirement for legacy file-writer retirement, r7-run-all-retirement for one-command wrapper retirement, r7-selector-adapter-retirement for product selector defaults, r7-workbench-retirement for Streamlit workbench retirement, r7-product-launcher for local launch changes, r7-runtime-terminal-integrity and r7-runtime-recovery for job lifecycle changes, r7-resource-envelope when runtime/storage changes, r7-browser-proof for UI changes, r7-legacy-write-freeze before retirement, rollback, and final parity |
