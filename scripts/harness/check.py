#!/usr/bin/env python3
"""Lightweight agent harness checks for StockTradebyZ.

These checks are intentionally credential-free and safe in a clean clone.
They validate the repository map, important source contracts, and fast Python
health checks. Runtime checks that need Tushare, Gemini, or local data stay out
of this entrypoint.
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

REQUIRED_DOCS = [
    Path("AGENTS.md"),
    Path("ARCHITECTURE.md"),
    Path("README.md"),
    Path("docs/agent-harness/index.md"),
    Path("docs/agent-harness/harness-engineering-principles.md"),
    Path("docs/agent-harness/issue-pr-governance.md"),
    Path("docs/agent-harness/product-refactor-charter.md"),
    Path("docs/agent-harness/business-logic-spec.md"),
    Path("docs/agent-harness/refactor-execution.md"),
    Path("docs/agent-harness/refactor-contracts.md"),
    Path("docs/agent-harness/refactor-preconditions.md"),
    Path("docs/agent-harness/uiux-quality-bar.md"),
    Path("docs/agent-harness/architecture-quality-bar.md"),
    Path("docs/agent-harness/target-architecture-design.md"),
    Path("docs/agent-harness/validation-gates.md"),
    Path("docs/agent-harness/r5-ui-browser-smoke.md"),
    Path("docs/agent-harness/r6-storage-cutover-plan.md"),
    Path("docs/agent-harness/r7-hardening-retirement-plan.md"),
    Path("docs/agent-harness/r7-dashboard-retirement.md"),
    Path("docs/agent-harness/r7-final-browser-proof.md"),
    Path("docs/agent-harness/r7-final-retirement-proof.md"),
    Path("docs/agent-harness/r7-gemini-api-review-retirement.md"),
    Path("docs/agent-harness/r7-gemini-cli-review-retirement.md"),
    Path("docs/agent-harness/r7-legacy-write-freeze.md"),
    Path("docs/agent-harness/r7-archive-retirement.md"),
    Path("docs/agent-harness/r7-preselect-cli-retirement.md"),
    Path("docs/agent-harness/r7-chart-export-retirement.md"),
    Path("docs/agent-harness/r7-product-launcher.md"),
    Path("docs/agent-harness/r7-run-all-retirement.md"),
    Path("docs/agent-harness/r7-selector-adapter-retirement.md"),
    Path("docs/agent-harness/r7-workbench-retirement.md"),
    Path("docs/agent-harness/r7-runtime-terminal-integrity.md"),
    Path("docs/agent-harness/r7-resource-envelope.md"),
    Path("docs/agent-harness/r7-runtime-recovery.md"),
    Path("docs/agent-harness/post-r7-product-hardening.md"),
    Path("docs/agent-harness/workflows.md"),
    Path("docs/agent-harness/quality-scorecard.md"),
]

LOCAL_LINK_RE = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
OPTIONAL_GENERATED_LINK_PREFIXES = ("data/",)
IGNORED_PYTHON_PATH_PARTS = {"node_modules", "dist"}
LEGACY_GENERATED_PATTERNS = (
    "data/candidates",
    "data/review",
    "data/history",
    "data/kline",
    "data/runs",
    "candidates_latest.json",
    "suggestion.json",
)
PRODUCT_LEGACY_READ_ALLOWLIST = {
    Path("apps/api/stocktrade_api/services/legacy_import.py"),
    Path("apps/api/stocktrade_api/services/legacy_verify.py"),
}


class HarnessError(RuntimeError):
    pass


def rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def read(path: str | Path) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def assert_contains(path: str | Path, needles: list[str]) -> None:
    text = read(path)
    missing = [needle for needle in needles if needle not in text]
    if missing:
        raise HarnessError(f"{path} missing required text: {', '.join(missing)}")


def iter_markdown_files() -> list[Path]:
    files = [ROOT / "AGENTS.md", ROOT / "ARCHITECTURE.md", ROOT / "README.md"]
    docs_dir = ROOT / "docs"
    if docs_dir.exists():
        files.extend(sorted(docs_dir.rglob("*.md")))
    return [path for path in files if path.exists()]


def check_local_markdown_links() -> None:
    errors: list[str] = []
    for path in iter_markdown_files():
        text = path.read_text(encoding="utf-8")
        for match in LOCAL_LINK_RE.finditer(text):
            target = match.group(1).strip()
            if not target or re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:", target):
                continue
            if target.startswith("#"):
                continue
            clean = target.split("#", 1)[0]
            if not clean:
                continue
            if clean == "data" or clean.startswith(OPTIONAL_GENERATED_LINK_PREFIXES):
                continue
            linked = (path.parent / clean).resolve()
            try:
                linked.relative_to(ROOT)
            except ValueError:
                errors.append(f"{rel(path)} links outside repo: {target}")
                continue
            if not linked.exists():
                errors.append(f"{rel(path)} broken link: {target}")
    if errors:
        raise HarnessError("\n".join(errors))


def check_docs() -> None:
    missing = [rel(ROOT / path) for path in REQUIRED_DOCS if not (ROOT / path).exists()]
    if missing:
        raise HarnessError(f"missing docs: {', '.join(missing)}")

    assert_contains(
        "AGENTS.md",
        [
            "ARCHITECTURE.md",
            "docs/agent-harness/index.md",
            "docs/agent-harness/issue-pr-governance.md",
            "docs/agent-harness/product-refactor-charter.md",
            "docs/agent-harness/business-logic-spec.md",
            "docs/agent-harness/refactor-execution.md",
            "docs/agent-harness/refactor-contracts.md",
            "docs/agent-harness/refactor-preconditions.md",
            "docs/agent-harness/uiux-quality-bar.md",
            "docs/agent-harness/architecture-quality-bar.md",
            "docs/agent-harness/target-architecture-design.md",
            "docs/agent-harness/r7-hardening-retirement-plan.md",
            "docs/agent-harness/r7-final-retirement-proof.md",
            "docs/agent-harness/r7-run-all-retirement.md",
            "docs/agent-harness/r7-selector-adapter-retirement.md",
            "scripts/harness/check.sh quick",
            "scripts/harness/check.sh product-refactor-readiness",
            "(code, strategy)",
        ],
    )
    assert_contains(
        "docs/agent-harness/index.md",
        [
            "Give agents a map",
            "issue-pr-governance.md",
            "target-architecture-design.md",
            "scripts/harness/check.sh quick",
            "scripts/harness/check.sh product-refactor-readiness",
            "scripts/harness/check.sh refactor-readiness",
            "scripts/harness/check.sh r7-retirement-plan",
            "scripts/harness/check.sh r7-final-retirement-proof",
            "scripts/harness/check.sh r7-dashboard-retirement",
            "scripts/harness/check.sh r7-browser-proof",
            "scripts/harness/check.sh r7-gemini-api-review-retirement",
            "scripts/harness/check.sh r7-gemini-cli-review-retirement",
            "scripts/harness/check.sh r7-legacy-write-freeze",
            "scripts/harness/check.sh r7-archive-retirement",
            "scripts/harness/check.sh r7-preselect-cli-retirement",
            "scripts/harness/check.sh r7-chart-export-retirement",
            "scripts/harness/check.sh r7-product-launcher",
            "scripts/harness/check.sh r7-run-all-retirement",
            "scripts/harness/check.sh r7-selector-adapter-retirement",
            "scripts/harness/check.sh r7-workbench-retirement",
            "scripts/harness/check.sh r7-runtime-terminal-integrity",
            "scripts/harness/check.sh r7-resource-envelope",
            "scripts/harness/check.sh r7-runtime-recovery",
            "scripts/harness/check.sh post-r7-product-hardening",
            "Maintenance Rule",
        ],
    )
    check_local_markdown_links()
    print("[docs] ok")


def check_contracts() -> None:
    assert_contains(
        "pipeline/pipeline_io.py",
        [
            "def merge_same_date_by_strategy",
            "candidate.code, candidate.strategy",
            "candidates_latest.json",
            "merge_same_date",
        ],
    )
    assert_contains(
        "pipeline/archive_results.py",
        [
            "def review_key",
            "review_matches_strategy",
            "recommended",
            "unreviewed",
            "data/history",
        ],
    )
    assert_contains(
        "agent/gemini_cli_review.py",
        [
            "gemini_cli_review_checkpoint.json",
            "retry_backoff_seconds",
            "save_raw_cli_io",
            "skip_existing",
            "Gemini CLI raw log",
        ],
    )
    assert_contains(
        "paper_trading/core.py",
        [
            "data/trading",
            "position_key",
            "plan_path",
            "fills",
            "auto_execute_confirmed_plan",
        ],
    )
    print("[contracts] ok")


def check_product_refactor_readiness() -> None:
    check_docs()
    check_contracts()
    assert_contains(
        "docs/agent-harness/product-refactor-charter.md",
        [
            "full product-level rewrite",
            "UI/UX",
            "React/Vite/TypeScript",
            "FastAPI",
            "Core business logic remains Python",
            "frontend architecture",
            "backend architecture",
            "system architecture",
            "data architecture",
            "core business logic implementation",
        ],
    )
    assert_contains(
        "docs/agent-harness/issue-pr-governance.md",
        [
            "Every task starts with a GitHub issue",
            "Every repository change lands through a PR",
            "issue #23",
            "scripts/harness/check.sh product-refactor-readiness",
            "scripts/harness/check.sh quick",
        ],
    )
    assert_contains(
        "docs/agent-harness/business-logic-spec.md",
        [
            "Business logic means observable product behavior",
            "Golden Master Rule",
            "behavior parity",
            "(code, strategy)",
            "--merge-same-date",
            "Gemini Review",
            "Out of Scope",
        ],
    )
    assert_contains(
        "docs/agent-harness/refactor-execution.md",
        [
            "legacy system is the behavior",
            "R0 Product Charter and Decision Freeze",
            "R1 Business Logic Specification",
            "R2 Target Architecture and Data Model",
            "R3 Core Domain Rewrite",
            "R4 Backend Runtime and APIs",
            "R5 Frontend Product UI/UX",
            "R6 Data Migration and Storage Cutover",
            "R7 Hardening, Performance, and Legacy Retirement",
            "Stop Conditions",
            "rollback",
        ],
    )
    assert_contains(
        "docs/agent-harness/refactor-contracts.md",
        [
            "(code, strategy)",
            "candidates_latest.json",
            "--merge-same-date",
            "review_key",
            "suggestion.json",
            "data/history",
            "run_state.json",
            "gemini_cli_review_checkpoint.json",
            "Legacy Paper Trading State",
        ],
    )
    assert_contains(
        "docs/agent-harness/refactor-preconditions.md",
        [
            "Confirmed Decisions",
            "React + Vite + TypeScript",
            "FastAPI",
            "Core business logic remains Python",
            "SQLite",
            "DuckDB",
            "SQLAlchemy 2.x",
            "Alembic",
            "var/db/app.sqlite",
            "var/db/analytics.duckdb",
            "Remaining Confirmation Triggers",
            "strict-parity business behavior",
            "UI acceptance baseline",
            "Runtime resource envelope",
        ],
    )
    assert_contains(
        "docs/agent-harness/uiux-quality-bar.md",
        [
            "practical, attractive, modern, friendly",
            "Not overdesigned",
            "Accessible contrast",
            "Responsive layouts",
            "Loading states",
            "error states",
            "Playwright",
            "desktop screenshot",
            "mobile-width screenshot",
        ],
    )
    assert_contains(
        "docs/agent-harness/architecture-quality-bar.md",
        [
            "Frontend Architecture",
            "React + Vite + TypeScript",
            "TanStack Query",
            "TanStack Table",
            "Apache ECharts",
            "Backend Architecture",
            "FastAPI",
            "Pydantic v2",
            "SQLAlchemy 2.x",
            "Alembic",
            "Storage Architecture",
            "SQLite",
            "DuckDB",
            "var/db/app.sqlite",
            "var/db/analytics.duckdb",
            "Parquet",
            "System Architecture",
            "React web frontend plus FastAPI backend",
            "Runtime resource",
            "Observability",
            "Schema migrations",
            "External calls",
        ],
    )
    assert_contains(
        "docs/agent-harness/target-architecture-design.md",
        [
            "Issue: #29",
            "React + Vite + TypeScript",
            "FastAPI",
            "SQLite Ownership",
            "DuckDB Ownership",
            "FastAPI Route Groups",
            "Job Runtime",
            "Legacy Import and Migration",
            "Backup Format",
            "simulated trading is out of scope",
            "var/db/app.sqlite",
            "var/db/analytics.duckdb",
            "migration_quarantine",
        ],
    )
    check_r7_retirement_plan()
    check_r7_final_retirement_proof()
    print("[product-refactor-readiness] ok")


def check_refactor_readiness() -> None:
    check_product_refactor_readiness()
    print("[refactor-readiness] alias ok")


def python_files() -> list[str]:
    roots = ["agent", "apps", "dashboard", "paper_trading", "pipeline", "src", "tests"]
    files: list[str] = [
        "legacy_compat.py",
        "run_all.py",
        "scripts/harness/check.py",
        "scripts/harness/resource_envelope.py",
        "scripts/harness/seed_ui_smoke.py",
        "scripts/harness/tushare_e2e_acceptance.py",
        "scripts/harness/ui_smoke_app.py",
    ]
    for root in roots:
        root_path = ROOT / root
        if root_path.exists():
            files.extend(
                rel(path)
                for path in sorted(root_path.rglob("*.py"))
                if not (set(path.relative_to(ROOT).parts) & IGNORED_PYTHON_PATH_PARTS)
            )
    return files


def run_command(cmd: list[str], *, env: dict[str, str] | None = None) -> None:
    printable = " ".join(cmd)
    print(f"[run] {printable}", flush=True)
    result = subprocess.run(cmd, cwd=ROOT, env=env)
    if result.returncode != 0:
        raise HarnessError(f"command failed ({result.returncode}): {printable}")


def check_python() -> None:
    run_command([sys.executable, "-m", "py_compile", *python_files()])
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        [
            str(ROOT / "agent"),
            str(ROOT / "apps" / "api"),
            str(ROOT / "pipeline"),
            str(ROOT / "src"),
            env.get("PYTHONPATH", ""),
        ]
    )
    run_command([sys.executable, "-m", "unittest", "discover", "-s", "tests"], env=env)
    print("[python] ok")


def check_ui_smoke_fixture() -> None:
    run_command(
        [
            sys.executable,
            "-m",
            "py_compile",
            "scripts/harness/seed_ui_smoke.py",
            "scripts/harness/ui_smoke_app.py",
        ]
    )
    with tempfile.TemporaryDirectory(prefix="stocktrade-ui-smoke-") as tmpdir:
        tmp = Path(tmpdir)
        sqlite_path = tmp / "app.sqlite"
        duckdb_path = tmp / "analytics.duckdb"
        artifact_root = tmp / "artifacts"
        run_command(
            [
                sys.executable,
                "scripts/harness/seed_ui_smoke.py",
                "--sqlite-path",
                str(sqlite_path),
                "--duckdb-path",
                str(duckdb_path),
                "--artifact-root",
                str(artifact_root),
                "--force",
            ]
        )
        expected = [
            sqlite_path,
            duckdb_path,
            artifact_root / "run-ui-smoke-active" / "summary.txt",
            artifact_root / "run-ui-smoke-chart" / "charts" / "batch-ui-smoke" / "000001_b2.svg",
        ]
        missing = [str(path) for path in expected if not path.exists()]
        if missing:
            raise HarnessError(f"ui smoke fixture missing output: {', '.join(missing)}")
    print("[ui-smoke-fixture] ok")


def check_storage_cutover_plan() -> None:
    assert_contains(
        "docs/agent-harness/r6-storage-cutover-plan.md",
        [
            "Managing issue: #115",
            "candidate preselect results",
            "review results and recommendations",
            "provider review evidence",
            "chart artifacts",
            "archive/history snapshots",
            "backup and restore",
            "migration from legacy `data/`",
            "simulated or paper trading",
            "SQLite `candidate_batches` and `candidates`",
            "DuckDB `candidate_facts`",
            "SQLite `review_runs`, `reviews`, `recommendations`",
            "DuckDB `review_facts`",
            "SQLite `archive_snapshots` and `archive_rows`",
            "DuckDB `archive_facts`",
            "BackupService",
            "artifacts_manifest.json",
            "Cutover Sequence",
            "Rollback Rules",
            "artifact backup/restore",
        ],
    )
    print("[storage-cutover-plan] ok")


def check_r7_retirement_plan() -> None:
    assert_contains(
        "docs/agent-harness/r7-hardening-retirement-plan.md",
        [
            "Managing issue: #152",
            "R7 Hardening And Legacy Retirement Plan",
            "simulated or paper trading",
            "`data/trading`",
            "`pipeline/cli.py`",
            "`pipeline/pipeline_io.py`",
            "`pipeline/Selector.py`",
            "`agent/gemini_cli_review.py`",
            "`pipeline/archive_results.py`",
            "`dashboard/app.py`",
            "`workbench/app.py`",
            "`start_workbench`",
            "`run_all.py`",
            "Legacy Surface Matrix",
            "R7 Sequence",
            "Runtime hardening",
            "runtime recovery",
            "r7-runtime-terminal-integrity",
            "Resource envelope",
            "r7-resource-envelope",
            "Final browser proof",
            "r7-browser-proof",
            "Final retirement proof",
            "r7-final-retirement-proof",
            "Legacy write freeze",
            "r7-legacy-write-freeze",
            "Archive writer retirement",
            "r7-archive-retirement",
            "Preselect CLI retirement",
            "r7-preselect-cli-retirement",
            "Gemini API reviewer retirement",
            "r7-gemini-api-review-retirement",
            "Gemini CLI reviewer retirement",
            "r7-gemini-cli-review-retirement",
            "Dashboard retirement",
            "r7-dashboard-retirement",
            "Product launcher",
            "r7-product-launcher",
            "Run-all retirement",
            "r7-run-all-retirement",
            "Selector adapter retirement",
            "r7-selector-adapter-retirement",
            "Workbench retirement",
            "r7-workbench-retirement",
            "Retirement PRs",
            "r7-chart-export-retirement",
            "Rollback Rules",
            "scripts/harness/check.sh r7-retirement-plan",
        ],
    )
    print("[r7-retirement-plan] ok")


def check_r7_final_retirement_proof() -> None:
    assert_contains(
        "docs/agent-harness/r7-final-retirement-proof.md",
        [
            "Managing issue: #152",
            "R7 Final Retirement Proof",
            "This document is the R7 completion audit packet and final completion claim",
            "Current verdict: complete for the confirmed scope",
            "Completion Boundary Matrix",
            "Primary research workflows run through the React/FastAPI product",
            "Strict-parity business areas pass without live credentials",
            "SQLite/DuckDB are the product-owned source of truth",
            "Legacy file-system and Streamlit/workbench paths",
            "Product UI/UX passes the documented quality bar",
            "Product runtime is hardened for local use",
            "Final cutover gates pass on the closing commit",
            "Final Closure Evidence",
            "scripts/harness/check.sh r7-final-retirement-proof",
            "scripts/harness/check.sh r7-retirement-plan",
            "scripts/harness/check.sh product-refactor-readiness",
            "scripts/harness/check.sh quick",
            "git diff --check",
            "Retained Compatibility Decisions",
            "pipeline/Selector.py",
            "Do not reopen simulated or paper trading",
        ],
    )
    assert_contains(
        "docs/agent-harness/product-refactor-status.md",
        [
            "r7-final-retirement-proof.md",
            "scripts/harness/check.sh r7-final-retirement-proof",
            "R7 final retirement proof records the completion verdict",
            "Final-cutover closure requires #152 and #23 issue comments",
            "Legacy selector classes still exist in `pipeline/Selector.py`",
            "The legacy file-system state remains a compatibility and migration source",
            "multi-process writes to the same",
            "There is no default next issue",
        ],
    )
    assert_contains(
        "docs/agent-harness/validation-gates.md",
        [
            "r7-final-retirement-proof",
            "R7 Final Retirement Proof Gate",
            "completion audit packet",
            "current verdict is `complete",
            "final-cutover closure packet",
        ],
    )
    assert_contains(
        "docs/agent-harness/index.md",
        [
            "r7-final-retirement-proof.md",
            "scripts/harness/check.sh r7-final-retirement-proof",
        ],
    )
    print("[r7-final-retirement-proof] ok")


def check_r7_preselect_cli_retirement() -> None:
    assert_contains(
        "docs/agent-harness/r7-preselect-cli-retirement.md",
        [
            "Managing issue: #152",
            "R7 Preselect CLI Retirement",
            "`pipeline.cli preselect`",
            "POST /api/runs/preselect",
            "STOCKTRADE_ALLOW_LEGACY_PRESELECT_CLI=1",
            "data/candidates",
            "candidates_latest.json",
            "simulated trading logic, which remains out of scope",
            "scripts/harness/check.sh r7-preselect-cli-retirement",
            "Rollback",
        ],
    )
    assert_contains(
        "legacy_compat.py",
        [
            "LEGACY_PRESELECT_CLI_ENV",
            "STOCKTRADE_ALLOW_LEGACY_PRESELECT_CLI",
            "LEGACY_PRESELECT_CLI_RETIRED_NOTICE",
            "legacy_preselect_cli_enabled",
        ],
    )
    assert_contains(
        "pipeline/cli.py",
        [
            "legacy_preselect_cli_enabled",
            "LEGACY_PRESELECT_CLI_RETIRED_NOTICE",
            "POST /api/runs/preselect",
            "raise SystemExit(2)",
            "save_candidates(",
        ],
    )
    text = read("pipeline/cli.py")
    guard_index = text.index("if not legacy_preselect_cli_enabled()")
    for needle in ("run_preselect(", "save_candidates("):
        if guard_index > text.index(needle, guard_index):
            raise HarnessError(f"pipeline/cli.py must stop before {needle}")
    assert_contains(
        "tests/test_preselect_cli_retirement_harness.py",
        [
            "test_preselect_cli_exits_before_legacy_execution_by_default",
            "test_preselect_cli_guard_runs_before_config_load_and_candidate_write",
            "test_preselect_cli_rollback_flag_is_explicit_and_suppressed_by_default",
        ],
    )
    run_command([sys.executable, "-m", "unittest", "tests.test_preselect_cli_retirement_harness"])
    print("[r7-preselect-cli-retirement] ok")


def check_r7_archive_retirement() -> None:
    assert_contains(
        "docs/agent-harness/r7-archive-retirement.md",
        [
            "Managing issue: #152",
            "R7 Archive Writer Retirement",
            "`pipeline.archive_results`",
            "POST /api/runs/archive",
            "STOCKTRADE_ALLOW_LEGACY_ARCHIVE_RESULTS=1",
            "data/history",
            "index.json",
            "build_rows",
            "simulated trading logic, which remains out of scope",
            "scripts/harness/check.sh r7-archive-retirement",
            "Rollback",
        ],
    )
    assert_contains(
        "legacy_compat.py",
        [
            "LEGACY_ARCHIVE_RESULTS_ENV",
            "STOCKTRADE_ALLOW_LEGACY_ARCHIVE_RESULTS",
            "LEGACY_ARCHIVE_RESULTS_RETIRED_NOTICE",
            "legacy_archive_results_enabled",
        ],
    )
    assert_contains(
        "pipeline/archive_results.py",
        [
            "legacy_archive_results_enabled",
            "LEGACY_ARCHIVE_RESULTS_RETIRED_NOTICE",
            "POST /api/runs/archive",
            "raise SystemExit(2)",
            "atomic_write_json",
        ],
    )
    text = read("pipeline/archive_results.py")
    guard_index = text.index("if not legacy_archive_results_enabled()")
    for needle in ("candidates_data = load_json", 'atomic_write_json(date_dir / "all.json"'):
        needle_index = text.index(needle, guard_index)
        if guard_index > needle_index:
            raise HarnessError(f"pipeline/archive_results.py must stop before {needle}")
    assert_contains(
        "docs/history-results-archive-design.md",
        [
            "POST /api/runs/archive",
            "legacy migration/rollback",
            "STOCKTRADE_ALLOW_LEGACY_ARCHIVE_RESULTS=1",
            "archive_snapshots",
            "archive_rows",
            "/api/archive/{pick_date}",
        ],
    )
    assert_contains(
        "docs/agent-harness/r6-storage-cutover-plan.md",
        [
            "rollback-only",
            "STOCKTRADE_ALLOW_LEGACY_ARCHIVE_RESULTS=1",
            "product archive writes",
            "queries use SQLite/DuckDB",
        ],
    )
    assert_contains(
        "docs/agent-harness/refactor-contracts.md",
        [
            "Product owner: `ArchiveRunService`",
            "Legacy compatibility owner: `pipeline/archive_results.py`",
            "STOCKTRADE_ALLOW_LEGACY_ARCHIVE_RESULTS=1",
            "archive_snapshots",
            "archive_rows",
            "archive_facts",
        ],
    )
    assert_contains(
        "tests/test_archive_retirement_harness.py",
        [
            "test_archive_writer_exits_before_legacy_file_reads_by_default",
            "test_archive_guard_runs_before_candidate_read_and_history_write",
            "test_archive_helpers_remain_importable_for_parity_tests",
            "test_archive_rollback_flag_is_explicit_and_suppressed_by_default",
        ],
    )
    run_command([sys.executable, "-m", "unittest", "tests.test_archive_retirement_harness"])
    print("[r7-archive-retirement] ok")


def check_r7_gemini_api_review_retirement() -> None:
    assert_contains(
        "docs/agent-harness/r7-gemini-api-review-retirement.md",
        [
            "Managing issue: #152",
            "R7 Gemini API Review Retirement",
            "`agent/gemini_review.py`",
            "POST /api/runs/review/provider",
            "STOCKTRADE_ALLOW_LEGACY_GEMINI_API_REVIEW=1",
            "simulated trading",
            "remains out of scope",
            "scripts/harness/check.sh r7-gemini-api-review-retirement",
            "Rollback",
        ],
    )
    assert_contains(
        "legacy_compat.py",
        [
            "LEGACY_GEMINI_API_REVIEW_ENV",
            "STOCKTRADE_ALLOW_LEGACY_GEMINI_API_REVIEW",
            "LEGACY_GEMINI_API_REVIEW_RETIRED_NOTICE",
            "legacy_gemini_api_review_enabled",
        ],
    )
    assert_contains(
        "agent/gemini_review.py",
        [
            "legacy_gemini_api_review_enabled",
            "LEGACY_GEMINI_API_REVIEW_RETIRED_NOTICE",
            "POST /api/runs/review/provider",
            "return 2",
            "raise SystemExit(main())",
        ],
    )
    text = read("agent/gemini_review.py")
    if text.index("if not legacy_gemini_api_review_enabled()") > text.index("config = load_config"):
        raise HarnessError("agent/gemini_review.py must stop before loading legacy review config")
    assert_contains(
        "tests/test_gemini_api_retirement_harness.py",
        [
            "test_gemini_api_script_stops_before_legacy_review_execution_by_default",
            "test_gemini_api_script_exits_without_sdk_or_config_by_default",
            "test_gemini_api_rollback_flag_is_explicit_and_suppressed_by_default",
        ],
    )
    print("[r7-gemini-api-review-retirement] ok")


def check_r7_gemini_cli_review_retirement() -> None:
    assert_contains(
        "docs/agent-harness/r7-gemini-cli-review-retirement.md",
        [
            "Managing issue: #152",
            "R7 Gemini CLI Review Retirement",
            "`agent/gemini_cli_review.py`",
            "POST /api/runs/review/provider",
            "provider=gemini-cli",
            "STOCKTRADE_ALLOW_LEGACY_GEMINI_CLI_REVIEW=1",
            "retry/backoff",
            "skip_existing",
            "gemini_cli_review_checkpoint.json",
            "simulated trading logic, which remains out of scope",
            "scripts/harness/check.sh r7-gemini-cli-review-retirement",
            "Rollback",
        ],
    )
    assert_contains(
        "legacy_compat.py",
        [
            "LEGACY_GEMINI_CLI_REVIEW_ENV",
            "STOCKTRADE_ALLOW_LEGACY_GEMINI_CLI_REVIEW",
            "LEGACY_GEMINI_CLI_REVIEW_RETIRED_NOTICE",
            "legacy_gemini_cli_review_enabled",
        ],
    )
    assert_contains(
        "agent/gemini_cli_review.py",
        [
            "legacy_gemini_cli_review_enabled",
            "LEGACY_GEMINI_CLI_REVIEW_RETIRED_NOTICE",
            "POST /api/runs/review/provider",
            "provider=gemini-cli",
            "raise SystemExit(2)",
            "print_legacy_write_freeze_notice",
            "gemini_cli_review_checkpoint.json",
            "skip_existing",
            "retry_backoff_seconds",
        ],
    )
    text = read("agent/gemini_cli_review.py")
    guard_index = text.index("if not legacy_gemini_cli_review_enabled()")
    for needle in ("config = load_config", "reviewer.run()"):
        needle_index = text.index(needle, guard_index)
        if guard_index > needle_index:
            raise HarnessError(f"agent/gemini_cli_review.py must stop before {needle}")
    assert_contains(
        "apps/api/stocktrade_api/services/gemini_cli_provider.py",
        [
            "GeminiCliReviewProviderExecutor",
            "retry_backoff_seconds",
            "skip_existing",
            "result_cache_dir",
            "gemini_cli_review_checkpoint.json",
            "provider_evidence_files",
            "raw_stdout",
            "raw_stderr",
            "usage_file",
        ],
    )
    assert_contains(
        "tests/test_gemini_cli_provider_contracts.py",
        [
            "test_provider_retries_rate_limit_and_writes_checkpoint_raw_logs_and_usage",
            "test_provider_uses_skip_existing_result_cache_without_cli_call",
            "test_provider_rejects_batch_order_mismatch_when_fallback_is_disabled",
        ],
    )
    assert_contains(
        "tests/test_gemini_cli_retirement_harness.py",
        [
            "test_gemini_cli_reviewer_exits_before_config_load_by_default",
            "test_gemini_cli_guard_runs_before_legacy_config_and_runner",
            "test_gemini_cli_helpers_remain_importable_for_parity_tests",
            "test_gemini_cli_rollback_flag_is_explicit_and_suppressed_by_default",
        ],
    )
    assert_contains(
        "docs/gemini-cli-review-plan.md",
        [
            "POST /api/runs/review/provider",
            "provider=gemini-cli",
            "STOCKTRADE_ALLOW_LEGACY_GEMINI_CLI_REVIEW=1",
        ],
    )
    run_command([sys.executable, "-m", "unittest", "tests.test_gemini_cli_retirement_harness"])
    run_command([sys.executable, "-m", "unittest", "tests.test_gemini_cli_provider_contracts"])
    print("[r7-gemini-cli-review-retirement] ok")


def check_r7_dashboard_retirement() -> None:
    assert_contains(
        "docs/agent-harness/r7-dashboard-retirement.md",
        [
            "Managing issue: #152",
            "R7 Dashboard Retirement",
            "`dashboard/app.py` is no longer a supported product entrypoint",
            "STOCKTRADE_ALLOW_LEGACY_DASHBOARD=1",
            "docs/agent-harness/r7-final-browser-proof.md",
            "r7-legacy-write-freeze",
            "simulated trading remains out of scope",
            "scripts/harness/check.sh r7-dashboard-retirement",
            "Rollback",
        ],
    )
    assert_contains(
        "legacy_compat.py",
        [
            "LEGACY_DASHBOARD_ENV",
            "STOCKTRADE_ALLOW_LEGACY_DASHBOARD",
            "LEGACY_DASHBOARD_RETIRED_NOTICE",
            "legacy_dashboard_enabled",
        ],
    )
    assert_contains(
        "dashboard/app.py",
        [
            "legacy_dashboard_enabled",
            "LEGACY_DASHBOARD_RETIRED_NOTICE",
            "st.stop()",
            "from components.charts import make_daily_chart",
        ],
    )
    text = read("dashboard/app.py")
    if text.index("st.stop()") > text.index("from components.charts import make_daily_chart"):
        raise HarnessError("dashboard/app.py must stop before loading legacy chart components")
    assert_contains(
        "tests/test_dashboard_retirement_harness.py",
        [
            "test_dashboard_app_stops_before_loading_legacy_chart_surface_by_default",
            "test_legacy_dashboard_flag_is_explicit_and_suppressed_by_default",
        ],
    )
    print("[r7-dashboard-retirement] ok")


def check_r7_runtime_recovery() -> None:
    assert_contains(
        "docs/agent-harness/r7-runtime-recovery.md",
        [
            "Managing issue: #152",
            "R7 Runtime Recovery And Concurrency",
            "FastAPI startup recovery",
            "JobRuntime.recover_interrupted_runs",
            "RunRepository.recover_interrupted_active_runs",
            "`queued` and `running` runs recover to `failed`",
            "`cancelling` runs recover to `cancelled`",
            "RuntimeRecovery",
            "in-process workflow lock",
            "Multiple API processes",
            "simulated trading remains out of scope",
            "scripts/harness/check.sh r7-runtime-recovery",
            "Rollback",
        ],
    )
    assert_contains(
        "apps/api/stocktrade_api/jobs/runtime.py",
        [
            "RLock",
            "_workflow_lock",
            "recover_interrupted_runs",
            "with self._workflow_lock",
        ],
    )
    assert_contains(
        "apps/api/stocktrade_api/storage/run_repository.py",
        [
            "recover_interrupted_active_runs",
            "RuntimeRecovery",
            "previous_status",
            "OperationalError",
        ],
    )
    assert_contains(
        "tests/test_job_runtime_contracts.py",
        [
            "test_app_startup_recovers_interrupted_active_runs",
            "test_product_workflow_jobs_are_serialized_in_process",
            "test_runtime_does_not_cancel_terminal_run_after_late_cancellation",
        ],
    )
    print("[r7-runtime-recovery] ok")


def check_post_r7_product_hardening() -> None:
    assert_contains(
        "docs/agent-harness/post-r7-product-hardening.md",
        [
            "Managing issue: #72",
            "Parent epic: #23",
            "Tushare Live Acceptance",
            "scripts/harness/tushare_e2e_acceptance.py",
            "POST /api/runs/market-data",
            "credential-free",
            "运行失败诊断",
            "market_data_missing_tushare_token",
            "post-r7-tushare-live-acceptance-2026-06-15.md",
            "Legacy Oracle/Rollback 删除计划",
            "replacement proof",
            "parity proof",
            "rollback proof",
            "destructive cleanup",
            "`data/trading`",
        ],
    )
    assert_contains(
        "apps/api/stocktrade_api/services/diagnostics.py",
        [
            "build_failure_payload",
            "format_failure_event",
            "market_data_missing_tushare_token",
            "market_data_tushare_rate_limited",
            "market_data_network_failure",
        ],
    )
    assert_contains(
        "apps/web/src/features/app/AppShell.tsx",
        [
            "FailureDiagnosticPanel",
            "failureDiagnosticFromRun",
            "If a run was created, diagnostics are recorded in Runs.",
        ],
    )
    assert_contains(
        "docs/product-usage-manual.md",
        [
            "Tushare Live Acceptance",
            "失败诊断",
            "market_data_missing_tushare_token",
            "var/acceptance/tushare-e2e",
        ],
    )
    print("[post-r7-product-hardening] ok")


def check_r7_product_launcher() -> None:
    assert_contains(
        "docs/agent-harness/r7-product-launcher.md",
        [
            "管理 issue：#152",
            "R7 产品启动器",
            "./start_product",
            "stocktrade_api.main:app",
            "127.0.0.1:8000",
            "127.0.0.1:5173",
            "start_workbench",
            "simulated trading",
            "语言：中文 / English",
            "主题模式：跟随系统 / 浅色 / 深色",
            "scripts/harness/check.sh r7-product-launcher",
            "回滚",
        ],
    )
    launcher = ROOT / "start_product"
    if not launcher.exists():
        raise HarnessError("missing start_product")
    if not os.access(launcher, os.X_OK):
        raise HarnessError("start_product must be executable")
    assert_contains(
        "start_product",
        [
            "stocktrade_api.main:app",
            "node node_modules/vite/bin/vite.js",
            "requires Node.js 23.x",
            "STOCKTRADE_API_PORT:-8000",
            "STOCKTRADE_WEB_PORT:-5173",
            "PYTHONPATH=\"apps/api:src:${PYTHONPATH:-}\"",
            "cleanup()",
        ],
    )
    assert_contains(
        "apps/web/vite.config.ts",
        [
            "process.env.STOCKTRADE_API_HOST",
            "process.env.STOCKTRADE_API_PORT",
            "target: apiTarget",
        ],
    )
    assert_contains("start_workbench", ["R7 legacy write freeze", "./start_product", "React/FastAPI workflows"])
    assert_contains(
        "tests/test_product_launcher_harness.py",
        [
            "test_start_product_is_executable_and_targets_react_fastapi_stack",
            "test_web_dev_proxy_tracks_product_launcher_api_port",
            "test_legacy_workbench_points_to_product_launcher",
            "test_frontend_display_preferences_are_documented_and_wired",
        ],
    )
    assert_contains(
        "apps/web/src/features/app/uiPreferences.tsx",
        [
            "stocktrade.ui.language",
            "stocktrade.ui.theme",
            "document.documentElement.lang",
            "document.documentElement.dataset.theme",
        ],
    )
    assert_contains(
        "apps/web/src/features/app/AppShell.tsx",
        [
            "UiPreferenceProvider",
            "sidebar-controls",
            "Theme mode",
        ],
    )
    assert_contains("apps/web/src/index.css", ["html[data-theme=\"dark\"]", "color-scheme: dark"])
    print("[r7-product-launcher] ok")


def check_r7_run_all_retirement() -> None:
    assert_contains(
        "docs/agent-harness/r7-run-all-retirement.md",
        [
            "Managing issue: #152",
            "R7 Run All Retirement",
            "`run_all.py`",
            "./start_product",
            "Run Center",
            "POST /api/runs/preselect",
            "POST /api/runs/chart-export",
            "POST /api/runs/review/provider",
            "POST /api/runs/archive",
            "STOCKTRADE_ALLOW_LEGACY_RUN_ALL=1",
            "STOCKTRADE_ALLOW_LEGACY_PRESELECT_CLI=1",
            "STOCKTRADE_ALLOW_LEGACY_CHART_EXPORT=1",
            "STOCKTRADE_ALLOW_LEGACY_GEMINI_CLI_REVIEW=1",
            "STOCKTRADE_ALLOW_LEGACY_ARCHIVE_RESULTS=1",
            "simulated trading",
            "scripts/harness/check.sh r7-run-all-retirement",
            "Rollback",
        ],
    )
    assert_contains(
        "legacy_compat.py",
        [
            "LEGACY_RUN_ALL_ENV",
            "STOCKTRADE_ALLOW_LEGACY_RUN_ALL",
            "LEGACY_RUN_ALL_RETIRED_NOTICE",
            "legacy_run_all_enabled",
        ],
    )
    assert_contains(
        "run_all.py",
        [
            "legacy_run_all_enabled",
            "LEGACY_RUN_ALL_RETIRED_NOTICE",
            "./start_product",
            "POST /api/runs/preselect",
            "POST /api/runs/chart-export",
            "POST /api/runs/review/provider",
            "POST /api/runs/archive",
            "raise SystemExit(2)",
            "_run(",
            "_print_recommendations()",
        ],
    )
    text = read("run_all.py")
    guard_index = text.index("if not legacy_run_all_enabled()")
    for needle in ("_run(", "_print_recommendations()"):
        needle_index = text.index(needle, guard_index)
        if guard_index > needle_index:
            raise HarnessError(f"run_all.py must stop before {needle}")
    assert_contains(
        "README.md",
        [
            "./start_product",
            "Run Center",
            "STOCKTRADE_ALLOW_LEGACY_RUN_ALL=1",
            "STOCKTRADE_ALLOW_LEGACY_GEMINI_CLI_REVIEW=1",
            "POST /api/runs/review/provider",
        ],
    )
    assert_contains(
        "docs/gemini-cli-review-plan.md",
        [
            "POST /api/runs/review/provider",
            "provider=gemini-cli",
            "STOCKTRADE_ALLOW_LEGACY_RUN_ALL=1",
            "STOCKTRADE_ALLOW_LEGACY_GEMINI_CLI_REVIEW=1",
        ],
    )
    assert_contains(
        "tests/test_run_all_retirement_harness.py",
        [
            "test_run_all_exits_before_legacy_subprocesses_and_file_reads_by_default",
            "test_run_all_guard_runs_before_subprocess_calls_and_recommendation_reads",
            "test_run_all_rollback_flag_is_explicit_and_suppressed_by_default",
        ],
    )
    run_command([sys.executable, "-m", "unittest", "tests.test_run_all_retirement_harness"])
    print("[r7-run-all-retirement] ok")


def check_r7_selector_adapter_retirement() -> None:
    assert_contains(
        "docs/agent-harness/r7-selector-adapter-retirement.md",
        [
            "Managing issue: #152",
            "R7 Selector Adapter Retirement",
            "ProductStrategyFormulaFactoryPort",
            "ProductB1Selector",
            "ProductB2Selector",
            "ProductBrickChartSelector",
            "LegacyStrategyFormulaFactoryPort",
            "pipeline/Selector.py",
            "src/stocktrade/domain/selection/selectors.py",
            "simulated or paper trading",
            "scripts/harness/check.sh r7-selector-adapter-retirement",
            "Rollback",
        ],
    )
    assert_contains(
        "src/stocktrade/domain/selection/selectors.py",
        [
            "class ProductB1Selector",
            "class ProductB2Selector",
            "class ProductBrickChartSelector",
            "class ProductStrategyFormulaFactoryPort",
            "compute_b1_pick_mask",
            "compute_b2_pick_mask",
            "compute_brick_pick_mask",
            "vec_picks_from_prepared",
            "brick_growth_on_date",
        ],
    )
    assert_contains(
        "src/stocktrade/domain/selection/preselect.py",
        [
            "ProductStrategyFormulaFactoryPort",
            "LegacyStrategyFormulaFactoryPort",
            "ProductStrategySelectorPort",
            "strategy_selectors or ProductStrategySelectorPort",
        ],
    )
    text = read("src/stocktrade/domain/selection/preselect.py")
    default_index = text.index("strategy_selectors or ProductStrategySelectorPort")
    product_index = text.index("ProductStrategyFormulaFactoryPort()", default_index)
    self_module_index = text.find("LegacyStrategyFormulaFactoryPort(lambda: self.module)", default_index)
    if self_module_index != -1 and self_module_index < product_index:
        raise HarnessError("default product strategy selector must not use LegacyStrategyFormulaFactoryPort")
    assert_contains(
        "src/stocktrade/domain/selection/__init__.py",
        [
            "ProductB1Selector",
            "ProductB2Selector",
            "ProductBrickChartSelector",
            "ProductStrategyFormulaFactoryPort",
        ],
    )
    assert_contains(
        "tests/test_preselect_domain_contracts.py",
        [
            "test_product_strategy_formula_factory_matches_legacy_selector_preparation",
            "ProductStrategyFormulaFactoryPort",
            "ProductB1Selector",
            "ProductB2Selector",
            "ProductBrickChartSelector",
            "LegacyStrategyFormulaFactoryPort",
        ],
    )
    run_command([sys.executable, "-m", "unittest", "tests.test_preselect_domain_contracts"])
    print("[r7-selector-adapter-retirement] ok")


def check_r7_workbench_retirement() -> None:
    assert_contains(
        "docs/agent-harness/r7-workbench-retirement.md",
        [
            "Managing issue: #152",
            "R7 Workbench Retirement",
            "./start_product",
            "STOCKTRADE_ALLOW_LEGACY_WORKBENCH=1",
            "workbench/app.py",
            "workbench/runner.py",
            "token lookup",
            "paper_trading.core",
            "data/runs",
            "simulated trading remains out of scope",
            "scripts/harness/check.sh r7-workbench-retirement",
            "Rollback",
        ],
    )
    assert_contains(
        "legacy_compat.py",
        [
            "LEGACY_WORKBENCH_ENV",
            "STOCKTRADE_ALLOW_LEGACY_WORKBENCH",
            "LEGACY_WORKBENCH_RETIRED_NOTICE",
            "legacy_workbench_enabled",
        ],
    )
    assert_contains(
        "start_workbench",
        [
            "STOCKTRADE_ALLOW_LEGACY_WORKBENCH",
            "R7 workbench retirement",
            "./start_product",
            "exit 2",
            "streamlit run workbench/app.py",
        ],
    )
    assert_contains(
        "workbench/app.py",
        [
            "legacy_workbench_enabled",
            "LEGACY_WORKBENCH_RETIRED_NOTICE",
            "_load_workbench_dependencies",
            "from paper_trading.core import",
            "st.stop()",
        ],
    )
    assert_contains(
        "workbench/runner.py",
        [
            "legacy_workbench_enabled",
            "LEGACY_WORKBENCH_RETIRED_NOTICE",
            "return 2",
            'config = load_json(run_dir / "run_config.json")',
        ],
    )
    run_command([sys.executable, "-m", "unittest", "tests.test_workbench_retirement_harness"])
    print("[r7-workbench-retirement] ok")


def iter_product_source_files() -> list[Path]:
    roots = [
        ROOT / "apps" / "api" / "stocktrade_api",
        ROOT / "apps" / "web" / "src",
        ROOT / "src" / "stocktrade",
    ]
    suffixes = {".py", ".ts", ".tsx"}
    files: list[Path] = []
    for root in roots:
        if root.exists():
            files.extend(path for path in sorted(root.rglob("*")) if path.suffix in suffixes)
    return files


def product_legacy_read_violations() -> list[str]:
    violations: list[str] = []
    for path in iter_product_source_files():
        relative = path.relative_to(ROOT)
        if relative in PRODUCT_LEGACY_READ_ALLOWLIST:
            continue
        text = path.read_text(encoding="utf-8")
        hits = [pattern for pattern in LEGACY_GENERATED_PATTERNS if pattern in text]
        if hits:
            violations.append(f"{relative.as_posix()}: {', '.join(hits)}")
    return violations


def check_r7_legacy_write_freeze() -> None:
    assert_contains(
        "docs/agent-harness/r7-legacy-write-freeze.md",
        [
            "Managing issue: #152",
            "R7 Legacy Write Freeze",
            "compatibility-only",
            "pipeline.cli preselect",
            "agent.gemini_cli_review",
            "dashboard.export_kline_charts",
            "pipeline.archive_results",
            "workbench.runner",
            "start_workbench",
            "run_all.py",
            "Product No-Read Guard",
            "apps/api/stocktrade_api/services/legacy_import.py",
            "apps/api/stocktrade_api/services/legacy_verify.py",
            "scripts/harness/check.sh r7-legacy-write-freeze",
            "simulated trading remains out of scope",
            "Rollback",
        ],
    )
    assert_contains("legacy_compat.py", ["R7 legacy write freeze", "compatibility-only"])
    expected_notices = {
        "pipeline/cli.py": ["print_legacy_write_freeze_notice", "data/candidates"],
        "pipeline/archive_results.py": ["print_legacy_write_freeze_notice", "data/history"],
        "agent/gemini_review.py": ["print_legacy_write_freeze_notice", "data/review"],
        "agent/gemini_cli_review.py": ["print_legacy_write_freeze_notice", "data/review"],
        "dashboard/export_kline_charts.py": ["print_legacy_write_freeze_notice", "data/kline"],
        "workbench/runner.py": ["print_legacy_write_freeze_notice", "data/runs"],
        "dashboard/app.py": ["LEGACY_UI_FREEZE_NOTICE"],
        "workbench/app.py": ["LEGACY_UI_FREEZE_NOTICE"],
        "start_workbench": ["R7 legacy write freeze", "compatibility-only"],
        "run_all.py": ["LEGACY_RUN_ALL_RETIRED_NOTICE", "legacy_run_all_enabled"],
    }
    for path, needles in expected_notices.items():
        assert_contains(path, needles)
    violations = product_legacy_read_violations()
    if violations:
        raise HarnessError("product code directly references legacy generated paths:\n" + "\n".join(violations))
    print("[r7-legacy-write-freeze] ok")


def check_r7_browser_proof() -> None:
    assert_contains(
        "docs/agent-harness/r7-final-browser-proof.md",
        [
            "Managing issue: #152",
            "R7 Final Browser Proof",
            "PYTHONPATH=apps/api:src python3 scripts/harness/seed_ui_smoke.py --force",
            "node node_modules/vite/bin/vite.js --host 127.0.0.1 --port 5173",
            "scripts/harness/check.sh r7-browser-proof",
            "desktop `1440x1000` and mobile `430x932`",
            "Overview",
            "Run Center",
            "Candidates",
            "Reviews",
            "Archive",
            "Analytics",
            "Settings",
            "Migrations",
            "documentElement.scrollWidth <= documentElement.clientWidth + 1",
            "Browser console error log: empty",
            "artifact-ui-smoke-chart-000001-b2",
            "simulated trading",
            "Residual Risk",
        ],
    )
    check_ui_smoke_fixture()
    print("[r7-browser-proof] ok")


def check_r7_resource_envelope() -> None:
    assert_contains(
        "docs/agent-harness/r7-resource-envelope.md",
        [
            "Managing issue: #152",
            "R7 Resource Envelope Evidence",
            "python3 scripts/harness/resource_envelope.py",
            "scripts/harness/check.sh r7-resource-envelope",
            "credential-free",
            "preselect -> chart export -> provider review -> archive",
            "SQLite growth",
            "DuckDB growth",
            "Artifact growth",
            "Simulated trading remains out of scope",
            "Conservative Guardrails",
        ],
    )
    run_command([sys.executable, "scripts/harness/resource_envelope.py"])
    print("[r7-resource-envelope] ok")


def check_r7_chart_export_retirement() -> None:
    assert_contains(
        "docs/agent-harness/r7-chart-export-retirement.md",
        [
            "Managing issue: #152",
            "R7 Chart Export Retirement",
            "dashboard/export_kline_charts.py",
            "POST /api/runs/chart-export",
            "STOCKTRADE_ALLOW_LEGACY_CHART_EXPORT=1",
            "R7 legacy retirement",
            "Product Replacement Proof",
            "Rollback",
            "scripts/harness/check.sh r7-chart-export-retirement",
            "Simulated trading remains out of scope",
        ],
    )
    assert_contains(
        "legacy_compat.py",
        [
            "LEGACY_CHART_EXPORT_ENV",
            "STOCKTRADE_ALLOW_LEGACY_CHART_EXPORT",
            "legacy_chart_export_enabled",
            "print_legacy_chart_export_retired_notice",
        ],
    )
    assert_contains(
        "dashboard/export_kline_charts.py",
        [
            "legacy_chart_export_enabled",
            "print_legacy_chart_export_retired_notice",
            "POST /api/runs/chart-export",
            "return 2",
            "_load_chart_export_dependencies",
            "raise SystemExit(main())",
        ],
    )
    run_command([sys.executable, "-m", "unittest", "tests.test_chart_export_retirement_harness"])
    print("[r7-chart-export-retirement] ok")


def check_r7_runtime_terminal_integrity() -> None:
    assert_contains(
        "docs/agent-harness/r7-runtime-terminal-integrity.md",
        [
            "Managing issue: #152",
            "R7 Runtime Terminal Integrity",
            "`succeeded`, `failed`, or `cancelled`",
            "TerminalRunTransitionError",
            "TerminalStepTransitionError",
            "late cancellation",
            "scripts/harness/check.sh r7-runtime-terminal-integrity",
            "Simulated trading remains out of scope",
        ],
    )
    assert_contains(
        "apps/api/stocktrade_api/storage/run_repository.py",
        [
            "TerminalRunTransitionError",
            "TerminalStepTransitionError",
            "run.status in TERMINAL_STATUSES",
            "step.status in TERMINAL_STATUSES",
            "already terminal",
        ],
    )
    run_command([sys.executable, "-m", "unittest", "tests.test_job_runtime_contracts"])
    print("[r7-runtime-terminal-integrity] ok")


def check_quick() -> None:
    check_docs()
    check_contracts()
    check_python()
    print("[quick] ok")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="StockTradebyZ agent harness checks")
    parser.add_argument(
        "gate",
        choices=[
            "docs",
            "contracts",
            "python",
            "product-refactor-readiness",
            "refactor-readiness",
            "ui-smoke-fixture",
            "storage-cutover-plan",
            "r7-retirement-plan",
            "r7-final-retirement-proof",
            "r7-dashboard-retirement",
            "r7-browser-proof",
            "r7-gemini-api-review-retirement",
            "r7-gemini-cli-review-retirement",
            "r7-legacy-write-freeze",
            "r7-archive-retirement",
            "r7-preselect-cli-retirement",
            "r7-chart-export-retirement",
            "r7-product-launcher",
            "r7-run-all-retirement",
            "r7-selector-adapter-retirement",
            "r7-workbench-retirement",
            "r7-runtime-terminal-integrity",
            "r7-resource-envelope",
            "r7-runtime-recovery",
            "post-r7-product-hardening",
            "quick",
        ],
        help="Validation gate to run",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.gate == "docs":
            check_docs()
        elif args.gate == "contracts":
            check_contracts()
        elif args.gate == "python":
            check_python()
        elif args.gate == "product-refactor-readiness":
            check_product_refactor_readiness()
        elif args.gate == "refactor-readiness":
            check_refactor_readiness()
        elif args.gate == "ui-smoke-fixture":
            check_ui_smoke_fixture()
        elif args.gate == "storage-cutover-plan":
            check_storage_cutover_plan()
        elif args.gate == "r7-retirement-plan":
            check_r7_retirement_plan()
        elif args.gate == "r7-final-retirement-proof":
            check_r7_final_retirement_proof()
        elif args.gate == "r7-dashboard-retirement":
            check_r7_dashboard_retirement()
        elif args.gate == "r7-browser-proof":
            check_r7_browser_proof()
        elif args.gate == "r7-gemini-api-review-retirement":
            check_r7_gemini_api_review_retirement()
        elif args.gate == "r7-gemini-cli-review-retirement":
            check_r7_gemini_cli_review_retirement()
        elif args.gate == "r7-legacy-write-freeze":
            check_r7_legacy_write_freeze()
        elif args.gate == "r7-archive-retirement":
            check_r7_archive_retirement()
        elif args.gate == "r7-preselect-cli-retirement":
            check_r7_preselect_cli_retirement()
        elif args.gate == "r7-chart-export-retirement":
            check_r7_chart_export_retirement()
        elif args.gate == "r7-product-launcher":
            check_r7_product_launcher()
        elif args.gate == "r7-run-all-retirement":
            check_r7_run_all_retirement()
        elif args.gate == "r7-selector-adapter-retirement":
            check_r7_selector_adapter_retirement()
        elif args.gate == "r7-workbench-retirement":
            check_r7_workbench_retirement()
        elif args.gate == "r7-runtime-terminal-integrity":
            check_r7_runtime_terminal_integrity()
        elif args.gate == "r7-resource-envelope":
            check_r7_resource_envelope()
        elif args.gate == "r7-runtime-recovery":
            check_r7_runtime_recovery()
        elif args.gate == "post-r7-product-hardening":
            check_post_r7_product_hardening()
        elif args.gate == "quick":
            check_quick()
        else:
            raise HarnessError(f"unknown gate: {args.gate}")
    except HarnessError as exc:
        print(f"[harness] failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
