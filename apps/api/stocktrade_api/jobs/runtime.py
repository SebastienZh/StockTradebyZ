from __future__ import annotations

from threading import RLock
from typing import TYPE_CHECKING

from ..services.cancellation import WorkflowCancellationRequested
from ..services.diagnostics import build_failure_payload, format_failure_event
from ..storage.run_repository import TERMINAL_STATUSES, RunRepository
from ..storage.sqlite_models import CandidateBatch, Run

if TYPE_CHECKING:
    from stocktrade.domain.selection import PreselectParameters, PreselectResult, PreselectService

    from ..schemas.archive import ArchiveRunCreateRequest
    from ..schemas.charts import ChartExportRunCreateRequest
    from ..schemas.market_data import MarketDataRunRequest
    from ..schemas.reviews import ReviewRunCreateRequest
    from ..services.archive_runs import ArchiveRunService
    from ..services.chart_runs import ChartExportRunService, CreatedChartExport
    from ..services.market_data_runs import CreatedMarketDataDownload, MarketDataDownloadService
    from ..services.review_runs import ReviewRunService
    from ..storage.archive_repository import CreatedArchive
    from ..storage.duckdb import DuckDBAnalyticsWriter
    from ..storage.review_repository import CreatedReviewRun

__all__ = ["JobRuntime"]


class JobRuntime:
    def __init__(self, repository: RunRepository):
        self.repository = repository
        self._workflow_lock = RLock()

    def recover_interrupted_runs(self, *, reason: str = "FastAPI startup recovery") -> list[Run]:
        return self.repository.recover_interrupted_active_runs(reason=reason)

    def queue_diagnostic_job(self) -> Run:
        run = self.repository.create_run(
            kind="diagnostic",
            summary={"mode": "diagnostic", "message": "queued without executing legacy business logic"},
        )
        self.repository.append_event(run.id, message="Diagnostic job queued")
        return run

    def run_diagnostic_job(self, *, fail: bool = False) -> Run:
        with self._workflow_lock:
            return self._run_diagnostic_job(fail=fail)

    def _run_diagnostic_job(self, *, fail: bool = False) -> Run:
        run = self.queue_diagnostic_job()
        step = self.repository.add_step(run.id, name="diagnostic")
        self.repository.transition_run(run.id, status="running")
        self.repository.transition_step(step.id, status="running")
        self.repository.append_event(run.id, step_id=step.id, message="Diagnostic job started")

        if fail:
            error = {"type": "DiagnosticFailure", "message": "diagnostic failure requested"}
            self.repository.transition_step(step.id, status="failed", error=error)
            self.repository.append_event(run.id, step_id=step.id, level="error", message=error["message"])
            return self.repository.transition_run(run.id, status="failed", summary=error)

        self.repository.transition_step(step.id, status="succeeded")
        self.repository.append_event(run.id, step_id=step.id, message="Diagnostic job succeeded")
        return self.repository.transition_run(
            run.id,
            status="succeeded",
            summary={"mode": "diagnostic", "message": "completed without executing legacy business logic"},
        )

    def request_cancellation(self, run_id: str) -> Run:
        return self.repository.request_cancellation(run_id)

    def mark_cancelled(self, run_id: str) -> Run:
        run = self.repository.get_run(run_id)
        if run.status in TERMINAL_STATUSES:
            return run
        self.repository.append_event(run_id, level="warning", message="Diagnostic job cancelled")
        return self.repository.transition_run(
            run_id,
            status="cancelled",
            summary={"mode": "diagnostic", "message": "cancelled before execution"},
        )

    def _cancellation_check(self, run_id: str):
        return lambda: self.repository.is_cancellation_requested(run_id)

    def _raise_if_cancelled(self, run_id: str, *, mode: str) -> None:
        if self.repository.is_cancellation_requested(run_id):
            raise WorkflowCancellationRequested(f"{mode} workflow cancelled by user request")

    def _mark_workflow_cancelled(self, run_id: str, step_id: int, *, mode: str) -> Run:
        self.repository.transition_step(
            step_id,
            status="cancelled",
            error={"type": "WorkflowCancellationRequested", "message": "cancelled by user request"},
        )
        self.repository.append_event(run_id, step_id=step_id, level="warning", message=f"{mode} workflow cancelled")
        return self.repository.transition_run(
            run_id,
            status="cancelled",
            summary={"mode": mode, "message": "cancelled by user request"},
        )

    def _mark_workflow_failed(self, run_id: str, step_id: int, *, mode: str, exc: BaseException) -> Run:
        error = build_failure_payload(exc, mode=mode)
        self.repository.transition_step(step_id, status="failed", error=error)
        self.repository.append_event(run_id, step_id=step_id, level="error", message=format_failure_event(error))
        return self.repository.transition_run(run_id, status="failed", summary=error)

    def run_preselect_job(
        self,
        parameters: "PreselectParameters",
        *,
        service: "PreselectService",
        analytics_writer: "DuckDBAnalyticsWriter | None" = None,
    ) -> tuple[Run, CandidateBatch, "PreselectResult"]:
        with self._workflow_lock:
            return self._run_preselect_job(parameters, service=service, analytics_writer=analytics_writer)

    def _run_preselect_job(
        self,
        parameters: "PreselectParameters",
        *,
        service: "PreselectService",
        analytics_writer: "DuckDBAnalyticsWriter | None" = None,
    ) -> tuple[Run, CandidateBatch, "PreselectResult"]:
        run = self.repository.create_run(
            kind="preselect",
            pick_date=parameters.pick_date,
            summary={"mode": "preselect", "message": "queued"},
        )
        step = self.repository.add_step(run.id, name="preselect")
        self.repository.append_event(run.id, message="Preselect job queued")
        self.repository.transition_run(run.id, status="running")
        self.repository.transition_step(step.id, status="running")
        self.repository.append_event(run.id, step_id=step.id, message="Preselect job started")

        try:
            self._raise_if_cancelled(run.id, mode="preselect")
            result = service.run(parameters)
            self._raise_if_cancelled(run.id, mode="preselect")
            batch = self.repository.create_candidate_batch(run_id=run.id, result=result)
            self._raise_if_cancelled(run.id, mode="preselect")
            if analytics_writer is not None:
                analytics_writer.record_candidate_import(
                    run_id=run.id,
                    batch=batch,
                    candidates=batch.candidates,
                )
            self._raise_if_cancelled(run.id, mode="preselect")
            self.repository.transition_step(step.id, status="succeeded")
            self.repository.append_event(
                run.id,
                step_id=step.id,
                message=f"Preselect job selected {len(result.candidates)} candidates",
            )
            final_run = self.repository.transition_run(
                run.id,
                status="succeeded",
                pick_date=result.pick_date,
                summary=result.meta,
            )
            return final_run, batch, result
        except WorkflowCancellationRequested:
            self._mark_workflow_cancelled(run.id, step.id, mode="preselect")
            raise
        except Exception as exc:
            self._mark_workflow_failed(run.id, step.id, mode="preselect", exc=exc)
            raise

    def run_market_data_job(
        self,
        request: "MarketDataRunRequest",
        *,
        service: "MarketDataDownloadService",
    ) -> tuple[Run, "CreatedMarketDataDownload"]:
        with self._workflow_lock:
            return self._run_market_data_job(request, service=service)

    def _run_market_data_job(
        self,
        request: "MarketDataRunRequest",
        *,
        service: "MarketDataDownloadService",
    ) -> tuple[Run, "CreatedMarketDataDownload"]:
        run = self.repository.create_run(
            kind="market_data",
            summary={
                "mode": "market_data",
                "config_path": request.config_path,
                "start": request.start,
                "end": request.end,
                "out_dir": request.out_dir,
                "message": "queued",
            },
        )
        step = self.repository.add_step(run.id, name="market_data")
        self.repository.append_event(run.id, message="Market data download queued")
        self.repository.transition_run(run.id, status="running")
        self.repository.transition_step(step.id, status="running")
        self.repository.append_event(run.id, step_id=step.id, message="Market data download started")

        try:
            self._raise_if_cancelled(run.id, mode="market_data")
            created = service.run(run_id=run.id, request=request, should_cancel=self._cancellation_check(run.id))
            self._raise_if_cancelled(run.id, mode="market_data")
            self.repository.transition_step(step.id, status="succeeded")
            self.repository.append_event(
                run.id,
                step_id=step.id,
                message=f"Market data download completed with {created.summary.get('csv_file_count', 0)} CSV files",
            )
            final_run = self.repository.transition_run(
                run.id,
                status="succeeded",
                summary=created.summary,
            )
            return final_run, created
        except WorkflowCancellationRequested:
            self._mark_workflow_cancelled(run.id, step.id, mode="market_data")
            raise
        except Exception as exc:
            self._mark_workflow_failed(run.id, step.id, mode="market_data", exc=exc)
            raise

    def run_review_job(
        self,
        request: "ReviewRunCreateRequest",
        *,
        service: "ReviewRunService",
    ) -> tuple[Run, "CreatedReviewRun"]:
        with self._workflow_lock:
            return self._run_review_job(request, service=service)

    def _run_review_job(
        self,
        request: "ReviewRunCreateRequest",
        *,
        service: "ReviewRunService",
    ) -> tuple[Run, "CreatedReviewRun"]:
        run = self.repository.create_run(
            kind="review",
            summary={
                "mode": "review",
                "candidate_batch_id": request.candidate_batch_id,
                "message": "queued",
            },
        )
        step = self.repository.add_step(run.id, name="review")
        self.repository.append_event(run.id, message="Review job queued")
        self.repository.transition_run(run.id, status="running")
        self.repository.transition_step(step.id, status="running")
        self.repository.append_event(run.id, step_id=step.id, message="Review job started")

        try:
            self._raise_if_cancelled(run.id, mode="review")
            created = service.run(run_id=run.id, request=request, should_cancel=self._cancellation_check(run.id))
            self._raise_if_cancelled(run.id, mode="review")
            self.repository.transition_step(step.id, status="succeeded")
            self.repository.append_event(
                run.id,
                step_id=step.id,
                message=f"Review job recorded {len(created.reviews)} reviews",
            )
            final_run = self.repository.transition_run(
                run.id,
                status="succeeded",
                pick_date=created.review_run.pick_date,
                summary=created.review_run.summary_json,
            )
            return final_run, created
        except WorkflowCancellationRequested:
            self._mark_workflow_cancelled(run.id, step.id, mode="review")
            raise
        except Exception as exc:
            self._mark_workflow_failed(run.id, step.id, mode="review", exc=exc)
            raise

    def run_archive_job(
        self,
        request: "ArchiveRunCreateRequest",
        *,
        service: "ArchiveRunService",
    ) -> tuple[Run, "CreatedArchive"]:
        with self._workflow_lock:
            return self._run_archive_job(request, service=service)

    def _run_archive_job(
        self,
        request: "ArchiveRunCreateRequest",
        *,
        service: "ArchiveRunService",
    ) -> tuple[Run, "CreatedArchive"]:
        run = self.repository.create_run(
            kind="archive",
            summary={
                "mode": "archive",
                "candidate_batch_id": request.candidate_batch_id,
                "review_run_id": request.review_run_id,
                "message": "queued",
            },
        )
        step = self.repository.add_step(run.id, name="archive")
        self.repository.append_event(run.id, message="Archive job queued")
        self.repository.transition_run(run.id, status="running")
        self.repository.transition_step(step.id, status="running")
        self.repository.append_event(run.id, step_id=step.id, message="Archive job started")

        try:
            self._raise_if_cancelled(run.id, mode="archive")
            created = service.run(run_id=run.id, request=request, should_cancel=self._cancellation_check(run.id))
            self._raise_if_cancelled(run.id, mode="archive")
            self.repository.transition_step(step.id, status="succeeded")
            self.repository.append_event(
                run.id,
                step_id=step.id,
                message=f"Archive job recorded {len(created.rows)} rows",
            )
            final_run = self.repository.transition_run(
                run.id,
                status="succeeded",
                pick_date=created.snapshot.pick_date,
                summary=created.snapshot.summary_json,
            )
            return final_run, created
        except WorkflowCancellationRequested:
            self._mark_workflow_cancelled(run.id, step.id, mode="archive")
            raise
        except Exception as exc:
            self._mark_workflow_failed(run.id, step.id, mode="archive", exc=exc)
            raise

    def run_chart_export_job(
        self,
        request: "ChartExportRunCreateRequest",
        *,
        service: "ChartExportRunService",
    ) -> tuple[Run, "CreatedChartExport"]:
        with self._workflow_lock:
            return self._run_chart_export_job(request, service=service)

    def _run_chart_export_job(
        self,
        request: "ChartExportRunCreateRequest",
        *,
        service: "ChartExportRunService",
    ) -> tuple[Run, "CreatedChartExport"]:
        run = self.repository.create_run(
            kind="chart_export",
            summary={
                "mode": "chart_export",
                "candidate_batch_id": request.candidate_batch_id,
                "message": "queued",
            },
        )
        step = self.repository.add_step(run.id, name="chart_export")
        self.repository.append_event(run.id, message="Chart export job queued")
        self.repository.transition_run(run.id, status="running")
        self.repository.transition_step(step.id, status="running")
        self.repository.append_event(run.id, step_id=step.id, message="Chart export job started")

        try:
            self._raise_if_cancelled(run.id, mode="chart_export")
            created = service.run(run_id=run.id, request=request, should_cancel=self._cancellation_check(run.id))
            self._raise_if_cancelled(run.id, mode="chart_export")
            self.repository.transition_step(step.id, status="succeeded")
            self.repository.append_event(
                run.id,
                step_id=step.id,
                message=f"Chart export job generated {len(created.artifacts)} artifacts",
            )
            final_run = self.repository.transition_run(
                run.id,
                status="succeeded",
                pick_date=str(created.summary.get("pick_date") or ""),
                summary=created.summary,
            )
            return final_run, created
        except WorkflowCancellationRequested:
            self._mark_workflow_cancelled(run.id, step.id, mode="chart_export")
            raise
        except Exception as exc:
            self._mark_workflow_failed(run.id, step.id, mode="chart_export", exc=exc)
            raise
