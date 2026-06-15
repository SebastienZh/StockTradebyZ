from __future__ import annotations

import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

import httpx
import yaml
from alembic import command
from alembic.config import Config

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "apps" / "api"))
sys.path.insert(0, str(ROOT / "src"))

from stocktrade_api.dependencies import get_market_data_service  # noqa: E402
from stocktrade_api.main import create_app  # noqa: E402
from stocktrade_api.schemas.market_data import MarketDataRunRequest  # noqa: E402
from stocktrade_api.services.market_data_runs import (  # noqa: E402
    CreatedMarketDataDownload,
    MarketDataDownloadService,
    MarketDataDownloadValidationError,
)
from stocktrade_api.storage.run_repository import RunRepository  # noqa: E402
from stocktrade_api.storage.sqlite import create_session_factory, create_sqlite_engine  # noqa: E402

SQLITE_MIGRATIONS = ROOT / "apps" / "api" / "stocktrade_api" / "migrations" / "sqlite"


def migrate_sqlite(db_path: Path) -> None:
    config = Config()
    config.set_main_option("script_location", str(SQLITE_MIGRATIONS))
    config.set_main_option("sqlalchemy.url", f"sqlite:///{db_path}")
    command.upgrade(config, "head")


def repository_for(db_path: Path) -> tuple[RunRepository, object]:
    engine = create_sqlite_engine(db_path)
    return RunRepository(create_session_factory(engine)), engine


class MarketDataRunApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_market_data_endpoint_records_product_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            db_path = tmp / "app.sqlite"
            migrate_sqlite(db_path)
            app = create_app(sqlite_path=db_path, duckdb_path=None, artifact_root=tmp / "artifacts")

            class FakeMarketDataService:
                def run(self, *, run_id: str, request: MarketDataRunRequest, should_cancel=None) -> CreatedMarketDataDownload:
                    self.run_id = run_id
                    self.request = request
                    self.cancel_requested = bool(should_cancel and should_cancel())
                    return CreatedMarketDataDownload(
                        summary={
                            "mode": "market_data",
                            "message": "completed",
                            "out_dir": "data/raw",
                            "csv_file_count": 3,
                            "local_latest_date": "2026-05-31",
                        },
                        artifacts=[],
                    )

            service = FakeMarketDataService()
            app.dependency_overrides[get_market_data_service] = lambda: service

            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                created = await client.post(
                    "/api/runs/market-data",
                    json={"config_path": "config/fetch_kline.yaml", "start": "2026-05-30", "out_dir": "data/raw", "workers": 2},
                )
                self.assertEqual(created.status_code, 200)
                payload = created.json()
                self.assertEqual(payload["run"]["kind"], "market_data")
                self.assertEqual(payload["run"]["status"], "succeeded")
                self.assertEqual(payload["summary"]["csv_file_count"], 3)

                detail = await client.get(f"/api/runs/{payload['run']['id']}")
                self.assertEqual(detail.status_code, 200)
                detail_payload = detail.json()
                self.assertEqual(detail_payload["steps"][0]["name"], "market_data")
                self.assertIn("Market data download completed", detail_payload["events"][-1]["message"])

            self.assertEqual(service.request.start, "2026-05-30")
            self.assertFalse(service.cancel_requested)
            if app.state.sqlite_engine is not None:
                app.state.sqlite_engine.dispose()

    async def test_market_data_failure_records_actionable_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            db_path = tmp / "app.sqlite"
            migrate_sqlite(db_path)
            app = create_app(sqlite_path=db_path, duckdb_path=None, artifact_root=tmp / "artifacts")

            class MissingTokenMarketDataService:
                def run(self, *, run_id: str, request: MarketDataRunRequest, should_cancel=None) -> CreatedMarketDataDownload:
                    raise MarketDataDownloadValidationError("请先设置环境变量 TUSHARE_TOKEN")

            app.dependency_overrides[get_market_data_service] = lambda: MissingTokenMarketDataService()

            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                created = await client.post("/api/runs/market-data", json={})
                self.assertEqual(created.status_code, 400)
                self.assertIn("TUSHARE_TOKEN", created.json()["detail"])

                listed = await client.get("/api/runs")
                self.assertEqual(listed.status_code, 200)
                failed_run = listed.json()["runs"][0]
                self.assertEqual(failed_run["kind"], "market_data")
                self.assertEqual(failed_run["status"], "failed")

                detail = await client.get(f"/api/runs/{failed_run['id']}")
                self.assertEqual(detail.status_code, 200)
                detail_payload = detail.json()
                diagnostic = detail_payload["summary"]["diagnostic"]
                self.assertEqual(diagnostic["code"], "market_data_missing_tushare_token")
                self.assertTrue(diagnostic["retryable"])
                self.assertIn("重新启动 ./start_product", diagnostic["next_actions"][1])
                self.assertEqual(
                    detail_payload["steps"][0]["error"]["diagnostic"]["code"],
                    "market_data_missing_tushare_token",
                )
                self.assertIn(
                    "[market_data_missing_tushare_token]",
                    detail_payload["events"][-1]["message"],
                )

            if app.state.sqlite_engine is not None:
                app.state.sqlite_engine.dispose()


class MarketDataDownloadServiceTests(unittest.TestCase):
    def test_service_writes_effective_config_and_product_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            db_path = tmp / "app.sqlite"
            migrate_sqlite(db_path)
            repository, engine = repository_for(db_path)
            run = repository.create_run(kind="market_data", run_id="run-market-data")
            source_config = tmp / "fetch_kline.yaml"
            output_dir = tmp / "raw"
            source_config.write_text(
                yaml.safe_dump(
                    {
                        "start": "20190101",
                        "end": "today",
                        "stocklist": str(tmp / "stocklist.csv"),
                        "exclude_boards": ["st"],
                        "out": str(tmp / "unused"),
                        "workers": 4,
                    },
                    allow_unicode=True,
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            captured: dict[str, object] = {}

            def fake_fetch_main(*, config_path: Path, log_path: Path) -> None:
                payload = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
                captured.update(payload)
                out_path = Path(str(payload["out"]))
                out_path.mkdir(parents=True, exist_ok=True)
                (out_path / "000001.csv").write_text("date,open,close,high,low,volume\n2026-05-31,1,2,3,1,100\n", encoding="utf-8")
                Path(log_path).write_text("download log\n", encoding="utf-8")

            fetch_module = types.ModuleType("pipeline.fetch_kline")
            fetch_module.main = fake_fetch_main
            fetch_module._latest_date_from_csv_dir = lambda _path: "2026-05-31"
            pipeline_module = types.ModuleType("pipeline")
            pipeline_module.fetch_kline = fetch_module

            service = MarketDataDownloadService(repository, artifact_root=tmp / "artifacts")
            request = MarketDataRunRequest(
                config_path=str(source_config),
                start="2026-05-30",
                end="today",
                out_dir=str(output_dir),
                workers=1,
            )

            with mock.patch.dict(os.environ, {"TUSHARE_TOKEN": "fake-token"}, clear=False):
                with mock.patch.dict(
                    sys.modules,
                    {"pipeline": pipeline_module, "pipeline.fetch_kline": fetch_module},
                ):
                    created = service.run(run_id=run.id, request=request)

            self.assertEqual(captured["start"], "20260530")
            self.assertEqual(captured["end"], "today")
            self.assertEqual(captured["out"], str(output_dir))
            self.assertEqual(captured["workers"], 1)
            self.assertEqual(created.summary["csv_file_count"], 1)
            self.assertEqual(created.summary["local_latest_date"], "2026-05-31")
            self.assertEqual({artifact.kind for artifact in created.artifacts}, {"config", "log"})
            event_messages = [event.message for event in repository.list_events(run.id)]
            self.assertTrue(any("Market data config loaded from" in message for message in event_messages))
            self.assertTrue(
                any("Market data fetch starting for 20260530 to today" in message for message in event_messages)
            )
            self.assertTrue(
                any("Market data fetch finished with 1 CSV files" in message for message in event_messages)
            )
            engine.dispose()


if __name__ == "__main__":
    unittest.main()
