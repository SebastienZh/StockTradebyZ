#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import httpx
import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "apps" / "api"))
sys.path.insert(0, str(ROOT / "src"))

from stocktrade_api.main import create_app  # noqa: E402


DEFAULT_SYMBOLS = ("000001.SZ:000001:平安银行", "600000.SH:600000:浦发银行")


def parse_args() -> argparse.Namespace:
    today = dt.date.today()
    default_start = (today - dt.timedelta(days=30)).strftime("%Y%m%d")
    parser = argparse.ArgumentParser(
        description="Run a live Tushare market-data acceptance check through the product FastAPI path."
    )
    parser.add_argument("--start", default=default_start, help="start date: YYYYMMDD, YYYY-MM-DD, or today")
    parser.add_argument("--end", default="today", help="end date: YYYYMMDD, YYYY-MM-DD, or today")
    parser.add_argument("--workers", type=int, default=1, help="small worker count for live acceptance")
    parser.add_argument(
        "--symbol",
        action="append",
        default=[],
        help="ts_code:symbol:name row for the temporary stocklist; can be repeated",
    )
    parser.add_argument(
        "--run-root",
        default="",
        help="optional output root; defaults to var/acceptance/tushare-e2e/<timestamp>",
    )
    parser.add_argument(
        "--allow-missing-token",
        action="store_true",
        help="write a skipped record and exit 0 when TUSHARE_TOKEN is missing",
    )
    return parser.parse_args()


async def main_async() -> int:
    args = parse_args()
    started = dt.datetime.now(dt.UTC)
    timestamp = started.strftime("%Y%m%dT%H%M%SZ")
    run_root = _resolve_run_root(args.run_root, timestamp)
    run_root.mkdir(parents=True, exist_ok=True)

    record: dict[str, Any] = {
        "issue": "#72",
        "kind": "tushare_market_data_e2e",
        "status": "running",
        "started_at": started.isoformat(),
        "finished_at": None,
        "duration_seconds": None,
        "token_present": bool((os.environ.get("TUSHARE_TOKEN") or "").strip()),
        "run_root": _display_path(run_root),
        "request": {
            "start": args.start,
            "end": args.end,
            "workers": args.workers,
            "symbols": args.symbol or list(DEFAULT_SYMBOLS),
        },
    }

    if not record["token_present"]:
        record.update(
            {
                "status": "skipped",
                "error": "TUSHARE_TOKEN is not set in the current process environment",
                "next_actions": [
                    "export TUSHARE_TOKEN=你的token",
                    "restart the product process or rerun this script from the same shell",
                ],
            }
        )
        _write_record(run_root, record, started)
        print(f"[tushare-e2e] skipped: TUSHARE_TOKEN is missing; record={_display_path(run_root / 'acceptance.json')}")
        return 0 if args.allow_missing_token else 2

    stocklist_path = run_root / "stocklist.csv"
    config_path = run_root / "fetch_kline.yaml"
    raw_dir = run_root / "raw"
    artifact_root = run_root / "artifacts"
    sqlite_path = run_root / "app.sqlite"

    _write_stocklist(stocklist_path, args.symbol or list(DEFAULT_SYMBOLS))
    config_path.write_text(
        yaml.safe_dump(
            {
                "start": args.start,
                "end": args.end,
                "stocklist": stocklist_path.as_posix(),
                "exclude_boards": [],
                "out": raw_dir.as_posix(),
                "workers": args.workers,
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    app = create_app(sqlite_path=sqlite_path, duckdb_path=None, artifact_root=artifact_root)
    started_monotonic = time.monotonic()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test", timeout=None) as client:
            response = await client.post(
                "/api/runs/market-data",
                json={
                    "config_path": config_path.as_posix(),
                    "start": args.start,
                    "end": args.end,
                    "out_dir": raw_dir.as_posix(),
                    "workers": args.workers,
                },
            )
            record["http_status"] = response.status_code
            if response.status_code == 200:
                payload = response.json()
                run_id = payload["run"]["id"]
                detail = await client.get(f"/api/runs/{run_id}")
                artifacts = await client.get(f"/api/runs/{run_id}/artifacts")
                record.update(
                    {
                        "status": "passed",
                        "run_id": run_id,
                        "run_status": payload["run"]["status"],
                        "summary": payload["summary"],
                        "events_tail": _events_tail(detail.json().get("events", [])),
                        "artifacts": artifacts.json().get("artifacts", []),
                        "csv_files": sorted(path.name for path in raw_dir.glob("*.csv")),
                    }
                )
            else:
                detail_payload = _json_or_text(response)
                listed = await client.get("/api/runs")
                latest = (listed.json().get("runs") or [None])[0] if listed.status_code == 200 else None
                record.update(
                    {
                        "status": "failed",
                        "error": detail_payload,
                        "latest_run": latest,
                    }
                )
                if latest and latest.get("id"):
                    record["run_id"] = latest["id"]
                    record["run_status"] = latest.get("status")
                    detail = await client.get(f"/api/runs/{latest['id']}")
                    record["events_tail"] = _events_tail(detail.json().get("events", []))
                    record["summary"] = detail.json().get("summary")
    finally:
        if getattr(app.state, "sqlite_engine", None) is not None:
            app.state.sqlite_engine.dispose()

    finished = dt.datetime.now(dt.UTC)
    record["finished_at"] = finished.isoformat()
    record["duration_seconds"] = round(time.monotonic() - started_monotonic, 3)
    _write_record(run_root, record, started)
    print(f"[tushare-e2e] {record['status']}: record={_display_path(run_root / 'acceptance.json')}")
    return 0 if record["status"] == "passed" else 1


def _resolve_run_root(value: str, timestamp: str) -> Path:
    if value.strip():
        path = Path(value).expanduser()
        return path if path.is_absolute() else (ROOT / path)
    return ROOT / "var" / "acceptance" / "tushare-e2e" / timestamp


def _write_stocklist(path: Path, rows: list[str]) -> None:
    path.write_text(
        "ts_code,symbol,name\n" + "\n".join(_stocklist_row(row) for row in rows) + "\n",
        encoding="utf-8",
    )


def _stocklist_row(value: str) -> str:
    parts = value.split(":")
    if len(parts) == 1:
        ts_code = parts[0].strip().upper()
        symbol = ts_code.split(".", 1)[0]
        name = symbol
    elif len(parts) == 2:
        ts_code, symbol = [part.strip() for part in parts]
        name = symbol
    else:
        ts_code, symbol, name = [part.strip() for part in parts[:3]]
    if not ts_code or not symbol:
        raise ValueError(f"invalid stocklist row: {value!r}")
    return f"{ts_code},{symbol},{name or symbol}"


def _events_tail(events: list[dict[str, Any]], limit: int = 12) -> list[dict[str, Any]]:
    return [
        {
            "level": event.get("level"),
            "message": event.get("message"),
            "created_at": event.get("created_at"),
        }
        for event in events[-limit:]
    ]


def _json_or_text(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return response.text


def _write_record(run_root: Path, record: dict[str, Any], started: dt.datetime) -> None:
    if record.get("finished_at") is None:
        finished = dt.datetime.now(dt.UTC)
        record["finished_at"] = finished.isoformat()
        record["duration_seconds"] = round((finished - started).total_seconds(), 3)
    (run_root / "acceptance.json").write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    (run_root / "summary.md").write_text(_summary_markdown(record), encoding="utf-8")


def _summary_markdown(record: dict[str, Any]) -> str:
    summary = record.get("summary") if isinstance(record.get("summary"), dict) else {}
    lines = [
        "# Tushare Live Acceptance",
        "",
        f"- issue: {record.get('issue')}",
        f"- status: {record.get('status')}",
        f"- run_id: {record.get('run_id', 'not-created')}",
        f"- http_status: {record.get('http_status', 'not-called')}",
        f"- duration_seconds: {record.get('duration_seconds')}",
        f"- csv_file_count: {summary.get('csv_file_count', 'not-set')}",
        f"- local_latest_date: {summary.get('local_latest_date', 'not-set')}",
        f"- record: {record.get('run_root')}/acceptance.json",
        "",
        "Token values, raw credentials, and generated CSV contents are intentionally not included.",
        "",
    ]
    return "\n".join(lines)


def _display_path(path: Path) -> str:
    try:
        return path.resolve(strict=False).relative_to(ROOT).as_posix()
    except ValueError:
        return str(path)


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
