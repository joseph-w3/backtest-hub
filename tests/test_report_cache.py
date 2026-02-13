import threading
import unittest
from datetime import datetime
from typing import Any
from unittest.mock import MagicMock

from fastapi.testclient import TestClient
from fastapi import FastAPI

from services.report_service import (
    ReportService,
    ReportServiceConfig,
    build_report_router,
)


def make_app(mapping: dict[str, Any], service: ReportService | None = None) -> FastAPI:
    if service is None:
        service = ReportService(
            ReportServiceConfig(report_batch_path="/v1/runs/backtest/reports/batch"),
            backtest_headers=lambda: {},
        )

    def get_run_entry(backtest_id: str) -> dict[str, Any] | None:
        entry = mapping.get(backtest_id)
        return entry if isinstance(entry, dict) else None

    def get_runs_by_ids(backtest_ids: list[str]) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for bid in backtest_ids:
            entry = get_run_entry(bid)
            if entry is not None:
                out[bid] = entry
        return out

    def list_submitted_ids(after_dt, before_dt) -> list[str]:
        results: list[str] = []
        for bid, entry in mapping.items():
            if not isinstance(entry, dict):
                continue
            submitted_at = entry.get("submitted_at")
            if not isinstance(submitted_at, str) or not submitted_at:
                continue
            try:
                dt = datetime.fromisoformat(submitted_at.replace("Z", "+00:00"))
            except Exception:
                continue
            if after_dt and dt < after_dt:
                continue
            if before_dt and dt >= before_dt:
                continue
            results.append(bid)
        # desc by submitted_at
        results.sort(key=lambda x: str(mapping.get(x, {}).get("submitted_at", "")), reverse=True)
        return results

    app = FastAPI()
    app.include_router(
        build_report_router(
            get_report_service=lambda: service,
            get_run_entry=get_run_entry,
            get_runs_by_ids=get_runs_by_ids,
            list_submitted_ids=list_submitted_ids,
        )
    )
    return app


class TestListBacktestIds(unittest.TestCase):
    def test_filters_by_submitted_at_range(self) -> None:
        mapping = {
            "id_1": {"submitted_at": "2026-02-01T00:00:00Z", "backtest_api_base": "http://a"},
            "id_2": {"submitted_at": "2026-02-03T00:00:00Z", "backtest_api_base": "http://a"},
            "id_3": {"submitted_at": "2026-02-05T00:00:00Z", "backtest_api_base": "http://a"},
            "id_queued": {"status": "queued"},  # no submitted_at
        }
        client = TestClient(make_app(mapping))

        resp = client.get(
            "/runs/backtest/ids",
            params={"submitted_after": "2026-02-02T00:00:00Z", "submitted_before": "2026-02-04T00:00:00Z"},
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["count"], 1)
        self.assertEqual(data["backtest_ids"], ["id_2"])

    def test_no_filters_returns_all_submitted(self) -> None:
        mapping = {
            "id_a": {"submitted_at": "2026-02-01T00:00:00Z"},
            "id_b": {"submitted_at": "2026-02-02T00:00:00Z"},
            "id_queued": {"status": "queued"},
        }
        client = TestClient(make_app(mapping))

        resp = client.get("/runs/backtest/ids")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["count"], 2)
        # sorted by submitted_at desc
        self.assertEqual(data["backtest_ids"], ["id_b", "id_a"])

    def test_invalid_datetime_returns_400(self) -> None:
        client = TestClient(make_app({}))
        resp = client.get("/runs/backtest/ids", params={"submitted_after": "bad"})
        self.assertEqual(resp.status_code, 400)

    def test_after_gte_before_returns_400(self) -> None:
        client = TestClient(make_app({}))
        resp = client.get(
            "/runs/backtest/ids",
            params={"submitted_after": "2026-02-05T00:00:00Z", "submitted_before": "2026-02-01T00:00:00Z"},
        )
        self.assertEqual(resp.status_code, 400)


class TestBatchGetReports(unittest.TestCase):
    def test_groups_by_base_and_returns_reports(self) -> None:
        mapping = {
            "id_1": {"backtest_api_base": "http://a", "submitted_at": "2026-02-01T00:00:00Z"},
            "id_2": {"backtest_api_base": "http://a", "submitted_at": "2026-02-02T00:00:00Z"},
            "id_3": {"backtest_api_base": "http://b", "submitted_at": "2026-02-03T00:00:00Z"},
        }
        service = ReportService(
            ReportServiceConfig(report_batch_path="/v1/runs/backtest/reports/batch"),
            backtest_headers=lambda: {},
        )

        def mock_batch(base_url: str, bids: list[str]) -> dict[str, dict[str, Any]]:
            return {bid: {"pnl": 100, "sharpe": 1.5} for bid in bids}

        service.fetch_reports_batch = mock_batch  # type: ignore[method-assign]

        client = TestClient(make_app(mapping, service))
        resp = client.post("/runs/backtest/reports", json={"backtest_ids": ["id_1", "id_2", "id_3"]})
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["count"], 3)
        self.assertEqual(data["not_found"], [])
        self.assertEqual(data["errors"], {})

        self.assertEqual(data["results"]["id_1"]["backtest_api_base"], "http://a")
        self.assertEqual(data["results"]["id_3"]["backtest_api_base"], "http://b")
        self.assertIn("report", data["results"]["id_1"])

    def test_not_found_ids(self) -> None:
        mapping = {
            "id_1": {"backtest_api_base": "http://a"},
        }
        service = ReportService(
            ReportServiceConfig(report_batch_path="/v1/runs/backtest/reports/batch"),
            backtest_headers=lambda: {},
        )
        service.fetch_reports_batch = lambda base_url, bids: {bid: {"pnl": 0} for bid in bids}  # type: ignore[method-assign]

        client = TestClient(make_app(mapping, service))
        resp = client.post("/runs/backtest/reports", json={"backtest_ids": ["id_1", "id_missing"]})
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["count"], 1)
        self.assertIn("id_missing", data["not_found"])

    def test_empty_backtest_ids_returns_400(self) -> None:
        client = TestClient(make_app({}))
        resp = client.post("/runs/backtest/reports", json={"backtest_ids": []})
        self.assertEqual(resp.status_code, 400)
