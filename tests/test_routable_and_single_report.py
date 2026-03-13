"""
Tests for:
1. ensure_backtest_routable error messages (queued vs completed vs failed)
2. GET /runs/backtest/{id}/report single report endpoint
3. GET /runs/backtest/{id} allows completed status
"""

import os
import unittest
from typing import Any
from unittest.mock import patch, MagicMock

os.environ.setdefault("BACKTEST_API_BASES", "http://dummy")

from fastapi.testclient import TestClient
from fastapi import FastAPI

import app
from services.report_service import (
    ReportService,
    ReportServiceConfig,
    ReportHttpError,
    ReportFetchError,
    ReportInvalidPayload,
    build_report_router,
)


# ---------------------------------------------------------------------------
# Helper: build a report-router test app (same pattern as test_report_cache)
# ---------------------------------------------------------------------------
def make_report_app(mapping: dict[str, Any], service: ReportService | None = None) -> FastAPI:
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

    test_app = FastAPI()
    test_app.include_router(
        build_report_router(
            get_report_service=lambda: service,
            get_run_entry=get_run_entry,
            get_runs_by_ids=get_runs_by_ids,
            list_submitted_ids=lambda a, b: [],
        )
    )
    return test_app


# ===========================================================================
# 1. ensure_backtest_routable error messages
# ===========================================================================
class TestEnsureBacktestRoutableMessages(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app.app)

    def test_queued_no_worker_returns_409_with_queued_message(self) -> None:
        entry = {"status": "queued"}
        with patch.object(app, "get_mapping_entry", return_value=entry):
            resp = self.client.get("/runs/backtest/bt1/download_csv")
            self.assertEqual(resp.status_code, 409)
            self.assertIn("still queued", resp.json()["detail"])

    def test_completed_returns_409_with_status_and_hint(self) -> None:
        entry = {"status": "completed", "backtest_api_base": "http://worker"}
        with patch.object(app, "get_mapping_entry", return_value=entry):
            resp = self.client.get("/runs/backtest/bt1/download_csv")
            self.assertEqual(resp.status_code, 409)
            detail = resp.json()["detail"]
            self.assertIn("completed", detail)
            self.assertIn("logs/download", detail)
            self.assertNotIn("still queued", detail)

    def test_failed_returns_409_with_failed_status(self) -> None:
        entry = {"status": "failed", "backtest_api_base": "http://worker"}
        with patch.object(app, "get_mapping_entry", return_value=entry):
            resp = self.client.get("/runs/backtest/bt1/download_csv")
            self.assertEqual(resp.status_code, 409)
            detail = resp.json()["detail"]
            self.assertIn("failed", detail)
            self.assertNotIn("still queued", detail)

    def test_not_found_returns_404(self) -> None:
        with patch.object(app, "get_mapping_entry", return_value=None):
            resp = self.client.get("/runs/backtest/bt1/download_csv")
            self.assertEqual(resp.status_code, 404)


# ===========================================================================
# 2. GET /runs/backtest/{id} allows completed status
# ===========================================================================
class TestGetBacktestStatusCompleted(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app.app)

    def test_completed_status_returns_200(self) -> None:
        entry = {"status": "completed", "backtest_api_base": "http://worker"}
        mock_payload = {"backtest_id": "bt1", "status": "completed"}
        with patch.object(app, "get_mapping_entry", return_value=entry):
            with patch.object(app, "fetch_backtest_status", return_value=mock_payload):
                resp = self.client.get("/runs/backtest/bt1")
                self.assertEqual(resp.status_code, 200)
                self.assertEqual(resp.json()["status"], "completed")

    def test_running_status_still_works(self) -> None:
        entry = {"status": "running", "backtest_api_base": "http://worker"}
        mock_payload = {"backtest_id": "bt1", "status": "running"}
        with patch.object(app, "get_mapping_entry", return_value=entry):
            with patch.object(app, "fetch_backtest_status", return_value=mock_payload):
                resp = self.client.get("/runs/backtest/bt1")
                self.assertEqual(resp.status_code, 200)

    def test_failed_status_returns_200(self) -> None:
        entry = {"status": "failed", "backtest_api_base": "http://worker"}
        mock_payload = {"backtest_id": "bt1", "status": "failed"}
        with patch.object(app, "get_mapping_entry", return_value=entry):
            with patch.object(app, "fetch_backtest_status", return_value=mock_payload):
                resp = self.client.get("/runs/backtest/bt1")
                self.assertEqual(resp.status_code, 200)
                self.assertEqual(resp.json()["status"], "failed")

    def test_queued_no_worker_returns_409(self) -> None:
        entry = {"status": "queued"}
        with patch.object(app, "get_mapping_entry", return_value=entry):
            resp = self.client.get("/runs/backtest/bt1")
            self.assertEqual(resp.status_code, 409)

    def test_fetch_status_upstream_http_error_forwarded(self) -> None:
        import urllib.error

        entry = {"status": "completed", "backtest_api_base": "http://worker"}
        http_error = urllib.error.HTTPError(
            url="http://worker/v1/runs/backtest/bt1",
            code=503,
            msg="Service Unavailable",
            hdrs={},  # type: ignore
            fp=None,
        )
        http_error.read = MagicMock(return_value=b"worker unavailable")
        with patch.object(app, "get_mapping_entry", return_value=entry):
            with patch("urllib.request.urlopen", side_effect=http_error):
                resp = self.client.get("/runs/backtest/bt1")
                self.assertEqual(resp.status_code, 503)

    def test_fetch_status_network_error_returns_502(self) -> None:
        entry = {"status": "completed", "backtest_api_base": "http://worker"}
        with patch.object(app, "get_mapping_entry", return_value=entry):
            with patch("urllib.request.urlopen", side_effect=Exception("Connection refused")):
                resp = self.client.get("/runs/backtest/bt1")
                self.assertEqual(resp.status_code, 502)


# ===========================================================================
# 3. GET /runs/backtest/{id}/report single report endpoint
# ===========================================================================
class TestSingleReportEndpoint(unittest.TestCase):
    def test_returns_report_for_existing_backtest(self) -> None:
        mapping = {
            "bt1": {"backtest_api_base": "http://worker-a"},
        }
        service = ReportService(
            ReportServiceConfig(report_batch_path="/batch"),
            backtest_headers=lambda: {},
        )
        service.fetch_reports_batch = lambda base_url, bids: {  # type: ignore[method-assign]
            bid: {"pnl": 1390, "sharpe": 1.44} for bid in bids
        }

        client = TestClient(make_report_app(mapping, service))
        resp = client.get("/runs/backtest/bt1/report")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["backtest_id"], "bt1")
        self.assertEqual(data["backtest_api_base"], "http://worker-a")
        self.assertEqual(data["report"]["pnl"], 1390)
        self.assertEqual(data["report"]["sharpe"], 1.44)

    def test_not_found_backtest_returns_404(self) -> None:
        client = TestClient(make_report_app({}))
        resp = client.get("/runs/backtest/nonexistent/report")
        self.assertEqual(resp.status_code, 404)
        self.assertIn("not found", resp.json()["detail"].lower())

    def test_no_worker_assigned_returns_404(self) -> None:
        mapping = {"bt1": {"status": "queued"}}
        client = TestClient(make_report_app(mapping))
        resp = client.get("/runs/backtest/bt1/report")
        self.assertEqual(resp.status_code, 404)
        self.assertIn("queued", resp.json()["detail"].lower())

    def test_report_not_available_on_worker_returns_404(self) -> None:
        mapping = {"bt1": {"backtest_api_base": "http://worker-a"}}
        service = ReportService(
            ReportServiceConfig(report_batch_path="/batch"),
            backtest_headers=lambda: {},
        )
        service.fetch_reports_batch = lambda base_url, bids: {}  # type: ignore[method-assign]

        client = TestClient(make_report_app(mapping, service))
        resp = client.get("/runs/backtest/bt1/report")
        self.assertEqual(resp.status_code, 404)
        self.assertIn("not available", resp.json()["detail"].lower())

    def test_worker_http_error_returns_status(self) -> None:
        mapping = {"bt1": {"backtest_api_base": "http://worker-a"}}
        service = ReportService(
            ReportServiceConfig(report_batch_path="/batch"),
            backtest_headers=lambda: {},
        )

        def raise_http(*a, **kw):
            raise ReportHttpError(503, "worker down")

        service.fetch_reports_batch = raise_http  # type: ignore[method-assign]

        client = TestClient(make_report_app(mapping, service))
        resp = client.get("/runs/backtest/bt1/report")
        self.assertEqual(resp.status_code, 503)

    def test_worker_fetch_error_returns_502(self) -> None:
        mapping = {"bt1": {"backtest_api_base": "http://worker-a"}}
        service = ReportService(
            ReportServiceConfig(report_batch_path="/batch"),
            backtest_headers=lambda: {},
        )

        def raise_fetch(*a, **kw):
            raise ReportFetchError("connection refused")

        service.fetch_reports_batch = raise_fetch  # type: ignore[method-assign]

        client = TestClient(make_report_app(mapping, service))
        resp = client.get("/runs/backtest/bt1/report")
        self.assertEqual(resp.status_code, 502)


class TestRunSpecEndpoint(unittest.TestCase):
    def test_returns_worker_run_spec_for_existing_backtest(self) -> None:
        mapping = {
            "bt1": {"backtest_api_base": "http://worker-a", "run_id": "run-1", "requested_by": "tester"},
        }
        service = ReportService(
            ReportServiceConfig(report_batch_path="/batch"),
            backtest_headers=lambda: {},
        )
        service.fetch_run_spec = lambda base_url, backtest_id: {  # type: ignore[method-assign]
            "backtest_id": backtest_id,
            "run_id": "run-1",
            "requested_by": "tester",
            "run_spec": {"symbols": ["BTCUSDT", "BTCUSDT-PERP"]},
        }

        client = TestClient(make_report_app(mapping, service))
        resp = client.get("/runs/backtest/bt1/run_spec")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["run_id"], "run-1")
        self.assertEqual(data["requested_by"], "tester")
        self.assertEqual(data["run_spec"]["symbols"], ["BTCUSDT", "BTCUSDT-PERP"])

    def test_falls_back_to_local_run_spec_when_worker_missing(self) -> None:
        mapping = {
            "bt1": {"status": "queued", "run_id": "run-1", "requested_by": "tester"},
        }
        service = ReportService(
            ReportServiceConfig(report_batch_path="/batch"),
            backtest_headers=lambda: {},
        )

        app = FastAPI()
        app.include_router(
            build_report_router(
                get_report_service=lambda: service,
                get_run_entry=lambda backtest_id: mapping.get(backtest_id),
                get_runs_by_ids=lambda backtest_ids: {bid: mapping[bid] for bid in backtest_ids if bid in mapping},
                list_submitted_ids=lambda a, b: [],
                load_run_spec=lambda backtest_id: {"symbols": ["ETHUSDT", "ETHUSDT-PERP"]},
            )
        )
        client = TestClient(app)
        resp = client.get("/runs/backtest/bt1/run_spec")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["source"], "hub_local")
        self.assertEqual(data["run_spec"]["symbols"], ["ETHUSDT", "ETHUSDT-PERP"])

    def test_falls_back_to_local_run_spec_when_worker_lacks_endpoint(self) -> None:
        mapping = {
            "bt1": {"backtest_api_base": "http://worker-a", "run_id": "run-1", "requested_by": "tester"},
        }
        service = ReportService(
            ReportServiceConfig(report_batch_path="/batch"),
            backtest_headers=lambda: {},
        )

        def missing_endpoint(base_url: str, backtest_id: str) -> dict[str, Any]:
            raise ReportHttpError(404, "Not Found")

        service.fetch_run_spec = missing_endpoint  # type: ignore[method-assign]

        app = FastAPI()
        app.include_router(
            build_report_router(
                get_report_service=lambda: service,
                get_run_entry=lambda backtest_id: mapping.get(backtest_id),
                get_runs_by_ids=lambda backtest_ids: {bid: mapping[bid] for bid in backtest_ids if bid in mapping},
                list_submitted_ids=lambda a, b: [],
                load_run_spec=lambda backtest_id: {"symbols": ["SOLUSDT", "SOLUSDT-PERP"]},
            )
        )
        client = TestClient(app)
        resp = client.get("/runs/backtest/bt1/run_spec")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["source"], "hub_local")
        self.assertEqual(data["run_spec"]["symbols"], ["SOLUSDT", "SOLUSDT-PERP"])

    def test_worker_invalid_payload_returns_502(self) -> None:
        mapping = {"bt1": {"backtest_api_base": "http://worker-a"}}
        service = ReportService(
            ReportServiceConfig(report_batch_path="/batch"),
            backtest_headers=lambda: {},
        )

        def raise_invalid(*a, **kw):
            raise ReportInvalidPayload("invalid json response")

        service.fetch_reports_batch = raise_invalid  # type: ignore[method-assign]

        client = TestClient(make_report_app(mapping, service))
        resp = client.get("/runs/backtest/bt1/report")
        self.assertEqual(resp.status_code, 502)

    def test_invalid_id_returns_400(self) -> None:
        client = TestClient(make_report_app({}))
        resp = client.get("/runs/backtest/test.id/report")
        self.assertEqual(resp.status_code, 400)
        self.assertIn("invalid characters", resp.json()["detail"])
