import tempfile
import unittest
from pathlib import Path
from typing import Any

from services.report_service import (
    ReportCache,
    ReportCacheConfig,
    ReportNotFound,
    ReportNotReady,
    ReportService,
    ReportServiceConfig,
)


class TestReportCache(unittest.TestCase):
    def test_report_cache_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "report_cache.sqlite3"
            cache = ReportCache(ReportCacheConfig(path=db_path))
            cache.init_db()

            report = {"report_available": True, "backtest_id": "b1"}
            cache.upsert(
                "b1",
                report,
                "alice",
                "2026-02-04T00:00:00Z",
            )
            loaded = cache.get("b1")
            self.assertEqual(loaded, report)

    def test_collect_report_page_uses_cache_and_filters(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "report_cache.sqlite3"
            service = ReportService(
                ReportServiceConfig(
                    cache_path=db_path,
                    report_path="/v1/runs/backtest/{backtest_id}/report",
                    report_batch_path="/v1/runs/backtest/reports/batch",
                    status_path="/v1/runs/backtest/{backtest_id}",
                    data_download_path="/v1/runs/backtest/{backtest_id}/download_data",
                    runs_path="/v1/runs",
                    max_page_size=50,
                ),
                backtest_headers=lambda: {},
                ensure_backtest_routable=lambda backtest_id, allow_running_only=True: {
                    "backtest_api_base": "http://dummy"
                },
                update_mapping=lambda backtest_id, updates: None,
                load_run_spec_payload=lambda backtest_id: {},
            )
            service.init_cache()

            backtest_id_new = "20260204T000000Z_new"
            backtest_id_old = "20260203T000000Z_old"
            backtest_id_skip = "20260202T000000Z_skip"

            mapping = {
                backtest_id_new: {
                    "backtest_api_base": "http://docker-a",
                    "created_at": "2026-02-04T00:00:00Z",
                    "requested_by": "alice",
                },
                backtest_id_old: {
                    "backtest_api_base": "http://docker-b",
                    "created_at": "2026-02-03T00:00:00Z",
                    "requested_by": "alice",
                },
                backtest_id_skip: {
                    "backtest_api_base": "http://docker-c",
                    "created_at": "2026-02-02T00:00:00Z",
                    "requested_by": "alice",
                },
            }

            cached_report = {
                "report_available": True,
                "backtest_id": backtest_id_old,
                "requested_by": "alice",
            }
            service.cache.upsert(
                backtest_id_old,
                cached_report,
                "alice",
                "2026-02-03T00:00:00Z",
            )

            def batch_fetcher(base_url: str, backtest_ids: list[str]) -> dict[str, dict[str, Any]]:
                if base_url == "http://docker-b":
                    raise AssertionError("cached report should avoid batch fetch")
                if base_url == "http://docker-a":
                    if backtest_id_new in backtest_ids:
                        return {
                            backtest_id_new: {
                                "report_available": True,
                                "backtest_id": backtest_id_new,
                                "requested_by": "alice",
                            }
                        }
                return {}

            service.fetch_reports_batch = batch_fetcher  # type: ignore[method-assign]

            items = service.get_report_page(
                page=1,
                limit=2,
                requested_by="alice",
                mapping=mapping,
            )

            self.assertEqual(len(items), 2)
            self.assertEqual(items[0]["backtest_id"], backtest_id_new)
            self.assertEqual(items[1]["backtest_id"], backtest_id_old)

            cached_new = service.cache.get(backtest_id_new)
            self.assertIsNotNone(cached_new)

    def test_get_report_caches_non_running_unavailable_as_negative(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "report_cache.sqlite3"
            service = ReportService(
                ReportServiceConfig(
                    cache_path=db_path,
                    report_path="/v1/runs/backtest/{backtest_id}/report",
                    report_batch_path="/v1/runs/backtest/reports/batch",
                    status_path="/v1/runs/backtest/{backtest_id}",
                    data_download_path="/v1/runs/backtest/{backtest_id}/download_data",
                    runs_path="/v1/runs",
                    max_page_size=50,
                ),
                backtest_headers=lambda: {},
                ensure_backtest_routable=lambda backtest_id, allow_running_only=True: {
                    "backtest_api_base": "http://docker-a",
                    "created_at": "2026-02-04T00:00:00Z",
                    "requested_by": "alice",
                },
                update_mapping=lambda backtest_id, updates: None,
                load_run_spec_payload=lambda backtest_id: {},
            )
            service.init_cache()

            backtest_id = "20260204T000000Z_finished"
            calls = {"report": 0, "status": 0}

            def report_fetcher(base_url: str, requested_backtest_id: str) -> dict[str, Any]:
                calls["report"] += 1
                self.assertEqual(base_url, "http://docker-a")
                self.assertEqual(requested_backtest_id, backtest_id)
                return {"report_available": False, "backtest_id": requested_backtest_id}

            def status_fetcher(base_url: str, requested_backtest_id: str) -> dict[str, Any]:
                calls["status"] += 1
                self.assertEqual(base_url, "http://docker-a")
                self.assertEqual(requested_backtest_id, backtest_id)
                return {"status": "finished"}

            service.fetch_report = report_fetcher  # type: ignore[method-assign]
            service.fetch_status = status_fetcher  # type: ignore[method-assign]

            with self.assertRaises(ReportNotFound):
                service.get_report(backtest_id, allow_running_only=False)

            self.assertEqual(calls["report"], 1)
            self.assertEqual(calls["status"], 1)
            cached = service.cache.get(backtest_id)
            self.assertIsNotNone(cached)
            self.assertEqual(cached.get("report_available"), False)
            self.assertEqual(cached.get("status"), "finished")

            service.fetch_report = (  # type: ignore[method-assign]
                lambda base_url, requested_backtest_id: (_ for _ in ()).throw(AssertionError("should use cache"))
            )
            service.fetch_status = (  # type: ignore[method-assign]
                lambda base_url, requested_backtest_id: (_ for _ in ()).throw(AssertionError("should use cache"))
            )
            with self.assertRaises(ReportNotFound):
                service.get_report(backtest_id, allow_running_only=False)

    def test_get_report_running_unavailable_not_cached(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "report_cache.sqlite3"
            service = ReportService(
                ReportServiceConfig(
                    cache_path=db_path,
                    report_path="/v1/runs/backtest/{backtest_id}/report",
                    report_batch_path="/v1/runs/backtest/reports/batch",
                    status_path="/v1/runs/backtest/{backtest_id}",
                    data_download_path="/v1/runs/backtest/{backtest_id}/download_data",
                    runs_path="/v1/runs",
                    max_page_size=50,
                ),
                backtest_headers=lambda: {},
                ensure_backtest_routable=lambda backtest_id, allow_running_only=True: {
                    "backtest_api_base": "http://docker-a",
                    "created_at": "2026-02-04T00:00:00Z",
                },
                update_mapping=lambda backtest_id, updates: None,
                load_run_spec_payload=lambda backtest_id: {},
            )
            service.init_cache()

            backtest_id = "20260204T000000Z_running"
            calls = {"report": 0, "status": 0}

            def report_fetcher(base_url: str, requested_backtest_id: str) -> dict[str, Any]:
                calls["report"] += 1
                return {"report_available": False, "backtest_id": requested_backtest_id}

            def status_fetcher(base_url: str, requested_backtest_id: str) -> dict[str, Any]:
                calls["status"] += 1
                return {"status": "running"}

            service.fetch_report = report_fetcher  # type: ignore[method-assign]
            service.fetch_status = status_fetcher  # type: ignore[method-assign]

            with self.assertRaises(ReportNotReady):
                service.get_report(backtest_id, allow_running_only=False)
            self.assertIsNone(service.cache.get(backtest_id))

            with self.assertRaises(ReportNotReady):
                service.get_report(backtest_id, allow_running_only=False)

            self.assertEqual(calls["report"], 2)
            self.assertEqual(calls["status"], 2)

    def test_report_page_skips_negative_cache_records(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "report_cache.sqlite3"
            service = ReportService(
                ReportServiceConfig(
                    cache_path=db_path,
                    report_path="/v1/runs/backtest/{backtest_id}/report",
                    report_batch_path="/v1/runs/backtest/reports/batch",
                    status_path="/v1/runs/backtest/{backtest_id}",
                    data_download_path="/v1/runs/backtest/{backtest_id}/download_data",
                    runs_path="/v1/runs",
                    max_page_size=50,
                ),
                backtest_headers=lambda: {},
                ensure_backtest_routable=lambda backtest_id, allow_running_only=True: {
                    "backtest_api_base": "http://dummy"
                },
                update_mapping=lambda backtest_id, updates: None,
                load_run_spec_payload=lambda backtest_id: {},
            )
            service.init_cache()

            positive_id = "20260205T000000Z_positive"
            negative_id = "20260204T000000Z_negative"
            mapping = {
                positive_id: {
                    "backtest_api_base": "http://docker-a",
                    "created_at": "2026-02-05T00:00:00Z",
                },
                negative_id: {
                    "backtest_api_base": "http://docker-a",
                    "created_at": "2026-02-04T00:00:00Z",
                },
            }

            service.cache.upsert(
                positive_id,
                {"report_available": True, "backtest_id": positive_id},
                None,
                "2026-02-05T00:00:00Z",
            )
            service.cache.upsert(
                negative_id,
                {"report_available": False, "backtest_id": negative_id, "status": "failed"},
                None,
                "2026-02-04T00:00:00Z",
            )

            items = service.get_report_page(page=1, limit=10, requested_by=None, mapping=mapping)
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["backtest_id"], positive_id)
