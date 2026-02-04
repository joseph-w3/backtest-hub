import tempfile
import unittest
from pathlib import Path

from services.report_service import ReportCache, ReportCacheConfig, ReportService, ReportServiceConfig


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

            def fetcher(base_url: str, backtest_id: str):
                if backtest_id == backtest_id_new:
                    return {
                        "report_available": True,
                        "backtest_id": backtest_id_new,
                        "requested_by": "alice",
                    }
                if backtest_id == backtest_id_old:
                    raise AssertionError("cached report should avoid fetch")
                return {"report_available": False}

            service.fetch_report = fetcher  # type: ignore[method-assign]

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
