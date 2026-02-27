"""Tests for startup recovery of orphan submitted tasks.

Validates that _recover_submitted_tasks queries workers for submitted tasks
and recovers orphaned ones.
"""

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("BACKTEST_API_BASES", "http://dummy")

import app
from services.run_store_sqlite import SqliteRunStore


class TestGetRunsByStatus(unittest.TestCase):
    """SqliteRunStore.get_runs_by_status must return rows matching a status."""

    def test_returns_submitted_runs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = SqliteRunStore(Path(td) / "hub.sqlite3")
            store.upsert_run("bt_1", {
                "status": "submitted",
                "backtest_api_base": "http://worker1:10001",
                "required_memory_gb": 8.0,
            })
            store.upsert_run("bt_2", {
                "status": "completed",
                "backtest_api_base": "http://worker1:10001",
            })
            store.upsert_run("bt_3", {
                "status": "submitted",
                "backtest_api_base": "http://worker2:10001",
                "required_memory_gb": 4.0,
            })
            store.upsert_run("bt_4", {"status": "queued"})

            submitted = store.get_runs_by_status("submitted")
            ids = {r["backtest_id"] for r in submitted}
            self.assertEqual(ids, {"bt_1", "bt_3"})

    def test_returns_empty_for_no_matches(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = SqliteRunStore(Path(td) / "hub.sqlite3")
            store.upsert_run("bt_1", {"status": "completed"})

            submitted = store.get_runs_by_status("submitted")
            self.assertEqual(submitted, [])

    def test_includes_backtest_id_in_result(self) -> None:
        """Each returned dict must include backtest_id."""
        with tempfile.TemporaryDirectory() as td:
            store = SqliteRunStore(Path(td) / "hub.sqlite3")
            store.upsert_run("bt_x", {
                "status": "submitted",
                "backtest_api_base": "http://w:10001",
            })
            runs = store.get_runs_by_status("submitted")
            self.assertEqual(len(runs), 1)
            self.assertEqual(runs[0]["backtest_id"], "bt_x")


class TestRecoverSubmittedTasks(unittest.IsolatedAsyncioTestCase):
    """_recover_submitted_tasks must query workers and update status."""

    def _setup_store(self, td: str) -> SqliteRunStore:
        db_path = Path(td) / "hub.sqlite3"
        store = SqliteRunStore(db_path)
        app.HUB_DB_PATH = db_path
        app.RUN_STORE = None
        return store

    async def test_completed_task_recovered(self) -> None:
        """A submitted task whose worker reports 'completed' gets its status updated."""
        with tempfile.TemporaryDirectory() as td:
            store = self._setup_store(td)
            store.upsert_run("bt_done", {
                "status": "submitted",
                "backtest_api_base": "http://worker1:10001",
                "required_memory_gb": 4.0,
            })

            mock_status = {"status": "completed"}

            with patch("app.fetch_backtest_status", return_value=mock_status) as mock_fetch:
                await app._recover_submitted_tasks()

            mock_fetch.assert_called_once_with("http://worker1:10001", "bt_done")
            entry = store.get_run("bt_done")
            self.assertIsNotNone(entry)
            self.assertEqual(entry["status"], "completed")

    async def test_failed_task_recovered(self) -> None:
        """A submitted task whose worker reports 'failed' gets its status updated."""
        with tempfile.TemporaryDirectory() as td:
            store = self._setup_store(td)
            store.upsert_run("bt_fail", {
                "status": "submitted",
                "backtest_api_base": "http://worker1:10001",
                "required_memory_gb": 2.0,
            })

            mock_status = {"status": "failed"}

            with patch("app.fetch_backtest_status", return_value=mock_status):
                await app._recover_submitted_tasks()

            entry = store.get_run("bt_fail")
            self.assertEqual(entry["status"], "failed")

    async def test_running_task_not_changed(self) -> None:
        """A submitted task whose worker reports 'running' stays as submitted."""
        with tempfile.TemporaryDirectory() as td:
            store = self._setup_store(td)
            store.upsert_run("bt_run", {
                "status": "submitted",
                "backtest_api_base": "http://worker1:10001",
                "required_memory_gb": 8.0,
            })

            mock_status = {"status": "running"}

            with patch("app.fetch_backtest_status", return_value=mock_status):
                await app._recover_submitted_tasks()

            entry = store.get_run("bt_run")
            self.assertEqual(entry["status"], "submitted")

    async def test_unreachable_worker_logs_warning(self) -> None:
        """When a worker is unreachable, task stays unchanged."""
        with tempfile.TemporaryDirectory() as td:
            store = self._setup_store(td)
            store.upsert_run("bt_orphan", {
                "status": "submitted",
                "backtest_api_base": "http://dead-worker:10001",
                "required_memory_gb": 4.0,
            })

            with patch("app.fetch_backtest_status", side_effect=Exception("connection refused")):
                await app._recover_submitted_tasks()

            entry = store.get_run("bt_orphan")
            self.assertEqual(entry["status"], "submitted")

    async def test_no_base_url_skipped(self) -> None:
        """Submitted tasks without backtest_api_base are skipped (no crash)."""
        with tempfile.TemporaryDirectory() as td:
            store = self._setup_store(td)
            store.upsert_run("bt_nobase", {
                "status": "submitted",
            })

            with patch("app.fetch_backtest_status") as mock_fetch:
                await app._recover_submitted_tasks()

            mock_fetch.assert_not_called()


if __name__ == "__main__":
    unittest.main()
