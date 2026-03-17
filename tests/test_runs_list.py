import os
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

os.environ.setdefault("BACKTEST_API_BASES", "http://dummy")

import app


class TestRunsListEndpoint(unittest.TestCase):
    def setUp(self) -> None:
        self.test_dir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.test_dir.name)
        app.DATA_MOUNT_PATH = self.tmp_path
        app.QUEUE_PATH = self.tmp_path / "submit_queue.json"
        app.HUB_DB_PATH = self.tmp_path / "hub.sqlite3"
        app.RUN_STORAGE_PATH = self.tmp_path / "runs"
        app.RUN_STORE = None
        self.client = TestClient(app.app)

        store = app.get_run_store()
        store.upsert_run("bt_old", {"status": "failed", "requested_by": "joe", "created_at": "2026-03-17T18:14:40Z"})
        store.upsert_run("bt_new", {"status": "queued", "requested_by": "alice", "created_at": "2026-03-17T20:01:25Z"})

    def tearDown(self) -> None:
        self.test_dir.cleanup()

    def test_runs_list_returns_sorted_page(self) -> None:
        response = self.client.get("/runs/list", params={"page": 1, "pageSize": 10})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["total"], 2)
        self.assertEqual([item["backtest_id"] for item in payload["items"]], ["bt_new", "bt_old"])

    def test_runs_list_search_filters_results(self) -> None:
        response = self.client.get("/runs/list", params={"search": "alice"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["items"][0]["backtest_id"], "bt_new")


if __name__ == "__main__":
    unittest.main()
