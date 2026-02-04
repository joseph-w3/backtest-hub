import json
import os
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("BACKTEST_API_BASES", "http://dummy")

import app
from services.scheduler import count_running_from_runs_payload


class TestQueuePersistence(unittest.TestCase):
    def test_count_running_from_runs_payload(self) -> None:
        payload = {
            "count": 4,
            "runs": [
                {"run_id": "1", "status": "running"},
                {"run_id": "2", "status": "succeeded"},
                {"run_id": "3", "status": "running"},
                {"run_id": "4", "status": "failed"},
            ],
        }
        self.assertEqual(count_running_from_runs_payload(payload), 2)

    def test_queue_roundtrip_and_position(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            qpath = Path(td) / "queue.json"
            self.assertEqual(app.read_queue(path=qpath), [])

            app.enqueue_backtest("a", queued_at="t1", path=qpath)
            app.enqueue_backtest("b", queued_at="t2", path=qpath)

            items = app.read_queue(path=qpath)
            self.assertEqual([x["backtest_id"] for x in items], ["a", "b"])
            self.assertEqual(app.queue_position("a", path=qpath), 0)
            self.assertEqual(app.queue_position("b", path=qpath), 1)
            self.assertIsNone(app.queue_position("c", path=qpath))

            item = app.dequeue_backtest(path=qpath)
            self.assertIsNotNone(item)
            self.assertEqual(item["backtest_id"], "a")
            items = app.read_queue(path=qpath)
            self.assertEqual([x["backtest_id"] for x in items], ["b"])

    def test_remove_batch(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            qpath = Path(td) / "queue.json"
            app.write_queue(
                [
                    {"backtest_id": "a", "queued_at": "t1"},
                    {"backtest_id": "b", "queued_at": "t2"},
                    {"backtest_id": "c", "queued_at": "t3"},
                ],
                path=qpath,
            )
            removed = app.remove_from_queue_batch(["b", "x", "b"], path=qpath)
            self.assertEqual(removed, {"b"})
            items = app.read_queue(path=qpath)
            self.assertEqual([x["backtest_id"] for x in items], ["a", "c"])


class TestQueueFileCorruption(unittest.TestCase):
    def test_read_queue_invalid_json_returns_empty(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            qpath = Path(td) / "queue.json"
            qpath.write_text("{not-json", encoding="utf-8")
            self.assertEqual(app.read_queue(path=qpath), [])

    def test_read_queue_non_list_returns_empty(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            qpath = Path(td) / "queue.json"
            qpath.write_text(json.dumps({"runs": []}), encoding="utf-8")
            self.assertEqual(app.read_queue(path=qpath), [])
