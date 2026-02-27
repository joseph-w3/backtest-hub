"""
Tests for SQLite-backed queue in SqliteRunStore.

Covers:
- enqueue / dequeue basics
- FIFO ordering
- concurrent enqueue safety
- crash recovery (WAL durability)
- empty queue edge case
- queue_length
- remove_from_queue_batch
- queue_position lookup
"""

import os
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

os.environ.setdefault("BACKTEST_API_BASES", "http://dummy")

from services.run_store_sqlite import SqliteRunStore


class TestEnqueueDequeue(unittest.TestCase):
    """Basic enqueue and dequeue operations."""

    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.db_path = Path(self._td.name) / "hub.sqlite3"
        self.store = SqliteRunStore(self.db_path)

    def tearDown(self) -> None:
        self._td.cleanup()

    def test_enqueue_single_item(self) -> None:
        self.store.enqueue("bt_1", "2026-02-01T00:00:00Z")
        items = self.store.read_queue()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["backtest_id"], "bt_1")
        self.assertEqual(items[0]["queued_at"], "2026-02-01T00:00:00Z")

    def test_dequeue_removes_first_item(self) -> None:
        self.store.enqueue("bt_1", "2026-02-01T00:00:00Z")
        self.store.enqueue("bt_2", "2026-02-01T00:00:01Z")
        self.store.dequeue("bt_1")
        items = self.store.read_queue()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["backtest_id"], "bt_2")

    def test_dequeue_nonexistent_id_is_noop(self) -> None:
        self.store.enqueue("bt_1", "2026-02-01T00:00:00Z")
        self.store.dequeue("bt_999")
        items = self.store.read_queue()
        self.assertEqual(len(items), 1)

    def test_enqueue_duplicate_id_raises_or_replaces(self) -> None:
        """Enqueueing the same backtest_id twice should not create duplicates."""
        self.store.enqueue("bt_1", "2026-02-01T00:00:00Z")
        # Second enqueue with same id — should be idempotent (INSERT OR REPLACE)
        self.store.enqueue("bt_1", "2026-02-01T00:00:05Z")
        items = self.store.read_queue()
        ids = [x["backtest_id"] for x in items]
        self.assertEqual(ids.count("bt_1"), 1, "No duplicate entries allowed")


class TestQueueOrdering(unittest.TestCase):
    """Queue must maintain FIFO order via queue_position."""

    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.db_path = Path(self._td.name) / "hub.sqlite3"
        self.store = SqliteRunStore(self.db_path)

    def tearDown(self) -> None:
        self._td.cleanup()

    def test_fifo_ordering(self) -> None:
        self.store.enqueue("bt_a", "2026-02-01T00:00:00Z")
        self.store.enqueue("bt_b", "2026-02-01T00:00:01Z")
        self.store.enqueue("bt_c", "2026-02-01T00:00:02Z")
        items = self.store.read_queue()
        ids = [x["backtest_id"] for x in items]
        self.assertEqual(ids, ["bt_a", "bt_b", "bt_c"])

    def test_dequeue_preserves_remaining_order(self) -> None:
        self.store.enqueue("bt_a", "2026-02-01T00:00:00Z")
        self.store.enqueue("bt_b", "2026-02-01T00:00:01Z")
        self.store.enqueue("bt_c", "2026-02-01T00:00:02Z")
        self.store.dequeue("bt_a")
        items = self.store.read_queue()
        ids = [x["backtest_id"] for x in items]
        self.assertEqual(ids, ["bt_b", "bt_c"])

    def test_dequeue_middle_preserves_order(self) -> None:
        self.store.enqueue("bt_a", "2026-02-01T00:00:00Z")
        self.store.enqueue("bt_b", "2026-02-01T00:00:01Z")
        self.store.enqueue("bt_c", "2026-02-01T00:00:02Z")
        self.store.dequeue("bt_b")
        items = self.store.read_queue()
        ids = [x["backtest_id"] for x in items]
        self.assertEqual(ids, ["bt_a", "bt_c"])


class TestConcurrentEnqueue(unittest.TestCase):
    """Multiple threads enqueueing simultaneously must not lose data."""

    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.db_path = Path(self._td.name) / "hub.sqlite3"
        self.store = SqliteRunStore(self.db_path)

    def tearDown(self) -> None:
        self._td.cleanup()

    def test_concurrent_enqueue_no_data_loss(self) -> None:
        n_threads = 10
        barrier = threading.Barrier(n_threads)
        errors: list[Exception] = []

        def worker(idx: int) -> None:
            try:
                barrier.wait(timeout=5)
                self.store.enqueue(f"bt_{idx}", f"2026-02-01T00:00:{idx:02d}Z")
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        self.assertEqual(errors, [], f"Threads raised errors: {errors}")
        self.assertEqual(self.store.queue_length(), n_threads)
        items = self.store.read_queue()
        ids = {x["backtest_id"] for x in items}
        expected = {f"bt_{i}" for i in range(n_threads)}
        self.assertEqual(ids, expected)


class TestCrashRecovery(unittest.TestCase):
    """WAL journal ensures committed writes survive even if process crashes."""

    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.db_path = Path(self._td.name) / "hub.sqlite3"
        self.store = SqliteRunStore(self.db_path)

    def tearDown(self) -> None:
        self._td.cleanup()

    def test_data_persists_after_reconnect(self) -> None:
        self.store.enqueue("bt_1", "2026-02-01T00:00:00Z")
        self.store.enqueue("bt_2", "2026-02-01T00:00:01Z")

        # Simulate "crash" by creating a fresh store instance against the same db
        store2 = SqliteRunStore(self.db_path)
        items = store2.read_queue()
        ids = [x["backtest_id"] for x in items]
        self.assertEqual(ids, ["bt_1", "bt_2"])

    def test_wal_checkpoint_does_not_lose_data(self) -> None:
        self.store.enqueue("bt_1", "2026-02-01T00:00:00Z")
        # Force WAL checkpoint
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()

        store2 = SqliteRunStore(self.db_path)
        self.assertEqual(store2.queue_length(), 1)


class TestEmptyQueue(unittest.TestCase):
    """Edge cases for empty queue."""

    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.db_path = Path(self._td.name) / "hub.sqlite3"
        self.store = SqliteRunStore(self.db_path)

    def tearDown(self) -> None:
        self._td.cleanup()

    def test_read_empty_queue_returns_empty_list(self) -> None:
        self.assertEqual(self.store.read_queue(), [])

    def test_queue_length_on_empty(self) -> None:
        self.assertEqual(self.store.queue_length(), 0)

    def test_dequeue_from_empty_is_noop(self) -> None:
        # Should not raise
        self.store.dequeue("bt_nonexistent")
        self.assertEqual(self.store.queue_length(), 0)


class TestQueueLength(unittest.TestCase):

    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.db_path = Path(self._td.name) / "hub.sqlite3"
        self.store = SqliteRunStore(self.db_path)

    def tearDown(self) -> None:
        self._td.cleanup()

    def test_queue_length_reflects_enqueue_dequeue(self) -> None:
        self.assertEqual(self.store.queue_length(), 0)
        self.store.enqueue("bt_1", "2026-02-01T00:00:00Z")
        self.assertEqual(self.store.queue_length(), 1)
        self.store.enqueue("bt_2", "2026-02-01T00:00:01Z")
        self.assertEqual(self.store.queue_length(), 2)
        self.store.dequeue("bt_1")
        self.assertEqual(self.store.queue_length(), 1)
        self.store.dequeue("bt_2")
        self.assertEqual(self.store.queue_length(), 0)


class TestRemoveFromQueueBatch(unittest.TestCase):

    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.db_path = Path(self._td.name) / "hub.sqlite3"
        self.store = SqliteRunStore(self.db_path)

    def tearDown(self) -> None:
        self._td.cleanup()

    def test_remove_batch_returns_removed_ids(self) -> None:
        self.store.enqueue("bt_a", "t1")
        self.store.enqueue("bt_b", "t2")
        self.store.enqueue("bt_c", "t3")
        removed = self.store.remove_from_queue_batch(["bt_b", "bt_x", "bt_b"])
        self.assertEqual(removed, {"bt_b"})
        items = self.store.read_queue()
        ids = [x["backtest_id"] for x in items]
        self.assertEqual(ids, ["bt_a", "bt_c"])

    def test_remove_batch_empty_list(self) -> None:
        self.store.enqueue("bt_a", "t1")
        removed = self.store.remove_from_queue_batch([])
        self.assertEqual(removed, set())
        self.assertEqual(self.store.queue_length(), 1)

    def test_remove_batch_all_items(self) -> None:
        self.store.enqueue("bt_a", "t1")
        self.store.enqueue("bt_b", "t2")
        removed = self.store.remove_from_queue_batch(["bt_a", "bt_b"])
        self.assertEqual(removed, {"bt_a", "bt_b"})
        self.assertEqual(self.store.queue_length(), 0)


class TestQueuePosition(unittest.TestCase):

    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.db_path = Path(self._td.name) / "hub.sqlite3"
        self.store = SqliteRunStore(self.db_path)

    def tearDown(self) -> None:
        self._td.cleanup()

    def test_queue_position_returns_index(self) -> None:
        self.store.enqueue("bt_a", "t1")
        self.store.enqueue("bt_b", "t2")
        self.store.enqueue("bt_c", "t3")
        self.assertEqual(self.store.queue_position("bt_a"), 0)
        self.assertEqual(self.store.queue_position("bt_b"), 1)
        self.assertEqual(self.store.queue_position("bt_c"), 2)

    def test_queue_position_missing_returns_none(self) -> None:
        self.store.enqueue("bt_a", "t1")
        self.assertIsNone(self.store.queue_position("bt_nonexistent"))

    def test_queue_position_after_dequeue(self) -> None:
        self.store.enqueue("bt_a", "t1")
        self.store.enqueue("bt_b", "t2")
        self.store.enqueue("bt_c", "t3")
        self.store.dequeue("bt_a")
        self.assertEqual(self.store.queue_position("bt_b"), 0)
        self.assertEqual(self.store.queue_position("bt_c"), 1)


if __name__ == "__main__":
    unittest.main()
