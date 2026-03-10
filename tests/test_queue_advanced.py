import unittest
import asyncio
import os
import tempfile
from pathlib import Path

# Set environment variables before importing app
os.environ["BACKTEST_API_BASES"] = "http://host1:8000"

from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime, timezone
import app

class TestQueueSchedulerAdvanced(unittest.IsolatedAsyncioTestCase):
    """
    针对 queue_scheduler 的高级功能测试：
    1. 成功提交后的 30s 延时
    2. 提交失败后的回队逻辑
    3. Double-check 时使用 local_reserved 验证
    """

    def setUp(self):
        self.test_dir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.test_dir.name)

        # Override paths to avoid PermissionError
        app.DATA_MOUNT_PATH = self.tmp_path
        app.QUEUE_PATH = self.tmp_path / "submit_queue.json"
        app.HUB_DB_PATH = self.tmp_path / "hub.sqlite3"
        app.RUN_STORAGE_PATH = self.tmp_path / "runs"
        app.RUN_STORE = None

        app.BACKTEST_API_BASES = ["http://host1:8000"]
        app.BACKTEST_BRONZE_API_BASES = []
        app.QUEUE_DISPATCH_DELAY_SECONDS = 30.0
        app.QUEUE_POLL_INTERVAL_SECONDS = 0.1
        app.INFLIGHT_MEMORY_GB.clear()
        app.INFLIGHT_COUNTS.clear()

    def tearDown(self):
        self.test_dir.cleanup()

    @patch("app.load_run_spec_payload")
    @patch("app.pick_backtest_target")
    @patch("app.load_run_assets")
    @patch("app.submit_to_backtest")
    @patch("app.asyncio.sleep")
    async def test_queue_scheduler_30s_delay_on_success(
        self, mock_sleep, mock_submit, mock_assets, mock_pick, mock_load_spec
    ):
        """测试成功提交后调用 asyncio.sleep(30)"""
        queue_item = {"backtest_id": "bt_1", "queued_at": "2026-02-04T10:00:00Z"}

        mock_store = MagicMock()
        # read_queue called twice: once at start of loop, once during double-check
        mock_store.read_queue.side_effect = [
            [queue_item],  # initial read
            [queue_item],  # double-check read
            [], [], []     # next iterations
        ]
        mock_store.remove_from_queue_batch.return_value = {"bt_1"}

        mock_load_spec.return_value = {"symbols": ["BTCUSDT"]}

        mock_selection = MagicMock()
        mock_selection.base_url = "http://host1:8000"
        mock_selection.metrics.memory_free_gb = 100.0
        mock_pick.return_value = mock_selection

        mock_assets.return_value = (b"{}", "strategy.py", b"code", None, None, b"runner")
        mock_submit.return_value = "docker_run_id_1"

        # 第一个 sleep 是 30s 延时，第二个是循环末尾的 poll sleep
        mock_sleep.side_effect = [None, asyncio.CancelledError()]

        with patch("app.get_run_store", return_value=mock_store):
            try:
                await app.queue_scheduler()
            except asyncio.CancelledError:
                pass

        # 验证是否调用了 30s 延时 (QUEUE_DISPATCH_DELAY_SECONDS)
        mock_sleep.assert_any_call(30.0)

    @patch("app.load_run_spec_payload")
    @patch("app.pick_backtest_target")
    @patch("app.load_run_assets")
    @patch("app.submit_to_backtest")
    @patch("app.update_mapping")
    async def test_queue_scheduler_requeue_on_failure(
        self, mock_update, mock_submit, mock_assets, mock_pick, mock_load_spec
    ):
        """提交失败时，任务应放回队列并记录错误"""
        queue_item = {"backtest_id": "bt_1", "queued_at": "2026-02-04T10:00:00Z"}

        mock_store = MagicMock()
        mock_store.read_queue.side_effect = [
            [queue_item],  # initial read
            [queue_item],  # double-check read
            [], [], []
        ]
        mock_store.remove_from_queue_batch.return_value = {"bt_1"}

        mock_load_spec.return_value = {"symbols": ["BTCUSDT"]}

        mock_selection = MagicMock()
        mock_selection.base_url = "http://host1:8000"
        mock_selection.metrics.memory_free_gb = 100.0
        mock_pick.return_value = mock_selection

        mock_assets.return_value = (b"{}", "strategy.py", b"code", None, None, b"runner")
        # 模拟提交失败
        mock_submit.side_effect = Exception("Submit failed")

        with patch("app.get_run_store", return_value=mock_store):
            with patch("app.asyncio.sleep", side_effect=[asyncio.CancelledError()]):
                try:
                    await app.queue_scheduler()
                except asyncio.CancelledError:
                    pass

        # 验证是否重新入队
        mock_store.enqueue.assert_called_once()
        enqueue_args = mock_store.enqueue.call_args[0]
        self.assertEqual(enqueue_args[0], "bt_1")
        self.assertEqual(enqueue_args[1], "2026-02-04T10:00:00Z")

        # 验证更新了错误信息
        mock_update.assert_any_call("bt_1", unittest.mock.ANY)

        found_error = False
        for call in mock_update.call_args_list:
            if call.args[0] == "bt_1" and "last_error" in call.args[1]:
                if call.args[1]["last_error"] == "Submit failed":
                    found_error = True
                    break
        self.assertTrue(found_error, "Should have called update_mapping with 'Submit failed'")

    @patch("app.load_run_spec_payload")
    @patch("app.pick_backtest_target")
    @patch("app.load_run_assets")
    @patch("app.submit_to_backtest")
    async def test_queue_scheduler_double_check_uses_local_reserved(
        self, mock_submit, mock_assets, mock_pick, mock_load_spec
    ):
        """
        验证 double-check 逻辑使用 local_reserved。
        如果 local_reserved 扣除后内存不足，则不应提交。
        """
        # 模拟两个任务在同一批次处理
        item1 = {"backtest_id": "bt_1", "queued_at": "2026-02-04T10:00:00Z"}
        item2 = {"backtest_id": "bt_2", "queued_at": "2026-02-04T10:00:01Z"}

        mock_store = MagicMock()
        mock_store.read_queue.side_effect = [
            [item1, item2],  # initial read
            [item1, item2],  # double-check for bt_1
            [item1, item2],  # double-check for bt_2
            [], [], []       # next iterations
        ]
        mock_store.remove_from_queue_batch.return_value = {"bt_1"}

        # 每个任务需要 10GB
        mock_load_spec.return_value = {"symbols": ["A"] * 10}

        # 模拟 server 只有 15GB 内存
        mock_selection = MagicMock()
        mock_selection.base_url = "http://host1:8000"
        mock_selection.metrics.memory_free_gb = 15.0
        mock_pick.return_value = mock_selection

        mock_assets.return_value = (b"{}", "strategy.py", b"code", None, None, b"runner")

        # 第一个任务成功后会有一个 30s 延时
        with patch("app.get_run_store", return_value=mock_store):
            with patch("app.asyncio.sleep", side_effect=[None, asyncio.CancelledError()]):
                try:
                    await app.queue_scheduler()
                except asyncio.CancelledError:
                    pass

        # 第一个任务 bt_1 应该成功 (15 >= 10)
        # 第二个任务 bt_2 应该在 double-check 失败
        self.assertEqual(mock_submit.call_count, 1)
        mock_store.remove_from_queue_batch.assert_called_once_with(["bt_1"])

    @patch("app.select_backtest_docker")
    @patch("app.load_run_assets")
    @patch("app.submit_to_backtest")
    @patch("app.asyncio.sleep")
    async def test_queue_scheduler_blocks_second_large_run_when_existing_submitted_reservation_present(
        self, mock_sleep, mock_submit, mock_assets, mock_select
    ):
        """已 submitted 的大任务要继续占用 reservation，避免再次 dispatch 同级大任务。"""
        queue_item = {"backtest_id": "bt_new", "queued_at": "2026-03-10T11:00:00Z"}
        submitted_run = {
            "backtest_id": "bt_existing",
            "backtest_api_base": "http://host1:8000",
            "required_memory_gb": 190.0,
        }

        mock_store = MagicMock()
        mock_store.read_queue.side_effect = [
            [queue_item],
            [queue_item],
            [queue_item],
        ]

        def _get_runs_by_status(status: str):
            if status == "submitted":
                return [submitted_run]
            if status == "running":
                return []
            return []

        mock_store.get_runs_by_status.side_effect = _get_runs_by_status

        def _select_backtest_docker(**kwargs):
            reserved = kwargs["reserved_memory_gb"]
            self.assertEqual(reserved.get("http://host1:8000"), 190.0)
            return None

        mock_select.side_effect = _select_backtest_docker
        mock_sleep.side_effect = asyncio.CancelledError()

        with patch("app.get_run_store", return_value=mock_store), \
             patch.object(app, "MEMORY_PER_SYMBOL_GB", 2.5):
            with patch("app.load_run_spec_payload", return_value={"symbols": ["A"] * 76}):
                try:
                    await app.queue_scheduler()
                except asyncio.CancelledError:
                    pass

        mock_select.assert_called_once()
        mock_assets.assert_not_called()
        mock_submit.assert_not_called()
        mock_store.remove_from_queue_batch.assert_not_called()

    @patch("app.select_backtest_docker")
    @patch("app.load_run_assets")
    @patch("app.submit_to_backtest")
    @patch("app.asyncio.sleep")
    async def test_queue_scheduler_allows_small_run_when_combined_reservation_still_fits(
        self, mock_sleep, mock_submit, mock_assets, mock_select
    ):
        """已有中等任务时，只要总内存仍然 fit，小任务应继续 dispatch。"""
        queue_item = {"backtest_id": "bt_small", "queued_at": "2026-03-10T11:00:00Z"}
        submitted_run = {
            "backtest_id": "bt_existing",
            "backtest_api_base": "http://host1:8000",
            "required_memory_gb": 50.0,
        }

        mock_store = MagicMock()
        mock_store.read_queue.side_effect = [
            [queue_item],
            [queue_item],
            [],
        ]
        mock_store.remove_from_queue_batch.return_value = {"bt_small"}

        def _get_runs_by_status(status: str):
            if status == "submitted":
                return [submitted_run]
            if status == "running":
                return []
            return []

        mock_store.get_runs_by_status.side_effect = _get_runs_by_status

        def _select_backtest_docker(**kwargs):
            reserved = kwargs["reserved_memory_gb"]
            self.assertEqual(reserved.get("http://host1:8000"), 50.0)
            selection = MagicMock()
            selection.base_url = "http://host1:8000"
            selection.metrics.memory_free_gb = 120.0
            return selection

        mock_select.side_effect = _select_backtest_docker
        mock_assets.return_value = (b"{}", "strategy.py", b"code", None, None, b"runner")
        mock_submit.return_value = "docker_run_small"
        mock_sleep.side_effect = [None, asyncio.CancelledError()]

        with patch("app.get_run_store", return_value=mock_store), \
             patch.object(app, "MEMORY_PER_SYMBOL_GB", 2.5):
            with patch("app.load_run_spec_payload", return_value={"symbols": ["A"] * 10}):
                try:
                    await app.queue_scheduler()
                except asyncio.CancelledError:
                    pass

        mock_submit.assert_called_once()
        mock_store.remove_from_queue_batch.assert_called_once_with(["bt_small"])

if __name__ == "__main__":
    unittest.main()
