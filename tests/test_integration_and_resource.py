import unittest
import asyncio
import os

# Set environment variables before importing app
os.environ["BACKTEST_API_BASES"] = "http://main:8000"

from unittest.mock import patch, MagicMock
import app
from services.scheduler import DockerMetrics, DockerSelection

class TestIntegrationAndResource(unittest.IsolatedAsyncioTestCase):
    """
    1. submit_with_queue_control 集成测试
    2. 青铜分组调度优先级测试
    3. INFLIGHT 并发控制资源跟踪测试
    """

    def setUp(self):
        app.INFLIGHT_MEMORY_GB.clear()
        app.INFLIGHT_COUNTS.clear()
        app.BACKTEST_API_BASES = ["http://main:8000"]
        app.BACKTEST_BRONZE_API_BASES = ["http://bronze:8000"]

    @patch("app.update_mapping")
    async def test_submit_with_queue_control_returns_queued(self, mock_update):
        """验证 submit_with_queue_control 正确入队并返回 ("queued", None)"""
        mock_store = MagicMock()
        with patch("app.get_run_store", return_value=mock_store):
            res_status, docker_run_id = await app.submit_with_queue_control(
                backtest_id="bt_1",
                run_spec_bytes=b"{}",
                strategy_filename="s.py",
                strategy_bytes=b"code",
                runner_bytes=b"runner",
                required_memory_gb=1.0,
                symbol_count=1
            )
        self.assertEqual(res_status, "queued")
        self.assertIsNone(docker_run_id)
        mock_store.enqueue.assert_called_once()
        self.assertEqual(mock_store.enqueue.call_args[0][0], "bt_1")

    @patch("app.select_backtest_docker")
    async def test_bronze_scheduling_priority(self, mock_select):
        """测试 symbol_count < 6 时优先选择青铜节点"""
        # 模拟 select_backtest_docker，第一次调用（青铜节点）返回成功
        mock_bronze_selection = MagicMock(spec=DockerSelection)
        mock_bronze_selection.base_url = "http://bronze:8000"

        # 第一次调用返回青铜，不再调用第二次
        mock_select.return_value = mock_bronze_selection

        selection = await app.pick_backtest_target(
            required_memory_gb=2.0,
            symbol_count=3  # < BRONZE_SYMBOLS_THRESHOLD (6)
        )

        self.assertEqual(selection.base_url, "http://bronze:8000")
        # 验证是否使用了 BACKTEST_BRONZE_API_BASES 调用
        mock_select.assert_called_once()
        args, kwargs = mock_select.call_args
        self.assertEqual(kwargs["base_urls"], app.BACKTEST_BRONZE_API_BASES)

    @patch("app.select_backtest_docker")
    async def test_fallback_to_main_when_bronze_full(self, mock_select):
        """测试青铜节点不可用时回退到主节点"""
        mock_main_selection = MagicMock(spec=DockerSelection)
        mock_main_selection.base_url = "http://main:8000"

        # 第一次调用返回 None (青铜节点满了)，第二次返回主节点
        mock_select.side_effect = [None, mock_main_selection]

        selection = await app.pick_backtest_target(
            required_memory_gb=2.0,
            symbol_count=3
        )

        self.assertEqual(selection.base_url, "http://main:8000")
        self.assertEqual(mock_select.call_count, 2)

        # 检查第二次调用的参数是否为主节点
        args, kwargs = mock_select.call_args_list[1]
        self.assertEqual(kwargs["base_urls"], app.BACKTEST_API_BASES)

    def test_inflight_resource_tracking(self):
        """测试 reserve_inflight 和 release_inflight 正确跟踪资源"""
        base_url = "http://host1:8000"

        # 1. 预留资源
        app.reserve_inflight(base_url, 10.5)
        self.assertEqual(app.INFLIGHT_MEMORY_GB[base_url], 10.5)
        self.assertEqual(app.INFLIGHT_COUNTS[base_url], 1)

        # 2. 再次预留
        app.reserve_inflight(base_url, 5.0)
        self.assertEqual(app.INFLIGHT_MEMORY_GB[base_url], 15.5)
        self.assertEqual(app.INFLIGHT_COUNTS[base_url], 2)

        # 3. 释放一个
        app.release_inflight(base_url, 10.5)
        self.assertEqual(app.INFLIGHT_MEMORY_GB[base_url], 5.0)
        self.assertEqual(app.INFLIGHT_COUNTS[base_url], 1)

        # 4. 释放最后一个，确保键被清理
        app.release_inflight(base_url, 5.0)
        self.assertNotIn(base_url, app.INFLIGHT_MEMORY_GB)
        self.assertNotIn(base_url, app.INFLIGHT_COUNTS)

if __name__ == "__main__":
    unittest.main()
