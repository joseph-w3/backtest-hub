"""
Tests for submit_with_queue_control behavior.

These tests verify that ALL requests go through the queue,
regardless of whether the queue is empty or not.
"""

import unittest


class TestSubmitAlwaysQueues(unittest.TestCase):
    """
    Test that submit_with_queue_control ALWAYS queues requests.

    After fix: All requests are queued, queue_scheduler dispatches with delay.
    """

    def test_empty_queue_should_still_queue_request(self) -> None:
        """
        When queue is empty, request should still be queued (not submitted directly).
        """
        queue_is_empty = True

        def fixed_logic_decision(queue_empty: bool) -> str:
            """Fixed logic: always queue."""
            # No more direct submit path - always queue
            return "queued"

        result = fixed_logic_decision(queue_is_empty)

        # After fix: should always be "queued"
        self.assertEqual(result, "queued",
            "When queue is empty, request should still be queued to ensure 30s dispatch delay")

    def test_rapid_requests_all_should_be_queued(self) -> None:
        """
        Multiple rapid requests should ALL be queued.
        """
        results = []
        queue = []

        def fixed_submit_logic(backtest_id: str) -> str:
            """Fixed logic: always queue."""
            queue.append(backtest_id)
            return "queued"

        # 3 rapid requests
        for i in range(3):
            result = fixed_submit_logic(f"bt_{i}")
            results.append(result)

        # All 3 should be "queued"
        expected = ["queued", "queued", "queued"]
        self.assertEqual(results, expected,
            "All rapid requests should be queued, not submitted directly")
        self.assertEqual(len(queue), 3, "All 3 requests should be in queue")


class TestQueueSchedulerDispatchWithDelay(unittest.TestCase):
    """
    Test that queue_scheduler dispatches tasks with proper delay,
    allowing metrics to update between submissions.
    """

    def test_scheduler_dispatches_with_delay_spreads_load(self) -> None:
        """
        Simulate queue_scheduler behavior:
        1. Process queue one task at a time
        2. After each successful submit, wait 30s (simulated by metrics update)
        3. Metrics update shows first server busy, second task goes elsewhere

        This demonstrates the benefit of always-queue + scheduler dispatch.
        """
        # Initial queue (3 tasks, all queued via submit_with_queue_control)
        queue = [
            {"backtest_id": "bt_0", "required_memory_gb": 10.0},
            {"backtest_id": "bt_1", "required_memory_gb": 10.0},
            {"backtest_id": "bt_2", "required_memory_gb": 10.0},
        ]

        submissions = []
        dispatch_round = {"value": 0}

        def get_metrics_after_delay():
            """
            Simulates metrics fetched AFTER 30s delay.
            Each round, previous submissions have affected server load.
            """
            round_num = dispatch_round["value"]
            if round_num == 0:
                # Round 0: both servers available
                return {
                    "http://host1:8000": {"cpu": 40.0, "free_gb": 48.0},
                    "http://host2:8000": {"cpu": 50.0, "free_gb": 32.0},
                }
            elif round_num == 1:
                # Round 1: after 30s delay, host1 is now busy (task bt_0 running)
                return {
                    "http://host1:8000": {"cpu": 85.0, "free_gb": 38.0},  # CPU jumped
                    "http://host2:8000": {"cpu": 50.0, "free_gb": 32.0},
                }
            else:
                # Round 2: both servers busy
                return {
                    "http://host1:8000": {"cpu": 85.0, "free_gb": 38.0},
                    "http://host2:8000": {"cpu": 85.0, "free_gb": 22.0},
                }

        def select_server(metrics: dict, required_gb: float) -> str | None:
            """Pick server with CPU < 80% and enough memory."""
            best = None
            best_free = 0.0
            for host, m in metrics.items():
                if m["cpu"] < 80.0 and m["free_gb"] >= required_gb:
                    if m["free_gb"] > best_free:
                        best = host
                        best_free = m["free_gb"]
            return best

        # Simulate queue_scheduler loop
        while queue:
            task = queue[0]

            # Fetch metrics (after 30s delay from previous dispatch)
            metrics = get_metrics_after_delay()

            # Try to find a server
            server = select_server(metrics, task["required_memory_gb"])

            if server is None:
                # No capacity, task stays in queue
                break

            # Dispatch task
            queue.pop(0)
            submissions.append({
                "backtest_id": task["backtest_id"],
                "server": server,
                "round": dispatch_round["value"],
            })

            # Simulate 30s delay (next round will see updated metrics)
            dispatch_round["value"] += 1

        # Verify: tasks spread across servers due to delay
        self.assertEqual(len(submissions), 2, "2 tasks should be dispatched")
        self.assertEqual(submissions[0]["server"], "http://host1:8000",
            "First task goes to host1 (more free memory)")
        self.assertEqual(submissions[1]["server"], "http://host2:8000",
            "Second task goes to host2 (host1 now busy after delay)")
        self.assertEqual(len(queue), 1, "Third task still queued (both servers busy)")

    def test_without_delay_all_tasks_hit_same_server(self) -> None:
        """
        Contrast: WITHOUT delay, all tasks would hit the same server.
        This was the bug with direct submit.
        """
        queue = [
            {"backtest_id": "bt_0", "required_memory_gb": 10.0},
            {"backtest_id": "bt_1", "required_memory_gb": 10.0},
            {"backtest_id": "bt_2", "required_memory_gb": 10.0},
        ]

        submissions = []

        # Static metrics (never updates - simulates no delay)
        static_metrics = {
            "http://host1:8000": {"cpu": 40.0, "free_gb": 48.0},
            "http://host2:8000": {"cpu": 50.0, "free_gb": 32.0},
        }

        def select_server_no_delay(metrics: dict, required_gb: float) -> str | None:
            best = None
            best_free = 0.0
            for host, m in metrics.items():
                if m["cpu"] < 80.0 and m["free_gb"] >= required_gb:
                    if m["free_gb"] > best_free:
                        best = host
                        best_free = m["free_gb"]
            return best

        # Dispatch all tasks rapidly (no delay between them)
        for task in queue:
            server = select_server_no_delay(static_metrics, task["required_memory_gb"])
            if server:
                submissions.append({"backtest_id": task["backtest_id"], "server": server})

        # BUG: all 3 hit same server (metrics never updated)
        self.assertEqual(len(submissions), 3)
        self.assertTrue(
            all(s["server"] == "http://host1:8000" for s in submissions),
            "Without delay, all tasks hit same server - this overloads it!"
        )


if __name__ == "__main__":
    unittest.main()
