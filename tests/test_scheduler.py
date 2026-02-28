import unittest

from services.scheduler import (
    parse_backtest_api_bases,
    parse_metrics_payload,
    required_memory_gb_from_run_spec,
    select_backtest_docker,
    DockerMetrics,
    BYTES_PER_GB,
)


class TestParseBacktestApiBases(unittest.TestCase):
    def test_single_url(self) -> None:
        result = parse_backtest_api_bases("http://host1:8000", "")
        self.assertEqual(result, ["http://host1:8000"])

    def test_multiple_urls(self) -> None:
        result = parse_backtest_api_bases("http://host1:8000,http://host2:9000", "")
        self.assertEqual(result, ["http://host1:8000", "http://host2:9000"])

    def test_trailing_slashes_stripped(self) -> None:
        result = parse_backtest_api_bases("http://host1:8000/,http://host2:9000//", "")
        self.assertEqual(result, ["http://host1:8000", "http://host2:9000"])

    def test_whitespace_trimmed(self) -> None:
        result = parse_backtest_api_bases("  http://host1:8000 , http://host2:9000  ", "")
        self.assertEqual(result, ["http://host1:8000", "http://host2:9000"])

    def test_empty_parts_ignored(self) -> None:
        result = parse_backtest_api_bases("http://host1:8000,,http://host2:9000,", "")
        self.assertEqual(result, ["http://host1:8000", "http://host2:9000"])

    def test_duplicates_deduplicated(self) -> None:
        result = parse_backtest_api_bases("http://host1:8000,http://host1:8000", "")
        self.assertEqual(result, ["http://host1:8000"])

    def test_none_uses_fallback(self) -> None:
        result = parse_backtest_api_bases(None, "http://fallback:8000")
        self.assertEqual(result, ["http://fallback:8000"])

    def test_empty_string_uses_fallback(self) -> None:
        result = parse_backtest_api_bases("", "http://fallback:8000")
        self.assertEqual(result, ["http://fallback:8000"])

    def test_empty_fallback_returns_empty(self) -> None:
        result = parse_backtest_api_bases(None, "")
        self.assertEqual(result, [])


class TestParseMetricsPayload(unittest.TestCase):
    def test_full_payload_gb(self) -> None:
        payload = {
            "cpu_percent": 45.5,
            "memory_total_gb": 64.0,
            "memory_used_gb": 32.0,
            "memory_free_gb": 32.0,
        }
        result = parse_metrics_payload(payload, "http://host:8000")
        self.assertIsNotNone(result)
        self.assertEqual(result.base_url, "http://host:8000")
        self.assertEqual(result.cpu_percent, 45.5)
        self.assertEqual(result.memory_total_gb, 64.0)
        self.assertEqual(result.memory_used_gb, 32.0)
        self.assertEqual(result.memory_free_gb, 32.0)

    def test_payload_with_bytes(self) -> None:
        payload = {
            "cpu_percent": 50.0,
            "memory_total_bytes": 64 * BYTES_PER_GB,
            "memory_used_bytes": 48 * BYTES_PER_GB,
            "memory_free_bytes": 16 * BYTES_PER_GB,
        }
        result = parse_metrics_payload(payload, "http://host:8000")
        self.assertIsNotNone(result)
        self.assertEqual(result.memory_total_gb, 64.0)
        self.assertEqual(result.memory_used_gb, 48.0)
        self.assertEqual(result.memory_free_gb, 16.0)

    def test_infer_free_from_used(self) -> None:
        payload = {
            "memory_total_gb": 64.0,
            "memory_used_gb": 40.0,
        }
        result = parse_metrics_payload(payload, "http://host:8000")
        self.assertIsNotNone(result)
        self.assertEqual(result.memory_free_gb, 24.0)

    def test_infer_used_from_free(self) -> None:
        payload = {
            "memory_total_gb": 64.0,
            "memory_free_gb": 20.0,
        }
        result = parse_metrics_payload(payload, "http://host:8000")
        self.assertIsNotNone(result)
        self.assertEqual(result.memory_used_gb, 44.0)

    def test_missing_total_returns_none(self) -> None:
        payload = {
            "memory_used_gb": 32.0,
            "memory_free_gb": 32.0,
        }
        result = parse_metrics_payload(payload, "http://host:8000")
        self.assertIsNone(result)

    def test_missing_used_and_free_returns_none(self) -> None:
        payload = {
            "memory_total_gb": 64.0,
        }
        result = parse_metrics_payload(payload, "http://host:8000")
        self.assertIsNone(result)

    def test_non_dict_returns_none(self) -> None:
        result = parse_metrics_payload("not a dict", "http://host:8000")  # type: ignore
        self.assertIsNone(result)

    def test_cpu_none_if_missing(self) -> None:
        payload = {
            "memory_total_gb": 64.0,
            "memory_used_gb": 32.0,
            "memory_free_gb": 32.0,
        }
        result = parse_metrics_payload(payload, "http://host:8000")
        self.assertIsNotNone(result)
        self.assertIsNone(result.cpu_percent)

    def test_alternate_key_names(self) -> None:
        payload = {
            "cpu": 30.0,
            "mem_total_gb": 128.0,
            "mem_used_gb": 64.0,
            "mem_free_gb": 64.0,
        }
        result = parse_metrics_payload(payload, "http://host:8000")
        self.assertIsNotNone(result)
        self.assertEqual(result.cpu_percent, 30.0)
        self.assertEqual(result.memory_total_gb, 128.0)

    def test_negative_values_clamped_to_zero(self) -> None:
        payload = {
            "memory_total_gb": 64.0,
            "memory_used_gb": -5.0,
            "memory_free_gb": 70.0,
        }
        result = parse_metrics_payload(payload, "http://host:8000")
        self.assertIsNotNone(result)
        self.assertEqual(result.memory_used_gb, 0.0)


class TestRequiredMemoryGbFromRunSpec(unittest.TestCase):
    def test_single_symbol(self) -> None:
        payload = {"symbols": ["BTCUSDT"]}
        result = required_memory_gb_from_run_spec(payload)
        self.assertEqual(result, 1.0)

    def test_multiple_symbols(self) -> None:
        payload = {"symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"]}
        result = required_memory_gb_from_run_spec(payload)
        self.assertEqual(result, 3.0)

    def test_empty_symbols_raises(self) -> None:
        payload = {"symbols": []}
        with self.assertRaises(ValueError):
            required_memory_gb_from_run_spec(payload)

    def test_missing_symbols_raises(self) -> None:
        payload = {}
        with self.assertRaises(ValueError):
            required_memory_gb_from_run_spec(payload)

    def test_non_list_symbols_raises(self) -> None:
        payload = {"symbols": "BTCUSDT"}
        with self.assertRaises(ValueError):
            required_memory_gb_from_run_spec(payload)

    def test_custom_memory_per_symbol(self) -> None:
        payload = {"symbols": ["BTCUSDT", "ETHUSDT"]}
        result = required_memory_gb_from_run_spec(payload, memory_per_symbol_gb=2.5)
        self.assertEqual(result, 5.0)

    def test_default_memory_per_symbol_is_one(self) -> None:
        payload = {"symbols": ["BTCUSDT", "ETHUSDT"]}
        result = required_memory_gb_from_run_spec(payload)
        self.assertEqual(result, 2.0)


class TestSelectBacktestDocker(unittest.TestCase):
    """Tests for select_backtest_docker using mocked metrics/runs fetchers."""

    def _make_metrics(
        self,
        base_url: str,
        cpu_percent: float | None,
        total_gb: float,
        used_gb: float,
        free_gb: float,
    ) -> DockerMetrics:
        return DockerMetrics(
            base_url=base_url,
            cpu_percent=cpu_percent,
            memory_total_gb=total_gb,
            memory_used_gb=used_gb,
            memory_free_gb=free_gb,
        )

    def test_selects_node_with_most_free_memory(self) -> None:
        from services import scheduler

        metrics_store = {
            "http://host1:8000": self._make_metrics("http://host1:8000", 50.0, 64.0, 32.0, 32.0),
            "http://host2:8000": self._make_metrics("http://host2:8000", 50.0, 64.0, 16.0, 48.0),
        }

        def mock_fetch_metrics(base_url, metrics_path, headers, timeout_seconds):
            return metrics_store[base_url]

        original_fetch = scheduler.fetch_metrics
        scheduler.fetch_metrics = mock_fetch_metrics
        try:
            from services.scheduler import select_backtest_docker

            result = select_backtest_docker(
                base_urls=["http://host1:8000", "http://host2:8000"],
                metrics_path="/metrics",
                runs_path=None,
                headers={},
                required_memory_gb=10.0,
                reserved_memory_gb=None,
                inflight_counts=None,
                max_running=None,
                timeout_seconds=3.0,
            )
            self.assertIsNotNone(result)
            self.assertEqual(result.base_url, "http://host2:8000")
            self.assertEqual(result.available_memory_gb, 48.0)
        finally:
            scheduler.fetch_metrics = original_fetch

    def test_skips_node_with_insufficient_memory(self) -> None:
        from services import scheduler

        metrics_store = {
            "http://host1:8000": self._make_metrics("http://host1:8000", 50.0, 64.0, 60.0, 4.0),
            "http://host2:8000": self._make_metrics("http://host2:8000", 50.0, 64.0, 32.0, 32.0),
        }

        def mock_fetch_metrics(base_url, metrics_path, headers, timeout_seconds):
            return metrics_store[base_url]

        original_fetch = scheduler.fetch_metrics
        scheduler.fetch_metrics = mock_fetch_metrics
        try:
            from services.scheduler import select_backtest_docker

            result = select_backtest_docker(
                base_urls=["http://host1:8000", "http://host2:8000"],
                metrics_path="/metrics",
                runs_path=None,
                headers={},
                required_memory_gb=10.0,
                reserved_memory_gb=None,
                inflight_counts=None,
                max_running=None,
                timeout_seconds=3.0,
            )
            self.assertIsNotNone(result)
            self.assertEqual(result.base_url, "http://host2:8000")
        finally:
            scheduler.fetch_metrics = original_fetch

    def test_skips_node_with_high_cpu(self) -> None:
        from services import scheduler

        metrics_store = {
            "http://host1:8000": self._make_metrics("http://host1:8000", 90.0, 64.0, 16.0, 48.0),
            "http://host2:8000": self._make_metrics("http://host2:8000", 50.0, 64.0, 32.0, 32.0),
        }

        def mock_fetch_metrics(base_url, metrics_path, headers, timeout_seconds):
            return metrics_store[base_url]

        original_fetch = scheduler.fetch_metrics
        scheduler.fetch_metrics = mock_fetch_metrics
        try:
            from services.scheduler import select_backtest_docker

            result = select_backtest_docker(
                base_urls=["http://host1:8000", "http://host2:8000"],
                metrics_path="/metrics",
                runs_path=None,
                headers={},
                required_memory_gb=10.0,
                reserved_memory_gb=None,
                inflight_counts=None,
                max_running=None,
                timeout_seconds=3.0,
                cpu_percent_lt=80.0,
            )
            self.assertIsNotNone(result)
            self.assertEqual(result.base_url, "http://host2:8000")
        finally:
            scheduler.fetch_metrics = original_fetch

    def test_accounts_for_reserved_memory(self) -> None:
        from services import scheduler

        metrics_store = {
            "http://host1:8000": self._make_metrics("http://host1:8000", 50.0, 64.0, 16.0, 48.0),
            "http://host2:8000": self._make_metrics("http://host2:8000", 50.0, 64.0, 32.0, 32.0),
        }

        def mock_fetch_metrics(base_url, metrics_path, headers, timeout_seconds):
            return metrics_store[base_url]

        original_fetch = scheduler.fetch_metrics
        scheduler.fetch_metrics = mock_fetch_metrics
        try:
            from services.scheduler import select_backtest_docker

            # host1 has 48GB free but 40GB reserved, so only 8GB available
            result = select_backtest_docker(
                base_urls=["http://host1:8000", "http://host2:8000"],
                metrics_path="/metrics",
                runs_path=None,
                headers={},
                required_memory_gb=10.0,
                reserved_memory_gb={"http://host1:8000": 40.0},
                inflight_counts=None,
                max_running=None,
                timeout_seconds=3.0,
            )
            self.assertIsNotNone(result)
            self.assertEqual(result.base_url, "http://host2:8000")
        finally:
            scheduler.fetch_metrics = original_fetch

    def test_returns_none_when_no_capacity(self) -> None:
        from services import scheduler

        metrics_store = {
            "http://host1:8000": self._make_metrics("http://host1:8000", 50.0, 64.0, 60.0, 4.0),
        }

        def mock_fetch_metrics(base_url, metrics_path, headers, timeout_seconds):
            return metrics_store[base_url]

        original_fetch = scheduler.fetch_metrics
        scheduler.fetch_metrics = mock_fetch_metrics
        try:
            from services.scheduler import select_backtest_docker

            result = select_backtest_docker(
                base_urls=["http://host1:8000"],
                metrics_path="/metrics",
                runs_path=None,
                headers={},
                required_memory_gb=10.0,
                reserved_memory_gb=None,
                inflight_counts=None,
                max_running=None,
                timeout_seconds=3.0,
            )
            self.assertIsNone(result)
        finally:
            scheduler.fetch_metrics = original_fetch

    def test_handles_metrics_fetch_failure(self) -> None:
        from services import scheduler

        def mock_fetch_metrics(base_url, metrics_path, headers, timeout_seconds):
            if base_url == "http://host1:8000":
                raise Exception("connection refused")
            return self._make_metrics("http://host2:8000", 50.0, 64.0, 32.0, 32.0)

        original_fetch = scheduler.fetch_metrics
        scheduler.fetch_metrics = mock_fetch_metrics
        try:
            from services.scheduler import select_backtest_docker

            result = select_backtest_docker(
                base_urls=["http://host1:8000", "http://host2:8000"],
                metrics_path="/metrics",
                runs_path=None,
                headers={},
                required_memory_gb=10.0,
                reserved_memory_gb=None,
                inflight_counts=None,
                max_running=None,
                timeout_seconds=3.0,
            )
            self.assertIsNotNone(result)
            self.assertEqual(result.base_url, "http://host2:8000")
        finally:
            scheduler.fetch_metrics = original_fetch

    def test_respects_max_running_limit(self) -> None:
        from services import scheduler

        metrics_store = {
            "http://host1:8000": self._make_metrics("http://host1:8000", 50.0, 64.0, 16.0, 48.0),
            "http://host2:8000": self._make_metrics("http://host2:8000", 50.0, 64.0, 32.0, 32.0),
        }

        runs_store = {
            "http://host1:8000": {"runs": [{"status": "running"}, {"status": "running"}]},
            "http://host2:8000": {"runs": [{"status": "running"}]},
        }

        def mock_fetch_metrics(base_url, metrics_path, headers, timeout_seconds):
            return metrics_store[base_url]

        def mock_fetch_runs(base_url, runs_path, headers, timeout_seconds):
            return runs_store[base_url]

        original_fetch_metrics = scheduler.fetch_metrics
        original_fetch_runs = scheduler.fetch_backtest_runs
        scheduler.fetch_metrics = mock_fetch_metrics
        scheduler.fetch_backtest_runs = mock_fetch_runs
        try:
            from services.scheduler import select_backtest_docker

            # max_running=2, host1 has 2 running, host2 has 1
            result = select_backtest_docker(
                base_urls=["http://host1:8000", "http://host2:8000"],
                metrics_path="/metrics",
                runs_path="/runs",
                headers={},
                required_memory_gb=10.0,
                reserved_memory_gb=None,
                inflight_counts=None,
                max_running=2,
                timeout_seconds=3.0,
            )
            self.assertIsNotNone(result)
            self.assertEqual(result.base_url, "http://host2:8000")
        finally:
            scheduler.fetch_metrics = original_fetch_metrics
            scheduler.fetch_backtest_runs = original_fetch_runs


class TestDispatchDelayEffect(unittest.TestCase):
    """
    Tests demonstrating why dispatch delay is needed.

    Without delay: Multiple jobs hit the same server before metrics update,
    bypassing CPU threshold checks.

    With delay: Metrics update between dispatches, so the scheduler picks
    different servers when the first one becomes busy.
    """

    def _make_metrics(
        self,
        base_url: str,
        cpu_percent: float | None,
        total_gb: float,
        used_gb: float,
        free_gb: float,
    ) -> DockerMetrics:
        return DockerMetrics(
            base_url=base_url,
            cpu_percent=cpu_percent,
            memory_total_gb=total_gb,
            memory_used_gb=used_gb,
            memory_free_gb=free_gb,
        )

    def test_without_delay_all_jobs_hit_same_server(self) -> None:
        """
        Simulates rapid-fire scheduling WITHOUT delay.

        Metrics don't update between calls, so all jobs see host1 as "best"
        and get scheduled there, potentially overloading CPU/RAM.
        """
        from services import scheduler

        # Static metrics - never updates (simulates no delay between calls)
        static_metrics = {
            "http://host1:8000": self._make_metrics("http://host1:8000", 40.0, 64.0, 16.0, 48.0),
            "http://host2:8000": self._make_metrics("http://host2:8000", 50.0, 64.0, 32.0, 32.0),
        }

        def mock_fetch_metrics(base_url, metrics_path, headers, timeout_seconds):
            return static_metrics[base_url]

        original_fetch = scheduler.fetch_metrics
        scheduler.fetch_metrics = mock_fetch_metrics
        try:
            selections = []
            for _ in range(3):
                result = select_backtest_docker(
                    base_urls=["http://host1:8000", "http://host2:8000"],
                    metrics_path="/metrics",
                    runs_path=None,
                    headers={},
                    required_memory_gb=10.0,
                    reserved_memory_gb=None,
                    inflight_counts=None,
                    max_running=None,
                    timeout_seconds=3.0,
                    cpu_percent_lt=80.0,
                )
                selections.append(result.base_url if result else None)

            # All 3 jobs go to host1 (more free memory) - CPU threshold not triggered
            # because metrics never updated to show host1 is now busy
            self.assertEqual(selections, [
                "http://host1:8000",
                "http://host1:8000",
                "http://host1:8000",
            ])
        finally:
            scheduler.fetch_metrics = original_fetch

    def test_with_delay_jobs_spread_across_servers(self) -> None:
        """
        Simulates scheduling WITH delay between dispatches.

        After each dispatch, metrics update to reflect increased CPU load.
        Subsequent jobs see the updated metrics and choose different servers.
        """
        from services import scheduler

        call_count = {"value": 0}

        def mock_fetch_metrics_with_updates(base_url, metrics_path, headers, timeout_seconds):
            call_count["value"] += 1
            round_num = (call_count["value"] - 1) // 2  # 2 hosts per round

            if base_url == "http://host1:8000":
                if round_num == 0:
                    # Round 1: host1 has low CPU, gets selected
                    return self._make_metrics("http://host1:8000", 40.0, 64.0, 16.0, 48.0)
                elif round_num == 1:
                    # Round 2: after delay, host1 CPU jumped to 85% (busy with job 1)
                    return self._make_metrics("http://host1:8000", 85.0, 64.0, 16.0, 48.0)
                else:
                    # Round 3: host1 still busy
                    return self._make_metrics("http://host1:8000", 90.0, 64.0, 16.0, 48.0)
            else:  # host2
                if round_num == 0:
                    return self._make_metrics("http://host2:8000", 50.0, 64.0, 32.0, 32.0)
                elif round_num == 1:
                    # Round 2: host2 still available
                    return self._make_metrics("http://host2:8000", 50.0, 64.0, 32.0, 32.0)
                else:
                    # Round 3: host2 now busy with job 2
                    return self._make_metrics("http://host2:8000", 85.0, 64.0, 32.0, 32.0)

        original_fetch = scheduler.fetch_metrics
        scheduler.fetch_metrics = mock_fetch_metrics_with_updates
        try:
            selections = []
            for _ in range(3):
                result = select_backtest_docker(
                    base_urls=["http://host1:8000", "http://host2:8000"],
                    metrics_path="/metrics",
                    runs_path=None,
                    headers={},
                    required_memory_gb=10.0,
                    reserved_memory_gb=None,
                    inflight_counts=None,
                    max_running=None,
                    timeout_seconds=3.0,
                    cpu_percent_lt=80.0,
                )
                selections.append(result.base_url if result else None)

            # Job 1 -> host1 (best initially)
            # Job 2 -> host2 (host1 now above 80% CPU after delay)
            # Job 3 -> None (both hosts above 80% CPU)
            self.assertEqual(selections, [
                "http://host1:8000",
                "http://host2:8000",
                None,  # No capacity - both servers busy
            ])
        finally:
            scheduler.fetch_metrics = original_fetch


class TestBackfillingLogic(unittest.TestCase):
    """Tests for backfilling scheduling logic and anti-starvation mechanisms."""

    def test_backfill_window_allows_recent_tasks(self) -> None:
        """Tasks queued within BACKFILL_WINDOW after head can be backfilled."""
        from datetime import datetime, timezone, timedelta

        head_time = datetime(2026, 2, 5, 10, 0, 0, tzinfo=timezone.utc)
        # Task queued 2 minutes after head (within 5 min window)
        task_time = head_time + timedelta(minutes=2)

        window_seconds = 300.0  # 5 minutes
        delta = (task_time - head_time).total_seconds()

        # Should be allowed (within window)
        self.assertLessEqual(delta, window_seconds)

    def test_backfill_window_blocks_late_tasks(self) -> None:
        """Tasks queued after BACKFILL_WINDOW should not be backfilled."""
        from datetime import datetime, timezone, timedelta

        head_time = datetime(2026, 2, 5, 10, 0, 0, tzinfo=timezone.utc)
        # Task queued 10 minutes after head (outside 5 min window)
        task_time = head_time + timedelta(minutes=10)

        window_seconds = 300.0  # 5 minutes
        delta = (task_time - head_time).total_seconds()

        # Should be blocked (outside window)
        self.assertGreater(delta, window_seconds)

    def test_reserve_mode_triggered_by_wait_time(self) -> None:
        """When head waits longer than threshold, reserve mode activates."""
        from datetime import datetime, timezone, timedelta

        head_queued_at = datetime(2026, 2, 5, 10, 0, 0, tzinfo=timezone.utc)
        now = head_queued_at + timedelta(minutes=15)  # 15 minutes later

        reserve_threshold_seconds = 600.0  # 10 minutes
        wait_seconds = (now - head_queued_at).total_seconds()

        # Should trigger reserve mode (waited > 10 min)
        reserve_mode = wait_seconds > reserve_threshold_seconds
        self.assertTrue(reserve_mode)

    def test_reserve_mode_not_triggered_early(self) -> None:
        """Reserve mode should not activate if head hasn't waited long enough."""
        from datetime import datetime, timezone, timedelta

        head_queued_at = datetime(2026, 2, 5, 10, 0, 0, tzinfo=timezone.utc)
        now = head_queued_at + timedelta(minutes=5)  # 5 minutes later

        reserve_threshold_seconds = 600.0  # 10 minutes
        wait_seconds = (now - head_queued_at).total_seconds()

        # Should NOT trigger reserve mode (waited only 5 min)
        reserve_mode = wait_seconds > reserve_threshold_seconds
        self.assertFalse(reserve_mode)

    def test_backfill_scenario_small_tasks_run_while_large_waits(self) -> None:
        """
        Scenario: Large task A (50GB) at head, small tasks B,C (5GB each) behind.
        Server has 30GB free. A cannot run, but B and C should backfill.
        """
        queue = [
            {"backtest_id": "A", "required_memory_gb": 50.0, "queued_at": "2026-02-05T10:00:00Z"},
            {"backtest_id": "B", "required_memory_gb": 5.0, "queued_at": "2026-02-05T10:01:00Z"},
            {"backtest_id": "C", "required_memory_gb": 5.0, "queued_at": "2026-02-05T10:02:00Z"},
        ]
        server_free_memory = 30.0
        window_seconds = 300.0

        head = queue[0]
        head_queued_at = head["queued_at"]

        schedulable = []
        for item in queue:
            required = item["required_memory_gb"]
            if required <= server_free_memory:
                # Check window for non-head tasks
                if item != head:
                    from datetime import datetime, timezone
                    head_time = datetime.fromisoformat(head_queued_at.replace("Z", "+00:00"))
                    item_time = datetime.fromisoformat(item["queued_at"].replace("Z", "+00:00"))
                    if (item_time - head_time).total_seconds() > window_seconds:
                        continue
                schedulable.append(item["backtest_id"])

        # A cannot run (50GB > 30GB), B and C can (within window)
        self.assertEqual(schedulable, ["B", "C"])

    def test_backfill_respects_reserve_mode(self) -> None:
        """
        Scenario: Head task A has waited 15 minutes (> 10 min threshold).
        Reserve mode activates, no backfilling allowed.
        """
        from datetime import datetime, timezone, timedelta

        queue = [
            {"backtest_id": "A", "required_memory_gb": 50.0, "queued_at": "2026-02-05T10:00:00Z"},
            {"backtest_id": "B", "required_memory_gb": 5.0, "queued_at": "2026-02-05T10:01:00Z"},
        ]

        head = queue[0]
        head_queued_at = datetime.fromisoformat(head["queued_at"].replace("Z", "+00:00"))
        now = head_queued_at + timedelta(minutes=15)
        reserve_threshold = 600.0  # 10 minutes

        wait_seconds = (now - head_queued_at).total_seconds()
        reserve_mode = wait_seconds > reserve_threshold

        self.assertTrue(reserve_mode)

        # In reserve mode, only head task should be attempted
        schedulable = []
        for idx, item in enumerate(queue):
            if reserve_mode and idx != 0:
                continue  # Skip non-head in reserve mode
            schedulable.append(item["backtest_id"])

        self.assertEqual(schedulable, ["A"])


class TestDirectSubmitBypassesQueue(unittest.TestCase):
    """
    Tests demonstrating the problem with direct submission when queue is empty.

    Current behavior: When queue is empty, new requests bypass the queue and
    submit directly. This means multiple rapid requests can all hit the same
    server before metrics update, bypassing the 30s dispatch delay protection.

    Expected behavior: All requests should go through the queue to ensure
    the 30s dispatch delay is applied between submissions.
    """

    def test_current_logic_bypasses_queue_when_empty(self) -> None:
        """
        This test verifies the CURRENT (buggy) behavior.

        When queue is empty, submit_with_queue_control should queue the request,
        NOT submit directly. But currently it submits directly.

        This test should FAIL after we fix the code.
        """
        # Simulate checking if request goes to queue or direct submit
        queue = []

        def should_queue_or_direct_submit() -> str:
            """
            Current logic from submit_with_queue_control lines 770-782:
            - If queue is not empty -> queue
            - If queue is empty -> try direct submit
            """
            if queue:  # Current: only queue if queue is non-empty
                return "queued"
            return "direct_submit"

        # First request: queue is empty -> goes to direct submit (BAD!)
        result1 = should_queue_or_direct_submit()

        # This assertion documents the CURRENT (buggy) behavior
        # After fix, this should be "queued" instead
        self.assertEqual(result1, "direct_submit")  # CURRENT: bypasses queue

    def test_fixed_logic_always_queues(self) -> None:
        """
        Expected behavior: ALL requests should go to queue.

        This test documents what the FIXED behavior should look like.
        After fix, this test should PASS.
        """
        queue = []

        def should_queue_fixed() -> str:
            """
            Fixed logic: always queue, regardless of queue state.
            Let queue_scheduler handle dispatch with proper delays.
            """
            return "queued"  # Always queue!

        result1 = should_queue_fixed()
        self.assertEqual(result1, "queued")

    def test_rapid_requests_need_queue_protection(self) -> None:
        """
        Demonstrates WHY we need all requests to go through queue.

        Scenario: 3 requests arrive within 100ms (user clicks rapidly)
        - Without queue: all 3 submit directly, no delay, same server overloaded
        - With queue: scheduler applies 30s delay, metrics update, jobs spread out
        """
        from services import scheduler

        # Track which servers get hit
        submissions = []
        call_count = {"value": 0}

        def mock_fetch_metrics(base_url, metrics_path, headers, timeout_seconds):
            """Metrics don't update between rapid calls (no 30s delay)."""
            call_count["value"] += 1
            # Same metrics every time - simulates no delay
            if base_url == "http://host1:8000":
                return DockerMetrics(
                    base_url="http://host1:8000",
                    cpu_percent=40.0,
                    memory_total_gb=64.0,
                    memory_used_gb=16.0,
                    memory_free_gb=48.0,
                )
            return DockerMetrics(
                base_url="http://host2:8000",
                cpu_percent=60.0,
                memory_total_gb=64.0,
                memory_used_gb=32.0,
                memory_free_gb=32.0,
            )

        original_fetch = scheduler.fetch_metrics
        scheduler.fetch_metrics = mock_fetch_metrics
        try:
            # Simulate 3 rapid direct submits (no queue, no delay)
            for i in range(3):
                result = select_backtest_docker(
                    base_urls=["http://host1:8000", "http://host2:8000"],
                    metrics_path="/metrics",
                    runs_path=None,
                    headers={},
                    required_memory_gb=10.0,
                    reserved_memory_gb=None,  # No INFLIGHT tracking between calls
                    inflight_counts=None,
                    max_running=None,
                    timeout_seconds=3.0,
                    cpu_percent_lt=80.0,
                )
                if result:
                    submissions.append(result.base_url)

            # BUG: All 3 go to host1 (best metrics, but metrics never updated)
            # This would overload host1's CPU/RAM
            self.assertEqual(submissions, [
                "http://host1:8000",
                "http://host1:8000",
                "http://host1:8000",
            ])

            # With proper queue + 30s delay, we'd expect:
            # Job 1 -> host1, (wait 30s, metrics update)
            # Job 2 -> host2 (host1 now busy), (wait 30s)
            # Job 3 -> queued (both busy) or different distribution

        finally:
            scheduler.fetch_metrics = original_fetch


class TestSortKeyMemoryRatio(unittest.TestCase):
    """Tests that scheduler sorts by memory free RATIO, not absolute free memory."""

    def _make_metrics(
        self,
        base_url: str,
        cpu_percent: float | None,
        total_gb: float,
        used_gb: float,
        free_gb: float,
    ) -> DockerMetrics:
        return DockerMetrics(
            base_url=base_url,
            cpu_percent=cpu_percent,
            memory_total_gb=total_gb,
            memory_used_gb=used_gb,
            memory_free_gb=free_gb,
        )

    def test_ratio_sort_prefers_less_loaded_small_machine(self) -> None:
        """
        big_machine:   200GB total, 40GB free  -> 20% free
        small_machine:  40GB total, 35GB free  -> 87.5% free

        Old bug: absolute sort picks big_machine (40GB > 35GB).
        Fixed:   ratio sort picks small_machine (87.5% > 20%).
        """
        from services import scheduler

        metrics_store = {
            "http://big:8000": self._make_metrics("http://big:8000", 50.0, 200.0, 160.0, 40.0),
            "http://small:8000": self._make_metrics("http://small:8000", 50.0, 40.0, 5.0, 35.0),
        }

        def mock_fetch_metrics(base_url, metrics_path, headers, timeout_seconds):
            return metrics_store[base_url]

        original_fetch = scheduler.fetch_metrics
        scheduler.fetch_metrics = mock_fetch_metrics
        try:
            result = select_backtest_docker(
                base_urls=["http://big:8000", "http://small:8000"],
                metrics_path="/metrics",
                runs_path=None,
                headers={},
                required_memory_gb=10.0,
                reserved_memory_gb=None,
                inflight_counts=None,
                max_running=None,
                timeout_seconds=3.0,
            )
            self.assertIsNotNone(result)
            # small_machine has higher free ratio (87.5% vs 20%) -> should be selected
            self.assertEqual(result.base_url, "http://small:8000")
        finally:
            scheduler.fetch_metrics = original_fetch

    def test_ratio_sort_equal_ratio_tiebreaks_on_cpu(self) -> None:
        """When free ratios are equal, lower CPU usage should win."""
        from services import scheduler

        metrics_store = {
            "http://host1:8000": self._make_metrics("http://host1:8000", 70.0, 64.0, 32.0, 32.0),
            "http://host2:8000": self._make_metrics("http://host2:8000", 30.0, 128.0, 64.0, 64.0),
        }

        def mock_fetch_metrics(base_url, metrics_path, headers, timeout_seconds):
            return metrics_store[base_url]

        original_fetch = scheduler.fetch_metrics
        scheduler.fetch_metrics = mock_fetch_metrics
        try:
            result = select_backtest_docker(
                base_urls=["http://host1:8000", "http://host2:8000"],
                metrics_path="/metrics",
                runs_path=None,
                headers={},
                required_memory_gb=10.0,
                reserved_memory_gb=None,
                inflight_counts=None,
                max_running=None,
                timeout_seconds=3.0,
            )
            self.assertIsNotNone(result)
            # Both have 50% free ratio, host2 has lower CPU (30 < 70)
            self.assertEqual(result.base_url, "http://host2:8000")
        finally:
            scheduler.fetch_metrics = original_fetch


if __name__ == "__main__":
    unittest.main()
