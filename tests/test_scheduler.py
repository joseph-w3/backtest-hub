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


if __name__ == "__main__":
    unittest.main()
