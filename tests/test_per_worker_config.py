import unittest

from services.scheduler import (
    parse_per_worker_max_running,
    select_backtest_docker,
    DockerMetrics,
)


class TestParsePerWorkerMaxRunning(unittest.TestCase):
    def test_parse_empty(self) -> None:
        """Empty string returns empty dict."""
        result = parse_per_worker_max_running("")
        self.assertEqual(result, {})

    def test_parse_whitespace_only(self) -> None:
        """Whitespace-only string returns empty dict."""
        result = parse_per_worker_max_running("   ")
        self.assertEqual(result, {})

    def test_parse_single(self) -> None:
        """Single worker parses correctly."""
        result = parse_per_worker_max_running("http://100.65.27.118:10001=5")
        self.assertEqual(result, {"http://100.65.27.118:10001": 5})

    def test_parse_multiple(self) -> None:
        """Multiple workers parse correctly."""
        result = parse_per_worker_max_running(
            "http://100.65.27.118:10001=5,http://100.97.194.7:10001=3"
        )
        self.assertEqual(result, {
            "http://100.65.27.118:10001": 5,
            "http://100.97.194.7:10001": 3,
        })

    def test_parse_with_whitespace(self) -> None:
        """Whitespace around entries is trimmed."""
        result = parse_per_worker_max_running(
            " http://host1:10001=5 , http://host2:10001=3 "
        )
        self.assertEqual(result, {
            "http://host1:10001": 5,
            "http://host2:10001": 3,
        })

    def test_parse_trailing_slash_normalized(self) -> None:
        """Trailing slashes on URLs are stripped (consistent with normalize_base_url)."""
        result = parse_per_worker_max_running("http://host1:10001/=5")
        self.assertEqual(result, {"http://host1:10001": 5})

    def test_parse_invalid_value_raises(self) -> None:
        """Non-integer value raises ValueError."""
        with self.assertRaises(ValueError):
            parse_per_worker_max_running("http://host1:10001=abc")

    def test_parse_entry_without_equals_ignored(self) -> None:
        """Entry without '=' is silently skipped."""
        result = parse_per_worker_max_running(
            "http://host1:10001=5,garbage,http://host2:10001=3"
        )
        self.assertEqual(result, {
            "http://host1:10001": 5,
            "http://host2:10001": 3,
        })


class TestPerWorkerMaxRunningInScheduler(unittest.TestCase):
    """Test that select_backtest_docker respects per-worker max_running limits."""

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

    def test_effective_max_running_per_worker(self) -> None:
        """Per-worker limit is used when configured; host1 limit=1, host2 limit=5."""
        from services import scheduler

        metrics_store = {
            "http://host1:8000": self._make_metrics("http://host1:8000", 30.0, 200.0, 50.0, 150.0),
            "http://host2:8000": self._make_metrics("http://host2:8000", 30.0, 40.0, 10.0, 30.0),
        }
        runs_store = {
            # host1 has 1 running (at its limit of 1)
            "http://host1:8000": {"runs": [{"status": "running"}]},
            # host2 has 1 running (well below its limit of 5)
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
            # host1 has more free memory (150GB) but is at its per-worker limit (1)
            # host2 has less free memory (30GB) but is below its per-worker limit (5)
            result = select_backtest_docker(
                base_urls=["http://host1:8000", "http://host2:8000"],
                metrics_path="/metrics",
                runs_path="/runs",
                headers={},
                required_memory_gb=5.0,
                reserved_memory_gb=None,
                inflight_counts=None,
                max_running=10,  # global fallback
                max_running_per_worker={
                    "http://host1:8000": 1,
                    "http://host2:8000": 5,
                },
                timeout_seconds=3.0,
            )
            self.assertIsNotNone(result)
            # Should pick host2 because host1 is at its limit
            self.assertEqual(result.base_url, "http://host2:8000")
        finally:
            scheduler.fetch_metrics = original_fetch_metrics
            scheduler.fetch_backtest_runs = original_fetch_runs

    def test_fallback_to_global_max_running(self) -> None:
        """Workers not in per-worker dict use the global max_running."""
        from services import scheduler

        metrics_store = {
            "http://host1:8000": self._make_metrics("http://host1:8000", 30.0, 200.0, 50.0, 150.0),
            "http://host2:8000": self._make_metrics("http://host2:8000", 30.0, 40.0, 10.0, 30.0),
        }
        runs_store = {
            # host1 has 2 running (at global limit of 2)
            "http://host1:8000": {"runs": [{"status": "running"}, {"status": "running"}]},
            # host2 has 1 running (below global limit of 2)
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
            # Neither host is in per_worker dict, so both use global max_running=2
            result = select_backtest_docker(
                base_urls=["http://host1:8000", "http://host2:8000"],
                metrics_path="/metrics",
                runs_path="/runs",
                headers={},
                required_memory_gb=5.0,
                reserved_memory_gb=None,
                inflight_counts=None,
                max_running=2,
                max_running_per_worker={},  # empty: no per-worker overrides
                timeout_seconds=3.0,
            )
            self.assertIsNotNone(result)
            # host1 is at limit (2 running >= 2), only host2 available
            self.assertEqual(result.base_url, "http://host2:8000")
        finally:
            scheduler.fetch_metrics = original_fetch_metrics
            scheduler.fetch_backtest_runs = original_fetch_runs

    def test_per_worker_none_uses_global(self) -> None:
        """When max_running_per_worker is None, behaves exactly like before."""
        from services import scheduler

        metrics_store = {
            "http://host1:8000": self._make_metrics("http://host1:8000", 30.0, 200.0, 50.0, 150.0),
            "http://host2:8000": self._make_metrics("http://host2:8000", 30.0, 40.0, 10.0, 30.0),
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
            result = select_backtest_docker(
                base_urls=["http://host1:8000", "http://host2:8000"],
                metrics_path="/metrics",
                runs_path="/runs",
                headers={},
                required_memory_gb=5.0,
                reserved_memory_gb=None,
                inflight_counts=None,
                max_running=2,
                max_running_per_worker=None,  # not set
                timeout_seconds=3.0,
            )
            self.assertIsNotNone(result)
            self.assertEqual(result.base_url, "http://host2:8000")
        finally:
            scheduler.fetch_metrics = original_fetch_metrics
            scheduler.fetch_backtest_runs = original_fetch_runs

    def test_mixed_per_worker_and_global(self) -> None:
        """Some workers have per-worker limits, others fall back to global."""
        from services import scheduler

        metrics_store = {
            "http://host1:8000": self._make_metrics("http://host1:8000", 30.0, 200.0, 50.0, 150.0),
            "http://host2:8000": self._make_metrics("http://host2:8000", 30.0, 100.0, 30.0, 70.0),
            "http://host3:8000": self._make_metrics("http://host3:8000", 30.0, 40.0, 10.0, 30.0),
        }
        runs_store = {
            # host1: 5 running, per-worker limit=8 -> OK
            "http://host1:8000": {"runs": [{"status": "running"}] * 5},
            # host2: 3 running, no per-worker -> global limit=3 -> AT LIMIT
            "http://host2:8000": {"runs": [{"status": "running"}] * 3},
            # host3: 2 running, per-worker limit=2 -> AT LIMIT
            "http://host3:8000": {"runs": [{"status": "running"}] * 2},
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
            result = select_backtest_docker(
                base_urls=["http://host1:8000", "http://host2:8000", "http://host3:8000"],
                metrics_path="/metrics",
                runs_path="/runs",
                headers={},
                required_memory_gb=5.0,
                reserved_memory_gb=None,
                inflight_counts=None,
                max_running=3,  # global fallback
                max_running_per_worker={
                    "http://host1:8000": 8,   # high limit for powerful machine
                    "http://host3:8000": 2,   # low limit for weak machine
                    # host2 not listed -> uses global=3
                },
                timeout_seconds=3.0,
            )
            self.assertIsNotNone(result)
            # Only host1 is below its limit
            self.assertEqual(result.base_url, "http://host1:8000")
        finally:
            scheduler.fetch_metrics = original_fetch_metrics
            scheduler.fetch_backtest_runs = original_fetch_runs


if __name__ == "__main__":
    unittest.main()
