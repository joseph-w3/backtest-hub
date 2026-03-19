import os
from pathlib import Path
import importlib.util
import sys
from unittest import mock
import unittest

os.environ.setdefault("BACKTEST_API_BASES", "http://dummy:10001")

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

MODULE_PATH = REPO_ROOT / "scripts" / "run_warmup_cycle_serial.py"
SPEC = importlib.util.spec_from_file_location("run_warmup_cycle_serial", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
run_warmup_cycle_serial = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(run_warmup_cycle_serial)


class TestRunWarmupCycleSerial(unittest.TestCase):
    def test_sample_worker_cache_stats_parses_successful_ssh_output(self) -> None:
        completed = mock.Mock(stdout="1\t4096\n", stderr="", returncode=0)
        with mock.patch.object(run_warmup_cycle_serial.subprocess, "run", return_value=completed):
            snapshot = run_warmup_cycle_serial._sample_worker_cache_stats(
                ssh_target="root@test-host",
                cache_dir="/tmp/cache",
            )

        self.assertEqual(snapshot["ssh_target"], "root@test-host")
        self.assertEqual(snapshot["cache_dir"], "/tmp/cache")
        self.assertTrue(snapshot["exists"])
        self.assertEqual(snapshot["bytes"], 4096)
        self.assertIsNone(snapshot["error"])

    def test_sample_worker_cache_stats_reports_error_without_raising(self) -> None:
        with mock.patch.object(
            run_warmup_cycle_serial.subprocess,
            "run",
            side_effect=RuntimeError("ssh failed"),
        ):
            snapshot = run_warmup_cycle_serial._sample_worker_cache_stats(
                ssh_target="root@test-host",
                cache_dir="/tmp/cache",
            )

        self.assertEqual(snapshot["ssh_target"], "root@test-host")
        self.assertIsNone(snapshot["bytes"])
        self.assertIn("ssh failed", snapshot["error"])

    def test_collect_unique_chunk_probes_deduplicates_updated_at(self) -> None:
        rows = [
            {
                "progress": {
                    "streaming_probe": {
                        "chunk_index": 11,
                        "updated_at": "2026-03-19T00:00:11+00:00",
                        "chunk_wall_ms": 1000.0,
                        "events_per_second": 200000.0,
                        "simulated_seconds_per_wall_second": 300.0,
                    }
                }
            },
            {
                "progress": {
                    "streaming_probe": {
                        "chunk_index": 11,
                        "updated_at": "2026-03-19T00:00:11+00:00",
                        "chunk_wall_ms": 1000.0,
                        "events_per_second": 200000.0,
                        "simulated_seconds_per_wall_second": 300.0,
                    }
                }
            },
            {
                "progress": {
                    "streaming_probe": {
                        "chunk_index": 12,
                        "updated_at": "2026-03-19T00:00:12+00:00",
                        "chunk_wall_ms": 800.0,
                        "events_per_second": 250000.0,
                        "simulated_seconds_per_wall_second": 320.0,
                    }
                }
            },
        ]

        probes = run_warmup_cycle_serial._collect_unique_chunk_probes(rows)

        self.assertEqual(len(probes), 2)
        self.assertEqual([probe["chunk_index"] for probe in probes], [11, 12])

    def test_summarize_progress_rows_uses_only_stable_chunk_window(self) -> None:
        rows = [
            {
                "progress": {
                    "streaming_probe": {
                        "chunk_index": 9,
                        "updated_at": "2026-03-19T00:00:09+00:00",
                        "chunk_wall_ms": 5000.0,
                        "events_per_second": 40000.0,
                        "simulated_seconds_per_wall_second": 100.0,
                    }
                }
            },
            {
                "progress": {
                    "streaming_probe": {
                        "chunk_index": 11,
                        "updated_at": "2026-03-19T00:00:11+00:00",
                        "chunk_wall_ms": 1000.0,
                        "events_per_second": 200000.0,
                        "simulated_seconds_per_wall_second": 300.0,
                    }
                }
            },
            {
                "progress": {
                    "streaming_probe": {
                        "chunk_index": 12,
                        "updated_at": "2026-03-19T00:00:12+00:00",
                        "chunk_wall_ms": 800.0,
                        "events_per_second": 250000.0,
                        "simulated_seconds_per_wall_second": 320.0,
                    }
                }
            },
            {
                "progress": {
                    "streaming_probe": {
                        "chunk_index": 13,
                        "updated_at": "2026-03-19T00:00:13+00:00",
                        "chunk_wall_ms": 900.0,
                        "events_per_second": 222222.0,
                        "simulated_seconds_per_wall_second": 310.0,
                    }
                }
            },
        ]

        summary = run_warmup_cycle_serial.summarize_progress_rows(rows, skip_initial_chunks=10)

        self.assertEqual(summary["unique_chunk_probe_count"], 4)
        self.assertEqual(summary["stable_chunk_probe_count"], 3)
        self.assertEqual(summary["last_chunk_index"], 13)
        self.assertEqual(summary["stable_chunk_wall_ms_median"], 900.0)
        self.assertEqual(summary["stable_events_per_second_median"], 222222.0)
        self.assertEqual(summary["stable_simulated_seconds_per_wall_second_median"], 310.0)

    def test_summarize_progress_rows_computes_progress_slopes_from_heartbeat(self) -> None:
        rows = [
            {
                "progress": {
                    "last_progress_at": "2026-03-19T01:00:00.000000+00:00",
                    "simulated_time": "2025-11-10T00:00:00.000000000Z",
                    "streaming_probe": {"chunk_index": 10},
                    "streaming_summary": {"events_seen": 2_000_000},
                }
            },
            {
                "progress": {
                    "last_progress_at": "2026-03-19T01:00:10.000000+00:00",
                    "simulated_time": "2025-11-10T00:05:00.000000000Z",
                    "streaming_probe": {"chunk_index": 12},
                    "streaming_summary": {"events_seen": 3_500_000},
                }
            },
            {
                "progress": {
                    "last_progress_at": "2026-03-19T01:00:20.000000+00:00",
                    "simulated_time": "2025-11-10T00:10:30.000000000Z",
                    "streaming_probe": {"chunk_index": 14},
                    "streaming_summary": {"events_seen": 5_000_000},
                }
            },
        ]

        summary = run_warmup_cycle_serial.summarize_progress_rows(rows, skip_initial_chunks=10)

        self.assertEqual(summary["progress_delta_sample_count"], 2)
        self.assertEqual(summary["progress_events_per_second_median"], 150000.0)
        self.assertEqual(summary["progress_simulated_seconds_per_wall_second_median"], 31.5)

    def test_summarize_progress_rows_reports_file_touch_deltas(self) -> None:
        rows = [
            {
                "progress": {
                    "last_progress_at": "2026-03-19T01:00:00.000000+00:00",
                    "simulated_time": "2025-11-10T00:00:00.000000000Z",
                    "streaming_probe": {"chunk_index": 10},
                    "streaming_summary": {
                        "events_seen": 2_000_000,
                        "file_touch_files_total": 20,
                        "file_touch_bytes_total": 10_000,
                        "file_touch_files_touched": 10,
                        "file_touch_bytes_touched": 4_000,
                    },
                }
            },
            {
                "progress": {
                    "last_progress_at": "2026-03-19T01:00:10.000000+00:00",
                    "simulated_time": "2025-11-10T00:05:00.000000000Z",
                    "streaming_probe": {"chunk_index": 12},
                    "streaming_summary": {
                        "events_seen": 3_500_000,
                        "file_touch_files_total": 20,
                        "file_touch_bytes_total": 10_000,
                        "file_touch_files_touched": 12,
                        "file_touch_bytes_touched": 4_500,
                    },
                }
            },
            {
                "progress": {
                    "last_progress_at": "2026-03-19T01:00:20.000000+00:00",
                    "simulated_time": "2025-11-10T00:10:30.000000000Z",
                    "streaming_probe": {"chunk_index": 14},
                    "streaming_summary": {
                        "events_seen": 5_000_000,
                        "file_touch_files_total": 20,
                        "file_touch_bytes_total": 10_000,
                        "file_touch_files_touched": 15,
                        "file_touch_bytes_touched": 5_100,
                    },
                }
            },
        ]

        summary = run_warmup_cycle_serial.summarize_progress_rows(rows, skip_initial_chunks=10)

        self.assertEqual(summary["progress_file_touch_delta_sample_count"], 2)
        self.assertEqual(summary["progress_new_files_touched_total"], 5)
        self.assertEqual(summary["progress_new_files_touched_median"], 2.5)
        self.assertEqual(summary["progress_new_bytes_touched_total"], 1100)
        self.assertEqual(summary["progress_new_bytes_touched_median"], 550.0)
        self.assertEqual(summary["progress_file_touch_active_ratio"], 1.0)
        self.assertEqual(summary["final_file_touch_files_total"], 20)
        self.assertEqual(summary["final_file_touch_files_touched"], 15)

    def test_summarize_progress_rows_reports_cache_dir_growth(self) -> None:
        rows = [
            {
                "progress": {
                    "last_progress_at": "2026-03-19T01:00:00.000000+00:00",
                    "simulated_time": "2025-11-10T00:00:00.000000000Z",
                    "streaming_probe": {"chunk_index": 10},
                    "streaming_summary": {"events_seen": 2_000_000},
                },
                "cache_stats": {"bytes": 1_000},
            },
            {
                "progress": {
                    "last_progress_at": "2026-03-19T01:00:10.000000+00:00",
                    "simulated_time": "2025-11-10T00:05:00.000000000Z",
                    "streaming_probe": {"chunk_index": 12},
                    "streaming_summary": {"events_seen": 3_500_000},
                },
                "cache_stats": {"bytes": 1_600},
            },
            {
                "progress": {
                    "last_progress_at": "2026-03-19T01:00:20.000000+00:00",
                    "simulated_time": "2025-11-10T00:10:30.000000000Z",
                    "streaming_probe": {"chunk_index": 14},
                    "streaming_summary": {"events_seen": 5_000_000},
                },
                "cache_stats": {"bytes": 2_000},
            },
        ]

        summary = run_warmup_cycle_serial.summarize_progress_rows(rows, skip_initial_chunks=10)

        self.assertEqual(summary["progress_cache_dir_bytes_growth_sample_count"], 2)
        self.assertEqual(summary["progress_cache_dir_bytes_growth_total"], 1000)
        self.assertEqual(summary["progress_cache_dir_bytes_growth_median"], 500.0)
        self.assertEqual(summary["progress_cache_dir_growth_active_ratio"], 1.0)
        self.assertEqual(summary["final_cache_dir_bytes"], 2000)


if __name__ == "__main__":
    unittest.main()
