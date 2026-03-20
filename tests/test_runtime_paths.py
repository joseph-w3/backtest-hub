from pathlib import Path
import unittest

from scripts.runtime_paths import normalize_backtest_runtime_paths


class TestRuntimePaths(unittest.TestCase):
    def test_rewrites_known_runtime_paths_to_unique_root(self) -> None:
        strategy_config = {
            "runtime_universe_commands_path": "/tmp/shared/runtime.json",
            "state_checkpoint_path": "/tmp/shared/state_checkpoint.json",
            "heartbeat_file_path": "/tmp/shared/heartbeat.json",
            "alert_file_path": "/tmp/shared/alerts.jsonl",
            "kill_switch_path": "/tmp/shared/kill_switch.flag",
            "dust_residual_file_path": "/tmp/shared/dust_residuals.jsonl",
            "unchanged": "keep-me",
        }

        normalized, rewritten = normalize_backtest_runtime_paths(
            strategy_config,
            runtime_root=Path("/opt/backtest_logs/bt_123/runtime"),
        )

        self.assertEqual(
            normalized["state_checkpoint_path"],
            "/opt/backtest_logs/bt_123/runtime/state_checkpoint.json",
        )
        self.assertEqual(
            normalized["runtime_universe_commands_path"],
            "/opt/backtest_logs/bt_123/runtime/runtime.json",
        )
        self.assertEqual(
            normalized["heartbeat_file_path"],
            "/opt/backtest_logs/bt_123/runtime/heartbeat.json",
        )
        self.assertEqual(normalized["unchanged"], "keep-me")
        self.assertEqual(set(rewritten), {
            "runtime_universe_commands_path",
            "state_checkpoint_path",
            "heartbeat_file_path",
            "alert_file_path",
            "kill_switch_path",
            "dust_residual_file_path",
        })

    def test_resolves_runtime_root_placeholder_and_preserves_empty_fields(self) -> None:
        strategy_config = {
            "runtime_universe_commands_path": "__RUNTIME_ROOT__/runtime.json",
            "state_checkpoint_path": "__RUNTIME_ROOT__/state_checkpoint.json",
            "alert_file_path": "",
            "kill_switch_path": "   ",
        }

        normalized, rewritten = normalize_backtest_runtime_paths(
            strategy_config,
            runtime_root=Path("/opt/backtest_logs/bt_456/runtime"),
        )

        self.assertEqual(
            normalized["runtime_universe_commands_path"],
            "/opt/backtest_logs/bt_456/runtime/runtime.json",
        )
        self.assertEqual(
            normalized["state_checkpoint_path"],
            "/opt/backtest_logs/bt_456/runtime/state_checkpoint.json",
        )
        self.assertEqual(normalized["alert_file_path"], "")
        self.assertEqual(normalized["kill_switch_path"], "   ")
        self.assertEqual(set(rewritten), {
            "runtime_universe_commands_path",
            "state_checkpoint_path",
        })


if __name__ == "__main__":
    unittest.main()
