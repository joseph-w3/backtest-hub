from __future__ import annotations

import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from scripts import warmup_cycle_harness


class TestWarmupCycleHarness(unittest.TestCase):
    def test_parse_spot_symbols_rejects_perp_suffix(self) -> None:
        with self.assertRaisesRegex(ValueError, "must not include -PERP suffix"):
            warmup_cycle_harness._parse_spot_symbols("ACTUSDT-PERP")

    def test_parse_spot_symbols_requires_explicit_usdt_spot_symbols(self) -> None:
        with self.assertRaisesRegex(ValueError, "explicit USDT spot symbols"):
            warmup_cycle_harness._parse_spot_symbols("ACT")

    def test_write_strategy_bundle_contains_minimal_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            bundle_path = Path(td) / "bundle" / warmup_cycle_harness.BUNDLE_FILENAME
            warmup_cycle_harness.write_strategy_bundle(bundle_path)

            with zipfile.ZipFile(bundle_path) as zf:
                names = sorted(name for name in zf.namelist() if not name.endswith("/"))

            self.assertEqual(names, sorted(warmup_cycle_harness.BUNDLE_FILES))

    def test_generate_harness_writes_all_modes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            output_dir = Path(td) / "harness"
            summary = warmup_cycle_harness.generate_harness(
                output_dir=output_dir,
                spot_symbols=["ACTUSDT", "DOTUSDT", "BCHUSDT"],
                start="2025-11-10T00:00:00.000Z",
                end="2025-11-24T00:00:00.000Z",
                catalog_root="/mnt/localB2fs/backtest/catalog",
                prewarm_threads=25,
                prefetch_ahead_hours=48,
                prefetch_max_files_per_batch=3,
                load_trade_ticks=False,
                optimize_file_loading=False,
            )

            self.assertTrue((output_dir / "bundle" / warmup_cycle_harness.BUNDLE_FILENAME).is_file())
            self.assertTrue((output_dir / "README.md").is_file())
            self.assertTrue((output_dir / "summary.json").is_file())
            self.assertEqual(len(summary["modes"]), 4)

            summary_from_disk = json.loads((output_dir / "summary.json").read_text())
            self.assertEqual(summary_from_disk["spot_symbols"], ["ACTUSDT", "DOTUSDT", "BCHUSDT"])
            self.assertEqual(
                summary_from_disk["symbols"],
                [
                    "ACTUSDT",
                    "ACTUSDT-PERP",
                    "DOTUSDT",
                    "DOTUSDT-PERP",
                    "BCHUSDT",
                    "BCHUSDT-PERP",
                ],
            )

            for mode in warmup_cycle_harness.MODES:
                mode_dir = output_dir / mode.name
                run_spec_path = mode_dir / "run_spec.json"
                env_path = mode_dir / "env.sh"
                run_script_path = mode_dir / "run.sh"

                self.assertTrue(run_spec_path.is_file())
                self.assertTrue(env_path.is_file())
                self.assertTrue(run_script_path.is_file())

                run_spec = json.loads(run_spec_path.read_text())
                self.assertEqual(run_spec["strategy_entry"], warmup_cycle_harness.STRATEGY_ENTRY)
                self.assertEqual(
                    run_spec["strategy_config_path"],
                    warmup_cycle_harness.STRATEGY_CONFIG_PATH,
                )
                self.assertEqual(run_spec["strategy_bundle"], "../bundle/strategies-harness.zip")
                self.assertFalse(run_spec["load_trade_ticks"])
                self.assertFalse(run_spec["optimize_file_loading"])
                self.assertEqual(run_spec["tags"]["mode"], mode.name)

                env_text = env_path.read_text()
                expected_backend = f"BACKTEST_PREFETCH_BACKEND={mode.replay_prefetch_backend}"
                self.assertIn(expected_backend, env_text)
                self.assertIn(
                    f"HARNESS_PREWARM_REQUIRED={'1' if mode.prewarm_before_run else '0'}",
                    env_text,
                )
                self.assertIn("HARNESS_PREWARM_SCRIPT=", env_text)
                self.assertIn("HARNESS_RUNNER_PATH=", env_text)

                run_script = run_script_path.read_text()
                self.assertIn('if [[ "${HARNESS_PREWARM_REQUIRED}" == "1" ]]', run_script)
                self.assertIn('export CATALOG_PATH="${HARNESS_CATALOG_ROOT}"', run_script)
                self.assertIn('export BACKTEST_LOGS_PATH="${SCRIPT_DIR}/logs"', run_script)
                self.assertIn('"${HARNESS_PREWARM_SCRIPT}"', run_script)
                self.assertIn('"${HARNESS_RUNNER_PATH}"', run_script)


if __name__ == "__main__":
    unittest.main()
