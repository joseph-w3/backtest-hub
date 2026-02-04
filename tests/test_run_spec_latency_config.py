import os
import unittest

os.environ.setdefault("BACKTEST_API_BASES", "http://dummy")

import app


def _base_payload() -> dict:
    return {
        "schema_version": "1.0",
        "requested_by": "tester",
        "strategy_file": "s.py",
        "strategy_entry": "strategies.s:Strategy",
        "strategy_config_path": "strategies.s:StrategyConfig",
        "strategy_config": {},
        "margin_init": 0.05,
        "margin_maint": 0.025,
        "spot_maker_fee": 0.001,
        "spot_taker_fee": 0.001,
        "futures_maker_fee": 0.001,
        "futures_taker_fee": 0.001,
        "symbols": ["ACTUSDT", "ACTUSDT-PERP"],
        "start": "2025-11-10T00:00:00.000Z",
        "end": "2025-11-11T00:00:00.000Z",
        "chunk_size": 100,
        "seed": 1,
        "tags": {"purpose": "test"},
    }


class TestRunSpecLatencyConfig(unittest.TestCase):
    def test_latency_config_optional(self) -> None:
        payload = _base_payload()
        sanitized = app.validate_run_spec(payload)
        self.assertNotIn("latency_config", sanitized)

    def test_latency_config_full_ok(self) -> None:
        payload = _base_payload()
        payload["latency_config"] = {
            "base_latency_nanos": 20_000_000,
            "insert_latency_nanos": 2_000_000,
            "update_latency_nanos": 3_000_000,
            "cancel_latency_nanos": 1_000_000,
        }
        sanitized = app.validate_run_spec(payload)
        self.assertIn("latency_config", sanitized)
        self.assertEqual(sanitized["latency_config"], payload["latency_config"])

    def test_latency_config_partial_ok(self) -> None:
        payload = _base_payload()
        payload["latency_config"] = {"base_latency_nanos": 1}
        sanitized = app.validate_run_spec(payload)
        self.assertEqual(sanitized["latency_config"], {"base_latency_nanos": 1})

    def test_latency_config_unknown_key_rejected(self) -> None:
        payload = _base_payload()
        payload["latency_config"] = {"oops": 1}
        with self.assertRaises(ValueError):
            app.validate_run_spec(payload)

    def test_latency_config_float_rejected(self) -> None:
        payload = _base_payload()
        payload["latency_config"] = {"base_latency_nanos": 1.0}
        with self.assertRaises(ValueError):
            app.validate_run_spec(payload)

    def test_latency_config_negative_rejected(self) -> None:
        payload = _base_payload()
        payload["latency_config"] = {"base_latency_nanos": -1}
        with self.assertRaises(ValueError):
            app.validate_run_spec(payload)
