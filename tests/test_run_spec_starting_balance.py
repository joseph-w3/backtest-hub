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


class TestRunSpecStartingBalance(unittest.TestCase):
    def test_starting_balance_optional(self) -> None:
        payload = _base_payload()
        sanitized = app.validate_run_spec(payload)
        self.assertNotIn("starting_balance_usdt", sanitized)

    def test_starting_balance_int_ok(self) -> None:
        payload = _base_payload()
        payload["starting_balance_usdt"] = 50000
        sanitized = app.validate_run_spec(payload)
        self.assertIn("starting_balance_usdt", sanitized)
        self.assertEqual(sanitized["starting_balance_usdt"], 50000)

    def test_starting_balance_float_ok(self) -> None:
        payload = _base_payload()
        payload["starting_balance_usdt"] = 123456.78
        sanitized = app.validate_run_spec(payload)
        self.assertEqual(sanitized["starting_balance_usdt"], 123456.78)

    def test_starting_balance_zero_rejected(self) -> None:
        payload = _base_payload()
        payload["starting_balance_usdt"] = 0
        with self.assertRaises(ValueError):
            app.validate_run_spec(payload)

    def test_starting_balance_negative_rejected(self) -> None:
        payload = _base_payload()
        payload["starting_balance_usdt"] = -1000
        with self.assertRaises(ValueError):
            app.validate_run_spec(payload)

    def test_starting_balance_string_rejected(self) -> None:
        payload = _base_payload()
        payload["starting_balance_usdt"] = "50000"
        with self.assertRaises(ValueError):
            app.validate_run_spec(payload)
