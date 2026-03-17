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

    def test_strategy_bundle_ok(self) -> None:
        payload = _base_payload()
        payload.pop("strategy_file", None)
        payload["strategy_bundle"] = "bundle.zip"
        sanitized = app.validate_run_spec(payload)
        self.assertIn("strategy_bundle", sanitized)
        self.assertNotIn("strategy_file", sanitized)

    def test_strategy_bundle_and_file_rejected(self) -> None:
        payload = _base_payload()
        payload["strategy_bundle"] = "bundle.zip"
        with self.assertRaises(ValueError):
            app.validate_run_spec(payload)

    def test_strategy_missing_rejected(self) -> None:
        payload = _base_payload()
        payload.pop("strategy_file", None)
        with self.assertRaises(ValueError):
            app.validate_run_spec(payload)


class TestRunSpecFillModelConfig(unittest.TestCase):
    def test_liquidity_consumption_true(self) -> None:
        payload = _base_payload()
        payload["liquidity_consumption"] = True
        sanitized = app.validate_run_spec(payload)
        self.assertTrue(sanitized["liquidity_consumption"])

    def test_liquidity_consumption_false(self) -> None:
        payload = _base_payload()
        payload["liquidity_consumption"] = False
        sanitized = app.validate_run_spec(payload)
        self.assertFalse(sanitized["liquidity_consumption"])

    def test_liquidity_consumption_int_rejected(self) -> None:
        payload = _base_payload()
        payload["liquidity_consumption"] = 1
        with self.assertRaises(ValueError):
            app.validate_run_spec(payload)

    def test_trade_execution_true(self) -> None:
        payload = _base_payload()
        payload["trade_execution"] = True
        sanitized = app.validate_run_spec(payload)
        self.assertTrue(sanitized["trade_execution"])

    def test_trade_execution_string_rejected(self) -> None:
        payload = _base_payload()
        payload["trade_execution"] = "true"
        with self.assertRaises(ValueError):
            app.validate_run_spec(payload)

    def test_load_trade_ticks_true(self) -> None:
        payload = _base_payload()
        payload["load_trade_ticks"] = True
        sanitized = app.validate_run_spec(payload)
        self.assertTrue(sanitized["load_trade_ticks"])

    def test_optimize_file_loading_true(self) -> None:
        payload = _base_payload()
        payload["optimize_file_loading"] = True
        sanitized = app.validate_run_spec(payload)
        self.assertTrue(sanitized["optimize_file_loading"])

    def test_load_trade_ticks_string_rejected(self) -> None:
        payload = _base_payload()
        payload["load_trade_ticks"] = "false"
        with self.assertRaises(ValueError):
            app.validate_run_spec(payload)

    def test_optimize_file_loading_string_rejected(self) -> None:
        payload = _base_payload()
        payload["optimize_file_loading"] = "true"
        with self.assertRaises(ValueError):
            app.validate_run_spec(payload)

    def test_fill_model_config_valid(self) -> None:
        payload = _base_payload()
        payload["fill_model_config"] = {
            "fill_model_path": "quant_trade_v1.backtest.models:FillModel",
            "config_path": "quant_trade_v1.backtest.config:FillModelConfig",
            "config": {"prob_slippage": 0.1},
        }
        sanitized = app.validate_run_spec(payload)
        self.assertEqual(
            sanitized["fill_model_config"]["fill_model_path"],
            "quant_trade_v1.backtest.models:FillModel",
        )

    def test_fill_model_config_missing_key_rejected(self) -> None:
        payload = _base_payload()
        payload["fill_model_config"] = {"fill_model_path": "quant_trade_v1.a:B"}
        with self.assertRaises(ValueError):
            app.validate_run_spec(payload)

    def test_fill_model_config_unknown_key_rejected(self) -> None:
        payload = _base_payload()
        payload["fill_model_config"] = {
            "fill_model_path": "quant_trade_v1.a:B",
            "config_path": "quant_trade_v1.c:D",
            "config": {},
            "extra": True,
        }
        with self.assertRaises(ValueError):
            app.validate_run_spec(payload)

    def test_fill_model_config_empty_path_rejected(self) -> None:
        payload = _base_payload()
        payload["fill_model_config"] = {
            "fill_model_path": "  ",
            "config_path": "quant_trade_v1.c:D",
            "config": {},
        }
        with self.assertRaises(ValueError):
            app.validate_run_spec(payload)

    def test_fill_model_config_bad_format_rejected(self) -> None:
        payload = _base_payload()
        payload["fill_model_config"] = {
            "fill_model_path": "quant_trade_v1.no_class_name",
            "config_path": "quant_trade_v1.backtest.config:FillModelConfig",
            "config": {},
        }
        with self.assertRaises(ValueError):
            app.validate_run_spec(payload)

    def test_fill_model_config_non_quant_trade_rejected(self) -> None:
        payload = _base_payload()
        payload["fill_model_config"] = {
            "fill_model_path": "os:system",
            "config_path": "quant_trade_v1.backtest.config:FillModelConfig",
            "config": {},
        }
        with self.assertRaises(ValueError):
            app.validate_run_spec(payload)

    def test_new_fields_absent_ok(self) -> None:
        payload = _base_payload()
        sanitized = app.validate_run_spec(payload)
        self.assertNotIn("liquidity_consumption", sanitized)
        self.assertNotIn("trade_execution", sanitized)
        self.assertNotIn("load_trade_ticks", sanitized)
        self.assertNotIn("optimize_file_loading", sanitized)
        self.assertNotIn("fill_model_config", sanitized)
