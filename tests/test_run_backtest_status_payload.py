import importlib.util
import json
import os
import sys
import threading
import types
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch


class _Dummy:
    def __init__(self, *args, **kwargs) -> None:
        pass


class _DummyCurrencyType:
    CRYPTO = "CRYPTO"


def _install_quant_trade_stubs() -> list[str]:
    try:
        import quant_trade_v1  # noqa: F401
        return []
    except Exception:
        pass

    added: list[str] = []

    def _add_module(name: str, *, is_package: bool = False) -> types.ModuleType:
        if name in sys.modules:
            return sys.modules[name]
        module = types.ModuleType(name)
        if is_package:
            module.__path__ = []
        sys.modules[name] = module
        added.append(name)
        return module

    _add_module("quant_trade_v1", is_package=True)
    _add_module("quant_trade_v1.backtest", is_package=True)
    backtest_config = _add_module("quant_trade_v1.backtest.config")
    backtest_node = _add_module("quant_trade_v1.backtest.node")
    config_root = _add_module("quant_trade_v1.config")
    model_root = _add_module("quant_trade_v1.model", is_package=True)
    currencies = _add_module("quant_trade_v1.model.currencies")
    enums = _add_module("quant_trade_v1.model.enums")
    identifiers = _add_module("quant_trade_v1.model.identifiers")
    instruments = _add_module("quant_trade_v1.model.instruments")
    objects = _add_module("quant_trade_v1.model.objects")
    _add_module("quant_trade_v1.persistence", is_package=True)
    catalog = _add_module("quant_trade_v1.persistence.catalog")

    for name in (
        "BacktestDataConfig",
        "BacktestEngineConfig",
        "BacktestRunConfig",
        "BacktestVenueConfig",
        "ImportableFillModelConfig",
        "ImportableLatencyModelConfig",
        "MarginModelConfig",
    ):
        setattr(backtest_config, name, _Dummy)
    backtest_node.BacktestNode = _Dummy

    for name in ("DataEngineConfig", "ImportableStrategyConfig", "LoggingConfig"):
        setattr(config_root, name, _Dummy)

    for name in ("FundingRateUpdate", "MarkPriceUpdate", "OrderBookDelta", "TradeTick"):
        setattr(model_root, name, _Dummy)

    currencies.USDT = object()
    enums.CurrencyType = _DummyCurrencyType

    for name in ("InstrumentId", "Symbol", "Venue"):
        setattr(identifiers, name, _Dummy)

    for name in ("CryptoPerpetual", "CurrencyPair"):
        setattr(instruments, name, _Dummy)

    for name in ("Currency", "Money", "Price", "Quantity"):
        setattr(objects, name, _Dummy)

    catalog.ParquetDataCatalog = _Dummy

    return added


def _load_run_backtest() -> types.ModuleType:
    module_name = "run_backtest_under_test"
    path = Path(__file__).resolve().parents[1] / "scripts" / "run_backtest.py"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load run_backtest module for test.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class TestRunBacktestStatusPayload(unittest.TestCase):
    @staticmethod
    def _expected_streaming_summary() -> dict:
        return {
            "chunks_seen": 0,
            "events_seen": 0,
            "file_touch_files_total": None,
            "file_touch_bytes_total": None,
            "file_touch_files_touched": 0,
            "file_touch_bytes_touched": 0,
            "file_touch_new_files_last_advance": 0,
            "file_touch_new_bytes_last_advance": 0,
            "file_touch_remaining_files": None,
            "file_touch_remaining_bytes": None,
            "file_touch_progress_samples_with_new_files": 0,
        }

    def test_status_payload_allows_strategy_bundle(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()
            run_spec = {
                "requested_by": "tester",
                "strategy_entry": "strategies.s:Strategy",
                "strategy_bundle": "bundle.zip",
                "symbols": ["ACTUSDT"],
                "start": "2025-11-10T00:00:00.000Z",
                "end": "2025-11-11T00:00:00.000Z",
                "tags": {"purpose": "test"},
            }
            payload = run_backtest._build_status_payload("bt-1", run_spec)
            self.assertEqual(payload["strategy_bundle"], "bundle.zip")
            self.assertIn("strategy_file", payload)
            self.assertIsNone(payload["strategy_file"])
            self.assertEqual(payload["symbol_count"], 1)
            self.assertEqual(payload["phase"], "initializing")
            self.assertEqual(payload["init_step"], "bootstrap")
            self.assertIn("last_progress_at", payload)
            self.assertIsNone(payload["simulated_time"])
            self.assertIsNone(payload["node_probe"])
            self.assertIsNone(payload["streaming_probe"])
            self.assertIsNone(payload["prefetch_probe"])
            self.assertEqual(payload["streaming_summary"], self._expected_streaming_summary())
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)

    def test_write_status_snapshot_updates_init_step_and_simulated_time(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()
            from tempfile import TemporaryDirectory

            with TemporaryDirectory() as td:
                status_path = Path(td) / "status.json"
                stdout_path = Path(td) / "stdout.log"
                stdout_path.write_text(
                    "2026-03-13T21:06:53.925544242Z [INFO] BACKTESTER-001.DataEngine: Registered BACKTEST\n",
                    encoding="utf-8",
                )
                status = run_backtest._build_status_payload(
                    "bt-1",
                    {
                        "requested_by": "tester",
                        "strategy_entry": "strategies.s:Strategy",
                        "strategy_bundle": "bundle.zip",
                        "symbols": ["ACTUSDT"],
                        "start": "2025-11-10T00:00:00.000Z",
                        "end": "2025-11-11T00:00:00.000Z",
                        "tags": {"purpose": "test"},
                    },
                )

                run_backtest._write_status_snapshot(
                    status_path=status_path,
                    status=status,
                    status_lock=threading.Lock(),
                    stdout_path=stdout_path,
                    updates={"init_step": "open_catalog"},
                )

                payload = json.loads(status_path.read_text(encoding="utf-8"))
                self.assertEqual(payload["init_step"], "open_catalog")
                self.assertEqual(payload["simulated_time"], "2026-03-13T21:06:53.925544242Z")
                self.assertEqual(payload["phase"], "initializing")
                self.assertIsNone(payload["node_probe"])
                self.assertIsNone(payload["prefetch_probe"])
                self.assertEqual(payload["streaming_summary"], self._expected_streaming_summary())
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)

    def test_write_status_uses_atomic_replace(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()
            from tempfile import TemporaryDirectory

            with TemporaryDirectory() as td:
                status_path = Path(td) / "status.json"
                status_path.write_text('{"old": true}', encoding="utf-8")
                replace_call: dict[str, Path] = {}
                real_replace = os.replace

                def _replace(src: str | os.PathLike[str], dst: str | os.PathLike[str]) -> None:
                    replace_call["src"] = Path(src)
                    replace_call["dst"] = Path(dst)
                    assert replace_call["src"].exists()
                    real_replace(src, dst)

                with patch("run_backtest_under_test.os.replace", side_effect=_replace):
                    run_backtest._write_status(status_path, {"status": "running", "phase": "engine_running"})

                payload = json.loads(status_path.read_text(encoding="utf-8"))
                self.assertEqual(payload["status"], "running")
                self.assertEqual(payload["phase"], "engine_running")
                self.assertEqual(replace_call["dst"], status_path)
                self.assertNotEqual(replace_call["src"], status_path)
                self.assertFalse(any(Path(td).glob(".status.json.*.tmp")))
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)

    def test_build_prefetch_probe_payload_preserves_updated_at_and_disable_reason(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()
            payload = run_backtest._build_prefetch_probe_payload(
                stage="advance",
                backend="local-read",
                ahead_hours=48,
                max_files_per_batch=3,
                updated_at="2026-03-19T00:00:00Z",
                cursor_time="2025-11-10T00:00:00Z",
                window_end_time="2025-11-13T00:00:00Z",
                pending_files=1,
                prefetched_files=2,
                requested_files_total=3,
                completed_files_total=2,
                last_batch_files=1,
                last_batch_bytes=1024,
                last_error=None,
                disable_reason="disabled for test",
            )
            self.assertEqual(payload["updated_at"], "2026-03-19T00:00:00Z")
            self.assertEqual(payload["disable_reason"], "disabled for test")
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)


class TestRunBacktestMarketDataProfile(unittest.TestCase):
    def test_validate_run_spec_accepts_catalog_controls(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()
            from tempfile import TemporaryDirectory

            with TemporaryDirectory() as td:
                root = Path(td)
                bundle_path = root / "bundle.zip"
                bundle_path.write_bytes(b"placeholder")
                run_spec_path = root / "run_spec.json"
                run_spec = {
                    "backtest_id": "bt-1",
                    "schema_version": "1.0",
                    "requested_by": "tester",
                    "strategy_entry": "strategies.s:Strategy",
                    "strategy_config_path": "strategies.s:StrategyConfig",
                    "strategy_config": {},
                    "strategy_bundle": str(bundle_path),
                    "margin_init": 0.05,
                    "margin_maint": 0.025,
                    "spot_maker_fee": 0.001,
                    "spot_taker_fee": 0.001,
                    "futures_maker_fee": 0.001,
                    "futures_taker_fee": 0.001,
                    "symbols": ["ACTUSDT"],
                    "start": "2025-11-10T00:00:00.000Z",
                    "end": "2025-11-11T00:00:00.000Z",
                    "chunk_size": 200000,
                    "seed": 12345,
                    "tags": {"purpose": "test"},
                    "catalog_controls": {
                        "prewarm_before_run": True,
                        "prewarm_threads": 25,
                        "prefetch_backend": "off",
                        "prefetch_ahead_hours": 48,
                        "prefetch_max_files_per_batch": 3,
                    },
                }
                run_spec_path.write_text(json.dumps(run_spec), encoding="utf-8")

                _, resolved_bundle_path = run_backtest._validate_run_spec(run_spec, run_spec_path)
                self.assertEqual(resolved_bundle_path, bundle_path)
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)

    def test_load_trade_ticks_defaults_true(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()
            self.assertTrue(run_backtest._load_trade_ticks_enabled({}))
            self.assertFalse(run_backtest._load_trade_ticks_enabled({"load_trade_ticks": False}))
            self.assertFalse(run_backtest._catalog_prewarm_enabled({}))
            self.assertEqual(run_backtest._catalog_prewarm_threads({}), 50)
            self.assertEqual(
                run_backtest._prefetch_runtime_settings({}),
                (None, 72, 4, None),
            )
            self.assertFalse(run_backtest._optimize_file_loading_enabled({}))
            self.assertTrue(
                run_backtest._optimize_file_loading_enabled({"optimize_file_loading": True})
            )
            self.assertFalse(
                run_backtest._optimize_file_loading_enabled({"optimize_file_loading": False})
            )
            run_backtest.OrderBookDelta = type("OrderBookDelta", (), {})
            run_backtest.TradeTick = type("TradeTick", (), {})
            run_backtest.FundingRateUpdate = type("FundingRateUpdate", (), {})
            self.assertTrue(
                run_backtest._optimize_file_loading_for_data_cls(
                    {"optimize_file_loading": True},
                    run_backtest.OrderBookDelta,
                )
            )
            self.assertTrue(
                run_backtest._optimize_file_loading_for_data_cls(
                    {"optimize_file_loading": True},
                    run_backtest.TradeTick,
                )
            )
            self.assertFalse(
                run_backtest._optimize_file_loading_for_data_cls(
                    {"optimize_file_loading": True},
                    run_backtest.FundingRateUpdate,
                )
            )
            self.assertEqual(
                run_backtest._prefetch_runtime_settings(
                    {
                        "catalog_controls": {
                            "prewarm_before_run": True,
                            "prewarm_threads": 25,
                            "prefetch_backend": "off",
                            "prefetch_ahead_hours": 48,
                            "prefetch_max_files_per_batch": 3,
                        }
                    }
                ),
                ("off", 48, 3, None),
            )
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)

    def test_prefetch_runtime_settings_disable_local_read_when_prewarm_enabled(self) -> None:
        added_modules = _install_quant_trade_stubs()
        original_backend = os.environ.get("BACKTEST_PREFETCH_BACKEND")
        try:
            run_backtest = _load_run_backtest()
            os.environ.pop("BACKTEST_PREFETCH_BACKEND", None)
            backend, ahead_hours, max_files_per_batch, disable_reason = (
                run_backtest._prefetch_runtime_settings(
                    {
                        "catalog_controls": {
                            "prewarm_before_run": True,
                            "prefetch_backend": "local-read",
                        }
                    }
                )
            )
            self.assertEqual((backend, ahead_hours, max_files_per_batch), ("off", 72, 4))
            self.assertIn("prewarm_before_run disables replay prefetch backend", disable_reason)

            backend, ahead_hours, max_files_per_batch, disable_reason = (
                run_backtest._prefetch_runtime_settings(
                    {
                        "catalog_controls": {
                            "prewarm_before_run": True,
                        }
                    }
                )
            )
            self.assertEqual((backend, ahead_hours, max_files_per_batch), ("off", 72, 4))
            self.assertIn("local-read", disable_reason)
        finally:
            if original_backend is None:
                os.environ.pop("BACKTEST_PREFETCH_BACKEND", None)
            else:
                os.environ["BACKTEST_PREFETCH_BACKEND"] = original_backend
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)

    def test_run_streaming_uses_node_run_spec_for_prefetch_settings(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()

            class FakeSession:
                def __init__(self, chunk_size: int) -> None:
                    self.chunk_size = chunk_size

                def to_query_result(self) -> list[object]:
                    return []

            class FakeController:
                instances: list["FakeController"] = []

                def __init__(self, *, files, backend, ahead_hours, max_files_per_batch) -> None:
                    self.files = files
                    self.backend = backend
                    self.ahead_hours = ahead_hours
                    self.max_files_per_batch = max_files_per_batch
                    self.advance_calls: list[int | None] = []
                    self.__class__.instances.append(self)

                def advance(self, cursor_ns):
                    self.advance_calls.append(cursor_ns)
                    return {
                        "stage": "advance",
                        "backend": self.backend,
                        "ahead_hours": self.ahead_hours,
                        "max_files_per_batch": self.max_files_per_batch,
                        "updated_at": "2026-03-19T00:00:00Z",
                    }

                def snapshot(self):
                    return {
                        "stage": "snapshot",
                        "backend": self.backend,
                        "ahead_hours": self.ahead_hours,
                        "max_files_per_batch": self.max_files_per_batch,
                        "updated_at": "2026-03-19T00:00:01Z",
                    }

                def close(self) -> None:
                    return None

            run_backtest.DataBackendSession = FakeSession
            run_backtest.ReplayPrefetchController = FakeController
            run_backtest.build_windowed_files = lambda files: list(files)
            run_backtest.build_prefetch_backend = lambda mode: f"backend:{mode}"
            run_backtest.time_like_to_ns = lambda value: 123
            run_backtest._read_current_rss_mb = lambda: 1.0

            backtest_node_mod = sys.modules["quant_trade_v1.backtest.node"]
            backtest_node_mod.Bar = type("Bar", (), {})
            backtest_node_mod.DataBackendSession = FakeSession
            backtest_node_mod.capsule_to_list = lambda chunk: list(chunk)
            backtest_node_mod.get_instrument_ids = lambda config: []
            backtest_node_mod.max_date = lambda a, b: None
            backtest_node_mod.min_date = lambda a, b: None

            node = run_backtest._InstrumentOverrideBacktestNode(
                configs=[],
                instruments=[],
                run_spec={
                    "catalog_controls": {
                        "prefetch_backend": "off",
                        "prefetch_ahead_hours": 48,
                        "prefetch_max_files_per_batch": 3,
                    }
                },
            )

            class FakeEngine:
                def add_data(self, events) -> None:
                    raise AssertionError("no events expected for empty session")

                def end(self) -> None:
                    return None

            node._run_streaming(
                run_config_id="rc1",
                engine=FakeEngine(),
                data_configs=[],
                chunk_size=10,
                start="2025-11-10T00:00:00.000Z",
                end="2025-11-11T00:00:00.000Z",
            )

            self.assertEqual(len(FakeController.instances), 1)
            controller = FakeController.instances[0]
            self.assertEqual(controller.backend, "backend:off")
            self.assertEqual(controller.ahead_hours, 48)
            self.assertEqual(controller.max_files_per_batch, 3)
            self.assertEqual(controller.advance_calls, [123])
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)

    def test_run_streaming_disables_local_read_prefetch_when_prewarm_enabled(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()

            class FakeSession:
                def __init__(self, chunk_size: int) -> None:
                    self.chunk_size = chunk_size

                def to_query_result(self) -> list[object]:
                    return []

            class FakeController:
                instances: list["FakeController"] = []

                def __init__(self, *, files, backend, ahead_hours, max_files_per_batch) -> None:
                    self.files = files
                    self.backend = backend
                    self.ahead_hours = ahead_hours
                    self.max_files_per_batch = max_files_per_batch
                    self.__class__.instances.append(self)

                def advance(self, cursor_ns):
                    return {
                        "stage": "disabled",
                        "backend": self.backend,
                        "ahead_hours": self.ahead_hours,
                        "max_files_per_batch": self.max_files_per_batch,
                        "updated_at": "2026-03-19T00:00:00Z",
                    }

                def snapshot(self):
                    return {
                        "stage": "disabled",
                        "backend": self.backend,
                        "ahead_hours": self.ahead_hours,
                        "max_files_per_batch": self.max_files_per_batch,
                        "updated_at": "2026-03-19T00:00:01Z",
                    }

                def close(self) -> None:
                    return None

            observed_prefetch_probes: list[dict] = []

            run_backtest.DataBackendSession = FakeSession
            run_backtest.ReplayPrefetchController = FakeController
            run_backtest.build_windowed_files = lambda files: list(files)
            run_backtest.build_prefetch_backend = lambda mode: f"backend:{mode}"
            run_backtest.time_like_to_ns = lambda value: 123
            run_backtest._read_current_rss_mb = lambda: 1.0

            backtest_node_mod = sys.modules["quant_trade_v1.backtest.node"]
            backtest_node_mod.Bar = type("Bar", (), {})
            backtest_node_mod.DataBackendSession = FakeSession
            backtest_node_mod.capsule_to_list = lambda chunk: list(chunk)
            backtest_node_mod.get_instrument_ids = lambda config: []
            backtest_node_mod.max_date = lambda a, b: None
            backtest_node_mod.min_date = lambda a, b: None

            node = run_backtest._InstrumentOverrideBacktestNode(
                configs=[],
                instruments=[],
                run_spec={
                    "catalog_controls": {
                        "prewarm_before_run": True,
                        "prefetch_backend": "local-read",
                    }
                },
                prefetch_probe_callback=lambda **payload: observed_prefetch_probes.append(payload),
            )

            class FakeEngine:
                def add_data(self, events) -> None:
                    raise AssertionError("no events expected for empty session")

                def end(self) -> None:
                    return None

            node._run_streaming(
                run_config_id="rc1",
                engine=FakeEngine(),
                data_configs=[],
                chunk_size=10,
                start="2025-11-10T00:00:00.000Z",
                end="2025-11-11T00:00:00.000Z",
            )

            self.assertEqual(len(FakeController.instances), 1)
            controller = FakeController.instances[0]
            self.assertEqual(controller.backend, "backend:off")
            self.assertTrue(observed_prefetch_probes)
            self.assertIn(
                "prewarm_before_run disables replay prefetch backend",
                observed_prefetch_probes[0]["disable_reason"],
            )
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)

    def test_optimize_file_loading_auto_enabled_for_large_long_v5_spread_arb(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()
            run_spec = {
                "strategy_entry": "strategies.spread_arb.v5_runtime_universe:SpreadArbV5RuntimeUniverse",
                "symbols": [f"SYM{i}" for i in range(40)],
                "start": "2025-12-01T00:00:00.000Z",
                "end": "2026-01-05T00:00:00.000Z",
            }
            self.assertTrue(run_backtest._should_default_optimize_file_loading(run_spec))
            self.assertTrue(run_backtest._optimize_file_loading_enabled(run_spec))
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)

    def test_optimize_file_loading_auto_disabled_for_small_v5_spread_arb(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()
            run_spec = {
                "strategy_entry": "strategies.spread_arb.v5_runtime_universe:SpreadArbV5RuntimeUniverse",
                "symbols": [f"SYM{i}" for i in range(10)],
                "start": "2025-12-01T00:00:00.000Z",
                "end": "2025-12-04T00:00:00.000Z",
            }
            self.assertFalse(run_backtest._should_default_optimize_file_loading(run_spec))
            self.assertFalse(run_backtest._optimize_file_loading_enabled(run_spec))
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)

    def test_optimize_file_loading_auto_disabled_for_non_v5_strategy(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()
            run_spec = {
                "strategy_entry": "strategies.alpha.foo:AlphaStrategy",
                "symbols": [f"SYM{i}" for i in range(80)],
                "start": "2025-12-01T00:00:00.000Z",
                "end": "2026-02-01T00:00:00.000Z",
            }
            self.assertFalse(run_backtest._should_default_optimize_file_loading(run_spec))
            self.assertFalse(run_backtest._optimize_file_loading_enabled(run_spec))
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)

    def test_market_data_classes_skip_trade_ticks_when_disabled(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()
            run_backtest.OrderBookDelta = type("OrderBookDelta", (), {})
            run_backtest.TradeTick = type("TradeTick", (), {})
            run_backtest.FundingRateUpdate = type("FundingRateUpdate", (), {})
            run_backtest.MarkPriceUpdate = type("MarkPriceUpdate", (), {})

            spot_classes = run_backtest._market_data_classes(
                include_futures_extras=False,
                load_trade_ticks=False,
            )
            futures_classes = run_backtest._market_data_classes(
                include_futures_extras=True,
                load_trade_ticks=False,
            )

            self.assertEqual(
                [cls.__name__ for cls in spot_classes],
                ["OrderBookDelta"],
            )
            self.assertEqual(
                [cls.__name__ for cls in futures_classes],
                ["OrderBookDelta", "FundingRateUpdate", "MarkPriceUpdate"],
            )
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)

    def test_backend_session_with_optimize_fallback_retries_file_mode(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()

            class Catalog:
                def __init__(self) -> None:
                    self.calls: list[dict] = []

                def backend_session(self, **kwargs):
                    self.calls.append(kwargs)
                    if kwargs["optimize_file_loading"]:
                        raise RuntimeError("Parquet error: Invalid Parquet file. Corrupt footer")
                    return "fallback-session"

            catalog = Catalog()
            session = run_backtest._backend_session_with_optimize_fallback(
                catalog=catalog,
                data_cls=type("OrderBookDelta", (), {}),
                identifiers=["TIAUSDT-PERP.BINANCE_FUTURES"],
                start="2025-12-01T00:00:00.000Z",
                end="2025-12-14T00:00:00.000Z",
                session="seed-session",
                files=["/tmp/one.parquet"],
                optimize_file_loading=True,
            )

            self.assertEqual(session, "fallback-session")
            self.assertEqual(len(catalog.calls), 2)
            self.assertTrue(catalog.calls[0]["optimize_file_loading"])
            self.assertFalse(catalog.calls[1]["optimize_file_loading"])
            self.assertEqual(catalog.calls[1]["files"], ["/tmp/one.parquet"])
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)

    def test_backend_session_with_optimize_fallback_skips_retry_when_disabled(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()

            class Catalog:
                def __init__(self) -> None:
                    self.calls: list[dict] = []

                def backend_session(self, **kwargs):
                    self.calls.append(kwargs)
                    return "direct-session"

            catalog = Catalog()
            session = run_backtest._backend_session_with_optimize_fallback(
                catalog=catalog,
                data_cls=type("OrderBookDelta", (), {}),
                identifiers=["TIAUSDT-PERP.BINANCE_FUTURES"],
                start="2025-12-01T00:00:00.000Z",
                end="2025-12-14T00:00:00.000Z",
                session="seed-session",
                files=["/tmp/one.parquet"],
                optimize_file_loading=False,
            )

            self.assertEqual(session, "direct-session")
            self.assertEqual(len(catalog.calls), 1)
            self.assertFalse(catalog.calls[0]["optimize_file_loading"])
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)

    def test_migrate_instrument_data_skips_missing_legacy_directory(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()

            class Catalog:
                def __init__(self) -> None:
                    self.query_calls = 0

                def query(self, *_args, **_kwargs):
                    self.query_calls += 1
                    return []

                def write(self, *_args, **_kwargs) -> None:
                    raise AssertionError("write should not be called when source path is missing")

            class InstrumentId:
                def __init__(self, value: str) -> None:
                    self.value = value

            from tempfile import TemporaryDirectory

            with TemporaryDirectory() as td:
                catalog = Catalog()
                migrated = run_backtest._migrate_instrument_data(
                    catalog=catalog,
                    catalog_root=Path(td),
                    source_id=InstrumentId("CRVUSDT.BINANCE"),
                    target_id=InstrumentId("CRVUSDT.BINANCE_SPOT"),
                    data_cls=run_backtest.OrderBookDelta,
                )

            self.assertEqual(migrated, 0)
            self.assertEqual(catalog.query_calls, 0)
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)

    def test_extract_latest_simulated_time_from_stdout_tail(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()
            from tempfile import TemporaryDirectory

            with TemporaryDirectory() as td:
                stdout_path = Path(td) / "stdout.log"
                stdout_path.write_text(
                    "\n".join(
                        [
                            "2026-03-13T21:06:51.000000000Z [INFO] bootstrap",
                            "2026-03-13T21:06:53.925544242Z [INFO] BACKTESTER-001.DataEngine: Registered BACKTEST",
                        ]
                    ),
                    encoding="utf-8",
                )
                self.assertEqual(
                    run_backtest._extract_latest_simulated_time(stdout_path),
                    "2026-03-13T21:06:53.925544242Z",
                )
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)

    def test_summarize_chunk_type_counts_orders_top_types(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()

            class OrderBookDelta:
                pass

            class TradeTick:
                pass

            events = [
                OrderBookDelta(),
                TradeTick(),
                OrderBookDelta(),
                OrderBookDelta(),
                TradeTick(),
            ]
            self.assertEqual(
                run_backtest._summarize_chunk_type_counts(events),
                {"OrderBookDelta": 3, "TradeTick": 2},
            )
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)

    def test_build_streaming_probe_payload_reports_deltas(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()

            payload = run_backtest._build_streaming_probe_payload(
                chunk_index=7,
                stage="after_clear",
                event_count=50000,
                event_type_counts={"OrderBookDelta": 40000, "TradeTick": 10000},
                rss_before_add_mb=100.0,
                rss_after_add_mb=130.5,
                rss_after_run_mb=140.25,
                rss_after_clear_mb=125.0,
                materialize_ms=12.5,
                add_ms=50.0,
                run_ms=250.0,
                clear_ms=10.25,
                chunk_wall_ms=322.75,
                chunk_start_time="2025-11-10T00:00:00.000000000Z",
                chunk_end_time="2025-11-10T00:10:00.000000000Z",
                chunk_span_seconds=600.0,
                events_per_second=154.918667,
                simulated_seconds_per_wall_second=1.858249,
                file_touch_summary={
                    "files_total": 12,
                    "bytes_total": 4096,
                    "files_touched": 3,
                    "bytes_touched": 1024,
                    "new_files_touched": 1,
                    "new_bytes_touched": 256,
                    "remaining_files": 9,
                    "remaining_bytes": 3072,
                },
            )

            self.assertEqual(payload["chunk_index"], 7)
            self.assertEqual(payload["stage"], "after_clear")
            self.assertEqual(payload["event_count"], 50000)
            self.assertEqual(payload["rss_delta_add_mb"], 30.5)
            self.assertEqual(payload["rss_delta_run_mb"], 9.75)
            self.assertEqual(payload["rss_delta_clear_mb"], -15.25)
            self.assertEqual(payload["rss_delta_chunk_mb"], 25.0)
            self.assertEqual(payload["materialize_ms"], 12.5)
            self.assertEqual(payload["add_ms"], 50.0)
            self.assertEqual(payload["run_ms"], 250.0)
            self.assertEqual(payload["clear_ms"], 10.25)
            self.assertEqual(payload["chunk_wall_ms"], 322.75)
            self.assertEqual(payload["chunk_span_seconds"], 600.0)
            self.assertEqual(payload["events_per_second"], 154.918667)
            self.assertEqual(payload["simulated_seconds_per_wall_second"], 1.858249)
            self.assertEqual(payload["file_touch_summary"]["new_files_touched"], 1)
            self.assertIn("updated_at", payload)
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)

    def test_build_node_probe_payload(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()

            payload = run_backtest._build_node_probe_payload(
                stage="after_build",
                run_config_id="rc-1",
                chunk_size=50000,
                rss_mb=1234.56,
            )

            self.assertEqual(payload["stage"], "after_build")
            self.assertEqual(payload["run_config_id"], "rc-1")
            self.assertEqual(payload["chunk_size"], 50000)
            self.assertEqual(payload["rss_mb"], 1234.56)
            self.assertIn("updated_at", payload)
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)

    def test_catalog_instruments_by_id_batches_lookup(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()

            class FakeCurrencyPair:
                def __init__(self, instrument_id) -> None:
                    self.id = instrument_id

            class FakeCryptoPerpetual(FakeCurrencyPair):
                pass

            run_backtest.CurrencyPair = FakeCurrencyPair
            run_backtest.CryptoPerpetual = FakeCryptoPerpetual

            class FakeCatalog:
                def __init__(self) -> None:
                    self.calls: list[list[str]] = []

                def instruments(self, instrument_ids=None):
                    ids = list(instrument_ids or [])
                    self.calls.append(ids)
                    return [FakeCurrencyPair(types.SimpleNamespace(value=value)) for value in ids]

            catalog = FakeCatalog()
            instrument_ids = [
                types.SimpleNamespace(value="ACTUSDT.BINANCE_SPOT"),
                types.SimpleNamespace(value="AAVEUSDT.BINANCE_SPOT"),
            ]

            mapping = run_backtest._catalog_instruments_by_id(catalog, instrument_ids)

            self.assertEqual(
                catalog.calls,
                [["ACTUSDT.BINANCE_SPOT", "AAVEUSDT.BINANCE_SPOT"]],
            )
            self.assertEqual(
                sorted(mapping.keys()),
                ["AAVEUSDT.BINANCE_SPOT", "ACTUSDT.BINANCE_SPOT"],
            )
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)

    def test_load_or_create_instrument_uses_preloaded_instruments(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()

            class FakeCurrencyPair:
                def __init__(self, **kwargs) -> None:
                    for key, value in kwargs.items():
                        setattr(self, key, value)
                    self.id = kwargs["instrument_id"]

            class FakeCryptoPerpetual(FakeCurrencyPair):
                pass

            run_backtest.CurrencyPair = FakeCurrencyPair
            run_backtest.CryptoPerpetual = FakeCryptoPerpetual

            existing = FakeCurrencyPair(
                instrument_id=types.SimpleNamespace(value="ACTUSDT.BINANCE_SPOT"),
                raw_symbol="ACTUSDT",
                base_currency="ACT",
                quote_currency="USDT",
                price_precision=4,
                size_precision=3,
                price_increment="0.0001",
                size_increment="0.001",
                ts_event=0,
                ts_init=0,
                lot_size=None,
                max_quantity=None,
                min_quantity=None,
                max_notional=None,
                min_notional="5",
                max_price=None,
                min_price=None,
                margin_init=Decimal("0"),
                margin_maint=Decimal("0"),
                maker_fee=Decimal("0.01"),
                taker_fee=Decimal("0.02"),
            )

            class FailingCatalog:
                def instruments(self, instrument_ids=None):
                    raise AssertionError("catalog.instruments should not be called")

            instrument = run_backtest._load_or_create_instrument(
                catalog=FailingCatalog(),
                instrument_id=types.SimpleNamespace(value="ACTUSDT.BINANCE_SPOT"),
                market="spot",
                base_code="ACT",
                quote_code="USDT",
                price_precision=4,
                size_precision=3,
                maker_fee=Decimal("0.001"),
                taker_fee=Decimal("0.002"),
                preloaded_instruments={"ACTUSDT.BINANCE_SPOT": existing},
            )

            self.assertEqual(instrument.id.value, "ACTUSDT.BINANCE_SPOT")
            self.assertEqual(instrument.maker_fee, Decimal("0.001"))
            self.assertEqual(instrument.taker_fee, Decimal("0.002"))
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)
