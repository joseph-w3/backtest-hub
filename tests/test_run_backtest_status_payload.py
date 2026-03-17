import importlib.util
import json
import sys
import threading
import types
import unittest
from pathlib import Path


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
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)


class TestRunBacktestMarketDataProfile(unittest.TestCase):
    def test_load_trade_ticks_defaults_true(self) -> None:
        added_modules = _install_quant_trade_stubs()
        try:
            run_backtest = _load_run_backtest()
            self.assertTrue(run_backtest._load_trade_ticks_enabled({}))
            self.assertFalse(run_backtest._load_trade_ticks_enabled({"load_trade_ticks": False}))
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
