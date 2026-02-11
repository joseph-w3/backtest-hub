import importlib.util
import sys
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
        finally:
            sys.modules.pop("run_backtest_under_test", None)
            for name in added_modules:
                sys.modules.pop(name, None)
