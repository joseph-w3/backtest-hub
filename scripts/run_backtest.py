from __future__ import annotations

import argparse
import importlib.util
import json
import os
import random
import secrets
import sys
import traceback
from datetime import datetime
from datetime import timezone
from decimal import Decimal
from pathlib import Path

from quant_trade_v1.backtest.config import BacktestDataConfig
from quant_trade_v1.backtest.config import BacktestEngineConfig
from quant_trade_v1.backtest.config import BacktestRunConfig
from quant_trade_v1.backtest.config import BacktestVenueConfig
from quant_trade_v1.backtest.config import ImportableLatencyModelConfig
from quant_trade_v1.backtest.config import MarginModelConfig
from quant_trade_v1.backtest.node import BacktestNode
from quant_trade_v1.config import DataEngineConfig
from quant_trade_v1.config import ImportableStrategyConfig
from quant_trade_v1.config import LoggingConfig
from quant_trade_v1.model import FundingRateUpdate
from quant_trade_v1.model import MarkPriceUpdate
from quant_trade_v1.model import OrderBookDelta
from quant_trade_v1.model import TradeTick
from quant_trade_v1.model.currencies import USDT
from quant_trade_v1.model.enums import CurrencyType
from quant_trade_v1.model.identifiers import InstrumentId
from quant_trade_v1.model.identifiers import Symbol
from quant_trade_v1.model.identifiers import Venue
from quant_trade_v1.model.instruments import CryptoPerpetual
from quant_trade_v1.model.instruments import CurrencyPair
from quant_trade_v1.model.objects import Currency
from quant_trade_v1.model.objects import Money
from quant_trade_v1.model.objects import Price
from quant_trade_v1.model.objects import Quantity
from quant_trade_v1.persistence.catalog import ParquetDataCatalog

BINANCE_SPOT_VENUE = Venue("BINANCE_SPOT")
BINANCE_FUTURES_VENUE = Venue("BINANCE_FUTURES")
BINANCE_LEGACY_VENUE = Venue("BINANCE")

PRICE_PRECISION = 5
SIZE_PRECISION = 5
DEFAULT_MIN_NOTIONAL = 10.0
DEFAULT_MARGIN_INIT = Decimal("0.05")
DEFAULT_MARGIN_MAINT = Decimal("0.025")
DEFAULT_BOOK_TYPE = "L2_MBP"

DEFAULT_LATENCY_CONFIG: dict[str, int] = {
    "base_latency_nanos": 20_000_000,
    "insert_latency_nanos": 2_000_000,
    "update_latency_nanos": 3_000_000,
    "cancel_latency_nanos": 1_000_000,
}
LATENCY_CONFIG_KEYS = set(DEFAULT_LATENCY_CONFIG.keys())

# CSV report outputs live alongside status.json under BACKTEST_LOGS_PATH/{backtest_id}/.
REPORTS_OUTPUT_ROOT = os.environ.get("BACKTEST_LOGS_PATH", "/opt/backtest_logs")

REQUIRED_FIELDS = {
    "backtest_id",
    "schema_version",
    "requested_by",
    "strategy_file",
    "strategy_entry",
    "strategy_config_path",
    "strategy_config",
    "margin_init",
    "margin_maint",
    "spot_maker_fee",
    "spot_taker_fee",
    "futures_maker_fee",
    "futures_taker_fee",
    "symbols",
    "start",
    "end",
    "chunk_size",
    "seed",
    "tags",
}

OPTIONAL_FIELDS = {
    "latency_config",
}

ALLOWED_FIELDS = REQUIRED_FIELDS | OPTIONAL_FIELDS


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run backtest from RunSpec JSON.")
    parser.add_argument("--run-spec", required=True, help="Path to run_spec.json.")
    return parser.parse_args()


def _load_run_spec(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"RunSpec not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError("RunSpec must be a JSON object.")
    return data


def _validate_run_spec(run_spec: dict, run_spec_path: Path) -> Path:
    missing = REQUIRED_FIELDS - set(run_spec.keys())
    extra = set(run_spec.keys()) - ALLOWED_FIELDS
    if missing:
        raise ValueError(f"RunSpec missing required fields: {sorted(missing)}")
    if extra:
        raise ValueError(f"RunSpec has unknown fields: {sorted(extra)}")

    if not isinstance(run_spec["strategy_config"], dict):
        raise ValueError("RunSpec strategy_config must be an object.")
    if not isinstance(run_spec["symbols"], list) or not run_spec["symbols"]:
        raise ValueError("RunSpec symbols must be a non-empty array.")
    if not isinstance(run_spec["chunk_size"], int) or run_spec["chunk_size"] <= 0:
        raise ValueError("RunSpec chunk_size must be a positive integer.")
    if not isinstance(run_spec["seed"], int):
        raise ValueError("RunSpec seed must be an integer.")
    if not isinstance(run_spec["tags"], dict):
        raise ValueError("RunSpec tags must be an object.")

    if "latency_config" in run_spec:
        _parse_latency_config(run_spec["latency_config"])

    for field in ("strategy_entry", "strategy_config_path"):
        value = run_spec[field]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"RunSpec {field} must be a non-empty string.")
        class_name = value.split(":", 1)[1] if ":" in value else value
        if not class_name:
            raise ValueError(f"RunSpec {field} missing class name.")

    if not isinstance(run_spec.get("strategy_file"), str) or not run_spec["strategy_file"].strip():
        raise ValueError("RunSpec strategy_file must be a non-empty string.")
    strategy_path = _resolve_strategy_file(run_spec_path, run_spec["strategy_file"])

    if not isinstance(run_spec.get("backtest_id"), str) or not run_spec["backtest_id"].strip():
        raise ValueError("RunSpec backtest_id must be a non-empty string.")

    _validate_time_order(run_spec["start"], run_spec["end"])
    return strategy_path


def _parse_decimal(field: str, value: object) -> Decimal:
    if isinstance(value, Decimal):
        parsed = value
    elif isinstance(value, (int, float, str)):
        try:
            parsed = Decimal(str(value))
        except Exception as exc:
            raise ValueError(f"RunSpec {field} must be a decimal-compatible value.") from exc
    else:
        raise ValueError(f"RunSpec {field} must be a decimal-compatible value.")
    if parsed < 0:
        raise ValueError(f"RunSpec {field} must be >= 0.")
    return parsed


def _parse_latency_config(value: object) -> dict[str, int]:
    if value is None:
        return dict(DEFAULT_LATENCY_CONFIG)
    if not isinstance(value, dict):
        raise ValueError("RunSpec latency_config must be an object.")
    unknown = set(value.keys()) - LATENCY_CONFIG_KEYS
    if unknown:
        raise ValueError(f"RunSpec latency_config has unknown keys: {sorted(unknown)}")
    parsed = dict(DEFAULT_LATENCY_CONFIG)
    for key, raw in value.items():
        if isinstance(raw, bool) or not isinstance(raw, int):
            raise ValueError(f"RunSpec latency_config.{key} must be a non-negative integer.")
        if raw < 0:
            raise ValueError(f"RunSpec latency_config.{key} must be >= 0.")
        parsed[str(key)] = raw
    return parsed


def _validate_time_order(start: str | int, end: str | int) -> None:
    start_dt = _parse_time(start)
    end_dt = _parse_time(end)
    if isinstance(start_dt, datetime) and isinstance(end_dt, datetime):
        if start_dt >= end_dt:
            raise ValueError("RunSpec start must be earlier than end.")
        return
    if isinstance(start_dt, (int, float)) and isinstance(end_dt, (int, float)):
        if start_dt >= end_dt:
            raise ValueError("RunSpec start must be earlier than end.")


def _parse_time(value: str | int) -> datetime | int:
    if isinstance(value, (int, float)):
        return int(value)
    if not isinstance(value, str):
        raise ValueError("RunSpec start/end must be ISO8601 strings or integers.")
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


def _resolve_strategy_file(run_spec_path: Path, strategy_file: str) -> Path:
    candidate = Path(strategy_file)
    if not candidate.is_absolute():
        candidate = run_spec_path.parent / candidate
    if not candidate.is_file():
        raise FileNotFoundError(f"Strategy file not found: {candidate}")
    return candidate


def _load_strategy_module(strategy_path: Path) -> str:
    module_name = f"strategy_{strategy_path.stem}_{secrets.token_hex(4)}"
    spec = importlib.util.spec_from_file_location(module_name, strategy_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Failed to load strategy module: {strategy_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module_name


def _rewrite_import_path(value: str, module_name: str) -> str:
    class_name = value.split(":", 1)[1] if ":" in value else value
    return f"{module_name}:{class_name}"


def _parse_symbols(symbols: list[str]) -> tuple[list[str], list[str]]:
    spot_symbols: list[str] = []
    futures_symbols: list[str] = []
    seen_spot = set()
    seen_futures = set()
    for raw in symbols:
        symbol = str(raw).strip().upper()
        if not symbol:
            continue
        if symbol.endswith("-PERP"):
            base = symbol[: -len("-PERP")]
            if not base:
                raise ValueError("Invalid futures symbol with '-PERP' suffix.")
            if base not in seen_futures:
                futures_symbols.append(base)
                seen_futures.add(base)
            continue
        if symbol not in seen_spot:
            spot_symbols.append(symbol)
            seen_spot.add(symbol)
    if not spot_symbols and not futures_symbols:
        raise ValueError("RunSpec symbols produced no spot or futures instruments.")
    return spot_symbols, futures_symbols


def _resolve_base_quote(symbol: str) -> tuple[str, str]:
    symbol = symbol.upper()
    quote_code = "USDT"
    if not symbol.endswith(quote_code):
        raise ValueError(f"Symbol '{symbol}' must end with quote '{quote_code}'.")
    base_code = symbol[: -len(quote_code)]
    if not base_code:
        raise ValueError(f"Symbol '{symbol}' has no base currency before quote '{quote_code}'.")
    return base_code, quote_code


def _build_currency(code: str, precision: int) -> Currency:
    return Currency(
        code=code,
        precision=precision,
        iso4217=0,
        name=code,
        currency_type=CurrencyType.CRYPTO,
    )


def _build_quote_currency(code: str, precision: int) -> Currency:
    if code == "USDT":
        return USDT
    return _build_currency(code, precision)


def _build_spot_instrument(
    instrument_id: InstrumentId,
    base_currency: Currency,
    quote_currency: Currency,
    price_precision: int,
    size_precision: int,
    maker_fee: Decimal,
    taker_fee: Decimal,
) -> CurrencyPair:
    price_increment = Price(1 / 10**price_precision, price_precision)
    size_increment = Quantity(1 / 10**size_precision, size_precision)
    return CurrencyPair(
        instrument_id=instrument_id,
        raw_symbol=instrument_id.symbol,
        base_currency=base_currency,
        quote_currency=quote_currency,
        price_precision=price_precision,
        size_precision=size_precision,
        price_increment=price_increment,
        size_increment=size_increment,
        ts_event=0,
        ts_init=0,
        lot_size=None,
        max_quantity=None,
        min_quantity=None,
        max_notional=None,
        min_notional=Money(DEFAULT_MIN_NOTIONAL, quote_currency),
        max_price=None,
        min_price=None,
        margin_init=Decimal(0),
        margin_maint=Decimal(0),
        maker_fee=maker_fee,
        taker_fee=taker_fee,
    )


def _build_perp_instrument(
    instrument_id: InstrumentId,
    base_currency: Currency,
    quote_currency: Currency,
    price_precision: int,
    size_precision: int,
    margin_init: Decimal,
    margin_maint: Decimal,
    maker_fee: Decimal,
    taker_fee: Decimal,
) -> CryptoPerpetual:
    price_increment = Price(1 / 10**price_precision, price_precision)
    size_increment = Quantity(1 / 10**size_precision, size_precision)
    return CryptoPerpetual(
        instrument_id=instrument_id,
        raw_symbol=instrument_id.symbol,
        base_currency=base_currency,
        quote_currency=quote_currency,
        settlement_currency=quote_currency,
        is_inverse=False,
        price_precision=price_precision,
        size_precision=size_precision,
        price_increment=price_increment,
        size_increment=size_increment,
        ts_event=0,
        ts_init=0,
        margin_init=margin_init,
        margin_maint=margin_maint,
        min_notional=Money(DEFAULT_MIN_NOTIONAL, quote_currency),
        maker_fee=maker_fee,
        taker_fee=taker_fee,
    )


def _load_or_create_instrument(
    catalog: ParquetDataCatalog,
    instrument_id: InstrumentId,
    market: str,
    base_code: str,
    quote_code: str,
    price_precision: int,
    size_precision: int,
    maker_fee: Decimal,
    taker_fee: Decimal,
    margin_init: Decimal | None = None,
    margin_maint: Decimal | None = None,
) -> CurrencyPair | CryptoPerpetual:
    instruments = catalog.instruments(instrument_ids=[instrument_id.value])
    if instruments:
        instrument = instruments[0]
        if market == "spot":
            if not isinstance(instrument, CurrencyPair):
                raise ValueError(f"Instrument {instrument_id} is not a spot CurrencyPair.")
            instrument = CurrencyPair(
                instrument_id=instrument.id,
                raw_symbol=instrument.raw_symbol,
                base_currency=instrument.base_currency,
                quote_currency=instrument.quote_currency,
                price_precision=instrument.price_precision,
                size_precision=instrument.size_precision,
                price_increment=instrument.price_increment,
                size_increment=instrument.size_increment,
                ts_event=instrument.ts_event,
                ts_init=instrument.ts_init,
                lot_size=instrument.lot_size,
                max_quantity=instrument.max_quantity,
                min_quantity=instrument.min_quantity,
                max_notional=instrument.max_notional,
                min_notional=instrument.min_notional,
                max_price=instrument.max_price,
                min_price=instrument.min_price,
                margin_init=instrument.margin_init,
                margin_maint=instrument.margin_maint,
                maker_fee=maker_fee,
                taker_fee=taker_fee,
            )
        elif market == "futures":
            if not isinstance(instrument, CryptoPerpetual):
                raise ValueError(f"Instrument {instrument_id} is not a futures CryptoPerpetual.")
            if margin_init is None or margin_maint is None:
                raise ValueError("Futures margin init/maint is required for instrument override.")
            instrument = CryptoPerpetual(
                instrument_id=instrument.id,
                raw_symbol=instrument.raw_symbol,
                base_currency=instrument.base_currency,
                quote_currency=instrument.quote_currency,
                settlement_currency=instrument.settlement_currency,
                is_inverse=instrument.is_inverse,
                price_precision=instrument.price_precision,
                size_precision=instrument.size_precision,
                price_increment=instrument.price_increment,
                size_increment=instrument.size_increment,
                ts_event=instrument.ts_event,
                ts_init=instrument.ts_init,
                margin_init=margin_init,
                margin_maint=margin_maint,
                min_notional=instrument.min_notional,
                maker_fee=maker_fee,
                taker_fee=taker_fee,
            )
        return instrument

    base_currency = _build_currency(base_code, price_precision)
    quote_currency = _build_quote_currency(quote_code, price_precision)
    if market == "spot":
        return _build_spot_instrument(
            instrument_id=instrument_id,
            base_currency=base_currency,
            quote_currency=quote_currency,
            price_precision=price_precision,
            size_precision=size_precision,
            maker_fee=maker_fee,
            taker_fee=taker_fee,
        )
    if margin_init is None or margin_maint is None:
        raise ValueError("Futures margin init/maint is required for instrument creation.")
    return _build_perp_instrument(
        instrument_id=instrument_id,
        base_currency=base_currency,
        quote_currency=quote_currency,
        price_precision=price_precision,
        size_precision=size_precision,
        margin_init=margin_init,
        margin_maint=margin_maint,
        maker_fee=maker_fee,
        taker_fee=taker_fee,
    )


def _migrate_instrument_data(
    catalog: ParquetDataCatalog,
    source_id: InstrumentId,
    target_id: InstrumentId,
    data_cls: type,
) -> int:
    data = catalog.query(data_cls, identifiers=[source_id.value])
    if not data:
        return 0
    catalog.write(data, instrument_id=target_id)
    return len(data)


def _migrate_legacy_catalog_data(
    catalog: ParquetDataCatalog,
    instrument_ids: list[InstrumentId],
) -> None:
    if not instrument_ids:
        return
    data_classes = (OrderBookDelta, TradeTick, FundingRateUpdate, MarkPriceUpdate)
    for instrument_id in instrument_ids:
        legacy_id = InstrumentId(instrument_id.symbol, BINANCE_LEGACY_VENUE)
        for data_cls in data_classes:
            migrated = _migrate_instrument_data(
                catalog=catalog,
                source_id=legacy_id,
                target_id=instrument_id,
                data_cls=data_cls,
            )
            if migrated:
                print(
                    f"[MIGRATE] {data_cls.__name__} "
                    f"{legacy_id.value} -> {instrument_id.value} ({migrated} rows)"
                )


def _build_status_payload(backtest_id: str, run_spec: dict) -> dict:
    return {
        "backtest_id": backtest_id,
        "requested_by": run_spec["requested_by"],
        "strategy_entry": run_spec["strategy_entry"],
        "strategy_file": run_spec["strategy_file"],
        "symbols": run_spec["symbols"],
        "start": run_spec["start"],
        "end": run_spec["end"],
        "tags": run_spec["tags"],
    }


def _write_status(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2)


def _export_csv_reports(
    engine: object,
    output_dir: Path,
    *,
    include_spot_account: bool,
    include_futures_account: bool,
) -> tuple[dict[str, str], list[str]]:
    """
    Export commonly-used reports to CSV, mirroring example.py behavior.

    Returns:
      - reports mapping: logical_name -> filename (within output_dir)
      - errors: list of string messages (best-effort; export failures do not abort the run)
    """

    output_dir.mkdir(parents=True, exist_ok=True)

    reports: dict[str, str] = {}
    errors: list[str] = []

    trader = getattr(engine, "trader", None)
    if trader is None:
        return reports, ["engine.trader not available; cannot export CSV reports"]

    def _write_df(name: str, df: object, filename: str) -> None:
        empty = getattr(df, "empty", None)
        if empty is True:
            return
        path = output_dir / filename
        df.to_csv(path)  # type: ignore[attr-defined]
        reports[name] = filename

    fills_report = None
    try:
        fills_report = trader.generate_fills_report()
        _write_df("fills", fills_report, "fills.csv")
    except Exception as exc:
        errors.append(f"fills export failed: {exc}")

    if include_spot_account:
        try:
            account_report_spot = trader.generate_account_report(BINANCE_SPOT_VENUE)
            _write_df("account_spot", account_report_spot, "account_spot.csv")
        except Exception as exc:
            errors.append(f"account_spot export failed: {exc}")

    if include_futures_account:
        try:
            account_report_futures = trader.generate_account_report(BINANCE_FUTURES_VENUE)
            _write_df("account_futures", account_report_futures, "account_futures.csv")
        except Exception as exc:
            errors.append(f"account_futures export failed: {exc}")

    try:
        order_fills_report = trader.generate_order_fills_report()
        _write_df("order_fills", order_fills_report, "order_fills.csv")
    except Exception as exc:
        errors.append(f"order_fills export failed: {exc}")

    try:
        positions_report = trader.generate_positions_report()
        _write_df("positions", positions_report, "positions.csv")
    except Exception as exc:
        errors.append(f"positions export failed: {exc}")

    # Best-effort concurrent max notional stats (stdout only), derived from fills report.
    _print_concurrent_max_position_from_fills(fills_report)

    return reports, errors


def _print_concurrent_max_position_from_fills(fills_report: object) -> None:
    if fills_report is None:
        print("\n" + "=" * 70)
        print("🌍 全局并发仓位统计 (Concurrent Max Position)")
        print("=" * 70)
        print("  No fills data available")
        print("=" * 70 + "\n")
        return

    empty = getattr(fills_report, "empty", None)
    if empty is True:
        print("\n" + "=" * 70)
        print("🌍 全局并发仓位统计 (Concurrent Max Position)")
        print("=" * 70)
        print("  No fills data available")
        print("=" * 70 + "\n")
        return

    # We avoid importing pandas here; use DataFrame-like APIs dynamically.
    required_cols = ("ts_event", "instrument_id", "order_side", "last_qty", "last_px")
    cols = getattr(fills_report, "columns", None)
    if cols is None or any(c not in cols for c in required_cols):
        print("\n" + "=" * 70)
        print("🌍 全局并发仓位统计 (Concurrent Max Position)")
        print("=" * 70)
        print("  Cannot compute: fills_report missing required columns")
        if cols is not None:
            print(f"  columns={list(cols)}")
        print("=" * 70 + "\n")
        return

    try:
        sorted_fills = fills_report.sort_values("ts_event")
    except Exception:
        sorted_fills = fills_report

    positions: dict[str, Decimal] = {}
    max_concurrent_spot_notional = Decimal("0")
    max_concurrent_perp_notional = Decimal("0")

    for _, fill in sorted_fills.iterrows():  # type: ignore[attr-defined]
        instrument_id = str(fill["instrument_id"])
        side = str(fill["order_side"]).upper()
        qty = Decimal(str(fill["last_qty"]))
        price = Decimal(str(fill["last_px"]))

        if instrument_id not in positions:
            positions[instrument_id] = Decimal("0")
        if side == "BUY":
            positions[instrument_id] += qty
        else:
            positions[instrument_id] -= qty

        current_spot_notional = Decimal("0")
        current_perp_notional = Decimal("0")

        for inst_id, pos in positions.items():
            notional = abs(pos) * price  # Approximation, mirrors example.py behavior
            inst_upper = inst_id.upper()
            is_perp = (
                "-PERP" in inst_upper
                or "PERP" in inst_upper
                or "FUTURES" in inst_upper
            )
            if is_perp:
                current_perp_notional += notional
            else:
                current_spot_notional += notional

        if current_spot_notional > max_concurrent_spot_notional:
            max_concurrent_spot_notional = current_spot_notional
        if current_perp_notional > max_concurrent_perp_notional:
            max_concurrent_perp_notional = current_perp_notional

    print("\n" + "=" * 70)
    print("🌍 全局并发仓位统计 (Concurrent Max Position)")
    print("=" * 70)
    print(f"  最大并发Spot名义价值: {max_concurrent_spot_notional:,.2f} USDT")
    print(f"  最大并发Perp名义价值: {max_concurrent_perp_notional:,.2f} USDT")
    print(
        f"  最大并发总名义价值: {max_concurrent_spot_notional + max_concurrent_perp_notional:,.2f} USDT"
    )
    print("=" * 70 + "\n")


def _print_reports_summary(output_dir: Path, reports: dict[str, str], errors: list[str]) -> None:
    print("\n" + "=" * 70)
    print(f"[REPORTS] CSV outputs saved to: {output_dir}")
    print("=" * 70)
    if reports:
        print("Files:")
        for name in sorted(reports):
            print(f"  - {name}: {reports[name]}")
    else:
        print("Files: (none)")
    if errors:
        print("-" * 70)
        print("Export errors:")
        for msg in errors:
            print(f"  - {msg}")
    print("=" * 70 + "\n")


def main() -> int:
    args = _parse_args()
    run_spec_path = Path(args.run_spec).expanduser()
    run_spec = _load_run_spec(run_spec_path)
    strategy_file_path = _validate_run_spec(run_spec, run_spec_path)

    backtest_id = run_spec["backtest_id"]
    log_root = REPORTS_OUTPUT_ROOT
    log_dir = Path(log_root) / backtest_id
    status_path = log_dir / "status.json"
    reports_dir = log_dir

    started_at = datetime.now(timezone.utc).isoformat()
    status = _build_status_payload(backtest_id, run_spec)
    status.update({"status": "running", "started_at": started_at})
    _write_status(status_path, status)

    try:
        random.seed(run_spec["seed"])
        module_name = _load_strategy_module(strategy_file_path)
        strategy_entry = _rewrite_import_path(run_spec["strategy_entry"], module_name)
        strategy_config_path = _rewrite_import_path(run_spec["strategy_config_path"], module_name)

        spot_symbols, futures_symbols = _parse_symbols(run_spec["symbols"])
        margin_init = _parse_decimal("margin_init", run_spec["margin_init"])
        margin_maint = _parse_decimal("margin_maint", run_spec["margin_maint"])
        spot_maker_fee = _parse_decimal("spot_maker_fee", run_spec["spot_maker_fee"])
        spot_taker_fee = _parse_decimal("spot_taker_fee", run_spec["spot_taker_fee"])
        futures_maker_fee = _parse_decimal("futures_maker_fee", run_spec["futures_maker_fee"])
        futures_taker_fee = _parse_decimal("futures_taker_fee", run_spec["futures_taker_fee"])
        catalog_path = Path(os.environ.get("CATALOG_PATH", "/opt/catalog")).expanduser()
        data_catalog = ParquetDataCatalog(catalog_path.as_posix())

        spot_instruments: list[CurrencyPair] = []
        futures_instruments: list[CryptoPerpetual] = []
        quote_code: str | None = None

        for data_symbol in spot_symbols:
            base_code, symbol_quote_code = _resolve_base_quote(data_symbol)
            if quote_code is None:
                quote_code = symbol_quote_code
            elif quote_code != symbol_quote_code:
                raise ValueError(
                    f"Mixed quote currencies are not supported: {quote_code} vs {symbol_quote_code}."
                )

            spot_id = InstrumentId(Symbol(data_symbol), BINANCE_SPOT_VENUE)
            spot_instrument = _load_or_create_instrument(
                catalog=data_catalog,
                instrument_id=spot_id,
                market="spot",
                base_code=base_code,
                quote_code=symbol_quote_code,
                price_precision=PRICE_PRECISION,
                size_precision=SIZE_PRECISION,
                maker_fee=spot_maker_fee,
                taker_fee=spot_taker_fee,
            )
            spot_instruments.append(spot_instrument)

        for base_symbol in futures_symbols:
            base_code, symbol_quote_code = _resolve_base_quote(base_symbol)
            if quote_code is None:
                quote_code = symbol_quote_code
            elif quote_code != symbol_quote_code:
                raise ValueError(
                    f"Mixed quote currencies are not supported: {quote_code} vs {symbol_quote_code}."
                )

            futures_symbol = f"{base_symbol}-PERP"
            futures_id = InstrumentId(Symbol(futures_symbol), BINANCE_FUTURES_VENUE)
            futures_instrument = _load_or_create_instrument(
                catalog=data_catalog,
                instrument_id=futures_id,
                market="futures",
                base_code=base_code,
                quote_code=symbol_quote_code,
                price_precision=PRICE_PRECISION,
                size_precision=SIZE_PRECISION,
                maker_fee=futures_maker_fee,
                taker_fee=futures_taker_fee,
                margin_init=margin_init,
                margin_maint=margin_maint,
            )
            futures_instruments.append(futures_instrument)

        _migrate_legacy_catalog_data(
            catalog=data_catalog,
            instrument_ids=[instrument.id for instrument in spot_instruments + futures_instruments],
        )

        data_configs: list[BacktestDataConfig] = []
        for instrument in spot_instruments + futures_instruments:
            data_configs.extend(
                [
                    BacktestDataConfig(
                        catalog_path=catalog_path.as_posix(),
                        data_cls=OrderBookDelta,
                        instrument_id=instrument.id,
                    ),
                    BacktestDataConfig(
                        catalog_path=catalog_path.as_posix(),
                        data_cls=TradeTick,
                        instrument_id=instrument.id,
                    ),
                ]
            )

        for instrument in futures_instruments:
            data_configs.append(
                BacktestDataConfig(
                    catalog_path=catalog_path.as_posix(),
                    data_cls=FundingRateUpdate,
                    instrument_id=instrument.id,
                )
            )
            data_configs.append(
                BacktestDataConfig(
                    catalog_path=catalog_path.as_posix(),
                    data_cls=MarkPriceUpdate,
                    instrument_id=instrument.id,
                )
            )

        latency_model_config = ImportableLatencyModelConfig(
            latency_model_path="quant_trade_v1.backtest.models:LatencyModel",
            config_path="quant_trade_v1.backtest.config:LatencyModelConfig",
            config=_parse_latency_config(run_spec.get("latency_config")),
        )

        strategy_config = dict(run_spec["strategy_config"])
        book_type = DEFAULT_BOOK_TYPE
        venues_configs: list[BacktestVenueConfig] = []
        if spot_instruments:
            venues_configs.append(
                BacktestVenueConfig(
                    name=BINANCE_SPOT_VENUE.value,
                    oms_type="NETTING",
                    account_type="CASH",
                    base_currency=None,
                    starting_balances=["100000 USDT"],
                    book_type=book_type,
                    latency_model=latency_model_config,
                )
            )
        if futures_instruments:
            venues_configs.append(
                BacktestVenueConfig(
                    name=BINANCE_FUTURES_VENUE.value,
                    oms_type="NETTING",
                    account_type="MARGIN",
                    base_currency=None,
                    starting_balances=["100000 USDT"],
                    book_type=book_type,
                    margin_model=MarginModelConfig(model_type="leveraged"),
                    latency_model=latency_model_config,
                )
            )

        strategy_config["spot_instrument_ids"] = [instrument.id for instrument in spot_instruments]
        strategy_config["futures_instrument_ids"] = [instrument.id for instrument in futures_instruments]
        strategy_config["book_type"] = book_type

        engine_config = BacktestEngineConfig(
            strategies=[
                ImportableStrategyConfig(
                    strategy_path=strategy_entry,
                    config_path=strategy_config_path,
                    config=strategy_config,
                )
            ],
            logging=LoggingConfig(
                log_level="INFO",
                log_directory=log_dir.as_posix(),
                log_level_file="WARN",
                log_file_name="engine",
                clear_log_file=True,
            ),
            data_engine=DataEngineConfig(buffer_deltas=True),
        )

        run_config = BacktestRunConfig(
            engine=engine_config,
            data=data_configs,
            venues=venues_configs,
            chunk_size=run_spec["chunk_size"],
            # Keep engine alive for report export, then dispose explicitly.
            dispose_on_completion=False,
            start=run_spec["start"],
            end=run_spec["end"],
        )

        node = BacktestNode(configs=[run_config])
        node.run()

        engine = node.get_engine(run_config.id)
        if engine is None:
            raise RuntimeError("Backtest engine not found for run config.")

        reports, report_errors = _export_csv_reports(
            engine,
            reports_dir,
            include_spot_account=bool(spot_instruments),
            include_futures_account=bool(futures_instruments),
        )
        _print_reports_summary(reports_dir, reports, report_errors)
        status["reports_output_dir"] = reports_dir.as_posix()
        status["reports"] = reports
        if report_errors:
            status["report_export_errors"] = report_errors

        try:
            engine.dispose()
        except Exception:
            pass

        status.update(
            {
                "status": "success",
                "finished_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        _write_status(status_path, status)
        return 0
    except Exception as exc:
        status.update(
            {
                "status": "failed",
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
        )
        _write_status(status_path, status)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
