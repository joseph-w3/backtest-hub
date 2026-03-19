from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Callable
import importlib
import importlib.util
import json
import os
import random
import re
import secrets
import shutil
import sys
import threading
import time
import traceback
import zipfile
from datetime import datetime
from datetime import timezone
from decimal import Decimal
from pathlib import Path, PurePosixPath

from quant_trade_v1.backtest.config import BacktestDataConfig
from quant_trade_v1.backtest.config import BacktestEngineConfig
from quant_trade_v1.backtest.config import BacktestRunConfig
from quant_trade_v1.backtest.config import BacktestVenueConfig
from quant_trade_v1.backtest.config import ImportableFillModelConfig
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

try:
    from scripts.catalog_controls import CatalogControls
    from scripts.catalog_controls import resolve_catalog_controls
    from scripts.catalog_prefetch import ReplayPrefetchController
    from scripts.catalog_prefetch import build_prefetch_backend
    from scripts.catalog_prefetch import build_windowed_files
    from scripts.catalog_prefetch import ns_to_iso
    from scripts.prewarm_catalog_cache import build_manifest
    from scripts.prewarm_catalog_cache import run_warmup
    from scripts.prewarm_catalog_cache import write_manifest
    from scripts.catalog_prefetch import time_like_to_ns
    try:
        from scripts.catalog_prefetch import ReplayFileTouchObserver as _ImportedReplayFileTouchObserver
    except ImportError:
        _ImportedReplayFileTouchObserver = None
except ImportError:  # pragma: no cover - direct script execution path
    from catalog_controls import CatalogControls
    from catalog_controls import resolve_catalog_controls
    from catalog_prefetch import ReplayPrefetchController
    from catalog_prefetch import build_prefetch_backend
    from catalog_prefetch import build_windowed_files
    from catalog_prefetch import ns_to_iso
    from prewarm_catalog_cache import build_manifest
    from prewarm_catalog_cache import run_warmup
    from prewarm_catalog_cache import write_manifest
    from catalog_prefetch import time_like_to_ns
    try:
        from catalog_prefetch import ReplayFileTouchObserver as _ImportedReplayFileTouchObserver
    except ImportError:
        _ImportedReplayFileTouchObserver = None


class _ReplayFileTouchObserverFallback:
    def __init__(self, *, files: list[object]) -> None:
        unique_paths: set[str] = set()
        normalized: list[object] = []
        for record in files:
            path = getattr(record, "path", None)
            if not isinstance(path, str) or path in unique_paths:
                continue
            unique_paths.add(path)
            normalized.append(record)
        self._files = sorted(
            normalized,
            key=lambda record: (
                getattr(record, "start_ns", None) if getattr(record, "start_ns", None) is not None else -1,
                getattr(record, "path", ""),
            ),
        )
        self._next_index = 0
        self._files_total = len(self._files)
        self._bytes_total = sum(self._record_size_bytes(record) for record in self._files)
        self._files_touched = 0
        self._bytes_touched = 0
        self._state = {
            "cursor_time": None,
            "files_total": self._files_total,
            "bytes_total": self._bytes_total,
            "files_touched": 0,
            "bytes_touched": 0,
            "new_files_touched": 0,
            "new_bytes_touched": 0,
            "remaining_files": self._files_total,
            "remaining_bytes": self._bytes_total,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    @staticmethod
    def _record_size_bytes(record: object) -> int:
        size_bytes = getattr(record, "size_bytes", None)
        if isinstance(size_bytes, int):
            return size_bytes
        path = getattr(record, "path", None)
        if not isinstance(path, str):
            return 0
        try:
            return Path(path).stat().st_size
        except OSError:
            return 0

    def advance(self, cursor_ns: int | None) -> dict:
        new_files_touched = 0
        new_bytes_touched = 0
        if cursor_ns is not None:
            while self._next_index < len(self._files):
                record = self._files[self._next_index]
                start_ns = getattr(record, "start_ns", None)
                if start_ns is not None and start_ns > cursor_ns:
                    break
                self._next_index += 1
                new_files_touched += 1
                new_bytes_touched += self._record_size_bytes(record)
        self._files_touched += new_files_touched
        self._bytes_touched += new_bytes_touched
        self._state = {
            "cursor_time": ns_to_iso(cursor_ns),
            "files_total": self._files_total,
            "bytes_total": self._bytes_total,
            "files_touched": self._files_touched,
            "bytes_touched": self._bytes_touched,
            "new_files_touched": new_files_touched,
            "new_bytes_touched": new_bytes_touched,
            "remaining_files": self._files_total - self._files_touched,
            "remaining_bytes": self._bytes_total - self._bytes_touched,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        return dict(self._state)

    def snapshot(self) -> dict:
        return dict(self._state)


ReplayFileTouchObserver = _ImportedReplayFileTouchObserver or _ReplayFileTouchObserverFallback


def build_catalog_config() -> dict:
    """Return catalog configuration derived from environment variables.

    When ``B2_KEY_ID`` **and** ``B2_APPLICATION_KEY`` are set the function
    returns S3-backed configuration (protocol ``"s3"``).  Otherwise it
    falls back to a local filesystem path.

    NOTE: Keep in sync with scripts/catalog_config.py
    """
    b2_key_id = os.environ.get("B2_KEY_ID")
    b2_app_key = os.environ.get("B2_APPLICATION_KEY")

    if b2_key_id and b2_app_key:
        endpoint = os.environ.get(
            "B2_S3_ENDPOINT", "https://s3.us-west-004.backblazeb2.com"
        )
        region = os.environ.get("B2_S3_REGION", "us-west-004")
        bucket = os.environ.get("B2_S3_BUCKET", "trade-data")
        catalog_prefix = os.environ.get("CATALOG_PREFIX", "backtest/catalog")

        catalog_fs_protocol = "s3"

        catalog_fs_storage_options = {
            "endpoint_url": endpoint,
            "key": b2_key_id,
            "secret": b2_app_key,
            "client_kwargs": {"region_name": region},
        }

        catalog_fs_rust_storage_options = {
            "endpoint_url": endpoint,
            "access_key_id": b2_key_id,
            "secret_access_key": b2_app_key,
            "region": region,
            "virtual_hosted_style_request": "false",
        }

        catalog_path = f"{bucket}/{catalog_prefix}"
    else:
        catalog_fs_protocol = None
        catalog_fs_storage_options = None
        catalog_fs_rust_storage_options = None
        catalog_path = Path(
            os.environ.get("CATALOG_PATH", "/opt/catalog")
        ).expanduser().as_posix()

    return {
        "catalog_path": catalog_path,
        "catalog_fs_protocol": catalog_fs_protocol,
        "catalog_fs_storage_options": catalog_fs_storage_options,
        "catalog_fs_rust_storage_options": catalog_fs_rust_storage_options,
    }


def _backend_session_with_optimize_fallback(
    *,
    catalog: ParquetDataCatalog,
    data_cls: type,
    identifiers: list[str] | None,
    start: str | None,
    end: str | None,
    session: object,
    files: list[str],
    optimize_file_loading: bool,
) -> object:
    if not optimize_file_loading:
        return catalog.backend_session(
            data_cls=data_cls,
            identifiers=identifiers,
            start=start,
            end=end,
            session=session,
            files=files,
            optimize_file_loading=False,
        )

    try:
        return catalog.backend_session(
            data_cls=data_cls,
            identifiers=identifiers,
            start=start,
            end=end,
            session=session,
            files=files,
            optimize_file_loading=True,
        )
    except Exception as exc:
        print(
            "[OPTIMIZE_FALLBACK] backend_session optimize_file_loading=true failed; "
            "retrying with file-level registration "
            f"data_cls={getattr(data_cls, '__name__', data_cls)} "
            f"identifiers={identifiers or []} "
            f"reason={exc!r}",
            flush=True,
        )
        return catalog.backend_session(
            data_cls=data_cls,
            identifiers=identifiers,
            start=start,
            end=end,
            session=session,
            files=files,
            optimize_file_loading=False,
        )


BINANCE_SPOT_VENUE = Venue("BINANCE_SPOT")
BINANCE_FUTURES_VENUE = Venue("BINANCE_FUTURES")
BINANCE_LEGACY_VENUE = Venue("BINANCE")

PRICE_PRECISION = 5
SIZE_PRECISION = 5
DEFAULT_MIN_NOTIONAL = 10.0
DEFAULT_MARGIN_INIT = Decimal("0.05")
DEFAULT_MARGIN_MAINT = Decimal("0.025")
DEFAULT_BOOK_TYPE = "L2_MBP"
DEFAULT_STARTING_BALANCES = ["100000 USDT"]

DEFAULT_LATENCY_CONFIG: dict[str, int] = {
    "base_latency_nanos": 20_000_000,
    "insert_latency_nanos": 2_000_000,
    "update_latency_nanos": 3_000_000,
    "cancel_latency_nanos": 1_000_000,
}
LATENCY_CONFIG_KEYS = set(DEFAULT_LATENCY_CONFIG.keys())

# CSV report outputs live alongside status.json under BACKTEST_LOGS_PATH/{backtest_id}/.
REPORTS_OUTPUT_ROOT = os.environ.get("BACKTEST_LOGS_PATH", "/opt/backtest_logs")
PROGRESS_UPDATE_SECONDS = max(
    1.0,
    float(os.environ.get("BACKTEST_PROGRESS_UPDATE_SECONDS", "15")),
)
SIMULATED_TIME_PATTERN = re.compile(
    r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)"
)

REQUIRED_FIELDS = {
    "backtest_id",
    "schema_version",
    "requested_by",
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
    "starting_balances_spot",
    "starting_balances_futures",
    "strategy_file",
    "strategy_bundle",
    "liquidity_consumption",
    "trade_execution",
    "load_trade_ticks",
    "optimize_file_loading",
    "catalog_controls",
    "fill_model_config",
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


def _validate_run_spec(run_spec: dict, run_spec_path: Path) -> tuple[Path | None, Path | None]:
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
    if "starting_balances_spot" in run_spec:
        _parse_starting_balances("starting_balances_spot", run_spec["starting_balances_spot"])
    if "starting_balances_futures" in run_spec:
        _parse_starting_balances("starting_balances_futures", run_spec["starting_balances_futures"])
    if "liquidity_consumption" in run_spec:
        if not isinstance(run_spec["liquidity_consumption"], bool):
            raise ValueError("RunSpec liquidity_consumption must be a boolean.")
    if "trade_execution" in run_spec:
        if not isinstance(run_spec["trade_execution"], bool):
            raise ValueError("RunSpec trade_execution must be a boolean.")
    if "load_trade_ticks" in run_spec:
        if not isinstance(run_spec["load_trade_ticks"], bool):
            raise ValueError("RunSpec load_trade_ticks must be a boolean.")
    if "optimize_file_loading" in run_spec:
        if not isinstance(run_spec["optimize_file_loading"], bool):
            raise ValueError("RunSpec optimize_file_loading must be a boolean.")
    if "fill_model_config" in run_spec:
        _validate_fill_model_config(run_spec["fill_model_config"])

    for field in ("strategy_entry", "strategy_config_path"):
        value = run_spec[field]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"RunSpec {field} must be a non-empty string.")
        class_name = value.split(":", 1)[1] if ":" in value else value
        if not class_name:
            raise ValueError(f"RunSpec {field} missing class name.")

    strategy_file = run_spec.get("strategy_file")
    strategy_bundle = run_spec.get("strategy_bundle")
    has_strategy_file = isinstance(strategy_file, str) and bool(strategy_file.strip())
    has_strategy_bundle = isinstance(strategy_bundle, str) and bool(strategy_bundle.strip())
    if has_strategy_file == has_strategy_bundle:
        raise ValueError("RunSpec must set exactly one of strategy_file or strategy_bundle.")
    if strategy_file is not None and not has_strategy_file:
        raise ValueError("RunSpec strategy_file must be a non-empty string.")
    if strategy_bundle is not None and not has_strategy_bundle:
        raise ValueError("RunSpec strategy_bundle must be a non-empty string.")
    strategy_path: Path | None = None
    bundle_path: Path | None = None
    if has_strategy_file:
        strategy_path = _resolve_strategy_file(run_spec_path, str(strategy_file))
    else:
        bundle_path = _resolve_strategy_bundle(run_spec_path, str(strategy_bundle))

    if has_strategy_bundle:
        for field in ("strategy_entry", "strategy_config_path"):
            value = run_spec[field]
            module_part = value.split(":", 1)[0] if ":" in value else ""
            if not module_part.strip():
                raise ValueError(f"RunSpec {field} must include module path when using strategy_bundle.")

    if not isinstance(run_spec.get("backtest_id"), str) or not run_spec["backtest_id"].strip():
        raise ValueError("RunSpec backtest_id must be a non-empty string.")

    _validate_time_order(run_spec["start"], run_spec["end"])
    return strategy_path, bundle_path


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


def _parse_starting_balances(field: str, value: object | None) -> list[str]:
    if value is None:
        return list(DEFAULT_STARTING_BALANCES)
    if not isinstance(value, list) or not value:
        raise ValueError(f"RunSpec {field} must be a non-empty array of strings.")
    parsed: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"RunSpec {field} must contain non-empty strings.")
        parsed.append(item.strip())
    return parsed


_FILL_MODEL_CONFIG_REQUIRED_KEYS = {"fill_model_path", "config_path", "config"}
_IMPORT_PATH_RE = re.compile(r"^[\w]+(\.[\w]+)*:[\w]+$")


def _validate_fill_model_config(value: object) -> None:
    if not isinstance(value, dict):
        raise ValueError("RunSpec fill_model_config must be an object.")
    missing = _FILL_MODEL_CONFIG_REQUIRED_KEYS - set(value.keys())
    if missing:
        raise ValueError(f"RunSpec fill_model_config missing required keys: {sorted(missing)}")
    unknown = set(value.keys()) - _FILL_MODEL_CONFIG_REQUIRED_KEYS
    if unknown:
        raise ValueError(f"RunSpec fill_model_config has unknown keys: {sorted(unknown)}")
    for key in ("fill_model_path", "config_path"):
        if not isinstance(value.get(key), str) or not value[key].strip():
            raise ValueError(f"RunSpec fill_model_config.{key} must be a non-empty string.")
        path = value[key].strip()
        if not _IMPORT_PATH_RE.match(path):
            raise ValueError(
                f"RunSpec fill_model_config.{key} must be 'module.path:ClassName' format."
            )
        if not path.startswith("quant_trade_v1."):
            raise ValueError(
                f"RunSpec fill_model_config.{key} must start with 'quant_trade_v1.'."
            )
    if not isinstance(value.get("config"), dict):
        raise ValueError("RunSpec fill_model_config.config must be an object.")


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


def _resolve_strategy_bundle(run_spec_path: Path, strategy_bundle: str) -> Path:
    candidate = Path(strategy_bundle)
    if not candidate.is_absolute():
        candidate = run_spec_path.parent / candidate
    if not candidate.is_file():
        raise FileNotFoundError(f"Strategy bundle not found: {candidate}")
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


def _validate_strategy_bundle_zip(bundle_path: Path) -> str:
    with zipfile.ZipFile(bundle_path) as zf:
        top_dirs: set[str] = set()
        has_file = False
        for info in zf.infolist():
            name = info.filename
            if not name:
                continue
            path = PurePosixPath(name)
            if path.is_absolute() or ".." in path.parts:
                raise ValueError(f"Strategy bundle contains unsafe path: {name}")
            if name.endswith("/"):
                if path.parts:
                    top_dirs.add(path.parts[0])
                continue
            parts = path.parts
            if len(parts) < 2:
                raise ValueError("Strategy bundle must contain a single top-level directory.")
            top_dirs.add(parts[0])
            has_file = True
        if not has_file:
            raise ValueError("Strategy bundle is empty.")
        if len(top_dirs) != 1:
            raise ValueError(f"Strategy bundle must contain exactly one top-level directory: {sorted(top_dirs)}")
        return next(iter(top_dirs))


def _extract_strategy_bundle(bundle_path: Path, extract_root: Path) -> Path:
    top_dir = _validate_strategy_bundle_zip(bundle_path)
    if extract_root.exists():
        shutil.rmtree(extract_root)
    extract_root.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(bundle_path) as zf:
        for info in zf.infolist():
            name = info.filename
            if not name or name.endswith("/"):
                continue
            rel_path = PurePosixPath(name)
            if rel_path.is_absolute() or ".." in rel_path.parts:
                raise ValueError(f"Strategy bundle contains unsafe path: {name}")
            target = extract_root / rel_path.as_posix()
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info) as src, target.open("wb") as dst:
                shutil.copyfileobj(src, dst)
    return extract_root / top_dir


def _prepare_bundle_import(bundle_root: Path) -> None:
    bundle_parent = bundle_root.parent.as_posix()
    if bundle_parent not in sys.path:
        sys.path.insert(0, bundle_parent)


def _ensure_strategy_importable(import_path: str) -> None:
    module_name = import_path.split(":", 1)[0] if ":" in import_path else ""
    if not module_name:
        raise ValueError(f"Invalid import path: {import_path}")
    importlib.import_module(module_name)


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

class _InstrumentOverrideCatalog:
    def __init__(
        self,
        base_catalog: ParquetDataCatalog,
        instruments: dict[str, CurrencyPair | CryptoPerpetual],
    ) -> None:
        self._base_catalog = base_catalog
        self._instruments = instruments

    def instruments(
        self,
        instrument_type: type | None = None,
        instrument_ids: list[object] | None = None,
        **kwargs: object,
    ) -> list[CurrencyPair | CryptoPerpetual]:
        instruments = list(self._instruments.values())
        if instrument_ids is not None:
            ids: set[str] = set()
            for instrument_id in instrument_ids:
                if hasattr(instrument_id, "value"):
                    ids.add(instrument_id.value)
                else:
                    ids.add(str(instrument_id))
            instruments = [instrument for instrument in instruments if instrument.id.value in ids]
            if not instruments:
                available = sorted(self._instruments.keys())
                raise ValueError(
                    "Instrument override missing for requested instrument_ids="
                    f"{sorted(ids)}; available overrides={available}"
                )
        if instrument_type is not None:
            instruments = [
                instrument for instrument in instruments if isinstance(instrument, instrument_type)
            ]
        return instruments

    def __getattr__(self, name: str) -> object:
        return getattr(self._base_catalog, name)


class _InstrumentOverrideBacktestNode(BacktestNode):
    _instrument_overrides: dict[str, CurrencyPair | CryptoPerpetual] = {}

    def __init__(
        self,
        configs: list[BacktestRunConfig],
        instruments: list[CurrencyPair | CryptoPerpetual],
        *,
        run_spec: dict[str, Any] | None = None,
        node_probe_callback: Callable[..., None] | None = None,
        streaming_probe_callback: Callable[..., None] | None = None,
        prepare_probe_callback: Callable[..., None] | None = None,
        prefetch_probe_callback: Callable[..., None] | None = None,
    ) -> None:
        super().__init__(configs)
        self.__class__._instrument_overrides = {
            instrument.id.value: instrument for instrument in instruments
        }
        self._node_probe_callback = node_probe_callback
        self._streaming_probe_callback = streaming_probe_callback
        self._prepare_probe_callback = prepare_probe_callback
        self._prefetch_probe_callback = prefetch_probe_callback
        self._run_spec = dict(run_spec or {})

    @classmethod
    def load_catalog(cls, config: BacktestDataConfig) -> ParquetDataCatalog:
        base_catalog = ParquetDataCatalog(
            path=config.catalog_path,
            fs_protocol=config.catalog_fs_protocol,
            fs_storage_options=config.catalog_fs_storage_options,
            fs_rust_storage_options=config.catalog_fs_rust_storage_options,
        )
        return _InstrumentOverrideCatalog(base_catalog, cls._instrument_overrides)

    def _emit_streaming_probe(
        self,
        *,
        chunk_index: int,
        stage: str,
        event_count: int | None = None,
        event_type_counts: dict[str, int] | None = None,
        rss_before_add_mb: float | None = None,
        rss_after_add_mb: float | None = None,
        rss_after_run_mb: float | None = None,
        rss_after_clear_mb: float | None = None,
        materialize_ms: float | None = None,
        add_ms: float | None = None,
        run_ms: float | None = None,
        clear_ms: float | None = None,
        chunk_wall_ms: float | None = None,
        chunk_start_time: str | None = None,
        chunk_end_time: str | None = None,
        chunk_span_seconds: float | None = None,
        events_per_second: float | None = None,
        simulated_seconds_per_wall_second: float | None = None,
        file_touch_summary: dict | None = None,
    ) -> None:
        if self._streaming_probe_callback is None:
            return
        self._streaming_probe_callback(
            chunk_index=chunk_index,
            stage=stage,
            event_count=event_count,
            event_type_counts=event_type_counts,
            rss_before_add_mb=rss_before_add_mb,
            rss_after_add_mb=rss_after_add_mb,
            rss_after_run_mb=rss_after_run_mb,
            rss_after_clear_mb=rss_after_clear_mb,
            materialize_ms=materialize_ms,
            add_ms=add_ms,
            run_ms=run_ms,
            clear_ms=clear_ms,
            chunk_wall_ms=chunk_wall_ms,
            chunk_start_time=chunk_start_time,
            chunk_end_time=chunk_end_time,
            chunk_span_seconds=chunk_span_seconds,
            events_per_second=events_per_second,
            simulated_seconds_per_wall_second=simulated_seconds_per_wall_second,
            file_touch_summary=file_touch_summary,
        )

    def _emit_node_probe(
        self,
        *,
        stage: str,
        run_config_id: str | None = None,
        chunk_size: int | None = None,
        rss_mb: float | None = None,
    ) -> None:
        if self._node_probe_callback is None:
            return
        self._node_probe_callback(
            stage=stage,
            run_config_id=run_config_id,
            chunk_size=chunk_size,
            rss_mb=rss_mb,
        )

    def _emit_prepare_probe(
        self,
        *,
        stage: str,
        config_index: int,
        config_count: int,
        data_type: str,
        instrument_id: str | None,
        identifier_count: int,
        identifier_sample: list[str],
        optimize_file_loading: bool,
        cache_hit: bool | None = None,
        file_list_count: int | None = None,
        filter_file_count: int | None = None,
        file_list_ms: float | None = None,
        filter_files_ms: float | None = None,
        register_session_ms: float | None = None,
        total_ms: float | None = None,
        rss_mb: float | None = None,
    ) -> None:
        if self._prepare_probe_callback is None:
            return
        self._prepare_probe_callback(
            stage=stage,
            config_index=config_index,
            config_count=config_count,
            data_type=data_type,
            instrument_id=instrument_id,
            identifier_count=identifier_count,
            identifier_sample=identifier_sample,
            optimize_file_loading=optimize_file_loading,
            cache_hit=cache_hit,
            file_list_count=file_list_count,
            filter_file_count=filter_file_count,
            file_list_ms=file_list_ms,
            filter_files_ms=filter_files_ms,
            register_session_ms=register_session_ms,
            total_ms=total_ms,
            rss_mb=rss_mb,
        )

    def _emit_prefetch_probe(
        self,
        *,
        stage: str,
        backend: str,
        ahead_hours: int,
        max_files_per_batch: int,
        updated_at: str | None = None,
        cursor_time: str | None = None,
        window_end_time: str | None = None,
        pending_files: int | None = None,
        prefetched_files: int | None = None,
        requested_files_total: int | None = None,
        completed_files_total: int | None = None,
        last_batch_files: int | None = None,
        last_batch_bytes: int | None = None,
        last_error: str | None = None,
        disable_reason: str | None = None,
    ) -> None:
        if getattr(self, "_prefetch_probe_callback", None) is None:
            return
        self._prefetch_probe_callback(
            stage=stage,
            backend=backend,
            ahead_hours=ahead_hours,
            max_files_per_batch=max_files_per_batch,
            updated_at=updated_at,
            cursor_time=cursor_time,
            window_end_time=window_end_time,
            pending_files=pending_files,
            prefetched_files=prefetched_files,
            requested_files_total=requested_files_total,
            completed_files_total=completed_files_total,
            last_batch_files=last_batch_files,
            last_batch_bytes=last_batch_bytes,
            last_error=last_error,
            disable_reason=disable_reason,
        )

    def run(self) -> list[object]:
        self._emit_node_probe(stage="before_build", rss_mb=_read_current_rss_mb())
        self.build()
        self._emit_node_probe(stage="after_build", rss_mb=_read_current_rss_mb())
        results: list[object] = []

        for config in self._configs.values():
            try:
                self._emit_node_probe(
                    stage="before_run_config",
                    run_config_id=config.id,
                    chunk_size=config.chunk_size,
                    rss_mb=_read_current_rss_mb(),
                )
                result = self._run(
                    run_config_id=config.id,
                    data_configs=config.data,
                    chunk_size=config.chunk_size,
                    dispose_on_completion=config.dispose_on_completion,
                    start=config.start,
                    end=config.end,
                )
                results.append(result)
                self._emit_node_probe(
                    stage="after_run_config",
                    run_config_id=config.id,
                    chunk_size=config.chunk_size,
                    rss_mb=_read_current_rss_mb(),
                )
            except Exception as exc:
                self._emit_node_probe(
                    stage="run_config_exception",
                    run_config_id=config.id,
                    chunk_size=config.chunk_size,
                    rss_mb=_read_current_rss_mb(),
                )
                if config.raise_exception:
                    raise exc

                self.log_backtest_exception(exc, config)

        return results

    def _run_streaming(
        self,
        run_config_id: str,
        engine: object,
        data_configs: list[BacktestDataConfig],
        chunk_size: int,
        start: str | int | None = None,
        end: str | int | None = None,
    ) -> None:
        from quant_trade_v1.backtest.node import Bar
        from quant_trade_v1.backtest.node import DataBackendSession
        from quant_trade_v1.backtest.node import capsule_to_list
        from quant_trade_v1.backtest.node import get_instrument_ids
        from quant_trade_v1.backtest.node import max_date
        from quant_trade_v1.backtest.node import min_date

        session = DataBackendSession(chunk_size=chunk_size)
        cached_file_lists: dict[tuple[str, str | None, type], list[str]] = {}
        prefetch_candidates: list[str] = []
        self._emit_streaming_probe(
            chunk_index=-1,
            stage="before_prepare_queries",
            rss_before_add_mb=_read_current_rss_mb(),
        )

        config_count = len(data_configs)
        for config_index, config in enumerate(data_configs, start=1):
            catalog = self.load_catalog(config)
            used_start = config.start_time
            used_end = config.end_time

            if used_start is not None or start is not None:
                result = max_date(used_start, start)
                used_start = result.isoformat() if result else None

            if used_end is not None or end is not None:
                result = min_date(used_end, end)
                used_end = result.isoformat() if result else None

            used_instrument_ids = get_instrument_ids(config)
            used_bar_types = []

            if config.data_type == Bar:
                if config.bar_types is None and config.instrument_ids is None:
                    assert config.instrument_id, "No `instrument_id` for Bar data config"
                    assert config.bar_spec, "No `bar_spec` for Bar data config"

                if config.instrument_id is not None and config.bar_spec is not None:
                    bar_type = f"{config.instrument_id}-{config.bar_spec}-EXTERNAL"
                    used_bar_types = [bar_type]
                elif config.bar_types is not None:
                    used_bar_types = config.bar_types
                elif config.instrument_ids is not None and config.bar_spec is not None:
                    for instrument_id in config.instrument_ids:
                        used_bar_types.append(f"{instrument_id}-{config.bar_spec}-EXTERNAL")

            identifiers = used_bar_types or used_instrument_ids
            cache_key = (config.catalog_path, config.catalog_fs_protocol, config.data_type)
            data_type_name = getattr(config.data_type, "__name__", str(config.data_type))
            instrument_id = config.instrument_id.value if config.instrument_id is not None else None
            identifier_values = [str(identifier) for identifier in identifiers]
            identifier_sample = identifier_values[:3]
            cache_hit = cache_key in cached_file_lists
            prepare_started_at = time.perf_counter()
            file_list_ms: float | None = None
            filter_files_ms: float | None = None
            register_session_ms: float | None = None

            self._emit_prepare_probe(
                stage="before_file_list",
                config_index=config_index,
                config_count=config_count,
                data_type=data_type_name,
                instrument_id=instrument_id,
                identifier_count=len(identifier_values),
                identifier_sample=identifier_sample,
                optimize_file_loading=config.optimize_file_loading,
                cache_hit=cache_hit,
                rss_mb=_read_current_rss_mb(),
            )
            if not cache_hit:
                file_list_started_at = time.perf_counter()
                cached_file_lists[cache_key] = catalog.get_file_list_from_data_cls(config.data_type)
                file_list_ms = round((time.perf_counter() - file_list_started_at) * 1000, 2)
            self._emit_prepare_probe(
                stage="after_file_list",
                config_index=config_index,
                config_count=config_count,
                data_type=data_type_name,
                instrument_id=instrument_id,
                identifier_count=len(identifier_values),
                identifier_sample=identifier_sample,
                optimize_file_loading=config.optimize_file_loading,
                cache_hit=cache_hit,
                file_list_count=len(cached_file_lists[cache_key]),
                file_list_ms=file_list_ms,
                rss_mb=_read_current_rss_mb(),
            )

            filter_started_at = time.perf_counter()
            filter_files = catalog.filter_files(
                data_cls=config.data_type,
                file_paths=cached_file_lists[cache_key],
                identifiers=identifiers,
                start=used_start,
                end=used_end,
            )
            filter_files_ms = round((time.perf_counter() - filter_started_at) * 1000, 2)
            prefetch_candidates.extend(str(path) for path in filter_files)
            self._emit_prepare_probe(
                stage="after_filter_files",
                config_index=config_index,
                config_count=config_count,
                data_type=data_type_name,
                instrument_id=instrument_id,
                identifier_count=len(identifier_values),
                identifier_sample=identifier_sample,
                optimize_file_loading=config.optimize_file_loading,
                cache_hit=cache_hit,
                file_list_count=len(cached_file_lists[cache_key]),
                filter_file_count=len(filter_files),
                file_list_ms=file_list_ms,
                filter_files_ms=filter_files_ms,
                rss_mb=_read_current_rss_mb(),
            )

            register_started_at = time.perf_counter()
            session = _backend_session_with_optimize_fallback(
                catalog=catalog,
                data_cls=config.data_type,
                identifiers=identifiers,
                start=used_start,
                end=used_end,
                session=session,
                files=filter_files,
                optimize_file_loading=config.optimize_file_loading,
            )
            register_session_ms = round((time.perf_counter() - register_started_at) * 1000, 2)
            self._emit_prepare_probe(
                stage="after_register_session",
                config_index=config_index,
                config_count=config_count,
                data_type=data_type_name,
                instrument_id=instrument_id,
                identifier_count=len(identifier_values),
                identifier_sample=identifier_sample,
                optimize_file_loading=config.optimize_file_loading,
                cache_hit=cache_hit,
                file_list_count=len(cached_file_lists[cache_key]),
                filter_file_count=len(filter_files),
                file_list_ms=file_list_ms,
                filter_files_ms=filter_files_ms,
                register_session_ms=register_session_ms,
                total_ms=round((time.perf_counter() - prepare_started_at) * 1000, 2),
                rss_mb=_read_current_rss_mb(),
            )

        self._emit_streaming_probe(
            chunk_index=-1,
            stage="after_prepare_queries",
            rss_before_add_mb=_read_current_rss_mb(),
        )
        (
            prefetch_backend_mode,
            prefetch_ahead_hours,
            prefetch_max_files_per_batch,
            prefetch_disable_reason,
        ) = (
            _prefetch_runtime_settings(self._run_spec)
        )
        if prefetch_disable_reason is not None:
            print(f"[PREFETCH] {prefetch_disable_reason}")
        windowed_files = build_windowed_files(prefetch_candidates)
        file_touch_observer = (
            ReplayFileTouchObserver(files=windowed_files)
            if not windowed_files or hasattr(windowed_files[0], "path")
            else None
        )
        initial_file_touch_summary = (
            file_touch_observer.advance(time_like_to_ns(start))
            if file_touch_observer is not None
            else None
        )
        self._emit_streaming_probe(
            chunk_index=-1,
            stage="before_query_result",
            rss_before_add_mb=_read_current_rss_mb(),
            file_touch_summary=initial_file_touch_summary,
        )
        prefetch_controller = ReplayPrefetchController(
            files=windowed_files,
            backend=build_prefetch_backend(mode=prefetch_backend_mode),
            ahead_hours=prefetch_ahead_hours,
            max_files_per_batch=prefetch_max_files_per_batch,
        )
        self._emit_prefetch_probe(
            **prefetch_controller.advance(time_like_to_ns(start)),
            disable_reason=prefetch_disable_reason,
        )
        try:
            for chunk_index, chunk in enumerate(session.to_query_result()):
                chunk_started_at = time.perf_counter()
                events = capsule_to_list(chunk)
                materialize_ms = round((time.perf_counter() - chunk_started_at) * 1000, 2)
                chunk_start_ns, chunk_end_ns = _chunk_time_window_ns(events)
                event_count = len(events)
                event_type_counts = _summarize_chunk_type_counts(events)
                rss_before_add_mb = _read_current_rss_mb()
                self._emit_streaming_probe(
                    chunk_index=chunk_index,
                    stage="after_materialize",
                    event_count=event_count,
                    event_type_counts=event_type_counts,
                    rss_before_add_mb=rss_before_add_mb,
                    materialize_ms=materialize_ms,
                    chunk_start_time=ns_to_iso(chunk_start_ns),
                    chunk_end_time=ns_to_iso(chunk_end_ns),
                )
                add_started_at = time.perf_counter()
                engine.add_data(
                    data=events,
                    validate=False,
                    sort=True,
                )
                add_ms = round((time.perf_counter() - add_started_at) * 1000, 2)
                rss_after_add_mb = _read_current_rss_mb()
                self._emit_streaming_probe(
                    chunk_index=chunk_index,
                    stage="after_add",
                    event_count=event_count,
                    event_type_counts=event_type_counts,
                    rss_before_add_mb=rss_before_add_mb,
                    rss_after_add_mb=rss_after_add_mb,
                    materialize_ms=materialize_ms,
                    add_ms=add_ms,
                    chunk_start_time=ns_to_iso(chunk_start_ns),
                    chunk_end_time=ns_to_iso(chunk_end_ns),
                )
                run_started_at = time.perf_counter()
                engine.run(
                    start=start,
                    end=end,
                    run_config_id=run_config_id,
                    streaming=True,
                )
                run_ms = round((time.perf_counter() - run_started_at) * 1000, 2)
                rss_after_run_mb = _read_current_rss_mb()
                clear_started_at = time.perf_counter()
                engine.clear_data()
                clear_ms = round((time.perf_counter() - clear_started_at) * 1000, 2)
                rss_after_clear_mb = _read_current_rss_mb()
                chunk_wall_ms = round((time.perf_counter() - chunk_started_at) * 1000, 2)
                chunk_span_seconds = _chunk_span_seconds(chunk_start_ns, chunk_end_ns)
                file_touch_summary = (
                    file_touch_observer.advance(chunk_end_ns or chunk_start_ns)
                    if file_touch_observer is not None
                    else None
                )
                self._emit_prefetch_probe(
                    **prefetch_controller.advance(chunk_end_ns or chunk_start_ns),
                    disable_reason=prefetch_disable_reason,
                )
                self._emit_streaming_probe(
                    chunk_index=chunk_index,
                    stage="after_clear",
                    event_count=event_count,
                    event_type_counts=event_type_counts,
                    rss_before_add_mb=rss_before_add_mb,
                    rss_after_add_mb=rss_after_add_mb,
                    rss_after_run_mb=rss_after_run_mb,
                    rss_after_clear_mb=rss_after_clear_mb,
                    materialize_ms=materialize_ms,
                    add_ms=add_ms,
                    run_ms=run_ms,
                    clear_ms=clear_ms,
                    chunk_wall_ms=chunk_wall_ms,
                    chunk_start_time=ns_to_iso(chunk_start_ns),
                    chunk_end_time=ns_to_iso(chunk_end_ns),
                    chunk_span_seconds=chunk_span_seconds,
                    events_per_second=_per_second(event_count, chunk_wall_ms),
                    simulated_seconds_per_wall_second=_per_second(
                        chunk_span_seconds, chunk_wall_ms
                    ),
                    file_touch_summary=file_touch_summary,
                )
        finally:
            self._emit_prefetch_probe(
                **prefetch_controller.snapshot(),
                disable_reason=prefetch_disable_reason,
            )
            prefetch_controller.close()

        engine.end()


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
    preloaded_instruments: dict[str, CurrencyPair | CryptoPerpetual] | None = None,
) -> CurrencyPair | CryptoPerpetual:
    instrument: CurrencyPair | CryptoPerpetual | None = None
    if preloaded_instruments is not None:
        instrument = preloaded_instruments.get(instrument_id.value)
    else:
        instruments = catalog.instruments(instrument_ids=[instrument_id.value])
        if instruments:
            instrument = instruments[0]
    if instrument is not None:
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


def _catalog_instruments_by_id(
    catalog: ParquetDataCatalog,
    instrument_ids: list[InstrumentId],
) -> dict[str, CurrencyPair | CryptoPerpetual]:
    if not instrument_ids:
        return {}
    # Batch metadata lookup once per market to avoid repeated catalog glob/query
    # work while building the run config on remote-backed mounts.
    instruments = catalog.instruments(
        instrument_ids=[instrument_id.value for instrument_id in instrument_ids]
    )
    return {
        instrument.id.value: instrument
        for instrument in instruments
        if isinstance(instrument, (CurrencyPair, CryptoPerpetual))
    }


def _migrate_instrument_data(
    catalog: ParquetDataCatalog,
    catalog_root: Path,
    source_id: InstrumentId,
    target_id: InstrumentId,
    data_cls: type,
) -> int:
    data_dir = _local_catalog_data_dir(data_cls)
    if data_dir is not None:
        source_path = catalog_root / "data" / data_dir / source_id.value
        if not source_path.exists():
            return 0
    data = catalog.query(data_cls, identifiers=[source_id.value])
    if not data:
        return 0
    catalog.write(data, instrument_id=target_id)
    return len(data)


def _migrate_legacy_catalog_data(
    catalog: ParquetDataCatalog,
    catalog_root: Path,
    spot_instrument_ids: list[InstrumentId],
    futures_instrument_ids: list[InstrumentId],
    *,
    load_trade_ticks: bool,
) -> None:
    if not spot_instrument_ids and not futures_instrument_ids:
        return
    for instrument_id in spot_instrument_ids:
        legacy_id = InstrumentId(instrument_id.symbol, BINANCE_LEGACY_VENUE)
        for data_cls in _market_data_classes(
            include_futures_extras=False,
            load_trade_ticks=load_trade_ticks,
        ):
            migrated = _migrate_instrument_data(
                catalog=catalog,
                catalog_root=catalog_root,
                source_id=legacy_id,
                target_id=instrument_id,
                data_cls=data_cls,
            )
            if migrated:
                print(
                    f"[MIGRATE] {data_cls.__name__} "
                    f"{legacy_id.value} -> {instrument_id.value} ({migrated} rows)"
                )
    for instrument_id in futures_instrument_ids:
        legacy_id = InstrumentId(instrument_id.symbol, BINANCE_LEGACY_VENUE)
        for data_cls in _market_data_classes(
            include_futures_extras=True,
            load_trade_ticks=load_trade_ticks,
        ):
            migrated = _migrate_instrument_data(
                catalog=catalog,
                catalog_root=catalog_root,
                source_id=legacy_id,
                target_id=instrument_id,
                data_cls=data_cls,
            )
            if migrated:
                print(
                    f"[MIGRATE] {data_cls.__name__} "
                    f"{legacy_id.value} -> {instrument_id.value} ({migrated} rows)"
                )


def _load_trade_ticks_enabled(run_spec: dict) -> bool:
    value = run_spec.get("load_trade_ticks")
    if value is None:
        return True
    return bool(value)


def _catalog_controls(run_spec: dict) -> CatalogControls:
    return resolve_catalog_controls(run_spec)


def _catalog_prewarm_enabled(run_spec: dict) -> bool:
    return _catalog_controls(run_spec).prewarm_before_run


def _catalog_prewarm_threads(run_spec: dict) -> int:
    return _catalog_controls(run_spec).prewarm_threads


_PREWARM_LOCAL_READ_PREFETCH_DISABLE_REASON = (
    "catalog_controls.prewarm_before_run disables replay prefetch backend "
    "'local-read' because full-file background reads against the same worker-local "
    "catalog mount have triggered JuiceFS I/O errors after prewarm; using backend "
    "'off' instead."
)


def _resolved_prefetch_backend_mode(run_spec: dict) -> str:
    controls = _catalog_controls(run_spec)
    if controls.prefetch_backend is not None:
        return controls.prefetch_backend.strip().lower()
    return os.environ.get("BACKTEST_PREFETCH_BACKEND", "local-read").strip().lower()


def _prefetch_runtime_settings(run_spec: dict) -> tuple[str | None, int, int, str | None]:
    controls = _catalog_controls(run_spec)
    ahead_hours = (
        controls.prefetch_ahead_hours
        if controls.prefetch_ahead_hours is not None
        else int(os.environ.get("BACKTEST_PREFETCH_AHEAD_HOURS", "72"))
    )
    max_files_per_batch = (
        controls.prefetch_max_files_per_batch
        if controls.prefetch_max_files_per_batch is not None
        else int(os.environ.get("BACKTEST_PREFETCH_MAX_FILES_PER_BATCH", "4"))
    )
    disable_reason: str | None = None
    effective_backend = controls.prefetch_backend
    # `local-read` prefetch performs full file reads on the same mount that replay
    # is actively consuming. After a launch-time JuiceFS warmup, that combination
    # has triggered mount-layer I/O errors on worker-local catalogs, so keep the
    # runtime on the safe path until the backend is redesigned.
    if controls.prewarm_before_run and _resolved_prefetch_backend_mode(run_spec) == "local-read":
        effective_backend = "off"
        disable_reason = _PREWARM_LOCAL_READ_PREFETCH_DISABLE_REASON
    return effective_backend, ahead_hours, max_files_per_batch, disable_reason


def _spread_arb_v5_runtime_universe_run(run_spec: dict) -> bool:
    strategy_entry = run_spec.get("strategy_entry")
    if not isinstance(strategy_entry, str):
        return False
    return strategy_entry == "strategies.spread_arb.v5_runtime_universe:SpreadArbV5RuntimeUniverse"


def _run_duration_days(run_spec: dict) -> float | None:
    start = run_spec.get("start")
    end = run_spec.get("end")
    if start is None or end is None:
        return None
    try:
        start_dt = _parse_time(start)
        end_dt = _parse_time(end)
    except Exception:
        return None
    if not isinstance(start_dt, datetime) or not isinstance(end_dt, datetime):
        return None
    return (end_dt - start_dt).total_seconds() / 86_400


def _should_default_optimize_file_loading(run_spec: dict) -> bool:
    if not _spread_arb_v5_runtime_universe_run(run_spec):
        return False
    symbols = run_spec.get("symbols")
    if not isinstance(symbols, list):
        return False
    symbol_count = sum(1 for symbol in symbols if isinstance(symbol, str) and symbol.strip())
    if symbol_count >= 40:
        return True
    duration_days = _run_duration_days(run_spec)
    if duration_days is None:
        return False
    return symbol_count >= 20 and duration_days >= 30


def _optimize_file_loading_enabled(run_spec: dict) -> bool:
    value = run_spec.get("optimize_file_loading")
    if value is not None:
        return bool(value)
    return _should_default_optimize_file_loading(run_spec)


def _optimize_file_loading_for_data_cls(run_spec: dict, data_cls: type) -> bool:
    if not _optimize_file_loading_enabled(run_spec):
        return False
    # Funding/mark-price parquet directories currently contain schema drift in the
    # production catalog. Keep directory registration limited to the stable high-volume
    # classes where it materially reduces DataFusion table explosion.
    return data_cls in (OrderBookDelta, TradeTick)


def _market_data_classes(
    *,
    include_futures_extras: bool,
    load_trade_ticks: bool,
) -> tuple[type, ...]:
    data_classes: list[type] = [OrderBookDelta]
    if load_trade_ticks:
        data_classes.append(TradeTick)
    if include_futures_extras:
        data_classes.extend([FundingRateUpdate, MarkPriceUpdate])
    return tuple(data_classes)


def _local_catalog_data_dir(data_cls: type) -> str | None:
    return {
        OrderBookDelta: "order_book_deltas",
        TradeTick: "trade_ticks",
        FundingRateUpdate: "funding_rate_updates",
        MarkPriceUpdate: "mark_price_updates",
    }.get(data_cls)


def _build_status_payload(backtest_id: str, run_spec: dict) -> dict:
    return {
        "backtest_id": backtest_id,
        "requested_by": run_spec["requested_by"],
        "strategy_entry": run_spec["strategy_entry"],
        "strategy_file": run_spec.get("strategy_file"),
        "strategy_bundle": run_spec.get("strategy_bundle"),
        "catalog_controls": run_spec.get("catalog_controls"),
        "symbols": run_spec["symbols"],
        "symbol_count": len(run_spec["symbols"]),
        "start": run_spec["start"],
        "end": run_spec["end"],
        "tags": run_spec["tags"],
        "phase": "initializing",
        "init_step": "bootstrap",
        "last_progress_at": datetime.now(timezone.utc).isoformat(),
        "simulated_time": None,
        "node_probe": None,
        "streaming_probe": None,
        "prepare_probe": None,
        "prefetch_probe": None,
        "streaming_summary": {
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
        },
    }


def _write_status(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    try:
        # Readers poll status.json concurrently; replace it atomically to avoid partial JSON reads.
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=True, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


def _extract_latest_simulated_time(stdout_path: Path) -> str | None:
    if not stdout_path.exists():
        return None

    try:
        size = stdout_path.stat().st_size
    except OSError:
        return None

    try:
        with stdout_path.open("rb") as handle:
            if size > 1024 * 1024:
                handle.seek(size - 1024 * 1024)
            text = handle.read().decode("utf-8", errors="replace")
    except OSError:
        return None

    matches = list(SIMULATED_TIME_PATTERN.finditer(text))
    if not matches:
        return None
    return matches[-1].group(1)


def _write_status_snapshot(
    *,
    status_path: Path,
    status: dict,
    status_lock: threading.Lock,
    stdout_path: Path | None,
    updates: dict | None = None,
    clear_keys: tuple[str, ...] = (),
) -> None:
    with status_lock:
        if updates:
            status.update(updates)
        for key in clear_keys:
            status.pop(key, None)
        status["last_progress_at"] = datetime.now(timezone.utc).isoformat()
        if stdout_path is not None:
            simulated_time = _extract_latest_simulated_time(stdout_path)
            if simulated_time:
                status["simulated_time"] = simulated_time
        snapshot = dict(status)
    _write_status(status_path, snapshot)


def _read_current_rss_mb() -> float | None:
    status_path = Path(f"/proc/{os.getpid()}/status")
    try:
        for line in status_path.read_text(encoding="utf-8").splitlines():
            if not line.startswith("VmRSS:"):
                continue
            parts = line.split()
            if len(parts) < 2:
                return None
            return round(int(parts[1]) / 1024.0, 2)
    except (OSError, ValueError):
        return None
    return None


def _summarize_chunk_type_counts(events: list[object], limit: int = 8) -> dict[str, int]:
    counts = Counter(type(event).__name__ for event in events)
    items = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return dict(items[:limit])


def _rss_delta_mb(after_mb: float | None, before_mb: float | None) -> float | None:
    if after_mb is None or before_mb is None:
        return None
    return round(after_mb - before_mb, 2)


def _event_timestamp_ns(event: object) -> int | None:
    value = getattr(event, "ts_event", None)
    if value is None:
        return None
    return time_like_to_ns(value)


def _chunk_time_window_ns(events: list[object]) -> tuple[int | None, int | None]:
    if not events:
        return None, None
    start_ns = _event_timestamp_ns(events[0])
    end_ns = _event_timestamp_ns(events[-1])
    return start_ns, end_ns


def _chunk_span_seconds(start_ns: int | None, end_ns: int | None) -> float | None:
    if start_ns is None or end_ns is None or end_ns < start_ns:
        return None
    return round((end_ns - start_ns) / 1_000_000_000, 6)


def _per_second(value: int | float | None, elapsed_ms: float | None) -> float | None:
    if value is None or elapsed_ms is None or elapsed_ms <= 0:
        return None
    return round(float(value) / (elapsed_ms / 1000.0), 6)


def _build_streaming_probe_payload(
    *,
    chunk_index: int,
    stage: str,
    event_count: int | None,
    event_type_counts: dict[str, int] | None,
    rss_before_add_mb: float | None,
    rss_after_add_mb: float | None,
    rss_after_run_mb: float | None,
    rss_after_clear_mb: float | None,
    materialize_ms: float | None = None,
    add_ms: float | None = None,
    run_ms: float | None = None,
    clear_ms: float | None = None,
    chunk_wall_ms: float | None = None,
    chunk_start_time: str | None = None,
    chunk_end_time: str | None = None,
    chunk_span_seconds: float | None = None,
    events_per_second: float | None = None,
    simulated_seconds_per_wall_second: float | None = None,
    file_touch_summary: dict | None = None,
) -> dict:
    payload = {
        "chunk_index": chunk_index,
        "stage": stage,
        "event_count": event_count,
        "event_type_counts": event_type_counts,
        "rss_before_add_mb": rss_before_add_mb,
        "rss_after_add_mb": rss_after_add_mb,
        "rss_after_run_mb": rss_after_run_mb,
        "rss_after_clear_mb": rss_after_clear_mb,
        "materialize_ms": materialize_ms,
        "add_ms": add_ms,
        "run_ms": run_ms,
        "clear_ms": clear_ms,
        "chunk_wall_ms": chunk_wall_ms,
        "chunk_start_time": chunk_start_time,
        "chunk_end_time": chunk_end_time,
        "chunk_span_seconds": chunk_span_seconds,
        "events_per_second": events_per_second,
        "simulated_seconds_per_wall_second": simulated_seconds_per_wall_second,
        "rss_delta_add_mb": _rss_delta_mb(rss_after_add_mb, rss_before_add_mb),
        "rss_delta_run_mb": _rss_delta_mb(rss_after_run_mb, rss_after_add_mb),
        "rss_delta_clear_mb": _rss_delta_mb(rss_after_clear_mb, rss_after_run_mb),
        "rss_delta_chunk_mb": _rss_delta_mb(rss_after_clear_mb, rss_before_add_mb),
        "file_touch_summary": file_touch_summary,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    return payload


def _build_node_probe_payload(
    *,
    stage: str,
    run_config_id: str | None,
    chunk_size: int | None,
    rss_mb: float | None,
) -> dict:
    return {
        "stage": stage,
        "run_config_id": run_config_id,
        "chunk_size": chunk_size,
        "rss_mb": rss_mb,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _build_prepare_probe_payload(
    *,
    stage: str,
    config_index: int,
    config_count: int,
    data_type: str,
    instrument_id: str | None,
    identifier_count: int,
    identifier_sample: list[str],
    optimize_file_loading: bool,
    cache_hit: bool | None,
    file_list_count: int | None,
    filter_file_count: int | None,
    file_list_ms: float | None,
    filter_files_ms: float | None,
    register_session_ms: float | None,
    total_ms: float | None,
    rss_mb: float | None,
) -> dict:
    return {
        "stage": stage,
        "config_index": config_index,
        "config_count": config_count,
        "data_type": data_type,
        "instrument_id": instrument_id,
        "identifier_count": identifier_count,
        "identifier_sample": identifier_sample,
        "optimize_file_loading": optimize_file_loading,
        "cache_hit": cache_hit,
        "file_list_count": file_list_count,
        "filter_file_count": filter_file_count,
        "file_list_ms": file_list_ms,
        "filter_files_ms": filter_files_ms,
        "register_session_ms": register_session_ms,
        "total_ms": total_ms,
        "rss_mb": rss_mb,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _build_prefetch_probe_payload(
    *,
    stage: str,
    backend: str,
    ahead_hours: int,
    max_files_per_batch: int,
    updated_at: str | None,
    cursor_time: str | None,
    window_end_time: str | None,
    pending_files: int | None,
    prefetched_files: int | None,
    requested_files_total: int | None,
    completed_files_total: int | None,
    last_batch_files: int | None,
    last_batch_bytes: int | None,
    last_error: str | None,
    disable_reason: str | None,
) -> dict:
    return {
        "stage": stage,
        "backend": backend,
        "ahead_hours": ahead_hours,
        "max_files_per_batch": max_files_per_batch,
        "cursor_time": cursor_time,
        "window_end_time": window_end_time,
        "pending_files": pending_files,
        "prefetched_files": prefetched_files,
        "requested_files_total": requested_files_total,
        "completed_files_total": completed_files_total,
        "last_batch_files": last_batch_files,
        "last_batch_bytes": last_batch_bytes,
        "last_error": last_error,
        "disable_reason": disable_reason,
        "updated_at": updated_at or datetime.now(timezone.utc).isoformat(),
    }


def _start_progress_heartbeat(
    *,
    status_path: Path,
    status: dict,
    status_lock: threading.Lock,
    stdout_path: Path | None,
) -> tuple[threading.Event, threading.Thread]:
    stop_event = threading.Event()

    def _tick() -> None:
        while not stop_event.wait(PROGRESS_UPDATE_SECONDS):
            _write_status_snapshot(
                status_path=status_path,
                status=status,
                status_lock=status_lock,
                stdout_path=stdout_path,
            )

    thread = threading.Thread(
        target=_tick,
        name="backtest-progress-heartbeat",
        daemon=True,
    )
    thread.start()
    return stop_event, thread


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
    strategy_file_path, bundle_path = _validate_run_spec(run_spec, run_spec_path)

    backtest_id = run_spec["backtest_id"]
    log_root = REPORTS_OUTPUT_ROOT
    log_dir = Path(log_root) / backtest_id
    status_path = log_dir / "status.json"
    reports_dir = log_dir

    started_at = datetime.now(timezone.utc).isoformat()
    status = _build_status_payload(backtest_id, run_spec)
    status.update({"status": "running", "started_at": started_at})
    status_lock = threading.Lock()
    stdout_path_raw = os.environ.get("BACKTEST_STDOUT_PATH")
    stdout_path = Path(stdout_path_raw).expanduser() if stdout_path_raw else None
    streaming_summary = {
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

    def _on_streaming_probe(
        *,
        chunk_index: int,
        stage: str,
        event_count: int | None = None,
        event_type_counts: dict[str, int] | None = None,
        rss_before_add_mb: float | None = None,
        rss_after_add_mb: float | None = None,
        rss_after_run_mb: float | None = None,
        rss_after_clear_mb: float | None = None,
        materialize_ms: float | None = None,
        add_ms: float | None = None,
        run_ms: float | None = None,
        clear_ms: float | None = None,
        chunk_wall_ms: float | None = None,
        chunk_start_time: str | None = None,
        chunk_end_time: str | None = None,
        chunk_span_seconds: float | None = None,
        events_per_second: float | None = None,
        simulated_seconds_per_wall_second: float | None = None,
        file_touch_summary: dict | None = None,
    ) -> None:
        if stage == "after_materialize" and event_count is not None:
            streaming_summary["chunks_seen"] = chunk_index + 1
            streaming_summary["events_seen"] += event_count
        if isinstance(file_touch_summary, dict):
            streaming_summary["file_touch_files_total"] = file_touch_summary.get("files_total")
            streaming_summary["file_touch_bytes_total"] = file_touch_summary.get("bytes_total")
            streaming_summary["file_touch_files_touched"] = file_touch_summary.get(
                "files_touched", 0
            )
            streaming_summary["file_touch_bytes_touched"] = file_touch_summary.get(
                "bytes_touched", 0
            )
            streaming_summary["file_touch_new_files_last_advance"] = file_touch_summary.get(
                "new_files_touched", 0
            )
            streaming_summary["file_touch_new_bytes_last_advance"] = file_touch_summary.get(
                "new_bytes_touched", 0
            )
            streaming_summary["file_touch_remaining_files"] = file_touch_summary.get(
                "remaining_files"
            )
            streaming_summary["file_touch_remaining_bytes"] = file_touch_summary.get(
                "remaining_bytes"
            )
            if int(file_touch_summary.get("new_files_touched", 0) or 0) > 0:
                streaming_summary["file_touch_progress_samples_with_new_files"] += 1
        _write_status_snapshot(
            status_path=status_path,
            status=status,
            status_lock=status_lock,
            stdout_path=stdout_path,
            updates={
                "streaming_probe": _build_streaming_probe_payload(
                    chunk_index=chunk_index,
                    stage=stage,
                    event_count=event_count,
                    event_type_counts=event_type_counts,
                    rss_before_add_mb=rss_before_add_mb,
                    rss_after_add_mb=rss_after_add_mb,
                    rss_after_run_mb=rss_after_run_mb,
                    rss_after_clear_mb=rss_after_clear_mb,
                    materialize_ms=materialize_ms,
                    add_ms=add_ms,
                    run_ms=run_ms,
                    clear_ms=clear_ms,
                    chunk_wall_ms=chunk_wall_ms,
                    chunk_start_time=chunk_start_time,
                    chunk_end_time=chunk_end_time,
                    chunk_span_seconds=chunk_span_seconds,
                    events_per_second=events_per_second,
                    simulated_seconds_per_wall_second=simulated_seconds_per_wall_second,
                    file_touch_summary=file_touch_summary,
                ),
                "streaming_summary": dict(streaming_summary),
            },
        )

    def _on_prepare_probe(
        *,
        stage: str,
        config_index: int,
        config_count: int,
        data_type: str,
        instrument_id: str | None,
        identifier_count: int,
        identifier_sample: list[str],
        optimize_file_loading: bool,
        cache_hit: bool | None = None,
        file_list_count: int | None = None,
        filter_file_count: int | None = None,
        file_list_ms: float | None = None,
        filter_files_ms: float | None = None,
        register_session_ms: float | None = None,
        total_ms: float | None = None,
        rss_mb: float | None = None,
    ) -> None:
        _write_status_snapshot(
            status_path=status_path,
            status=status,
            status_lock=status_lock,
            stdout_path=stdout_path,
            updates={
                "prepare_probe": _build_prepare_probe_payload(
                    stage=stage,
                    config_index=config_index,
                    config_count=config_count,
                    data_type=data_type,
                    instrument_id=instrument_id,
                    identifier_count=identifier_count,
                    identifier_sample=identifier_sample,
                    optimize_file_loading=optimize_file_loading,
                    cache_hit=cache_hit,
                    file_list_count=file_list_count,
                    filter_file_count=filter_file_count,
                    file_list_ms=file_list_ms,
                    filter_files_ms=filter_files_ms,
                    register_session_ms=register_session_ms,
                    total_ms=total_ms,
                    rss_mb=rss_mb,
                ),
            },
        )

    def _on_node_probe(
        *,
        stage: str,
        run_config_id: str | None = None,
        chunk_size: int | None = None,
        rss_mb: float | None = None,
    ) -> None:
        _write_status_snapshot(
            status_path=status_path,
            status=status,
            status_lock=status_lock,
            stdout_path=stdout_path,
            updates={
                "node_probe": _build_node_probe_payload(
                    stage=stage,
                    run_config_id=run_config_id,
                    chunk_size=chunk_size,
                    rss_mb=rss_mb,
                ),
            },
        )

    def _on_prefetch_probe(
        *,
        stage: str,
        backend: str,
        ahead_hours: int,
        max_files_per_batch: int,
        updated_at: str | None = None,
        cursor_time: str | None = None,
        window_end_time: str | None = None,
        pending_files: int | None = None,
        prefetched_files: int | None = None,
        requested_files_total: int | None = None,
        completed_files_total: int | None = None,
        last_batch_files: int | None = None,
        last_batch_bytes: int | None = None,
        last_error: str | None = None,
        disable_reason: str | None = None,
    ) -> None:
        _write_status_snapshot(
            status_path=status_path,
            status=status,
            status_lock=status_lock,
            stdout_path=stdout_path,
            updates={
                "prefetch_probe": _build_prefetch_probe_payload(
                    stage=stage,
                    backend=backend,
                    ahead_hours=ahead_hours,
                    max_files_per_batch=max_files_per_batch,
                    updated_at=updated_at,
                    cursor_time=cursor_time,
                    window_end_time=window_end_time,
                    pending_files=pending_files,
                    prefetched_files=prefetched_files,
                    requested_files_total=requested_files_total,
                    completed_files_total=completed_files_total,
                    last_batch_files=last_batch_files,
                    last_batch_bytes=last_batch_bytes,
                    last_error=last_error,
                    disable_reason=disable_reason,
                ),
            },
        )

    _write_status_snapshot(
        status_path=status_path,
        status=status,
        status_lock=status_lock,
        stdout_path=stdout_path,
    )
    heartbeat_stop: threading.Event | None = None
    heartbeat_thread: threading.Thread | None = None

    try:
        heartbeat_stop, heartbeat_thread = _start_progress_heartbeat(
            status_path=status_path,
            status=status,
            status_lock=status_lock,
            stdout_path=stdout_path,
        )

        _write_status_snapshot(
            status_path=status_path,
            status=status,
            status_lock=status_lock,
            stdout_path=stdout_path,
            updates={"init_step": "seed_rng"},
        )
        random.seed(run_spec["seed"])
        if bundle_path is not None:
            _write_status_snapshot(
                status_path=status_path,
                status=status,
                status_lock=status_lock,
                stdout_path=stdout_path,
                updates={"init_step": "prepare_strategy_bundle"},
            )
            bundle_root = _extract_strategy_bundle(bundle_path, run_spec_path.parent / "strategy_bundle")
            _prepare_bundle_import(bundle_root)
            _ensure_strategy_importable(run_spec["strategy_entry"])
            _ensure_strategy_importable(run_spec["strategy_config_path"])
            strategy_entry = run_spec["strategy_entry"]
            strategy_config_path = run_spec["strategy_config_path"]
        else:
            assert strategy_file_path is not None
            _write_status_snapshot(
                status_path=status_path,
                status=status,
                status_lock=status_lock,
                stdout_path=stdout_path,
                updates={"init_step": "load_strategy_module"},
            )
            module_name = _load_strategy_module(strategy_file_path)
            strategy_entry = _rewrite_import_path(run_spec["strategy_entry"], module_name)
            strategy_config_path = _rewrite_import_path(run_spec["strategy_config_path"], module_name)

        _write_status_snapshot(
            status_path=status_path,
            status=status,
            status_lock=status_lock,
            stdout_path=stdout_path,
            updates={"init_step": "parse_run_spec_fields"},
        )
        spot_symbols, futures_symbols = _parse_symbols(run_spec["symbols"])
        margin_init = _parse_decimal("margin_init", run_spec["margin_init"])
        margin_maint = _parse_decimal("margin_maint", run_spec["margin_maint"])
        spot_maker_fee = _parse_decimal("spot_maker_fee", run_spec["spot_maker_fee"])
        spot_taker_fee = _parse_decimal("spot_taker_fee", run_spec["spot_taker_fee"])
        futures_maker_fee = _parse_decimal("futures_maker_fee", run_spec["futures_maker_fee"])
        futures_taker_fee = _parse_decimal("futures_taker_fee", run_spec["futures_taker_fee"])
        catalog_controls = _catalog_controls(run_spec)
        # S3 catalog support (B2 or compatible S3 endpoint)
        _catalog_cfg = build_catalog_config()
        catalog_path_str = _catalog_cfg["catalog_path"]
        catalog_fs_protocol = _catalog_cfg["catalog_fs_protocol"]
        catalog_fs_storage_options = _catalog_cfg["catalog_fs_storage_options"]
        catalog_fs_rust_storage_options = _catalog_cfg["catalog_fs_rust_storage_options"]

        if catalog_controls.prewarm_before_run:
            if catalog_fs_protocol is not None:
                raise ValueError(
                    "catalog_controls.prewarm_before_run requires a worker-local catalog path"
                )
            catalog_root = Path(catalog_path_str)
            manifest_path = log_dir / "catalog-prewarm-manifest.txt"
            _write_status_snapshot(
                status_path=status_path,
                status=status,
                status_lock=status_lock,
                stdout_path=stdout_path,
                updates={"init_step": "catalog_prewarm"},
            )
            prewarm_result = build_manifest(run_spec=run_spec, catalog_root=catalog_root)
            if not prewarm_result.paths:
                raise ValueError("No catalog files matched the supplied run_spec for prewarm.")
            write_manifest(prewarm_result.paths, manifest_path)
            prewarm_summary = {
                "catalog_root": catalog_root.as_posix(),
                "manifest_path": manifest_path.as_posix(),
                "file_count": len(prewarm_result.paths),
                "category_counts": prewarm_result.category_counts,
                "missing_dir_count": len(prewarm_result.missing_dirs),
                "missing_dir_sample": prewarm_result.missing_dirs[:10],
                "threads": catalog_controls.prewarm_threads,
            }
            _write_status_snapshot(
                status_path=status_path,
                status=status,
                status_lock=status_lock,
                stdout_path=stdout_path,
                updates={"catalog_prewarm": prewarm_summary},
            )
            warmup_rc = run_warmup(
                manifest_path=manifest_path,
                threads=catalog_controls.prewarm_threads,
                background=False,
            )
            if warmup_rc != 0:
                raise RuntimeError(f"catalog prewarm failed with exit code {warmup_rc}")

        _write_status_snapshot(
            status_path=status_path,
            status=status,
            status_lock=status_lock,
            stdout_path=stdout_path,
            updates={"init_step": "open_catalog"},
        )
        data_catalog = ParquetDataCatalog(
            catalog_path_str,
            fs_protocol=catalog_fs_protocol,
            fs_storage_options=catalog_fs_storage_options,
            fs_rust_storage_options=catalog_fs_rust_storage_options,
        )

        spot_instruments: list[CurrencyPair] = []
        futures_instruments: list[CryptoPerpetual] = []
        quote_code: str | None = None

        _write_status_snapshot(
            status_path=status_path,
            status=status,
            status_lock=status_lock,
            stdout_path=stdout_path,
            updates={"init_step": "load_spot_instruments"},
        )
        spot_instrument_ids = [
            InstrumentId(Symbol(data_symbol), BINANCE_SPOT_VENUE)
            for data_symbol in spot_symbols
        ]
        spot_catalog_instruments = _catalog_instruments_by_id(
            data_catalog,
            spot_instrument_ids,
        )
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
                preloaded_instruments=spot_catalog_instruments,
            )
            spot_instruments.append(spot_instrument)

        _write_status_snapshot(
            status_path=status_path,
            status=status,
            status_lock=status_lock,
            stdout_path=stdout_path,
            updates={"init_step": "load_futures_instruments"},
        )
        futures_instrument_ids = [
            InstrumentId(Symbol(f"{base_symbol}-PERP"), BINANCE_FUTURES_VENUE)
            for base_symbol in futures_symbols
        ]
        futures_catalog_instruments = _catalog_instruments_by_id(
            data_catalog,
            futures_instrument_ids,
        )
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
                preloaded_instruments=futures_catalog_instruments,
            )
            futures_instruments.append(futures_instrument)

        load_trade_ticks = _load_trade_ticks_enabled(run_spec)
        if catalog_fs_protocol is None:
            catalog_root = Path(catalog_path_str)
            _write_status_snapshot(
                status_path=status_path,
                status=status,
                status_lock=status_lock,
                stdout_path=stdout_path,
                updates={"init_step": "migrate_legacy_catalog"},
            )
            _migrate_legacy_catalog_data(
                catalog=data_catalog,
                catalog_root=catalog_root,
                spot_instrument_ids=[instrument.id for instrument in spot_instruments],
                futures_instrument_ids=[instrument.id for instrument in futures_instruments],
                load_trade_ticks=load_trade_ticks,
            )

        _write_status_snapshot(
            status_path=status_path,
            status=status,
            status_lock=status_lock,
            stdout_path=stdout_path,
            updates={"init_step": "build_data_configs"},
        )
        data_configs: list[BacktestDataConfig] = []
        for instrument in spot_instruments:
            for data_cls in _market_data_classes(
                include_futures_extras=False,
                load_trade_ticks=load_trade_ticks,
            ):
                optimize_file_loading_for_cls = _optimize_file_loading_for_data_cls(
                    run_spec,
                    data_cls,
                )
                data_configs.append(
                    BacktestDataConfig(
                        catalog_path=catalog_path_str,
                        catalog_fs_protocol=catalog_fs_protocol,
                        catalog_fs_storage_options=catalog_fs_storage_options,
                        catalog_fs_rust_storage_options=catalog_fs_rust_storage_options,
                        data_cls=data_cls,
                        instrument_id=instrument.id,
                        optimize_file_loading=optimize_file_loading_for_cls,
                    )
                )

        for instrument in futures_instruments:
            for data_cls in _market_data_classes(
                include_futures_extras=True,
                load_trade_ticks=load_trade_ticks,
            ):
                optimize_file_loading_for_cls = _optimize_file_loading_for_data_cls(
                    run_spec,
                    data_cls,
                )
                data_configs.append(
                    BacktestDataConfig(
                        catalog_path=catalog_path_str,
                        catalog_fs_protocol=catalog_fs_protocol,
                        catalog_fs_storage_options=catalog_fs_storage_options,
                        catalog_fs_rust_storage_options=catalog_fs_rust_storage_options,
                        data_cls=data_cls,
                        instrument_id=instrument.id,
                        optimize_file_loading=optimize_file_loading_for_cls,
                    )
                )

        _write_status_snapshot(
            status_path=status_path,
            status=status,
            status_lock=status_lock,
            stdout_path=stdout_path,
            updates={"init_step": "build_engine_config"},
        )
        latency_model_config = ImportableLatencyModelConfig(
            latency_model_path="quant_trade_v1.backtest.models:LatencyModel",
            config_path="quant_trade_v1.backtest.config:LatencyModelConfig",
            config=_parse_latency_config(run_spec.get("latency_config")),
        )

        starting_balances_spot = _parse_starting_balances(
            "starting_balances_spot", run_spec.get("starting_balances_spot")
        )
        starting_balances_futures = _parse_starting_balances(
            "starting_balances_futures", run_spec.get("starting_balances_futures")
        )

        strategy_config = dict(run_spec["strategy_config"])
        book_type = DEFAULT_BOOK_TYPE

        liquidity_consumption = run_spec.get("liquidity_consumption", False)
        trade_execution = run_spec.get("trade_execution", False)

        fill_model_config = None
        raw_fill_model = run_spec.get("fill_model_config")
        if raw_fill_model is not None:
            fill_model_config = ImportableFillModelConfig(
                fill_model_path=raw_fill_model["fill_model_path"],
                config_path=raw_fill_model["config_path"],
                config=raw_fill_model["config"],
            )

        venues_configs: list[BacktestVenueConfig] = []
        if spot_instruments:
            venues_configs.append(
                BacktestVenueConfig(
                    name=BINANCE_SPOT_VENUE.value,
                    oms_type="NETTING",
                    account_type="CASH",
                    base_currency=None,
                    starting_balances=starting_balances_spot,
                    book_type=book_type,
                    latency_model=latency_model_config,
                    fill_model=fill_model_config,
                    liquidity_consumption=liquidity_consumption,
                    trade_execution=trade_execution,
                )
            )
        if futures_instruments:
            venues_configs.append(
                BacktestVenueConfig(
                    name=BINANCE_FUTURES_VENUE.value,
                    oms_type="NETTING",
                    account_type="MARGIN",
                    base_currency=None,
                    starting_balances=starting_balances_futures,
                    book_type=book_type,
                    margin_model=MarginModelConfig(model_type="leveraged"),
                    latency_model=latency_model_config,
                    fill_model=fill_model_config,
                    liquidity_consumption=liquidity_consumption,
                    trade_execution=trade_execution,
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

        _write_status_snapshot(
            status_path=status_path,
            status=status,
            status_lock=status_lock,
            stdout_path=stdout_path,
            updates={"init_step": "create_backtest_node"},
        )
        node = _InstrumentOverrideBacktestNode(
            configs=[run_config],
            instruments=spot_instruments + futures_instruments,
            run_spec=run_spec,
            node_probe_callback=_on_node_probe,
            streaming_probe_callback=_on_streaming_probe,
            prepare_probe_callback=_on_prepare_probe,
            prefetch_probe_callback=_on_prefetch_probe,
        )
        _write_status_snapshot(
            status_path=status_path,
            status=status,
            status_lock=status_lock,
            stdout_path=stdout_path,
            updates={"phase": "engine_running"},
            clear_keys=("init_step",),
        )
        node.run()
        if heartbeat_stop is not None:
            heartbeat_stop.set()
        if heartbeat_thread is not None:
            heartbeat_thread.join(timeout=1.0)

        engine = node.get_engine(run_config.id)
        if engine is None:
            raise RuntimeError("Backtest engine not found for run config.")

        _write_status_snapshot(
            status_path=status_path,
            status=status,
            status_lock=status_lock,
            stdout_path=stdout_path,
            updates={"phase": "exporting_reports"},
        )

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
                "phase": "completed",
                "finished_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        _write_status_snapshot(
            status_path=status_path,
            status=status,
            status_lock=status_lock,
            stdout_path=stdout_path,
            clear_keys=("init_step",),
        )
        return 0
    except Exception as exc:
        if heartbeat_stop is not None:
            heartbeat_stop.set()
        if heartbeat_thread is not None:
            heartbeat_thread.join(timeout=1.0)
        tb = traceback.format_exc()
        status.update(
            {
                "status": "failed",
                "phase": "failed",
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "error": str(exc),
                "traceback": tb,
            }
        )
        _write_status_snapshot(
            status_path=status_path,
            status=status,
            status_lock=status_lock,
            stdout_path=stdout_path,
            clear_keys=("init_step",),
        )
        print("\n" + "=" * 70, file=sys.stderr)
        print("[ERROR] Backtest failed", file=sys.stderr)
        print(f"Error: {exc}", file=sys.stderr)
        print("Traceback:", file=sys.stderr)
        print(tb, file=sys.stderr)
        print("=" * 70 + "\n", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
