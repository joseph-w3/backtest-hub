from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import secrets
import uuid
import threading
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
from typing import Any
from urllib.parse import urlparse, urlunparse

import aiohttp
from fastapi import Body, FastAPI, File, Header, HTTPException, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse, Response

from services.report_service import (
    build_report_router,
    ReportService,
    ReportServiceConfig,
)

from services.scheduler import (
    parse_backtest_api_bases,
    parse_per_worker_max_running,
    required_memory_gb_from_run_spec,
    select_backtest_docker,
)
from services.run_store_sqlite import SqliteRunStore

app = FastAPI()
report_service: "ReportService | None" = None

REPORT_SERVICE_TAG = "report_service"
REPORT_SERVICE_DOCS_URL = "/report-service/docs"
REPORT_SERVICE_OPENAPI_URL = "/report-service/openapi.json"


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("backtest_hub")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.propagate = False
    return logger


logger = setup_logging()


async def _recover_submitted_tasks() -> None:
    """Continuously poll workers for orphan 'submitted' tasks and update status.

    Runs as a background task via asyncio.create_task.  Each iteration checks
    all submitted tasks against their workers, then sleeps for a configurable
    interval (SUBMITTED_RECOVERY_INTERVAL_SECONDS, default 30s).
    """
    while True:
        try:
            store = get_run_store()
            submitted_runs = store.get_runs_by_status("submitted")
            if submitted_runs:
                logger.info("startup_recovery_start count=%d", len(submitted_runs))
                for run in submitted_runs:
                    base_url = run.get("backtest_api_base")
                    if not base_url:
                        continue
                    bid = run["backtest_id"]
                    try:
                        status_resp = await asyncio.to_thread(fetch_backtest_status, base_url, bid)
                        worker_status = status_resp.get("status")
                        _TERMINAL_STATUSES = {"completed", "succeeded", "failed", "terminated", "stopped"}
                        if worker_status in _TERMINAL_STATUSES:
                            if worker_status in ("terminated", "stopped"):
                                final_status = "failed"
                            elif worker_status == "succeeded":
                                final_status = "completed"
                            else:
                                final_status = worker_status
                            store.upsert_run(bid, {"status": final_status})
                            logger.info("recovered_orphan_task id=%s worker_status=%s final_status=%s", bid, worker_status, final_status)
                    except HTTPException as exc:
                        if exc.status_code == 404:
                            store.upsert_run(bid, {
                                "status": "failed",
                                "last_error": "task_not_found_on_worker",
                            })
                            logger.warning(
                                "orphan_task_not_found id=%s worker=%s", bid, base_url,
                            )
                        else:
                            logger.warning("orphan_task_unreachable id=%s worker=%s", bid, base_url)
                    except Exception:
                        logger.warning("orphan_task_unreachable id=%s worker=%s", bid, base_url)
        except Exception as exc:
            logger.exception("startup_recovery_failed error=%s", exc)

        interval = float(os.environ.get("SUBMITTED_RECOVERY_INTERVAL_SECONDS", "30"))
        await asyncio.sleep(interval)


@app.on_event("startup")
async def on_startup() -> None:
    logger.info("app_started")
    logger.info(
        "run_store_startup_check db_path=%s legacy_mapping_path=%s",
        HUB_DB_PATH,
        RUN_MAPPING_PATH,
    )
    # Initialize SQLite store early (and migrate legacy JSON mapping if needed).
    try:
        maybe_migrate_run_mapping_json()
    except Exception as exc:
        logger.exception("run_store_init_or_migrate_failed error=%s", exc)
    global report_service
    report_service = ReportService(
        ReportServiceConfig(
            report_batch_path=BACKTEST_REPORT_BATCH_PATH,
        ),
        backtest_headers=backtest_headers,
    )
    # Recover orphan submitted tasks in background (non-blocking).
    asyncio.create_task(_recover_submitted_tasks())
    # Start background scheduler for queued submissions.
    asyncio.create_task(queue_scheduler())


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = perf_counter()
    request_id = request.headers.get("X-Request-ID") or request.headers.get("X-Request-Id")
    client_host = request.client.host if request.client else "-"
    path = request.url.path
    if request.url.query:
        path = f"{path}?{request.url.query}"
    try:
        response = await call_next(request)
    except Exception:
        duration_ms = (perf_counter() - start) * 1000
        logger.exception(
            "request_failed method=%s path=%s client=%s duration_ms=%.2f request_id=%s",
            request.method,
            path,
            client_host,
            duration_ms,
            request_id or "-",
        )
        raise
    duration_ms = (perf_counter() - start) * 1000
    logger.info(
        "request_completed method=%s path=%s status=%s client=%s duration_ms=%.2f request_id=%s",
        request.method,
        path,
        response.status_code,
        client_host,
        duration_ms,
        request_id or "-",
    )
    return response


def env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_csv(value: str) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


BASE_DIR = Path(__file__).resolve().parent
DATA_MOUNT_PATH = Path(env_or_default("DATA_MOUNT_PATH", "/opt/backtest"))
RUN_STORAGE_PATH = Path(env_or_default("RUN_STORAGE_PATH", str(DATA_MOUNT_PATH / "runs")))
# Legacy (migration-only). New mapping storage is SQLite at HUB_DB_PATH.
RUN_MAPPING_PATH = Path(env_or_default("RUN_MAPPING_PATH", str(DATA_MOUNT_PATH / "run_mapping.json")))
HUB_DB_PATH = Path(env_or_default("HUB_DB_PATH", str(DATA_MOUNT_PATH / "hub.sqlite3")))
REPORT_CACHE_PATH = Path(env_or_default("REPORT_CACHE_PATH", str(DATA_MOUNT_PATH / "report_cache.sqlite3")))
HOST_API_KEY = os.getenv("HOST_API_KEY", "")

BACKTEST_API_BASES = parse_backtest_api_bases(os.getenv("BACKTEST_API_BASES"), "")
if not BACKTEST_API_BASES:
    raise RuntimeError("BACKTEST_API_BASES is required")
BACKTEST_BRONZE_API_BASES = parse_backtest_api_bases(os.getenv("BACKTEST_BRONZE_API_BASES"), "")
BACKTEST_API_KEY = os.getenv("BACKTEST_API_KEY", "")
BACKTEST_SUBMIT_PATH = env_or_default("BACKTEST_SUBMIT_PATH", "/v1/scripts/run_backtest")
BACKTEST_RUNS_PATH = env_or_default("BACKTEST_RUNS_PATH", "/v1/runs")
BACKTEST_LOGS_DOWNLOAD_PATH = "/v1/runs/backtest/{backtest_id}/logs/download"
BACKTEST_CSV_DOWNLOAD_PATH = "/v1/runs/backtest/{backtest_id}/download_csv"
BACKTEST_DATA_DOWNLOAD_PATH = env_or_default(
    "BACKTEST_DATA_DOWNLOAD_PATH",
    "/v1/runs/backtest/{backtest_id}/download_data",
)
BACKTEST_KILL_PATH = "/v1/runs/backtest/{backtest_id}/kill"
BACKTEST_REPORT_PATH = env_or_default("BACKTEST_REPORT_PATH", "/v1/runs/backtest/{backtest_id}/report")
BACKTEST_REPORT_BATCH_PATH = env_or_default(
    "BACKTEST_REPORT_BATCH_PATH",
    "/v1/runs/backtest/reports/batch",
)
BACKTEST_STATUS_PATH = env_or_default("BACKTEST_STATUS_PATH", "/v1/runs/backtest/{backtest_id}")
BACKTEST_WS_LOGS_PATH = env_or_default(
    "BACKTEST_WS_LOGS_PATH",
    "/v1/runs/backtest/{backtest_id}/logs/stream",
)
BACKTEST_METRICS_PATH = env_or_default("BACKTEST_METRICS_PATH", "v1/system/metrics")
BACKTEST_METRICS_TIMEOUT_SECONDS = float(os.getenv("BACKTEST_METRICS_TIMEOUT_SECONDS", "3"))

RUNNER_PATH = Path(env_or_default("BACKTEST_RUNNER_PATH", str(BASE_DIR / "scripts" / "run_backtest.py")))

MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "150"))
MAX_RANGE_DAYS = int(os.getenv("MAX_RANGE_DAYS", "90"))
MAX_RUNNING_BACKTESTS = int(os.getenv("MAX_RUNNING_BACKTESTS", "10"))
# Per-worker max running override. Format: "http://host1:port=5,http://host2:port=3"
# Workers not listed fall back to MAX_RUNNING_BACKTESTS.
PER_WORKER_MAX_RUNNING = parse_per_worker_max_running(os.getenv("MAX_RUNNING_PER_WORKER", ""))
QUEUE_POLL_INTERVAL_SECONDS = float(os.getenv("QUEUE_POLL_INTERVAL_SECONDS", "3"))
QUEUE_DISPATCH_DELAY_SECONDS = float(os.getenv("QUEUE_DISPATCH_DELAY_SECONDS", "30"))
# Backfilling: only backfill tasks queued within this window after head task (seconds)
BACKFILL_WINDOW_SECONDS = float(os.getenv("BACKFILL_WINDOW_SECONDS", "300"))
# Backfilling: stop backfilling when head task has waited longer than this (seconds)
BACKFILL_RESERVE_THRESHOLD_SECONDS = float(os.getenv("BACKFILL_RESERVE_THRESHOLD_SECONDS", "600"))
QUEUE_PATH = Path(env_or_default("QUEUE_PATH", str(DATA_MOUNT_PATH / "submit_queue.json")))
MAX_REPORT_PAGE_SIZE = int(os.getenv("MAX_REPORT_PAGE_SIZE", "50"))

MEMORY_PER_SYMBOL_GB = float(os.getenv("MEMORY_PER_SYMBOL_GB", "1.0"))
BRONZE_SYMBOLS_THRESHOLD = 6
CPU_PERCENT_LT = 80.0

CORS_ALLOW_ORIGINS = parse_csv(env_or_default("CORS_ALLOW_ORIGINS", "*"))
CORS_ALLOW_METHODS = parse_csv(env_or_default("CORS_ALLOW_METHODS", "*"))
CORS_ALLOW_HEADERS = parse_csv(env_or_default("CORS_ALLOW_HEADERS", "*"))
CORS_ALLOW_CREDENTIALS = parse_bool(os.getenv("CORS_ALLOW_CREDENTIALS"), False)
CORS_ALLOW_ORIGIN_REGEX = os.getenv("CORS_ALLOW_ORIGIN_REGEX") or None
CORS_MAX_AGE = int(os.getenv("CORS_MAX_AGE", "600"))

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS,
    allow_methods=CORS_ALLOW_METHODS,
    allow_headers=CORS_ALLOW_HEADERS,
    allow_credentials=CORS_ALLOW_CREDENTIALS,
    allow_origin_regex=CORS_ALLOW_ORIGIN_REGEX,
    max_age=CORS_MAX_AGE,
)

DEFAULT_LATENCY_CONFIG: dict[str, int] = {
    "base_latency_nanos": 20_000_000,
    "insert_latency_nanos": 2_000_000,
    "update_latency_nanos": 3_000_000,
    "cancel_latency_nanos": 1_000_000,
}
LATENCY_CONFIG_KEYS = set(DEFAULT_LATENCY_CONFIG.keys())

REQUIRED_FIELDS = {
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
    "fill_model_config",
}

ALLOWED_FIELDS = REQUIRED_FIELDS | OPTIONAL_FIELDS

RUN_STORE_INIT_LOCK = threading.Lock()
RUN_STORE: SqliteRunStore | None = None

# Protect queue + inflight submissions to avoid oversubmitting to backtest docker.
QUEUE_STATE_LOCK = asyncio.Lock()
INFLIGHT_MEMORY_GB: dict[str, float] = {}
INFLIGHT_COUNTS: dict[str, int] = {}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def get_run_store() -> SqliteRunStore:
    global RUN_STORE
    with RUN_STORE_INIT_LOCK:
        if RUN_STORE is None or RUN_STORE.db_path != HUB_DB_PATH:
            RUN_STORE = SqliteRunStore(HUB_DB_PATH)
        return RUN_STORE


def maybe_migrate_run_mapping_json() -> None:
    """
    One-time migration:
    - If DB is empty AND legacy run_mapping.json exists, import it into SQLite.
    - Then rename run_mapping.json to run_mapping.json.bak (or .bak.<ts> if exists).
    """
    logger.info(
        "run_store_migrate_check db_path=%s legacy_mapping_path=%s",
        HUB_DB_PATH,
        RUN_MAPPING_PATH,
    )
    store = get_run_store()
    existing_count = store.count_runs()
    if existing_count > 0:
        logger.info(
            "run_store_migrate_skip_db_not_empty db_path=%s existing_rows=%s",
            HUB_DB_PATH,
            existing_count,
        )
        return
    if not RUN_MAPPING_PATH.exists():
        logger.info("run_store_migrate_skip_legacy_missing legacy_mapping_path=%s", RUN_MAPPING_PATH)
        return
    start = perf_counter()
    imported = store.import_from_json_mapping(RUN_MAPPING_PATH)
    duration_ms = (perf_counter() - start) * 1000
    if imported <= 0:
        logger.info(
            "run_store_migrate_noop legacy_mapping_path=%s duration_ms=%.2f",
            RUN_MAPPING_PATH,
            duration_ms,
        )
        return

    bak_path = RUN_MAPPING_PATH.with_name(RUN_MAPPING_PATH.name + ".bak")
    if bak_path.exists():
        bak_path = RUN_MAPPING_PATH.with_name(RUN_MAPPING_PATH.name + f".bak.{utc_now().replace(':', '').replace('-', '')}")
    RUN_MAPPING_PATH.rename(bak_path)
    logger.info(
        "run_store_migrate_ok db_path=%s imported_rows=%s duration_ms=%.2f legacy_bak_path=%s",
        HUB_DB_PATH,
        imported,
        duration_ms,
        bak_path,
    )


def reserve_inflight(base_url: str, required_memory_gb: float) -> None:
    INFLIGHT_MEMORY_GB[base_url] = INFLIGHT_MEMORY_GB.get(base_url, 0.0) + required_memory_gb
    INFLIGHT_COUNTS[base_url] = INFLIGHT_COUNTS.get(base_url, 0) + 1


def release_inflight(base_url: str, required_memory_gb: float) -> None:
    if base_url in INFLIGHT_MEMORY_GB:
        INFLIGHT_MEMORY_GB[base_url] = max(0.0, INFLIGHT_MEMORY_GB[base_url] - required_memory_gb)
        if INFLIGHT_MEMORY_GB[base_url] == 0.0:
            INFLIGHT_MEMORY_GB.pop(base_url, None)
    if base_url in INFLIGHT_COUNTS:
        INFLIGHT_COUNTS[base_url] = max(0, INFLIGHT_COUNTS[base_url] - 1)
        if INFLIGHT_COUNTS[base_url] == 0:
            INFLIGHT_COUNTS.pop(base_url, None)


def parse_iso8601(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def parse_decimal_field(field: str, value: object) -> None:
    if not isinstance(value, (int, float, str)):
        raise ValueError(f"{field} must be a number or string")
    try:
        parsed = float(value)
    except Exception as exc:
        raise ValueError(f"{field} must be a decimal-compatible value") from exc
    if parsed < 0:
        raise ValueError(f"{field} must be >= 0")


def parse_starting_balances(field: str, value: object | None) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field} must be a non-empty list of strings")
    parsed: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{field} must be a non-empty list of strings")
        parsed.append(item.strip())
    return parsed


def make_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = uuid.uuid4().hex
    return f"{ts}_{suffix}"


def validate_run_spec(payload: dict[str, Any]) -> dict[str, Any]:
    unknown_fields = set(payload.keys()) - ALLOWED_FIELDS
    if unknown_fields:
        raise ValueError(f"Unknown fields: {sorted(unknown_fields)}")

    missing = REQUIRED_FIELDS - set(payload.keys())
    if missing:
        raise ValueError(f"Missing required fields: {sorted(missing)}")

    if not isinstance(payload.get("symbols"), list) or not payload["symbols"]:
        raise ValueError("symbols must be a non-empty list")
    if len(payload["symbols"]) > MAX_SYMBOLS:
        raise ValueError("symbols exceeds max size")

    for symbol in payload["symbols"]:
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("symbols must be list of strings")

    strategy_file = payload.get("strategy_file")
    strategy_bundle = payload.get("strategy_bundle")
    has_strategy_file = isinstance(strategy_file, str) and bool(strategy_file.strip())
    has_strategy_bundle = isinstance(strategy_bundle, str) and bool(strategy_bundle.strip())
    if has_strategy_file == has_strategy_bundle:
        raise ValueError("Exactly one of strategy_file or strategy_bundle must be set")
    if strategy_file is not None and not has_strategy_file:
        raise ValueError("strategy_file must be a non-empty string")
    if strategy_bundle is not None and not has_strategy_bundle:
        raise ValueError("strategy_bundle must be a non-empty string")
    if not isinstance(payload.get("strategy_config"), dict):
        raise ValueError("strategy_config must be an object")
    if not isinstance(payload.get("chunk_size"), int) or payload["chunk_size"] <= 0:
        raise ValueError("chunk_size must be a positive integer")
    if not isinstance(payload.get("seed"), int):
        raise ValueError("seed must be an integer")
    if not isinstance(payload.get("tags"), dict):
        raise ValueError("tags must be an object")

    if "latency_config" in payload:
        payload["latency_config"] = parse_latency_config(payload["latency_config"])
    if "starting_balances_spot" in payload:
        payload["starting_balances_spot"] = parse_starting_balances(
            "starting_balances_spot", payload["starting_balances_spot"]
        )
    if "starting_balances_futures" in payload:
        payload["starting_balances_futures"] = parse_starting_balances(
            "starting_balances_futures", payload["starting_balances_futures"]
        )

    if "liquidity_consumption" in payload:
        if not isinstance(payload["liquidity_consumption"], bool):
            raise ValueError("liquidity_consumption must be a boolean")
    if "trade_execution" in payload:
        if not isinstance(payload["trade_execution"], bool):
            raise ValueError("trade_execution must be a boolean")
    if "fill_model_config" in payload:
        payload["fill_model_config"] = parse_fill_model_config(payload["fill_model_config"])

    start_value = payload["start"]
    end_value = payload["end"]
    if isinstance(start_value, (int, float)) and isinstance(end_value, (int, float)):
        if start_value >= end_value:
            raise ValueError("start must be before end")
    elif isinstance(start_value, str) and isinstance(end_value, str):
        start = parse_iso8601(start_value)
        end = parse_iso8601(end_value)
        if start >= end:
            raise ValueError("start must be before end")
        if end - start > timedelta(days=MAX_RANGE_DAYS):
            raise ValueError("time range exceeds limit")
    else:
        raise ValueError("start/end must both be ISO8601 strings or numeric timestamps")

    strategy_entry = payload["strategy_entry"]
    if not isinstance(strategy_entry, str) or not strategy_entry.strip():
        raise ValueError("strategy_entry must be a string")

    strategy_config_path = payload["strategy_config_path"]
    if not isinstance(strategy_config_path, str) or not strategy_config_path.strip():
        raise ValueError("strategy_config_path must be a string")

    parse_decimal_field("margin_init", payload["margin_init"])
    parse_decimal_field("margin_maint", payload["margin_maint"])
    parse_decimal_field("spot_maker_fee", payload["spot_maker_fee"])
    parse_decimal_field("spot_taker_fee", payload["spot_taker_fee"])
    parse_decimal_field("futures_maker_fee", payload["futures_maker_fee"])
    parse_decimal_field("futures_taker_fee", payload["futures_taker_fee"])

    sanitized: dict[str, Any] = {}
    for field in REQUIRED_FIELDS:
        sanitized[field] = payload[field]
    if has_strategy_file:
        sanitized["strategy_file"] = str(strategy_file)
    if has_strategy_bundle:
        sanitized["strategy_bundle"] = str(strategy_bundle)
    if "latency_config" in payload:
        sanitized["latency_config"] = payload["latency_config"]
    if "starting_balances_spot" in payload:
        sanitized["starting_balances_spot"] = payload["starting_balances_spot"]
    if "starting_balances_futures" in payload:
        sanitized["starting_balances_futures"] = payload["starting_balances_futures"]
    if "liquidity_consumption" in payload:
        sanitized["liquidity_consumption"] = payload["liquidity_consumption"]
    if "trade_execution" in payload:
        sanitized["trade_execution"] = payload["trade_execution"]
    if "fill_model_config" in payload:
        sanitized["fill_model_config"] = payload["fill_model_config"]
    return sanitized


def parse_latency_config(value: object) -> dict[str, int]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("latency_config must be an object")
    unknown = set(value.keys()) - LATENCY_CONFIG_KEYS
    if unknown:
        raise ValueError(f"latency_config has unknown keys: {sorted(unknown)}")
    parsed: dict[str, int] = {}
    for key, raw in value.items():
        if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
            raise ValueError(f"latency_config.{key} must be a non-negative integer")
        parsed[str(key)] = raw
    return parsed


FILL_MODEL_CONFIG_REQUIRED_KEYS = {"fill_model_path", "config_path", "config"}
_IMPORT_PATH_RE = re.compile(r"^[\w]+(\.[\w]+)*:[\w]+$")


def parse_fill_model_config(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("fill_model_config must be an object")
    missing = FILL_MODEL_CONFIG_REQUIRED_KEYS - set(value.keys())
    if missing:
        raise ValueError(f"fill_model_config missing required keys: {sorted(missing)}")
    unknown = set(value.keys()) - FILL_MODEL_CONFIG_REQUIRED_KEYS
    if unknown:
        raise ValueError(f"fill_model_config has unknown keys: {sorted(unknown)}")
    if not isinstance(value["fill_model_path"], str) or not value["fill_model_path"].strip():
        raise ValueError("fill_model_config.fill_model_path must be a non-empty string")
    if not isinstance(value["config_path"], str) or not value["config_path"].strip():
        raise ValueError("fill_model_config.config_path must be a non-empty string")
    if not isinstance(value["config"], dict):
        raise ValueError("fill_model_config.config must be an object")
    for field in ("fill_model_path", "config_path"):
        path = value[field].strip()
        if not _IMPORT_PATH_RE.match(path):
            raise ValueError(
                f"fill_model_config.{field} must be 'module.path:ClassName' format"
            )
        if not path.startswith("quant_trade_v1."):
            raise ValueError(
                f"fill_model_config.{field} must start with 'quant_trade_v1.'"
            )
    return {
        "fill_model_path": value["fill_model_path"].strip(),
        "config_path": value["config_path"].strip(),
        "config": value["config"],
    }


def require_api_key(api_key: str | None) -> None:
    if not HOST_API_KEY:
        raise HTTPException(status_code=500, detail="HOST_API_KEY not configured")
    if not api_key or api_key != HOST_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

def update_mapping(backtest_id: str, updates: dict[str, Any]) -> None:
    store = get_run_store()
    store.upsert_run(backtest_id, updates)


def normalize_join_url(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}/" + path.lstrip("/")


def get_report_service() -> ReportService:
    global report_service
    if report_service is None:
        report_service = ReportService(
            ReportServiceConfig(
                report_batch_path=BACKTEST_REPORT_BATCH_PATH,
            ),
            backtest_headers=backtest_headers,
        )
    return report_service


def get_report_service_openapi() -> dict[str, Any]:
    report_routes = [
        route
        for route in app.routes
        if getattr(route, "tags", None) and REPORT_SERVICE_TAG in route.tags
    ]
    return get_openapi(
        title="backtest-hub report service",
        version=app.version,
        routes=report_routes,
    )


def get_main_openapi() -> dict[str, Any]:
    main_routes = [
        route
        for route in app.routes
        if not (getattr(route, "tags", None) and REPORT_SERVICE_TAG in route.tags)
    ]
    return get_openapi(
        title=app.title or "FastAPI",
        version=app.version,
        routes=main_routes,
    )


app.openapi = get_main_openapi


def get_active_base_urls() -> list[str]:
    merged = BACKTEST_API_BASES + BACKTEST_BRONZE_API_BASES
    seen: set[str] = set()
    unique: list[str] = []
    for base_url in merged:
        if base_url not in seen:
            seen.add(base_url)
            unique.append(base_url)
    return unique


app.include_router(
    build_report_router(
        get_report_service=get_report_service,
        get_run_entry=lambda backtest_id: get_run_store().get_run(backtest_id),
        get_runs_by_ids=lambda backtest_ids: get_run_store().get_runs_by_ids(backtest_ids),
        list_submitted_ids=lambda after_dt, before_dt: get_run_store().list_submitted_ids(after_dt, before_dt),
    )
)


def read_queue(path: Path = QUEUE_PATH) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception as exc:
        logger.error("queue_file_read_failed path=%s error=%s", path, exc, exc_info=True)
        return []
    if not isinstance(payload, list):
        return []
    items: list[dict[str, str]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        backtest_id = item.get("backtest_id")
        queued_at = item.get("queued_at")
        if isinstance(backtest_id, str) and backtest_id and isinstance(queued_at, str) and queued_at:
            items.append({"backtest_id": backtest_id, "queued_at": queued_at})
    return items


def write_queue(items: list[dict[str, str]], path: Path = QUEUE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(items, handle, ensure_ascii=True, indent=2)


def queue_position(backtest_id: str, path: Path = QUEUE_PATH) -> int | None:
    items = read_queue(path=path)
    for idx, item in enumerate(items):
        if item.get("backtest_id") == backtest_id:
            return idx
    return None


def enqueue_backtest(backtest_id: str, queued_at: str | None = None, path: Path = QUEUE_PATH) -> None:
    items = read_queue(path=path)
    items.append({"backtest_id": backtest_id, "queued_at": queued_at or utc_now()})
    write_queue(items, path=path)


def dequeue_backtest(path: Path = QUEUE_PATH) -> dict[str, str] | None:
    items = read_queue(path=path)
    if not items:
        return None
    item = items.pop(0)
    write_queue(items, path=path)
    return item


def remove_from_queue_batch(backtest_ids: list[str], path: Path = QUEUE_PATH) -> set[str]:
    wanted = {bid for bid in backtest_ids if isinstance(bid, str) and bid}
    if not wanted:
        return set()
    items = read_queue(path=path)
    kept: list[dict[str, str]] = []
    removed: set[str] = set()
    for item in items:
        bid = item.get("backtest_id")
        if bid in wanted:
            removed.add(bid)
        else:
            kept.append(item)
    write_queue(kept, path=path)
    return removed




def build_multipart_form(
    fields: list[tuple[str, str]],
    files: list[tuple[str, str, str, bytes]],
) -> tuple[bytes, str]:
    boundary = "----backtest-hub-" + secrets.token_hex(16)
    body = bytearray()
    for name, value in fields:
        body.extend(f"--{boundary}\r\n".encode("utf-8"))
        body.extend(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode("utf-8"))
        body.extend(value.encode("utf-8"))
        body.extend(b"\r\n")
    for field_name, filename, content_type, content in files:
        body.extend(f"--{boundary}\r\n".encode("utf-8"))
        body.extend(
            f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"\r\n'.encode(
                "utf-8"
            )
        )
        body.extend(f"Content-Type: {content_type}\r\n\r\n".encode("utf-8"))
        body.extend(content)
        body.extend(b"\r\n")
    body.extend(f"--{boundary}--\r\n".encode("utf-8"))
    content_type = f"multipart/form-data; boundary={boundary}"
    return bytes(body), content_type


def backtest_headers() -> dict[str, str]:
    headers: dict[str, str] = {}
    if BACKTEST_API_KEY:
        headers["X-API-KEY"] = BACKTEST_API_KEY
    return headers


def resolve_backtest_api_base(backtest_id: str | None) -> str:
    if backtest_id:
        entry = get_mapping_entry(backtest_id)
        if entry:
            base_url = entry.get("backtest_api_base")
            if isinstance(base_url, str) and base_url:
                return base_url
    return BACKTEST_API_BASES[0]


def get_mapping_entry(backtest_id: str) -> dict[str, Any] | None:
    store = get_run_store()
    entry = store.get_run(backtest_id)
    if not isinstance(entry, dict):
        return None
    return entry


def ensure_backtest_routable(backtest_id: str, *, allow_running_only: bool = True) -> dict[str, Any]:
    entry = get_mapping_entry(backtest_id)
    if not entry:
        raise HTTPException(status_code=404, detail="backtest_id not found")
    base_url = entry.get("backtest_api_base")
    status = entry.get("status")
    if not isinstance(base_url, str) or not base_url:
        raise HTTPException(status_code=409, detail="Backtest is still queued (no worker assigned yet)")
    if allow_running_only and status not in {"submitted", "running"}:
        hint = (
            "Use /runs/backtest/{id}/logs/download, "
            "POST /runs/backtest/reports, or "
            "GET /runs/backtest/{id}/report instead"
        )
        raise HTTPException(
            status_code=409,
            detail=f"Backtest has {status}. This endpoint is only available while running. {hint}",
        )
    return entry


def build_ws_url(base_url: str, path: str) -> str:
    parsed = urlparse(base_url)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    base_path = parsed.path.rstrip("/")
    normalized_path = "/" + path.lstrip("/")
    full_path = f"{base_path}{normalized_path}"
    return urlunparse((scheme, parsed.netloc, full_path, "", "", ""))


def submit_to_backtest(
    base_url: str,
    backtest_id: str,
    run_spec_bytes: bytes,
    strategy_filename: str | None,
    strategy_bytes: bytes | None,
    bundle_filename: str | None,
    bundle_bytes: bytes | None,
    runner_bytes: bytes,
) -> str:
    url = normalize_join_url(base_url, BACKTEST_SUBMIT_PATH)
    logger.info("submit_to_backtest start backtest_id=%s url=%s", backtest_id, url)
    # backtest docker 最新接口字段名：backtest_id / runer / strategies / strategy_bundle / configs
    files = [
        ("runer", "run_backtest.py", "text/x-python", runner_bytes),
        ("configs", "run_spec.json", "application/json", run_spec_bytes),
    ]
    if bundle_filename and bundle_bytes is not None:
        files.append(("strategy_bundle", bundle_filename, "application/zip", bundle_bytes))
    elif strategy_filename and strategy_bytes is not None:
        files.append(("strategies", strategy_filename, "text/x-python", strategy_bytes))
    else:
        raise ValueError("strategy_file/strategy_bundle missing for backtest submission")

    form_body, content_type = build_multipart_form(
        fields=[("backtest_id", backtest_id)],
        files=files,
    )
    headers = {"Content-Type": content_type, **backtest_headers()}
    req = urllib.request.Request(url, data=form_body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            body = resp.read()
            payload = json.loads(body.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8")
        logger.warning(
            "backtest_http_error backtest_id=%s status=%s detail=%s",
            backtest_id,
            exc.code,
            detail[:200] if detail else "",
        )
        raise HTTPException(status_code=exc.code, detail=detail or "backtest error") from exc
    except Exception as exc:
        logger.exception("backtest_request_failed backtest_id=%s", backtest_id)
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    backtest_docker_run_id = payload.get("run_id")
    if not backtest_docker_run_id:
        logger.warning("backtest_id_missing backtest_id=%s payload_keys=%s", backtest_id, sorted(payload.keys()))
        raise HTTPException(status_code=502, detail="backtest_docker_run_id missing from response")
    logger.info("submit_to_backtest success backtest_id=%s backtest_docker_run_id=%s", backtest_id, backtest_docker_run_id)
    return str(backtest_docker_run_id)


def fetch_backtest_status(base_url: str, backtest_id: str) -> dict[str, Any]:
    url = normalize_join_url(base_url, BACKTEST_STATUS_PATH.format(backtest_id=backtest_id))
    req = urllib.request.Request(url, headers=backtest_headers(), method="GET")
    try:
        logger.info("fetch_backtest_status start backtest_id=%s url=%s", backtest_id, url)
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read()
            payload = json.loads(body.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8")
        logger.warning(
            "fetch_backtest_status_http_error backtest_id=%s status=%s detail=%s",
            backtest_id,
            exc.code,
            detail[:200] if detail else "",
        )
        raise HTTPException(status_code=exc.code, detail=detail or "backtest error") from exc
    except Exception as exc:
        logger.exception("fetch_backtest_status_failed backtest_id=%s", backtest_id)
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    if not isinstance(payload, dict):
        logger.warning("fetch_backtest_status_invalid_payload backtest_id=%s", backtest_id)
        raise HTTPException(status_code=502, detail="backtest status invalid response")

    return {
        "status": payload.get("status"),
        "pid": payload.get("pid"),
        "started_at": payload.get("started_at"),
        "finished_at": payload.get("finished_at"),
    }


def parse_run_spec_bytes(run_spec_bytes: bytes) -> dict[str, Any]:
    try:
        run_spec_payload = json.loads(run_spec_bytes.decode("utf-8"))
    except Exception as exc:
        raise ValueError(f"invalid stored run_spec.json: {exc}") from exc
    if not isinstance(run_spec_payload, dict):
        raise ValueError("stored run_spec.json must be an object")
    return run_spec_payload


def load_run_spec_payload(backtest_id: str) -> dict[str, Any]:
    run_dir = RUN_STORAGE_PATH / backtest_id
    run_spec_path = run_dir / "run_spec.json"
    run_spec_bytes = run_spec_path.read_bytes()
    return parse_run_spec_bytes(run_spec_bytes)


def load_run_assets(backtest_id: str) -> tuple[bytes, str | None, bytes | None, str | None, bytes | None, bytes]:
    run_dir = RUN_STORAGE_PATH / backtest_id
    run_spec_path = run_dir / "run_spec.json"
    run_spec_bytes = run_spec_path.read_bytes()
    run_spec_payload = parse_run_spec_bytes(run_spec_bytes)
    strategy_filename: str | None = None
    strategy_bytes: bytes | None = None
    bundle_filename: str | None = None
    bundle_bytes: bytes | None = None
    if run_spec_payload.get("strategy_bundle"):
        bundle_filename = Path(str(run_spec_payload.get("strategy_bundle"))).name
        bundle_path = run_dir / bundle_filename
        bundle_bytes = bundle_path.read_bytes()
    elif run_spec_payload.get("strategy_file"):
        strategy_filename = Path(str(run_spec_payload.get("strategy_file"))).name
        strategy_path = run_dir / strategy_filename
        strategy_bytes = strategy_path.read_bytes()
    else:
        raise ValueError("strategy_file/strategy_bundle missing in stored run_spec.json")
    if not RUNNER_PATH.is_file():
        raise FileNotFoundError(f"run_backtest.py not found: {RUNNER_PATH}")
    runner_bytes = RUNNER_PATH.read_bytes()
    return run_spec_bytes, strategy_filename, strategy_bytes, bundle_filename, bundle_bytes, runner_bytes


async def pick_backtest_target(
    required_memory_gb: float,
    symbol_count: int,
    reserved_override: dict[str, float] | None = None,
    counts_override: dict[str, int] | None = None,
):
    if reserved_override is not None and counts_override is not None:
        reserved_snapshot = reserved_override
        inflight_counts_snapshot = counts_override
    else:
        async with QUEUE_STATE_LOCK:
            reserved_snapshot = dict(INFLIGHT_MEMORY_GB)
            inflight_counts_snapshot = dict(INFLIGHT_COUNTS)

    max_running = MAX_RUNNING_BACKTESTS if MAX_RUNNING_BACKTESTS > 0 else None
    per_worker = PER_WORKER_MAX_RUNNING or None
    # Need runs_path if global or any per-worker limit is configured
    runs_path = BACKTEST_RUNS_PATH if (max_running is not None or per_worker) else None

    if symbol_count < BRONZE_SYMBOLS_THRESHOLD and BACKTEST_BRONZE_API_BASES:
        bronze = await asyncio.to_thread(
            select_backtest_docker,
            base_urls=BACKTEST_BRONZE_API_BASES,
            metrics_path=BACKTEST_METRICS_PATH,
            runs_path=runs_path,
            headers=backtest_headers(),
            required_memory_gb=required_memory_gb,
            reserved_memory_gb=reserved_snapshot,
            inflight_counts=inflight_counts_snapshot,
            max_running=max_running,
            timeout_seconds=BACKTEST_METRICS_TIMEOUT_SECONDS,
            cpu_percent_lt=CPU_PERCENT_LT,
            require_memory_gt=True,
            max_running_per_worker=per_worker,
        )
        if bronze is not None:
            return bronze

    return await asyncio.to_thread(
        select_backtest_docker,
        base_urls=BACKTEST_API_BASES,
        metrics_path=BACKTEST_METRICS_PATH,
        runs_path=runs_path,
        headers=backtest_headers(),
        required_memory_gb=required_memory_gb,
        reserved_memory_gb=reserved_snapshot,
        inflight_counts=inflight_counts_snapshot,
        max_running=max_running,
        timeout_seconds=BACKTEST_METRICS_TIMEOUT_SECONDS,
        cpu_percent_lt=CPU_PERCENT_LT,
        max_running_per_worker=per_worker,
    )


async def submit_with_queue_control(
    backtest_id: str,
    run_spec_bytes: bytes,
    strategy_filename: str | None,
    strategy_bytes: bytes | None,
    runner_bytes: bytes,
    required_memory_gb: float,
    symbol_count: int,
) -> tuple[str, str | None]:
    """
    Returns: (hub_status, backtest_docker_run_id)
      - hub_status: "queued" (always queued, scheduler dispatches with delay)
      - backtest_docker_run_id: always None (set by scheduler after dispatch)
    """
    # Always queue requests. Let queue_scheduler dispatch with proper delay
    # to ensure metrics update between submissions and prevent server overload.
    async with QUEUE_STATE_LOCK:
        get_run_store().enqueue(backtest_id, utc_now())
        update_mapping(
            backtest_id,
            {
                "status": "queued",
                "queued_at": utc_now(),
                "required_memory_gb": required_memory_gb,
            },
        )
    return "queued", None


async def queue_scheduler() -> None:
    while True:
        try:
            async with QUEUE_STATE_LOCK:
                queued = get_run_store().read_queue()
                # Snapshot global state for this iteration
                local_reserved = dict(INFLIGHT_MEMORY_GB)
                local_counts = dict(INFLIGHT_COUNTS)

            if not queued:
                await asyncio.sleep(QUEUE_POLL_INTERVAL_SECONDS)
                continue

            head = queued[0]
            head_queued_at = head.get("queued_at", "")
            head_wait_seconds = 0.0
            if head_queued_at:
                try:
                    head_wait_seconds = (datetime.now(timezone.utc) - parse_iso8601(head_queued_at)).total_seconds()
                except Exception:
                    pass

            # Check if head has waited too long - enter reserve mode (no backfilling)
            reserve_mode = head_wait_seconds > BACKFILL_RESERVE_THRESHOLD_SECONDS
            if reserve_mode:
                logger.info(
                    "queue_scheduler_reserve_mode head_backtest_id=%s wait_seconds=%.1f threshold=%.1f",
                    head.get("backtest_id"),
                    head_wait_seconds,
                    BACKFILL_RESERVE_THRESHOLD_SECONDS,
                )

            made_progress = False

            # Iterate through all tasks (Backfilling)
            for idx, item in enumerate(queued):
                backtest_id = item.get("backtest_id")
                if not backtest_id:
                    continue

                is_head = idx == 0
                item_queued_at = item.get("queued_at", "")

                # Anti-starvation: in reserve mode, only try head task
                if reserve_mode and not is_head:
                    logger.info(
                        "queue_scheduler_skip_reserve_mode backtest_id=%s head_backtest_id=%s",
                        backtest_id,
                        head.get("backtest_id"),
                    )
                    continue

                # Anti-starvation: only backfill tasks queued within window after head
                if not is_head and head_queued_at and item_queued_at:
                    try:
                        head_time = parse_iso8601(head_queued_at)
                        item_time = parse_iso8601(item_queued_at)
                        if (item_time - head_time).total_seconds() > BACKFILL_WINDOW_SECONDS:
                            logger.info(
                                "queue_scheduler_skip_outside_window backtest_id=%s queued_at=%s head_queued_at=%s window=%.1f",
                                backtest_id,
                                item_queued_at,
                                head_queued_at,
                                BACKFILL_WINDOW_SECONDS,
                            )
                            continue
                    except Exception:
                        pass

                try:
                    run_spec_payload = await asyncio.to_thread(load_run_spec_payload, backtest_id)
                    required_memory_gb = required_memory_gb_from_run_spec(run_spec_payload, MEMORY_PER_SYMBOL_GB)
                    symbol_count = len(run_spec_payload["symbols"])
                except Exception as exc:
                    update_mapping(
                        backtest_id,
                        {"status": "queued", "last_error": str(exc), "last_error_at": utc_now()},
                    )
                    logger.warning("queue_scheduler_load_failed backtest_id=%s error=%s", backtest_id, exc)
                    continue

                # Use local state to pick target (avoids double-booking in one pass)
                selection = await pick_backtest_target(
                    required_memory_gb,
                    symbol_count,
                    reserved_override=local_reserved,
                    counts_override=local_counts,
                )

                if selection is None:
                    # Cannot fit this task, skip to next (Backfilling)
                    continue

                # Found a potential slot. Try to acquire lock and real resource.
                should_run = False

                async with QUEUE_STATE_LOCK:
                    # Re-verify queue presence (race condition check)
                    current_queue_ids = {x.get("backtest_id") for x in get_run_store().read_queue()}
                    if backtest_id not in current_queue_ids:
                        continue  # Removed by someone else

                    # Re-verify global resource (race condition check)
                    # 使用 local_reserved（这一轮中累积的预留），而不是 INFLIGHT_MEMORY_GB（成功后会被释放）
                    local_for_base = local_reserved.get(selection.base_url, 0.0)
                    real_available = selection.metrics.memory_free_gb - local_for_base

                    if real_available >= required_memory_gb:
                        # Success! Reserve and dequeue
                        reserve_inflight(selection.base_url, required_memory_gb)
                        get_run_store().remove_from_queue_batch([backtest_id])
                        should_run = True

                        # Update local state for next task in this loop
                        local_reserved[selection.base_url] = local_reserved.get(selection.base_url, 0.0) + required_memory_gb
                        local_counts[selection.base_url] = local_counts.get(selection.base_url, 0) + 1

                if not should_run:
                    continue

                # Submit task
                try:
                    (
                        run_spec_bytes,
                        strategy_filename,
                        strategy_bytes,
                        bundle_filename,
                        bundle_bytes,
                        runner_bytes,
                    ) = await asyncio.to_thread(load_run_assets, backtest_id)
                    backtest_docker_run_id = await asyncio.to_thread(
                        submit_to_backtest,
                        selection.base_url,
                        backtest_id,
                        run_spec_bytes,
                        strategy_filename,
                        strategy_bytes,
                        bundle_filename,
                        bundle_bytes,
                        runner_bytes,
                    )
                    update_mapping(
                        backtest_id,
                        {
                            "status": "submitted",
                            "submitted_at": utc_now(),
                            "backtest_docker_run_id": backtest_docker_run_id,
                            "backtest_api_base": selection.base_url,
                            "required_memory_gb": required_memory_gb,
                        },
                    )
                    logger.info(
                        "queue_scheduler_submitted backtest_id=%s backtest_docker_run_id=%s base=%s",
                        backtest_id,
                        backtest_docker_run_id,
                        selection.base_url,
                    )
                    made_progress = True

                    # Delay before dispatching next job to let CPU/RAM stabilize
                    if QUEUE_DISPATCH_DELAY_SECONDS > 0:
                        logger.info(
                            "queue_scheduler_dispatch_delay backtest_id=%s delay_seconds=%.1f",
                            backtest_id,
                            QUEUE_DISPATCH_DELAY_SECONDS,
                        )
                        await asyncio.sleep(QUEUE_DISPATCH_DELAY_SECONDS)

                except Exception as exc:
                    # If submission fails, put back in queue
                    async with QUEUE_STATE_LOCK:
                        get_run_store().enqueue(backtest_id, item.get("queued_at", utc_now()))
                        release_inflight(selection.base_url, required_memory_gb)

                    update_mapping(
                        backtest_id,
                        {"status": "queued", "last_error": str(exc), "last_error_at": utc_now()},
                    )
                    logger.warning("queue_scheduler_submit_failed backtest_id=%s error=%s", backtest_id, exc)
                else:
                    # On success, release inflight reservation
                    async with QUEUE_STATE_LOCK:
                        release_inflight(selection.base_url, required_memory_gb)

            if not made_progress:
                await asyncio.sleep(QUEUE_POLL_INTERVAL_SECONDS)
        except Exception as exc:
            logger.exception("queue_scheduler_crashed error=%s", exc)
            await asyncio.sleep(QUEUE_POLL_INTERVAL_SECONDS)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "timestamp": utc_now()}


@app.get(REPORT_SERVICE_OPENAPI_URL, include_in_schema=False)
async def report_service_openapi() -> JSONResponse:
    return JSONResponse(get_report_service_openapi())


@app.get(REPORT_SERVICE_DOCS_URL, include_in_schema=False)
async def report_service_docs() -> Response:
    return get_swagger_ui_html(openapi_url=REPORT_SERVICE_OPENAPI_URL, title="Report Service API")


@app.post("/runs")
async def create_run(
    run_spec: UploadFile = File(...),
    strategy_file: UploadFile | None = File(None),
    strategy_bundle: UploadFile | None = File(None),
) -> JSONResponse:
    # 入口：research docker 上传 run_spec.json 与策略文件（multipart/form-data）
    logger.info(
        "create_run_received run_spec=%s strategy_file=%s strategy_bundle=%s",
        run_spec.filename or "-",
        strategy_file.filename if strategy_file else "-",
        strategy_bundle.filename if strategy_bundle else "-",
    )

    try:
        # 1) 读取并解析 run_spec.json
        run_spec_raw = await run_spec.read()
        payload = json.loads(run_spec_raw.decode("utf-8"))
    except Exception as exc:
        logger.warning("create_run_invalid_spec_json error=%s", exc)
        raise HTTPException(status_code=400, detail="Invalid run_spec.json") from exc

    if not isinstance(payload, dict):
        logger.warning("create_run_spec_not_object type=%s", type(payload).__name__)
        raise HTTPException(status_code=400, detail="run_spec.json must be an object")

    if strategy_file and strategy_bundle:
        raise HTTPException(status_code=400, detail="strategy_file and strategy_bundle are mutually exclusive")
    if not strategy_file and not strategy_bundle:
        raise HTTPException(status_code=400, detail="strategy_file or strategy_bundle is required")

    # 2) 生成 run_id，并在本地保存上传内容，便于排查与追踪
    backtest_id = make_run_id()
    logger.info("create_run_generated backtest_id=%s", backtest_id)

    # 3) 读取策略文件或策略包（只保留文件名，避免路径穿越）
    strategy_filename: str | None = None
    strategy_bytes: bytes | None = None
    bundle_filename: str | None = None
    bundle_bytes: bytes | None = None
    if strategy_file:
        strategy_filename = Path(strategy_file.filename or "strategy.py").name
        strategy_bytes = await strategy_file.read()
        if not strategy_bytes:
            logger.warning("create_run_empty_strategy backtest_id=%s", backtest_id)
            raise HTTPException(status_code=400, detail="strategy_file is empty")
        payload["strategy_file"] = strategy_filename
        payload.pop("strategy_bundle", None)
    else:
        assert strategy_bundle is not None
        bundle_filename = Path(strategy_bundle.filename or "strategy_bundle.zip").name
        bundle_bytes = await strategy_bundle.read()
        if not bundle_bytes:
            logger.warning("create_run_empty_bundle backtest_id=%s", backtest_id)
            raise HTTPException(status_code=400, detail="strategy_bundle is empty")
        payload["strategy_bundle"] = bundle_filename
        payload.pop("strategy_file", None)

    try:
        # 4) 校验 run_spec 内容（字段、时间范围、费用等）
        run_spec_payload = validate_run_spec(payload)
    except ValueError as exc:
        logger.warning("create_run_spec_validation_failed error=%s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    required_memory_gb = required_memory_gb_from_run_spec(run_spec_payload, MEMORY_PER_SYMBOL_GB)
    symbol_count = len(run_spec_payload["symbols"])

    requested_by = run_spec_payload.get("requested_by")
    if isinstance(requested_by, str) and requested_by:
        update_mapping(backtest_id, {"requested_by": requested_by})

    # 5) 落盘保存策略文件/策略包与 run_spec
    run_dir = RUN_STORAGE_PATH / backtest_id
    run_dir.mkdir(parents=True, exist_ok=True)

    if strategy_filename and strategy_bytes is not None:
        strategy_path = run_dir / strategy_filename
        strategy_path.write_bytes(strategy_bytes)
    if bundle_filename and bundle_bytes is not None:
        bundle_path = run_dir / bundle_filename
        bundle_path.write_bytes(bundle_bytes)

    run_spec_payload["backtest_id"] = backtest_id
    run_spec_path = run_dir / "run_spec.json"
    run_spec_bytes = json.dumps(run_spec_payload, ensure_ascii=True, indent=2).encode("utf-8")
    run_spec_path.write_bytes(run_spec_bytes)
    logger.info(
        "create_run_saved backtest_id=%s strategy_filename=%s strategy_bundle=%s run_spec_bytes=%s strategy_bytes=%s bundle_bytes=%s",
        backtest_id,
        strategy_filename or "-",
        bundle_filename or "-",
        len(run_spec_bytes),
        len(strategy_bytes or b""),
        len(bundle_bytes or b""),
    )

    # 6) 从本仓库读取 run_backtest.py，并一并转发给 backtest docker
    if not RUNNER_PATH.is_file():
        logger.error("create_run_missing_runner backtest_id=%s path=%s", backtest_id, RUNNER_PATH)
        raise HTTPException(status_code=500, detail="run_backtest.py not found")
    runner_bytes = RUNNER_PATH.read_bytes()

    # 7) 调用 backtest docker 提交任务，获取 backtest_docker_run_id
    hub_status, backtest_docker_run_id = await submit_with_queue_control(
        backtest_id=backtest_id,
        run_spec_bytes=run_spec_bytes,
        strategy_filename=strategy_filename,
        strategy_bytes=strategy_bytes,
        runner_bytes=runner_bytes,
        required_memory_gb=required_memory_gb,
        symbol_count=symbol_count,
    )
    logger.info(
        "create_run_done backtest_id=%s status=%s backtest_docker_run_id=%s",
        backtest_id,
        hub_status,
        backtest_docker_run_id or "-",
    )

    return JSONResponse(
        {
            "backtest_id": backtest_id,
            "backtest_docker_run_id": backtest_docker_run_id,
            "status": hub_status,
        }
    )


@app.get("/queue")
async def get_queue(
    limit: int = 100,
    offset: int = 0,
    backtest_id: str | None = None,
) -> JSONResponse:
    if limit < 0 or offset < 0:
        raise HTTPException(status_code=400, detail="limit/offset must be >= 0")
    async with QUEUE_STATE_LOCK:
        store = get_run_store()
        items = store.read_queue()
        total = len(items)
        sliced = items[offset : offset + limit] if limit else items[offset:]
        pos = None
        if backtest_id:
            pos = store.queue_position(backtest_id)
        position_1_based = (pos + 1) if pos is not None else None
    return JSONResponse(
        {
            "count": total,
            "items": sliced,
            "position": position_1_based,
        }
    )


@app.delete("/queue")
async def delete_queue(
    payload: dict[str, Any] = Body(..., example={"backtest_ids": ["bt_123", "bt_456"]})
) -> JSONResponse:
    backtest_ids = payload.get("backtest_ids")
    if not isinstance(backtest_ids, list) or not all(isinstance(x, str) and x for x in backtest_ids):
        raise HTTPException(status_code=400, detail="backtest_ids must be a non-empty list of strings")

    async with QUEUE_STATE_LOCK:
        removed = get_run_store().remove_from_queue_batch(backtest_ids)

    store = get_run_store()
    existing = store.existing_ids(backtest_ids)

    not_queued: list[str] = []
    not_found: list[str] = []
    for bid in backtest_ids:
        if bid in removed:
            update_mapping(bid, {"status": "cancelled", "cancelled_at": utc_now()})
            continue
        if bid in existing:
            not_queued.append(bid)
        else:
            not_found.append(bid)

    return JSONResponse(
        {
            "removed": sorted(removed),
            "not_queued": not_queued,
            "not_found": not_found,
        }
    )


@app.get("/runs/{backtest_id}")
async def get_run(backtest_id: str) -> JSONResponse:
    logger.info("get_run_requested backtest_id=%s", backtest_id)
    entry = get_mapping_entry(backtest_id)
    if not entry:
        logger.warning("get_run_not_found backtest_id=%s", backtest_id)
        raise HTTPException(status_code=404, detail="backtest_id not found")
    payload = {"backtest_id": backtest_id, **entry}
    return JSONResponse(payload)


@app.get("/runs/backtest/{backtest_id}")
async def get_backtest_status(backtest_id: str) -> JSONResponse:
    logger.info("get_backtest_status_requested backtest_id=%s", backtest_id)
    entry = ensure_backtest_routable(backtest_id, allow_running_only=False)
    base_url = entry["backtest_api_base"]
    payload = fetch_backtest_status(base_url, backtest_id)
    return JSONResponse(payload)


@app.websocket("/runs/backtest/{backtest_id}/logs/stream")
async def stream_backtest_logs(websocket: WebSocket, backtest_id: str) -> None:
    try:
        entry = ensure_backtest_routable(backtest_id)
        base_url = entry["backtest_api_base"]
    except HTTPException as exc:
        await websocket.close(code=1008, reason=str(exc.detail))
        return

    await websocket.accept()
    upstream_url = build_ws_url(
        base_url,
        BACKTEST_WS_LOGS_PATH.format(backtest_id=backtest_id),
    )
    logger.info("ws_logs_connect backtest_id=%s url=%s", backtest_id, upstream_url)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(upstream_url, headers=backtest_headers()) as upstream:

                async def client_to_upstream() -> None:
                    try:
                        while True:
                            message = await websocket.receive()
                            if message.get("type") == "websocket.disconnect":
                                break
                            if "text" in message and message["text"] is not None:
                                await upstream.send_str(message["text"])
                            elif "bytes" in message and message["bytes"] is not None:
                                await upstream.send_bytes(message["bytes"])
                    except WebSocketDisconnect:
                        pass
                    except Exception as exc:
                        logger.warning("ws_client_to_upstream_error backtest_id=%s error=%s", backtest_id, exc)

                async def upstream_to_client() -> None:
                    try:
                        async for message in upstream:
                            if message.type == aiohttp.WSMsgType.TEXT:
                                await websocket.send_text(message.data)
                            elif message.type == aiohttp.WSMsgType.BINARY:
                                await websocket.send_bytes(message.data)
                            elif message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                                break
                            elif message.type == aiohttp.WSMsgType.ERROR:
                                break
                    except Exception as exc:
                        logger.warning("ws_upstream_to_client_error backtest_id=%s error=%s", backtest_id, exc)

                tasks = [
                    asyncio.create_task(client_to_upstream()),
                    asyncio.create_task(upstream_to_client()),
                ]
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in pending:
                    task.cancel()
                for task in done:
                    if task.cancelled():
                        continue
                    exc = task.exception()
                    if exc:
                        logger.warning("ws_proxy_task_failed backtest_id=%s error=%s", backtest_id, exc)
    except Exception as exc:
        logger.warning("ws_upstream_connect_failed backtest_id=%s error=%s", backtest_id, exc)
        await websocket.close(code=1011)


@app.get("/runs/{backtest_id}/logs")
async def get_logs(backtest_id: str) -> Response:
    logger.info("get_logs_requested backtest_id=%s", backtest_id)
    entry = ensure_backtest_routable(backtest_id)
    base_url = entry["backtest_api_base"]
    url = f"{base_url.rstrip('/')}{BACKTEST_LOGS_DOWNLOAD_PATH.format(backtest_id=backtest_id)}"
    req = urllib.request.Request(url, headers=backtest_headers(), method="GET")
    try:
        logger.info("get_logs_fetching backtest_id=%s url=%s", backtest_id, url)
        with urllib.request.urlopen(req) as resp:
            data = resp.read()
            content_type = resp.headers.get("Content-Type", "application/octet-stream")
            headers: dict[str, str] = {}
            content_disposition = resp.headers.get("Content-Disposition")
            if content_disposition:
                headers["Content-Disposition"] = content_disposition
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8")
        logger.warning(
            "get_logs_http_error backtest_id=%s status=%s detail=%s",
            backtest_id,
            exc.code,
            detail[:200] if detail else "",
        )
        raise HTTPException(status_code=exc.code, detail=detail or "backtest error") from exc
    except Exception as exc:
        logger.exception("get_logs_request_failed backtest_id=%s", backtest_id)
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return Response(content=data, media_type=content_type, headers=headers)


@app.get("/runs/backtest/{backtest_id}/download_csv")
async def download_backtest_csv(backtest_id: str) -> Response:
    logger.info("download_csv_requested backtest_id=%s", backtest_id)
    entry = ensure_backtest_routable(backtest_id)
    base_url = entry["backtest_api_base"]
    url = f"{base_url.rstrip('/')}{BACKTEST_CSV_DOWNLOAD_PATH.format(backtest_id=backtest_id)}"
    req = urllib.request.Request(url, headers=backtest_headers(), method="GET")
    try:
        logger.info("download_csv_fetching backtest_id=%s url=%s", backtest_id, url)
        with urllib.request.urlopen(req) as resp:
            data = resp.read()
            content_type = resp.headers.get("Content-Type", "application/zip")
            headers: dict[str, str] = {}
            content_disposition = resp.headers.get("Content-Disposition")
            if content_disposition:
                headers["Content-Disposition"] = content_disposition
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8")
        logger.warning(
            "download_csv_http_error backtest_id=%s status=%s detail=%s",
            backtest_id,
            exc.code,
            detail[:200] if detail else "",
        )
        raise HTTPException(status_code=exc.code, detail=detail or "backtest error") from exc
    except Exception as exc:
        logger.exception("download_csv_request_failed backtest_id=%s", backtest_id)
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return Response(content=data, media_type=content_type, headers=headers)


@app.post("/runs/backtest/{backtest_id}/kill")
async def kill_backtest_run(backtest_id: str) -> JSONResponse:
    logger.info("kill_backtest_requested backtest_id=%s", backtest_id)

    entry = get_mapping_entry(backtest_id)
    if not entry:
        raise HTTPException(status_code=404, detail="backtest_id not found")

    status = entry.get("status")
    base_url = entry.get("backtest_api_base")

    # If still queued, remove from queue and cancel
    if status == "queued" or not base_url:
        async with QUEUE_STATE_LOCK:
            removed = get_run_store().remove_from_queue_batch([backtest_id])
        if backtest_id in removed:
            update_mapping(backtest_id, {"status": "cancelled", "cancelled_at": utc_now()})
            logger.info("kill_backtest_cancelled_queued backtest_id=%s", backtest_id)
            return JSONResponse({"backtest_id": backtest_id, "status": "cancelled"})
        # Not in queue but no base_url - inconsistent state
        raise HTTPException(status_code=409, detail="Backtest is still queued but not in queue")

    # Forward kill to backtest docker
    url = f"{base_url.rstrip('/')}{BACKTEST_KILL_PATH.format(backtest_id=backtest_id)}"
    req = urllib.request.Request(url, headers=backtest_headers(), method="POST")
    try:
        logger.info("kill_backtest_fetching backtest_id=%s url=%s", backtest_id, url)
        with urllib.request.urlopen(req) as resp:
            body = resp.read()
        payload = json.loads(body.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8")
        logger.warning(
            "kill_backtest_http_error backtest_id=%s status=%s detail=%s",
            backtest_id,
            exc.code,
            detail[:200] if detail else "",
        )
        raise HTTPException(status_code=exc.code, detail=detail or "backtest error") from exc
    except Exception as exc:
        logger.exception("kill_backtest_request_failed backtest_id=%s", backtest_id)
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail="backtest kill invalid response")
    return JSONResponse(payload)
