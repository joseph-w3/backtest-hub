from __future__ import annotations

import asyncio
import json
import logging
import os
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
from fastapi.responses import JSONResponse, Response

from services.report_service import (
    ReportFetchError,
    ReportHttpError,
    ReportInvalidPayload,
    ReportService,
    ReportServiceConfig,
)
from services.scheduler import (
    parse_backtest_api_bases,
    required_memory_gb_from_run_spec,
    select_backtest_docker,
)

app = FastAPI()
report_service: "ReportService | None" = None


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


@app.on_event("startup")
async def on_startup() -> None:
    logger.info("app_started")
    global report_service
    report_service = ReportService(
        ReportServiceConfig(
            cache_path=REPORT_CACHE_PATH,
            report_path=BACKTEST_REPORT_PATH,
            max_page_size=MAX_REPORT_PAGE_SIZE,
        ),
        backtest_headers=backtest_headers,
        ensure_backtest_routable=ensure_backtest_routable,
        update_mapping=update_mapping,
        load_run_spec_payload=load_run_spec_payload,
    )
    try:
        report_service.init_cache()
    except Exception as exc:
        logger.exception("report_cache_init_failed error=%s", exc)
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


BASE_DIR = Path(__file__).resolve().parent
DATA_MOUNT_PATH = Path(env_or_default("DATA_MOUNT_PATH", "/opt/backtest"))
RUN_STORAGE_PATH = Path(env_or_default("RUN_STORAGE_PATH", str(DATA_MOUNT_PATH / "runs")))
RUN_MAPPING_PATH = Path(env_or_default("RUN_MAPPING_PATH", str(DATA_MOUNT_PATH / "run_mapping.json")))
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
BACKTEST_KILL_PATH = "/v1/runs/backtest/{backtest_id}/kill"
BACKTEST_REPORT_PATH = env_or_default("BACKTEST_REPORT_PATH", "/v1/runs/backtest/{backtest_id}/report")
BACKTEST_STATUS_PATH = env_or_default("BACKTEST_STATUS_PATH", "/v1/runs/backtest/{backtest_id}")
BACKTEST_WS_LOGS_PATH = env_or_default(
    "BACKTEST_WS_LOGS_PATH",
    "/v1/runs/backtest/{backtest_id}/logs/stream",
)
BACKTEST_METRICS_PATH = env_or_default("BACKTEST_METRICS_PATH", "v1/docker/metrics")
BACKTEST_METRICS_TIMEOUT_SECONDS = float(os.getenv("BACKTEST_METRICS_TIMEOUT_SECONDS", "3"))

RUNNER_PATH = Path(env_or_default("BACKTEST_RUNNER_PATH", str(BASE_DIR / "scripts" / "run_backtest.py")))

MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "50"))
MAX_RANGE_DAYS = int(os.getenv("MAX_RANGE_DAYS", "90"))
MAX_RUNNING_BACKTESTS = int(os.getenv("MAX_RUNNING_BACKTESTS", "10"))
QUEUE_POLL_INTERVAL_SECONDS = float(os.getenv("QUEUE_POLL_INTERVAL_SECONDS", "3"))
QUEUE_PATH = Path(env_or_default("QUEUE_PATH", str(DATA_MOUNT_PATH / "submit_queue.json")))
MAX_REPORT_PAGE_SIZE = int(os.getenv("MAX_REPORT_PAGE_SIZE", "50"))

BRONZE_SYMBOLS_THRESHOLD = 6
CPU_PERCENT_LT = 80.0

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
OPTIONAL_FIELDS = {"latency_config"}

ALLOWED_FIELDS = REQUIRED_FIELDS | OPTIONAL_FIELDS

MAPPING_LOCK = threading.Lock()

# Protect queue + inflight submissions to avoid oversubmitting to backtest docker.
QUEUE_STATE_LOCK = asyncio.Lock()
INFLIGHT_MEMORY_GB: dict[str, float] = {}
INFLIGHT_COUNTS: dict[str, int] = {}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


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

    if not isinstance(payload.get("strategy_file"), str) or not payload["strategy_file"].strip():
        raise ValueError("strategy_file must be a non-empty string")
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
    if "latency_config" in payload:
        sanitized["latency_config"] = payload["latency_config"]
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


def require_api_key(api_key: str | None) -> None:
    if not HOST_API_KEY:
        raise HTTPException(status_code=500, detail="HOST_API_KEY not configured")
    if not api_key or api_key != HOST_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


def read_mapping(path: Path = RUN_MAPPING_PATH) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_mapping(mapping: dict[str, Any], path: Path = RUN_MAPPING_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(mapping, handle, ensure_ascii=True, indent=2)


def update_mapping(backtest_id: str, updates: dict[str, Any]) -> None:
    with MAPPING_LOCK:
        mapping = read_mapping()
        entry = mapping.get(backtest_id)
        if isinstance(entry, dict):
            entry_dict: dict[str, Any] = dict(entry)
        elif entry is None:
            entry_dict = {}
        else:
            entry_dict = {"backtest_docker_run_id": entry}

        entry_dict.update(updates)
        entry_dict.setdefault("created_at", utc_now())
        mapping[backtest_id] = entry_dict
        write_mapping(mapping)


def normalize_join_url(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}/" + path.lstrip("/")


def get_report_service() -> ReportService:
    global report_service
    if report_service is None:
        report_service = ReportService(
            ReportServiceConfig(
                cache_path=REPORT_CACHE_PATH,
                report_path=BACKTEST_REPORT_PATH,
                max_page_size=MAX_REPORT_PAGE_SIZE,
            ),
            backtest_headers=backtest_headers,
            ensure_backtest_routable=ensure_backtest_routable,
            update_mapping=update_mapping,
            load_run_spec_payload=load_run_spec_payload,
        )
    return report_service


def read_queue(path: Path = QUEUE_PATH) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
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
        with MAPPING_LOCK:
            mapping = read_mapping()
        entry = mapping.get(backtest_id)
        if isinstance(entry, dict):
            base_url = entry.get("backtest_api_base")
            if isinstance(base_url, str) and base_url:
                return base_url
    return BACKTEST_API_BASES[0]


def get_mapping_entry(backtest_id: str) -> dict[str, Any] | None:
    with MAPPING_LOCK:
        mapping = read_mapping()
    entry = mapping.get(backtest_id)
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
        raise HTTPException(status_code=409, detail="Backtest is still queued")
    if allow_running_only and status not in {"submitted", "running"}:
        raise HTTPException(status_code=409, detail="Backtest is still queued")
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
    strategy_filename: str,
    strategy_bytes: bytes,
    runner_bytes: bytes,
) -> str:
    url = normalize_join_url(base_url, BACKTEST_SUBMIT_PATH)
    logger.info("submit_to_backtest start backtest_id=%s url=%s", backtest_id, url)
    # backtest docker 最新接口字段名：backtest_id / runer / strategies / configs
    form_body, content_type = build_multipart_form(
        fields=[("backtest_id", backtest_id)],
        files=[
            ("runer", "run_backtest.py", "text/x-python", runner_bytes),
            ("strategies", strategy_filename, "text/x-python", strategy_bytes),
            ("configs", "run_spec.json", "application/json", run_spec_bytes),
        ],
    )
    headers = {"Content-Type": content_type, **backtest_headers()}
    req = urllib.request.Request(url, data=form_body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req) as resp:
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
        with urllib.request.urlopen(req) as resp:
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


def load_run_assets(backtest_id: str) -> tuple[bytes, str, bytes, bytes]:
    run_dir = RUN_STORAGE_PATH / backtest_id
    run_spec_path = run_dir / "run_spec.json"
    run_spec_bytes = run_spec_path.read_bytes()
    run_spec_payload = parse_run_spec_bytes(run_spec_bytes)
    strategy_filename = Path(str(run_spec_payload.get("strategy_file") or "strategy.py")).name
    strategy_path = run_dir / strategy_filename
    strategy_bytes = strategy_path.read_bytes()
    if not RUNNER_PATH.is_file():
        raise FileNotFoundError(f"run_backtest.py not found: {RUNNER_PATH}")
    runner_bytes = RUNNER_PATH.read_bytes()
    return run_spec_bytes, strategy_filename, strategy_bytes, runner_bytes


async def pick_backtest_target(required_memory_gb: float, symbol_count: int):
    async with QUEUE_STATE_LOCK:
        reserved_snapshot = dict(INFLIGHT_MEMORY_GB)
        inflight_counts_snapshot = dict(INFLIGHT_COUNTS)

    max_running = MAX_RUNNING_BACKTESTS if MAX_RUNNING_BACKTESTS > 0 else None
    runs_path = BACKTEST_RUNS_PATH if max_running is not None else None

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
    )


async def submit_with_queue_control(
    backtest_id: str,
    run_spec_bytes: bytes,
    strategy_filename: str,
    strategy_bytes: bytes,
    runner_bytes: bytes,
    required_memory_gb: float,
    symbol_count: int,
) -> tuple[str, str | None]:
    """
    Returns: (hub_status, backtest_docker_run_id)
      - hub_status: "submitted" | "queued"
    """
    # If the queue is not empty, enforce FIFO fairness (no bypass).
    async with QUEUE_STATE_LOCK:
        if read_queue():
            enqueue_backtest(backtest_id)
            update_mapping(
                backtest_id,
                {
                    "status": "queued",
                    "queued_at": utc_now(),
                    "required_memory_gb": required_memory_gb,
                },
            )
            return "queued", None

    selection = await pick_backtest_target(required_memory_gb, symbol_count)
    if selection is None:
        async with QUEUE_STATE_LOCK:
            enqueue_backtest(backtest_id)
            update_mapping(
                backtest_id,
                {
                    "status": "queued",
                    "queued_at": utc_now(),
                    "required_memory_gb": required_memory_gb,
                    "last_error": "no_capacity",
                },
            )
            return "queued", None

    async with QUEUE_STATE_LOCK:
        if read_queue():
            enqueue_backtest(backtest_id)
            update_mapping(
                backtest_id,
                {
                    "status": "queued",
                    "queued_at": utc_now(),
                    "required_memory_gb": required_memory_gb,
                },
            )
            return "queued", None

        current_reserved = INFLIGHT_MEMORY_GB.get(selection.base_url, 0.0)
        available = selection.metrics.memory_free_gb - current_reserved
        if available < required_memory_gb:
            enqueue_backtest(backtest_id)
            update_mapping(
                backtest_id,
                {
                    "status": "queued",
                    "queued_at": utc_now(),
                    "required_memory_gb": required_memory_gb,
                    "last_error": "insufficient_memory",
                },
            )
            return "queued", None

        reserve_inflight(selection.base_url, required_memory_gb)

    try:
        backtest_docker_run_id = await asyncio.to_thread(
            submit_to_backtest,
            selection.base_url,
            backtest_id,
            run_spec_bytes,
            strategy_filename,
            strategy_bytes,
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
        return "submitted", backtest_docker_run_id
    finally:
        async with QUEUE_STATE_LOCK:
            release_inflight(selection.base_url, required_memory_gb)


async def queue_scheduler() -> None:
    while True:
        try:
            async with QUEUE_STATE_LOCK:
                queued = read_queue()
            if not queued:
                await asyncio.sleep(QUEUE_POLL_INTERVAL_SECONDS)
                continue
            made_progress = False
            while True:
                async with QUEUE_STATE_LOCK:
                    queued = read_queue()
                    if not queued:
                        break
                    head = queued[0]

                backtest_id = head["backtest_id"]
                try:
                    run_spec_payload = await asyncio.to_thread(load_run_spec_payload, backtest_id)
                    required_memory_gb = required_memory_gb_from_run_spec(run_spec_payload)
                    symbol_count = len(run_spec_payload["symbols"])
                except Exception as exc:
                    update_mapping(
                        backtest_id,
                        {"status": "queued", "last_error": str(exc), "last_error_at": utc_now()},
                    )
                    logger.warning("queue_scheduler_load_failed backtest_id=%s error=%s", backtest_id, exc)
                    break

                selection = await pick_backtest_target(required_memory_gb, symbol_count)
                if selection is None:
                    break

                item = None
                async with QUEUE_STATE_LOCK:
                    queued = read_queue()
                    if not queued or queued[0].get("backtest_id") != backtest_id:
                        continue

                    current_reserved = INFLIGHT_MEMORY_GB.get(selection.base_url, 0.0)
                    available = selection.metrics.memory_free_gb - current_reserved
                    if available < required_memory_gb:
                        break

                    item = dequeue_backtest()
                    if not item:
                        continue

                    reserve_inflight(selection.base_url, required_memory_gb)

                try:
                    run_spec_bytes, strategy_filename, strategy_bytes, runner_bytes = await asyncio.to_thread(
                        load_run_assets, backtest_id
                    )
                    backtest_docker_run_id = await asyncio.to_thread(
                        submit_to_backtest,
                        selection.base_url,
                        backtest_id,
                        run_spec_bytes,
                        strategy_filename,
                        strategy_bytes,
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
                except Exception as exc:
                    # Put back to queue head to retry later.
                    if item:
                        async with QUEUE_STATE_LOCK:
                            items = read_queue()
                            items.insert(0, item)
                            write_queue(items)
                    update_mapping(
                        backtest_id,
                        {"status": "queued", "last_error": str(exc), "last_error_at": utc_now()},
                    )
                    logger.warning("queue_scheduler_submit_failed backtest_id=%s error=%s", backtest_id, exc)
                    break
                finally:
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


@app.post("/runs")
async def create_run(
    run_spec: UploadFile = File(...),
    strategy_file: UploadFile = File(...),
) -> JSONResponse:
    # 入口：research docker 上传 run_spec.json 与策略文件（multipart/form-data）
    logger.info(
        "create_run_received run_spec=%s strategy_file=%s",
        run_spec.filename or "-",
        strategy_file.filename or "-",
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

    try:
        # 2) 校验 run_spec 内容（字段、时间范围、费用等）
        run_spec_payload = validate_run_spec(payload)
    except ValueError as exc:
        logger.warning("create_run_spec_validation_failed error=%s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    required_memory_gb = required_memory_gb_from_run_spec(run_spec_payload)
    symbol_count = len(run_spec_payload["symbols"])

    # 3) 生成 run_id，并在本地保存上传内容，便于排查与追踪
    backtest_id = make_run_id()
    logger.info("create_run_generated backtest_id=%s", backtest_id)

    run_dir = RUN_STORAGE_PATH / backtest_id
    run_dir.mkdir(parents=True, exist_ok=True)

    requested_by = run_spec_payload.get("requested_by")
    if isinstance(requested_by, str) and requested_by:
        update_mapping(backtest_id, {"requested_by": requested_by})

    # 4) 保存策略文件（只保留文件名，避免路径穿越）
    strategy_filename = Path(strategy_file.filename or "strategy.py").name
    strategy_bytes = await strategy_file.read()
    if not strategy_bytes:
        logger.warning("create_run_empty_strategy backtest_id=%s", backtest_id)
        raise HTTPException(status_code=400, detail="strategy_file is empty")
    strategy_path = run_dir / strategy_filename
    strategy_path.write_bytes(strategy_bytes)

    # 5) 将 run_spec 中的 strategy_file 更新为实际保存的文件名，并落盘
    run_spec_payload["strategy_file"] = strategy_filename
    run_spec_payload["backtest_id"] = backtest_id
    run_spec_path = run_dir / "run_spec.json"
    run_spec_bytes = json.dumps(run_spec_payload, ensure_ascii=True, indent=2).encode("utf-8")
    run_spec_path.write_bytes(run_spec_bytes)
    logger.info(
        "create_run_saved backtest_id=%s strategy_filename=%s run_spec_bytes=%s strategy_bytes=%s",
        backtest_id,
        strategy_filename,
        len(run_spec_bytes),
        len(strategy_bytes),
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
        items = read_queue()
        total = len(items)
        sliced = items[offset : offset + limit] if limit else items[offset:]
        pos = None
        if backtest_id:
            for idx, item in enumerate(items):
                if item.get("backtest_id") == backtest_id:
                    pos = idx
                    break
        position_1_based = (pos + 1) if pos is not None else None
    return JSONResponse(
        {
            "count": total,
            "items": sliced,
            "position": position_1_based,
        }
    )


@app.delete("/queue")
async def delete_queue(payload: dict[str, Any] = Body(...)) -> JSONResponse:
    backtest_ids = payload.get("backtest_ids")
    if not isinstance(backtest_ids, list) or not all(isinstance(x, str) and x for x in backtest_ids):
        raise HTTPException(status_code=400, detail="backtest_ids must be a non-empty list of strings")

    async with QUEUE_STATE_LOCK:
        removed = remove_from_queue_batch(backtest_ids)

    with MAPPING_LOCK:
        mapping = read_mapping()

    not_queued: list[str] = []
    not_found: list[str] = []
    for bid in backtest_ids:
        if bid in removed:
            update_mapping(bid, {"status": "cancelled", "cancelled_at": utc_now()})
            continue
        if bid in mapping:
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
    with MAPPING_LOCK:
        mapping = read_mapping()
    entry = mapping.get(backtest_id)
    if not entry:
        logger.warning("get_run_not_found backtest_id=%s", backtest_id)
        raise HTTPException(status_code=404, detail="backtest_id not found")
    if isinstance(entry, dict):
        payload = {"backtest_id": backtest_id, **entry}
    else:
        payload = {"backtest_id": backtest_id, "backtest_docker_run_id": entry}
    return JSONResponse(payload)


@app.get("/runs/backtest/reports")
async def get_backtest_reports(
    page: int = 1,
    limit: int = 10,
    requested_by: str | None = None,
) -> JSONResponse:
    if page < 1 or limit < 1:
        raise HTTPException(status_code=400, detail="page/limit must be >= 1")
    if limit > MAX_REPORT_PAGE_SIZE:
        raise HTTPException(status_code=400, detail=f"limit exceeds max size {MAX_REPORT_PAGE_SIZE}")

    with MAPPING_LOCK:
        mapping = read_mapping()
    service = get_report_service()
    try:
        items = service.get_report_page(
            page=page,
            limit=limit,
            requested_by=requested_by,
            mapping=mapping,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(
        {
            "page": page,
            "limit": limit,
            "count": len(items),
            "items": items,
        }
    )


@app.get("/runs/backtest/{backtest_id}")
async def get_backtest_status(backtest_id: str) -> JSONResponse:
    logger.info("get_backtest_status_requested backtest_id=%s", backtest_id)
    entry = ensure_backtest_routable(backtest_id)
    base_url = entry["backtest_api_base"]
    payload = fetch_backtest_status(base_url, backtest_id)
    return JSONResponse(payload)


@app.get("/runs/backtest/{backtest_id}/report")
async def get_backtest_report(backtest_id: str) -> JSONResponse:
    logger.info("get_backtest_report_requested backtest_id=%s", backtest_id)
    service = get_report_service()
    try:
        payload = service.get_report(backtest_id, allow_running_only=False)
    except ReportHttpError as exc:
        logger.warning(
            "fetch_backtest_report_http_error backtest_id=%s status=%s detail=%s",
            backtest_id,
            exc.status_code,
            exc.detail[:200] if exc.detail else "",
        )
        raise HTTPException(status_code=exc.status_code, detail=exc.detail or "backtest error") from exc
    except ReportInvalidPayload as exc:
        logger.warning("fetch_backtest_report_invalid_payload backtest_id=%s error=%s", backtest_id, exc)
        raise HTTPException(status_code=502, detail="backtest report invalid response") from exc
    except ReportFetchError as exc:
        logger.exception("fetch_backtest_report_failed backtest_id=%s", backtest_id)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("fetch_backtest_report_failed backtest_id=%s", backtest_id)
        raise HTTPException(status_code=502, detail=str(exc)) from exc

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
    entry = ensure_backtest_routable(backtest_id, allow_running_only=False)
    base_url = entry["backtest_api_base"]
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
