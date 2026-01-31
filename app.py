from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import threading
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
from typing import Any
from urllib.parse import urlparse, urlunparse

import aiohttp
from fastapi import FastAPI, File, Header, HTTPException, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response

app = FastAPI()


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
HOST_API_KEY = os.getenv("HOST_API_KEY", "")

BACKTEST_API_BASE = env_or_default("BACKTEST_API_BASE", "http://100.97.194.7:10001")
BACKTEST_API_KEY = os.getenv("BACKTEST_API_KEY", "")
BACKTEST_SUBMIT_PATH = env_or_default("BACKTEST_SUBMIT_PATH", "/v1/scripts/run_backtest")
BACKTEST_LOGS_PATH = env_or_default("BACKTEST_LOGS_PATH", "/v1/runs/backtest/{backtest_id}/logs/download")
BACKTEST_STATUS_PATH = env_or_default("BACKTEST_STATUS_PATH", "/v1/runs/backtest/{backtest_id}")
BACKTEST_WS_LOGS_PATH = env_or_default(
    "BACKTEST_WS_LOGS_PATH",
    "/v1/runs/backtest/{backtest_id}/logs/stream",
)

RUNNER_PATH = Path(env_or_default("BACKTEST_RUNNER_PATH", str(BASE_DIR / "scripts" / "run_backtest.py")))

MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "50"))
MAX_RANGE_DAYS = int(os.getenv("MAX_RANGE_DAYS", "90"))

ALLOWED_FIELDS = {
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

REQUIRED_FIELDS = set(ALLOWED_FIELDS)

MAPPING_LOCK = threading.Lock()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


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
    suffix = secrets.token_hex(3)
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
    for field in ALLOWED_FIELDS:
        sanitized[field] = payload[field]
    return sanitized


def require_api_key(api_key: str | None) -> None:
    if not HOST_API_KEY:
        raise HTTPException(status_code=500, detail="HOST_API_KEY not configured")
    if not api_key or api_key != HOST_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


def read_mapping() -> dict[str, Any]:
    if not RUN_MAPPING_PATH.exists():
        return {}
    with RUN_MAPPING_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_mapping(mapping: dict[str, Any]) -> None:
    RUN_MAPPING_PATH.parent.mkdir(parents=True, exist_ok=True)
    with RUN_MAPPING_PATH.open("w", encoding="utf-8") as handle:
        json.dump(mapping, handle, ensure_ascii=True, indent=2)


def update_mapping(backtest_id: str, backtest_docker_run_id: str) -> None:
    with MAPPING_LOCK:
        mapping = read_mapping()
        mapping[backtest_id] = {"backtest_docker_run_id": backtest_docker_run_id, "created_at": utc_now()}
        write_mapping(mapping)


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


def build_ws_url(base_url: str, path: str) -> str:
    parsed = urlparse(base_url)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    base_path = parsed.path.rstrip("/")
    normalized_path = "/" + path.lstrip("/")
    full_path = f"{base_path}{normalized_path}"
    return urlunparse((scheme, parsed.netloc, full_path, "", "", ""))


def submit_to_backtest(
    backtest_id: str,
    run_spec_bytes: bytes,
    strategy_filename: str,
    strategy_bytes: bytes,
    runner_bytes: bytes,
) -> str:
    url = f"{BACKTEST_API_BASE.rstrip('/')}{BACKTEST_SUBMIT_PATH}"
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


def fetch_backtest_status(backtest_id: str) -> dict[str, Any]:
    url = f"{BACKTEST_API_BASE.rstrip('/')}{BACKTEST_STATUS_PATH.format(backtest_id=backtest_id)}"
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
    }


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

    # 3) 生成 run_id，并在本地保存上传内容，便于排查与追踪
    backtest_id = make_run_id()
    logger.info("create_run_generated backtest_id=%s", backtest_id)

    run_dir = RUN_STORAGE_PATH / backtest_id
    run_dir.mkdir(parents=True, exist_ok=True)

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
    backtest_docker_run_id = submit_to_backtest(
        backtest_id=backtest_id,
        run_spec_bytes=run_spec_bytes,
        strategy_filename=strategy_filename,
        strategy_bytes=strategy_bytes,
        runner_bytes=runner_bytes,
    )

    # 8) 维护 backtest_id -> backtest_docker_run_id 映射，供日志下载与状态查询
    update_mapping(backtest_id, backtest_docker_run_id)
    logger.info("create_run_submitted backtest_id=%s backtest_docker_run_id=%s", backtest_id, backtest_docker_run_id)

    return JSONResponse(
        {
            "backtest_id": backtest_id,
            "backtest_docker_run_id": backtest_docker_run_id,
            "status": "submitted",
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


@app.get("/runs/backtest/{backtest_id}")
async def get_backtest_status(backtest_id: str) -> JSONResponse:
    logger.info("get_backtest_status_requested backtest_id=%s", backtest_id)
    payload = fetch_backtest_status(backtest_id)
    return JSONResponse(payload)


@app.websocket("/runs/backtest/{backtest_id}/logs/stream")
async def stream_backtest_logs(websocket: WebSocket, backtest_id: str) -> None:
    await websocket.accept()
    upstream_url = build_ws_url(
        BACKTEST_API_BASE,
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
    url = f"{BACKTEST_API_BASE.rstrip('/')}{BACKTEST_LOGS_PATH.format(backtest_id=backtest_id)}"
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
