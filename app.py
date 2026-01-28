from __future__ import annotations

import json
import logging
import os
import queue
import secrets
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

app = FastAPI()
logger = logging.getLogger("host_api")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "50"))
MAX_RANGE_DAYS = int(os.getenv("MAX_RANGE_DAYS", "90"))

def env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


DATA_MOUNT_PATH = Path(env_or_default("DATA_MOUNT_PATH", "/opt/backtest"))
STRATEGY_ARTIFACTS_PATH = Path(
    env_or_default("STRATEGY_ARTIFACTS_PATH", str(DATA_MOUNT_PATH / "strategy_artifacts"))
)
BACKTEST_LOGS_PATH = Path(env_or_default("BACKTEST_LOGS_PATH", "/opt/backtest_logs"))
HOST_API_AUDIT_LOG = Path(
    env_or_default("HOST_API_AUDIT_LOG", str(DATA_MOUNT_PATH / "audit" / "host_api_audit.jsonl"))
)

HOST_API_KEY = os.getenv("HOST_API_KEY", "")
BACKTEST_CONTAINER_NAME = env_or_default("BACKTEST_CONTAINER_NAME", "backtest")
BACKTEST_MAX_CONCURRENCY = int(env_or_default("BACKTEST_MAX_CONCURRENCY", "5"))
BACKTEST_PYTHON = env_or_default("BACKTEST_PYTHON", "/usr/local/quantvenv/bin/python")
BACKTEST_RUNNER_PATH = env_or_default("BACKTEST_RUNNER_PATH", "nautilus_trader/scripts/run_backtest.py")

if BACKTEST_MAX_CONCURRENCY < 1:
    BACKTEST_MAX_CONCURRENCY = 1

ALLOWED_FIELDS = {
    "schema_version",
    "requested_by",
    "strategy_artifact",
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

REQUIRED_FIELDS = {
    "schema_version",
    "requested_by",
    "strategy_artifact",
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

RUN_QUEUE: queue.Queue[dict[str, Any]] = queue.Queue()
ACTIVE_RUNS: dict[str, Path] = {}
ACTIVE_LOCK = threading.Lock()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_iso8601(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def parse_decimal_field(field: str, value: object) -> Decimal:
    if isinstance(value, Decimal):
        parsed = value
    elif isinstance(value, (int, float, str)):
        try:
            parsed = Decimal(str(value))
        except Exception as exc:
            raise ValueError(f"{field} must be a decimal-compatible value") from exc
    else:
        raise ValueError(f"{field} must be a decimal-compatible value")
    if parsed < 0:
        raise ValueError(f"{field} must be >= 0")
    return parsed


def ensure_within(base: Path, target: Path) -> None:
    try:
        base_resolved = base.resolve(strict=False)
        target_resolved = target.resolve(strict=False)
    except FileNotFoundError:
        base_resolved = base.resolve()
        target_resolved = target
    if base_resolved not in target_resolved.parents and base_resolved != target_resolved:
        raise ValueError("Path is outside allowed base")


def make_run_id() -> str:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    suffix = secrets.token_hex(3)
    return f"{ts}_{suffix}"


def build_status_payload(run_id: str, run_spec: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "requested_by": run_spec["requested_by"],
        "strategy_entry": run_spec["strategy_entry"],
        "strategy_artifact": run_spec["strategy_artifact"],
        "symbols": run_spec["symbols"],
        "start": run_spec["start"],
        "end": run_spec["end"],
        "tags": run_spec["tags"],
    }


def write_status(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2)


def read_status(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def validate_payload(payload: dict[str, Any]) -> dict[str, Any]:
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

    if not isinstance(payload.get("strategy_config"), dict):
        raise ValueError("strategy_config must be an object")
    if not isinstance(payload.get("chunk_size"), int) or payload["chunk_size"] <= 0:
        raise ValueError("chunk_size must be a positive integer")
    if not isinstance(payload.get("seed"), int):
        raise ValueError("seed must be an integer")
    if not isinstance(payload.get("tags"), dict):
        raise ValueError("tags must be an object")

    start = parse_iso8601(payload["start"])
    end = parse_iso8601(payload["end"])
    if start >= end:
        raise ValueError("start must be before end")
    if end - start > timedelta(days=MAX_RANGE_DAYS):
        raise ValueError("time range exceeds limit")

    strategy_artifact = Path(payload["strategy_artifact"])
    if not strategy_artifact.is_absolute():
        raise ValueError("strategy_artifact must be absolute path")

    wheels_base = STRATEGY_ARTIFACTS_PATH / "wheels"
    ensure_within(wheels_base, strategy_artifact)
    if not strategy_artifact.exists():
        raise ValueError("strategy_artifact not found")

    strategy_entry = payload["strategy_entry"]
    if not isinstance(strategy_entry, str) or ":" not in strategy_entry:
        raise ValueError("strategy_entry must be module:ClassName")

    strategy_config_path = payload["strategy_config_path"]
    if not isinstance(strategy_config_path, str) or ":" not in strategy_config_path:
        raise ValueError("strategy_config_path must be module:ClassName")

    parse_decimal_field("margin_init", payload["margin_init"])
    parse_decimal_field("margin_maint", payload["margin_maint"])
    parse_decimal_field("spot_maker_fee", payload["spot_maker_fee"])
    parse_decimal_field("spot_taker_fee", payload["spot_taker_fee"])
    parse_decimal_field("futures_maker_fee", payload["futures_maker_fee"])
    parse_decimal_field("futures_taker_fee", payload["futures_taker_fee"])

    sanitized: dict[str, Any] = {}
    for field in ALLOWED_FIELDS:
        if field in payload:
            sanitized[field] = payload[field]
    return sanitized


def write_audit_log(run_id: str, requested_by: str, strategy_artifact: str, client_ip: str) -> None:
    HOST_API_AUDIT_LOG.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "run_id": run_id,
        "requested_by": requested_by,
        "strategy_artifact": strategy_artifact,
        "created_at": utc_now(),
        "client_ip": client_ip,
    }
    with open(HOST_API_AUDIT_LOG, "a") as handle:
        handle.write(json.dumps(entry) + "\n")


def active_count() -> int:
    with ACTIVE_LOCK:
        return len(ACTIVE_RUNS)


def reap_finished() -> None:
    with ACTIVE_LOCK:
        items = list(ACTIVE_RUNS.items())

    finished: list[str] = []
    for run_id, status_path in items:
        status = read_status(status_path)
        if status and status.get("status") in {"success", "failed"}:
            finished.append(run_id)

    if finished:
        with ACTIVE_LOCK:
            for run_id in finished:
                ACTIVE_RUNS.pop(run_id, None)


def trigger_backtest(run_id: str, run_spec_path: Path) -> tuple[str, str]:
    trigger_log_path = f"/opt/backtest_logs/{run_id}/trigger.log"
    trigger_cmd = " ".join(
        [
            "uv",
            "run",
            "--python",
            BACKTEST_PYTHON,
            "--no-sync",
            BACKTEST_RUNNER_PATH,
            "--run-spec",
            str(run_spec_path),
        ]
    )
    container_cmd = f"{trigger_cmd} >> {trigger_log_path} 2>&1"
    cmd = [
        "docker",
        "exec",
        "-d",
        "-w",
        "/opt",
        BACKTEST_CONTAINER_NAME,
        "sh",
        "-lc",
        container_cmd,
    ]
    logger.info(
        "Triggering backtest run_id=%s container=%s python=%s cmd=%s",
        run_id,
        BACKTEST_CONTAINER_NAME,
        BACKTEST_PYTHON,
        container_cmd,
    )
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr or result.stdout)
    exec_id = result.stdout.strip()
    return exec_id, trigger_cmd


def enqueue_run(run_id: str, run_spec_path: Path, run_spec: dict[str, Any]) -> None:
    status_path = BACKTEST_LOGS_PATH / run_id / "status.json"
    status = build_status_payload(run_id, run_spec)
    status.update({"status": "queued", "queued_at": utc_now()})
    write_status(status_path, status)
    RUN_QUEUE.put(
        {
            "run_id": run_id,
            "run_spec_path": run_spec_path,
            "status_path": status_path,
            "run_spec": run_spec,
        }
    )
    logger.info(
        "Run queued run_id=%s run_spec_path=%s",
        run_id,
        run_spec_path,
    )


def start_run(task: dict[str, Any]) -> None:
    exec_id, trigger_cmd = trigger_backtest(task["run_id"], task["run_spec_path"])
    status = build_status_payload(task["run_id"], task["run_spec"])
    status.update(
        {
            "status": "running",
            "started_at": utc_now(),
            "triggered_at": utc_now(),
            "exec_id": exec_id,
            "trigger_cmd": trigger_cmd,
            "backtest_python": BACKTEST_PYTHON,
        }
    )
    write_status(task["status_path"], status)
    with ACTIVE_LOCK:
        ACTIVE_RUNS[task["run_id"]] = task["status_path"]
    logger.info(
        "Run started run_id=%s exec_id=%s",
        task["run_id"],
        exec_id,
    )


def worker_loop() -> None:
    while True:
        reap_finished()
        if active_count() >= BACKTEST_MAX_CONCURRENCY:
            time.sleep(1)
            continue
        try:
            task = RUN_QUEUE.get(timeout=1)
        except queue.Empty:
            time.sleep(0.5)
            continue
        try:
            start_run(task)
        except Exception as exc:
            logger.exception(
                "Run failed to start run_id=%s error=%s",
                task.get("run_id"),
                exc,
            )
            status = build_status_payload(task["run_id"], task["run_spec"])
            status.update(
                {
                    "status": "failed",
                    "finished_at": utc_now(),
                    "error": str(exc),
                }
            )
            write_status(task["status_path"], status)
        finally:
            RUN_QUEUE.task_done()


def require_api_key(api_key: str | None) -> None:
    if not HOST_API_KEY:
        raise HTTPException(status_code=500, detail="HOST_API_KEY not configured")
    if not api_key or api_key != HOST_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


@app.on_event("startup")
def start_worker() -> None:
    thread = threading.Thread(target=worker_loop, daemon=True)
    thread.start()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "timestamp": utc_now()}


@app.post("/runs")
async def create_run(request: Request, x_api_key: str | None = Header(default=None)) -> JSONResponse:
    require_api_key(x_api_key)
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON") from exc

    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Invalid payload")

    try:
        run_spec = validate_payload(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    run_id = make_run_id()

    runs_dir = STRATEGY_ARTIFACTS_PATH / "runs" / run_id
    runs_dir.mkdir(parents=True, exist_ok=True)
    run_spec_path = runs_dir / "run_spec.json"

    with open(run_spec_path, "w") as handle:
        json.dump(run_spec, handle, indent=2)

    client_ip = request.client.host if request.client else "unknown"
    write_audit_log(run_id, run_spec["requested_by"], run_spec["strategy_artifact"], client_ip)

    try:
        enqueue_run(run_id, run_spec_path, run_spec)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return JSONResponse(
        {
            "run_id": run_id,
            "status": "queued",
            "run_spec_path": str(run_spec_path),
        }
    )


@app.get("/runs/{run_id}")
async def get_run(run_id: str, x_api_key: str | None = Header(default=None)) -> JSONResponse:
    require_api_key(x_api_key)
    status_file = BACKTEST_LOGS_PATH / run_id / "status.json"
    if not status_file.exists():
        raise HTTPException(status_code=404, detail="run_id not found")
    with open(status_file, "r") as handle:
        payload = json.load(handle)
    return JSONResponse(payload)
