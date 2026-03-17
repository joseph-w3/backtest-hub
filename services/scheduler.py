from __future__ import annotations

from datetime import datetime
import json
import urllib.request
from dataclasses import dataclass
import logging
from typing import Any

BYTES_PER_GB = 1024 * 1024 * 1024
LOGGER = logging.getLogger("backtest_hub.scheduler")

V5_RUNTIME_UNIVERSE_STRATEGY_ENTRY = (
    "strategies.spread_arb.v5_runtime_universe:SpreadArbV5RuntimeUniverse"
)
V5_SMALL_RUN_MEMORY_PER_SYMBOL_GB = 0.65
V5_LARGE_OPTIMIZED_MEMORY_PER_SYMBOL_GB = 0.8
V5_LARGE_RUN_SYMBOL_THRESHOLD = 40
V5_LARGE_RUN_MIN_SYMBOLS = 20
V5_LARGE_RUN_DURATION_DAYS = 30


@dataclass(frozen=True)
class DockerMetrics:
    base_url: str
    cpu_percent: float | None
    memory_total_gb: float
    memory_used_gb: float
    memory_free_gb: float


@dataclass(frozen=True)
class DockerSelection:
    base_url: str
    metrics: DockerMetrics
    running_count: int | None
    available_memory_gb: float


def parse_per_worker_max_running(raw: str) -> dict[str, int]:
    """Parse per-worker max running config string.

    Format: "http://host1:port=5,http://host2:port=3"
    Entries without '=' are silently skipped.
    """
    result: dict[str, int] = {}
    if not raw.strip():
        return result
    for part in raw.split(","):
        part = part.strip()
        if "=" not in part:
            continue
        url, val = part.rsplit("=", 1)
        result[url.strip().rstrip("/")] = int(val.strip())
    return result


def normalize_base_url(url: str) -> str:
    return url.rstrip("/")


def normalize_join_url(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}/" + path.lstrip("/")


def parse_backtest_api_bases(raw: str | None, fallback: str) -> list[str]:
    bases: list[str] = []
    if raw:
        for part in raw.split(","):
            value = part.strip()
            if not value:
                continue
            base = normalize_base_url(value)
            if base and base not in bases:
                bases.append(base)
    if not bases:
        base = normalize_base_url(fallback)
        if base:
            bases.append(base)
    return bases


def _coerce_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except Exception:
            return None
    return None


def _first_float(payload: dict[str, Any], keys: list[str]) -> float | None:
    for key in keys:
        if key in payload:
            value = _coerce_float(payload.get(key))
            if value is not None:
                return value
    return None


def _bytes_to_gb(value: float) -> float:
    return value / BYTES_PER_GB


def parse_metrics_payload(payload: dict[str, Any], base_url: str) -> DockerMetrics | None:
    if not isinstance(payload, dict):
        return None

    cpu_percent = _first_float(payload, ["cpu_percent", "cpu", "cpu_pct"])

    total_gb = _first_float(payload, ["memory_total_gb", "mem_total_gb"])
    used_gb = _first_float(payload, ["memory_used_gb", "mem_used_gb"])
    free_gb = _first_float(payload, ["memory_free_gb", "mem_free_gb"])

    total_bytes = _first_float(payload, ["memory_total_bytes", "mem_total_bytes"])
    used_bytes = _first_float(payload, ["memory_used_bytes", "mem_used_bytes"])
    free_bytes = _first_float(payload, ["memory_free_bytes", "mem_free_bytes"])

    if total_gb is None and total_bytes is not None:
        total_gb = _bytes_to_gb(total_bytes)
    if used_gb is None and used_bytes is not None:
        used_gb = _bytes_to_gb(used_bytes)
    if free_gb is None and free_bytes is not None:
        free_gb = _bytes_to_gb(free_bytes)

    if total_gb is None:
        return None

    if free_gb is None and used_gb is not None:
        free_gb = total_gb - used_gb
    if used_gb is None and free_gb is not None:
        used_gb = total_gb - free_gb

    if used_gb is None or free_gb is None:
        return None

    used_gb = max(0.0, used_gb)
    free_gb = max(0.0, free_gb)
    total_gb = max(0.0, total_gb)

    return DockerMetrics(
        base_url=base_url,
        cpu_percent=cpu_percent,
        memory_total_gb=total_gb,
        memory_used_gb=used_gb,
        memory_free_gb=free_gb,
    )


def fetch_metrics(
    base_url: str,
    metrics_path: str,
    headers: dict[str, str],
    timeout_seconds: float,
) -> DockerMetrics:
    url = normalize_join_url(base_url, metrics_path)
    LOGGER.info("scheduler_fetch_metrics_start base=%s url=%s", base_url, url)
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
        body = resp.read()
    payload = json.loads(body.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("metrics payload must be an object")
    metrics = parse_metrics_payload(payload, base_url)
    if metrics is None:
        raise ValueError("metrics payload missing memory fields")
    LOGGER.info(
        "scheduler_fetch_metrics_ok base=%s cpu_pct=%s mem_total_gb=%.3f mem_used_gb=%.3f mem_free_gb=%.3f",
        base_url,
        metrics.cpu_percent,
        metrics.memory_total_gb,
        metrics.memory_used_gb,
        metrics.memory_free_gb,
    )
    return metrics


def fetch_backtest_runs(
    base_url: str,
    runs_path: str,
    headers: dict[str, str],
    timeout_seconds: float,
) -> dict[str, Any]:
    url = normalize_join_url(base_url, runs_path)
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
        body = resp.read()
    payload = json.loads(body.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("runs payload must be an object")
    return payload


def count_running_from_runs_payload(payload: dict[str, Any]) -> int:
    runs = payload.get("runs")
    if not isinstance(runs, list):
        raise ValueError("invalid runs payload")
    running = 0
    for run in runs:
        if isinstance(run, dict) and run.get("status") == "running":
            running += 1
    return running


def _parse_iso_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _run_duration_days(payload: dict[str, Any]) -> float | None:
    start_dt = _parse_iso_time(payload.get("start"))
    end_dt = _parse_iso_time(payload.get("end"))
    if start_dt is None or end_dt is None:
        return None
    return (end_dt - start_dt).total_seconds() / 86_400


def _is_v5_runtime_universe(payload: dict[str, Any]) -> bool:
    return payload.get("strategy_entry") == V5_RUNTIME_UNIVERSE_STRATEGY_ENTRY


def _is_large_v5_runtime_universe_run(payload: dict[str, Any], symbol_count: int) -> bool:
    if not _is_v5_runtime_universe(payload):
        return False
    if symbol_count >= V5_LARGE_RUN_SYMBOL_THRESHOLD:
        return True
    duration_days = _run_duration_days(payload)
    if duration_days is None:
        return False
    return symbol_count >= V5_LARGE_RUN_MIN_SYMBOLS and duration_days >= V5_LARGE_RUN_DURATION_DAYS


def _v5_runtime_universe_optimized_loading_enabled(
    payload: dict[str, Any],
    symbol_count: int,
) -> bool:
    value = payload.get("optimize_file_loading")
    if isinstance(value, bool):
        return value
    return _is_large_v5_runtime_universe_run(payload, symbol_count)


def required_memory_gb_from_run_spec(
    payload: dict[str, Any],
    memory_per_symbol_gb: float = 1.0,
) -> float:
    symbols = payload.get("symbols")
    if not isinstance(symbols, list) or not symbols:
        raise ValueError("symbols must be a non-empty list")
    symbol_count = len(symbols)

    if _is_v5_runtime_universe(payload):
        if _is_large_v5_runtime_universe_run(payload, symbol_count):
            if _v5_runtime_universe_optimized_loading_enabled(payload, symbol_count):
                return float(symbol_count) * V5_LARGE_OPTIMIZED_MEMORY_PER_SYMBOL_GB
            return float(symbol_count) * memory_per_symbol_gb
        return float(symbol_count) * V5_SMALL_RUN_MEMORY_PER_SYMBOL_GB

    return float(symbol_count) * memory_per_symbol_gb


def select_backtest_docker(
    base_urls: list[str],
    metrics_path: str,
    runs_path: str | None,
    headers: dict[str, str],
    required_memory_gb: float,
    reserved_memory_gb: dict[str, float] | None,
    inflight_counts: dict[str, int] | None,
    max_running: int | None,
    timeout_seconds: float,
    cpu_percent_lt: float | None = None,
    require_memory_gt: bool = False,
    max_running_per_worker: dict[str, int] | None = None,
) -> DockerSelection | None:
    reserved_memory_gb = reserved_memory_gb or {}
    inflight_counts = inflight_counts or {}

    LOGGER.info(
        "scheduler_select_start required_memory_gb=%.3f base_urls=%s",
        required_memory_gb,
        base_urls,
    )
    candidates: list[DockerSelection] = []

    for base_url in base_urls:
        try:
            metrics = fetch_metrics(base_url, metrics_path, headers, timeout_seconds)
        except Exception as exc:
            LOGGER.info("scheduler_metrics_failed base=%s error=%s", base_url, exc)
            continue

        if cpu_percent_lt is not None:
            if metrics.cpu_percent is None or metrics.cpu_percent >= cpu_percent_lt:
                LOGGER.info(
                    "scheduler_skip_cpu_threshold base=%s cpu_pct=%s threshold=%s",
                    base_url,
                    metrics.cpu_percent,
                    cpu_percent_lt,
                )
                continue

        reserved = reserved_memory_gb.get(base_url, 0.0)
        available = metrics.memory_free_gb - reserved
        LOGGER.info(
            "scheduler_metrics_snapshot base=%s reserved_gb=%.3f available_gb=%.3f",
            base_url,
            reserved,
            available,
        )
        if require_memory_gt:
            if available <= required_memory_gb:
                LOGGER.info(
                    "scheduler_skip_insufficient_memory_gt base=%s required_gb=%.3f available_gb=%.3f",
                    base_url,
                    required_memory_gb,
                    available,
                )
                continue
        elif available < required_memory_gb:
            LOGGER.info(
                "scheduler_skip_insufficient_memory base=%s required_gb=%.3f available_gb=%.3f",
                base_url,
                required_memory_gb,
                available,
            )
            continue

        running_count = None
        effective_max = (max_running_per_worker or {}).get(base_url, max_running)
        if effective_max is not None and runs_path:
            try:
                runs_payload = fetch_backtest_runs(base_url, runs_path, headers, timeout_seconds)
                running_count = count_running_from_runs_payload(runs_payload)
            except Exception as exc:
                LOGGER.info("scheduler_runs_failed base=%s error=%s", base_url, exc)
                continue
            inflight = inflight_counts.get(base_url, 0)
            if running_count + inflight >= effective_max:
                LOGGER.info(
                    "scheduler_skip_running_limit base=%s running=%s inflight=%s effective_max=%s",
                    base_url,
                    running_count,
                    inflight,
                    effective_max,
                )
                continue

        candidates.append(
            DockerSelection(
                base_url=base_url,
                metrics=metrics,
                running_count=running_count,
                available_memory_gb=available,
            )
        )

    if not candidates:
        LOGGER.info("scheduler_no_candidates required_memory_gb=%.3f", required_memory_gb)
        return None

    def sort_key(selection: DockerSelection) -> tuple[float, float]:
        cpu_rank = selection.metrics.cpu_percent if selection.metrics.cpu_percent is not None else 1000.0
        return (selection.available_memory_gb, -cpu_rank)

    candidates.sort(key=sort_key, reverse=True)
    selected = candidates[0]
    LOGGER.info(
        "scheduler_selected base=%s available_gb=%.3f cpu_pct=%s running=%s",
        selected.base_url,
        selected.available_memory_gb,
        selected.metrics.cpu_percent,
        selected.running_count,
    )
    return selected
