from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass
from typing import Any

BYTES_PER_GB = 1024 * 1024 * 1024


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
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
        body = resp.read()
    payload = json.loads(body.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("metrics payload must be an object")
    metrics = parse_metrics_payload(payload, base_url)
    if metrics is None:
        raise ValueError("metrics payload missing memory fields")
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


def required_memory_gb_from_run_spec(payload: dict[str, Any]) -> float:
    symbols = payload.get("symbols")
    if not isinstance(symbols, list) or not symbols:
        raise ValueError("symbols must be a non-empty list")
    return float(len(symbols))


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
) -> DockerSelection | None:
    reserved_memory_gb = reserved_memory_gb or {}
    inflight_counts = inflight_counts or {}

    candidates: list[DockerSelection] = []

    for base_url in base_urls:
        try:
            metrics = fetch_metrics(base_url, metrics_path, headers, timeout_seconds)
        except Exception:
            continue

        reserved = reserved_memory_gb.get(base_url, 0.0)
        available = metrics.memory_free_gb - reserved
        if available < required_memory_gb:
            continue

        running_count = None
        if max_running is not None and runs_path:
            try:
                runs_payload = fetch_backtest_runs(base_url, runs_path, headers, timeout_seconds)
                running_count = count_running_from_runs_payload(runs_payload)
            except Exception:
                continue
            inflight = inflight_counts.get(base_url, 0)
            if running_count + inflight >= max_running:
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
        return None

    def sort_key(selection: DockerSelection) -> tuple[float, float]:
        cpu_rank = selection.metrics.cpu_percent if selection.metrics.cpu_percent is not None else 1000.0
        return (selection.available_memory_gb, -cpu_rank)

    candidates.sort(key=sort_key, reverse=True)
    return candidates[0]
