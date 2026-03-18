from __future__ import annotations

from dataclasses import dataclass
from typing import Any


DEFAULT_CATALOG_PREWARM_THREADS = 50
DEFAULT_CATALOG_PREFETCH_AHEAD_HOURS = 72
DEFAULT_CATALOG_PREFETCH_MAX_FILES_PER_BATCH = 4

ALLOWED_PREFETCH_BACKENDS = frozenset({"off", "local-read"})
ALLOWED_CATALOG_CONTROL_KEYS = frozenset(
    {
        "prewarm_before_run",
        "prewarm_threads",
        "prefetch_backend",
        "prefetch_ahead_hours",
        "prefetch_max_files_per_batch",
    }
)


@dataclass(frozen=True)
class CatalogControls:
    prewarm_before_run: bool = False
    prewarm_threads: int = DEFAULT_CATALOG_PREWARM_THREADS
    prefetch_backend: str | None = None
    prefetch_ahead_hours: int | None = None
    prefetch_max_files_per_batch: int | None = None


def parse_catalog_controls(value: object) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("catalog_controls must be an object")

    unknown = set(value.keys()) - ALLOWED_CATALOG_CONTROL_KEYS
    if unknown:
        raise ValueError(f"catalog_controls has unknown keys: {sorted(unknown)}")

    parsed: dict[str, Any] = {}

    if "prewarm_before_run" in value:
        raw = value["prewarm_before_run"]
        if not isinstance(raw, bool):
            raise ValueError("catalog_controls.prewarm_before_run must be a boolean")
        parsed["prewarm_before_run"] = raw

    if "prewarm_threads" in value:
        raw = value["prewarm_threads"]
        if isinstance(raw, bool) or not isinstance(raw, int) or raw <= 0:
            raise ValueError("catalog_controls.prewarm_threads must be a positive integer")
        parsed["prewarm_threads"] = raw

    if "prefetch_backend" in value:
        raw = value["prefetch_backend"]
        if not isinstance(raw, str) or not raw.strip():
            raise ValueError("catalog_controls.prefetch_backend must be a non-empty string")
        normalized = raw.strip().lower()
        if normalized not in ALLOWED_PREFETCH_BACKENDS:
            raise ValueError(
                "catalog_controls.prefetch_backend must be one of: "
                f"{sorted(ALLOWED_PREFETCH_BACKENDS)}"
            )
        parsed["prefetch_backend"] = normalized

    if "prefetch_ahead_hours" in value:
        raw = value["prefetch_ahead_hours"]
        if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
            raise ValueError(
                "catalog_controls.prefetch_ahead_hours must be a non-negative integer"
            )
        parsed["prefetch_ahead_hours"] = raw

    if "prefetch_max_files_per_batch" in value:
        raw = value["prefetch_max_files_per_batch"]
        if isinstance(raw, bool) or not isinstance(raw, int) or raw <= 0:
            raise ValueError(
                "catalog_controls.prefetch_max_files_per_batch must be a positive integer"
            )
        parsed["prefetch_max_files_per_batch"] = raw

    return parsed


def resolve_catalog_controls(run_spec: dict[str, Any]) -> CatalogControls:
    parsed = parse_catalog_controls(run_spec.get("catalog_controls"))
    return CatalogControls(
        prewarm_before_run=bool(parsed.get("prewarm_before_run", False)),
        prewarm_threads=int(
            parsed.get("prewarm_threads", DEFAULT_CATALOG_PREWARM_THREADS)
        ),
        prefetch_backend=parsed.get("prefetch_backend"),
        prefetch_ahead_hours=parsed.get("prefetch_ahead_hours"),
        prefetch_max_files_per_batch=parsed.get("prefetch_max_files_per_batch"),
    )
