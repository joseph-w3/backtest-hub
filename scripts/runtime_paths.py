from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Any

RUNTIME_ROOT_PLACEHOLDER = "__RUNTIME_ROOT__"
BACKTEST_RUNTIME_PATH_FIELDS = (
    "runtime_universe_commands_path",
    "state_checkpoint_path",
    "heartbeat_file_path",
    "alert_file_path",
    "kill_switch_path",
    "dust_residual_file_path",
)


def _normalize_runtime_path_value(value: str, runtime_root: Path) -> str:
    stripped = value.strip()
    if not stripped:
        return value
    if RUNTIME_ROOT_PLACEHOLDER in stripped:
        return stripped.replace(RUNTIME_ROOT_PLACEHOLDER, runtime_root.as_posix())
    basename = PurePosixPath(stripped).name
    if not basename:
        basename = "runtime-artifact"
    return (runtime_root / basename).as_posix()


def normalize_backtest_runtime_paths(
    strategy_config: dict[str, Any],
    *,
    runtime_root: Path,
) -> tuple[dict[str, Any], dict[str, str]]:
    """Rewrite known runtime file paths to a per-backtest root.

    Backtests must not share operational runtime files such as checkpoints,
    heartbeats, or command files. This helper rewrites any configured path-like
    values for the known spread-arb runtime controls into a unique root for the
    current backtest, while preserving empty / disabled fields.
    """

    normalized = dict(strategy_config)
    rewritten: dict[str, str] = {}
    for key in BACKTEST_RUNTIME_PATH_FIELDS:
        raw = normalized.get(key)
        if not isinstance(raw, str) or not raw.strip():
            continue
        candidate = _normalize_runtime_path_value(raw, runtime_root)
        if candidate == raw:
            continue
        normalized[key] = candidate
        rewritten[key] = candidate
    return normalized, rewritten
