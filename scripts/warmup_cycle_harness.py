from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path
import stat
import sys
from typing import Any
import zipfile


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CATALOG_ROOT = "/mnt/localB2fs/backtest/catalog"
DEFAULT_PREFETCH_AHEAD_HOURS = 72
DEFAULT_PREFETCH_MAX_FILES_PER_BATCH = 4
DEFAULT_PREWARM_THREADS = 50
DEFAULT_CHUNK_SIZE = 200000
DEFAULT_SCHEMA_VERSION = "1.0"
DEFAULT_REQUESTED_BY = "researcher_001"
DEFAULT_SEED = 12345
DEFAULT_MARGIN_INIT = 0.05
DEFAULT_MARGIN_MAINT = 0.025
DEFAULT_FEE = 0.001

STRATEGY_ENTRY = "strategies.simple_bundle_demo:SimpleBundleDemo"
STRATEGY_CONFIG_PATH = "strategies.simple_bundle_demo:SimpleBundleDemoConfig"
BUNDLE_FILENAME = "strategies-harness.zip"
BUNDLE_FILES = (
    "strategies/__init__.py",
    "strategies/simple_bundle_demo.py",
    "strategies/utils/__init__.py",
    "strategies/utils/common.py",
)


@dataclass(frozen=True)
class HarnessMode:
    name: str
    prewarm_before_run: bool
    replay_prefetch_backend: str
    description: str
    expected_primary_effect: str


MODES: tuple[HarnessMode, ...] = (
    HarnessMode(
        name="no_warmup",
        prewarm_before_run=False,
        replay_prefetch_backend="off",
        description="No prewarm before launch and no replay-time prefetch.",
        expected_primary_effect="Pure cold-start baseline for prepare and replay.",
    ),
    HarnessMode(
        name="prewarm_gate_only",
        prewarm_before_run=True,
        replay_prefetch_backend="off",
        description="Complete manifest-based prewarm before launching the runner.",
        expected_primary_effect="Should mainly improve submit -> before_query_result / first_chunk.",
    ),
    HarnessMode(
        name="replay_prefetch_only",
        prewarm_before_run=False,
        replay_prefetch_backend="local-read",
        description="No launch-time prewarm, only replay-time sliding-window prefetch.",
        expected_primary_effect="Should mainly improve first_chunk -> steady replay.",
    ),
    HarnessMode(
        name="prewarm_gate_plus_replay_prefetch",
        prewarm_before_run=True,
        replay_prefetch_backend="local-read",
        description="Launch-time prewarm plus replay-time sliding-window prefetch.",
        expected_primary_effect="Should improve both startup and post-first-chunk replay.",
    ),
)


def _parse_spot_symbols(raw: str) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()
    for item in raw.split(","):
        symbol = item.strip().upper()
        if not symbol:
            continue
        if symbol.endswith("-PERP"):
            raise ValueError(
                f"spot symbols must not include -PERP suffix: {symbol}"
            )
        if not symbol.endswith("USDT"):
            raise ValueError(
                f"spot symbols must be explicit USDT spot symbols, e.g. ACTUSDT: {symbol}"
            )
        if symbol in seen:
            raise ValueError(f"duplicate spot symbol: {symbol}")
        seen.add(symbol)
        symbols.append(symbol)
    if not symbols:
        raise ValueError("at least one spot symbol is required")
    return symbols


def _expand_symbol_universe(spot_symbols: list[str]) -> list[str]:
    symbols: list[str] = []
    for symbol in spot_symbols:
        symbols.append(symbol)
        symbols.append(f"{symbol}-PERP")
    return symbols


def _bundle_source_file(relative_path: str) -> Path:
    source = REPO_ROOT / relative_path
    if not source.is_file():
        raise FileNotFoundError(f"bundle source file missing: {source}")
    return source


def write_strategy_bundle(bundle_path: Path) -> None:
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for relative_path in BUNDLE_FILES:
            source = _bundle_source_file(relative_path)
            zf.write(source, relative_path)


def _base_run_spec(
    *,
    backtest_id: str,
    symbols: list[str],
    start: str,
    end: str,
    bundle_relative_path: str,
    mode: HarnessMode,
    load_trade_ticks: bool,
    optimize_file_loading: bool,
) -> dict[str, Any]:
    return {
        "backtest_id": backtest_id,
        "schema_version": DEFAULT_SCHEMA_VERSION,
        "requested_by": DEFAULT_REQUESTED_BY,
        "strategy_entry": STRATEGY_ENTRY,
        "strategy_config_path": STRATEGY_CONFIG_PATH,
        "strategy_config": {
            "log_on_start": True,
        },
        "margin_init": DEFAULT_MARGIN_INIT,
        "margin_maint": DEFAULT_MARGIN_MAINT,
        "spot_maker_fee": DEFAULT_FEE,
        "spot_taker_fee": DEFAULT_FEE,
        "futures_maker_fee": DEFAULT_FEE,
        "futures_taker_fee": DEFAULT_FEE,
        "symbols": symbols,
        "start": start,
        "end": end,
        "chunk_size": DEFAULT_CHUNK_SIZE,
        "seed": DEFAULT_SEED,
        "tags": {
            "purpose": "warmup_cycle_harness",
            "mode": mode.name,
            "pair_count": len(symbols) // 2,
            "symbol_count": len(symbols),
            "strategy": "simple_bundle_demo",
        },
        "strategy_bundle": bundle_relative_path,
        "load_trade_ticks": load_trade_ticks,
        "optimize_file_loading": optimize_file_loading,
    }


def _render_env(mode: HarnessMode, args: argparse.Namespace) -> str:
    lines = [
        "# Generated by scripts/warmup_cycle_harness.py",
        "set -a",
        f'HARNESS_MODE="{mode.name}"',
        f'HARNESS_PYTHON="{sys.executable}"',
        f'HARNESS_RUNNER_PATH="{(REPO_ROOT / "scripts" / "run_backtest.py").as_posix()}"',
        f'HARNESS_PREWARM_SCRIPT="{(REPO_ROOT / "scripts" / "prewarm_catalog_cache.py").as_posix()}"',
        f'HARNESS_CATALOG_ROOT="{args.catalog_root}"',
        f"HARNESS_PREWARM_REQUIRED={'1' if mode.prewarm_before_run else '0'}",
        f"HARNESS_PREWARM_THREADS={args.prewarm_threads}",
        f"BACKTEST_PREFETCH_BACKEND={mode.replay_prefetch_backend}",
        f"BACKTEST_PREFETCH_AHEAD_HOURS={args.prefetch_ahead_hours}",
        f"BACKTEST_PREFETCH_MAX_FILES_PER_BATCH={args.prefetch_max_files_per_batch}",
        "set +a",
        "",
    ]
    return "\n".join(lines)


def _render_run_script() -> str:
    return """#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/env.sh"

RUN_SPEC="${SCRIPT_DIR}/run_spec.json"
MANIFEST="${SCRIPT_DIR}/catalog-prewarm-manifest.txt"
export CATALOG_PATH="${HARNESS_CATALOG_ROOT}"
export BACKTEST_LOGS_PATH="${SCRIPT_DIR}/logs"
mkdir -p "${BACKTEST_LOGS_PATH}"

if [[ "${HARNESS_PREWARM_REQUIRED}" == "1" ]]; then
  "${HARNESS_PYTHON}" "${HARNESS_PREWARM_SCRIPT}" \
    --run-spec "${RUN_SPEC}" \
    --catalog-root "${HARNESS_CATALOG_ROOT}" \
    --manifest "${MANIFEST}" \
    --warmup \
    --threads "${HARNESS_PREWARM_THREADS}"
fi

"${HARNESS_PYTHON}" "${HARNESS_RUNNER_PATH}" --run-spec "${RUN_SPEC}"
"""


def _render_readme(
    *,
    spot_symbols: list[str],
    start: str,
    end: str,
    load_trade_ticks: bool,
    optimize_file_loading: bool,
) -> str:
    mode_lines = []
    for mode in MODES:
        mode_lines.extend(
            [
                f"- `{mode.name}`",
                f"  - {mode.description}",
                f"  - Expected effect: {mode.expected_primary_effect}",
            ]
        )

    return "\n".join(
        [
            "# Warmup Cycle Harness",
            "",
            "This directory isolates launch-time prewarm from replay-time prefetch.",
            "",
            f"- Spot symbols: {', '.join(spot_symbols)}",
            f"- Start: `{start}`",
            f"- End: `{end}`",
            f"- load_trade_ticks: `{str(load_trade_ticks).lower()}`",
            f"- optimize_file_loading: `{str(optimize_file_loading).lower()}`",
            "",
            "Generated modes:",
            *mode_lines,
            "",
            "Usage:",
            "",
            "1. Run each mode on the same worker shape and same catalog root.",
            "2. If you want a true cold-start comparison, put each run on a fresh or explicitly reset cache state.",
            "3. Compare `before_query_result`, `first_chunk`, and post-first-chunk replay speed separately.",
            "4. Each mode writes `status.json`, CSVs, and engine logs under its own `logs/` directory.",
            "",
            "This harness does not manage mounts or cache eviction. It only generates a reproducible runner-local layout.",
            "",
        ]
    )


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    current = path.stat().st_mode
    path.chmod(current | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def generate_harness(
    *,
    output_dir: Path,
    spot_symbols: list[str],
    start: str,
    end: str,
    catalog_root: str,
    prewarm_threads: int,
    prefetch_ahead_hours: int,
    prefetch_max_files_per_batch: int,
    load_trade_ticks: bool,
    optimize_file_loading: bool,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    bundle_dir = output_dir / "bundle"
    bundle_path = bundle_dir / BUNDLE_FILENAME
    write_strategy_bundle(bundle_path)

    symbols = _expand_symbol_universe(spot_symbols)
    summary: dict[str, Any] = {
        "output_dir": output_dir.as_posix(),
        "bundle_path": bundle_path.as_posix(),
        "spot_symbols": spot_symbols,
        "symbols": symbols,
        "start": start,
        "end": end,
        "catalog_root": catalog_root,
        "prefetch_ahead_hours": prefetch_ahead_hours,
        "prefetch_max_files_per_batch": prefetch_max_files_per_batch,
        "prewarm_threads": prewarm_threads,
        "load_trade_ticks": load_trade_ticks,
        "optimize_file_loading": optimize_file_loading,
        "modes": [],
    }

    args = argparse.Namespace(
        catalog_root=catalog_root,
        prewarm_threads=prewarm_threads,
        prefetch_ahead_hours=prefetch_ahead_hours,
        prefetch_max_files_per_batch=prefetch_max_files_per_batch,
    )

    for mode in MODES:
        mode_dir = output_dir / mode.name
        mode_dir.mkdir(parents=True, exist_ok=True)
        bundle_relative_path = os.path.relpath(bundle_path, mode_dir)
        run_spec = _base_run_spec(
            backtest_id=f"warmup_harness_{mode.name}",
            symbols=symbols,
            start=start,
            end=end,
            bundle_relative_path=bundle_relative_path.replace(os.sep, "/"),
            mode=mode,
            load_trade_ticks=load_trade_ticks,
            optimize_file_loading=optimize_file_loading,
        )

        (mode_dir / "run_spec.json").write_text(
            json.dumps(run_spec, indent=2, ensure_ascii=True) + "\n",
            encoding="utf-8",
        )
        (mode_dir / "env.sh").write_text(_render_env(mode, args), encoding="utf-8")
        _write_executable(mode_dir / "run.sh", _render_run_script())

        summary["modes"].append(
            {
                **asdict(mode),
                "directory": mode_dir.as_posix(),
                "run_spec_path": (mode_dir / "run_spec.json").as_posix(),
                "env_path": (mode_dir / "env.sh").as_posix(),
                "run_script_path": (mode_dir / "run.sh").as_posix(),
            }
        )

    (output_dir / "README.md").write_text(
        _render_readme(
            spot_symbols=spot_symbols,
            start=start,
            end=end,
            load_trade_ticks=load_trade_ticks,
            optimize_file_loading=optimize_file_loading,
        ),
        encoding="utf-8",
    )
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    return summary


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a minimal runner-local harness for validating "
            "prewarm vs replay-prefetch vs no-warmup."
        )
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory where the harness layout will be written.",
    )
    parser.add_argument(
        "--spot-symbols",
        required=True,
        help="Comma-separated explicit spot symbols, e.g. ACTUSDT,DOTUSDT,BCHUSDT",
    )
    parser.add_argument("--start", required=True, help="Inclusive run start ISO8601 timestamp.")
    parser.add_argument("--end", required=True, help="Exclusive run end ISO8601 timestamp.")
    parser.add_argument(
        "--catalog-root",
        default=DEFAULT_CATALOG_ROOT,
        help="Catalog root used by prewarm modes.",
    )
    parser.add_argument(
        "--prewarm-threads",
        type=int,
        default=DEFAULT_PREWARM_THREADS,
        help="Threads passed to juicefs warmup in prewarm modes.",
    )
    parser.add_argument(
        "--prefetch-ahead-hours",
        type=int,
        default=DEFAULT_PREFETCH_AHEAD_HOURS,
        help="Replay prefetch lookahead horizon for replay-prefetch modes.",
    )
    parser.add_argument(
        "--prefetch-max-files-per-batch",
        type=int,
        default=DEFAULT_PREFETCH_MAX_FILES_PER_BATCH,
        help="Replay prefetch batch size for replay-prefetch modes.",
    )
    parser.add_argument(
        "--load-trade-ticks",
        action="store_true",
        help="Include trade_tick data configs in the generated run specs.",
    )
    parser.add_argument(
        "--optimize-file-loading",
        action="store_true",
        help="Set optimize_file_loading=true in the generated run specs.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    spot_symbols = _parse_spot_symbols(args.spot_symbols)
    summary = generate_harness(
        output_dir=Path(args.output_dir).expanduser().resolve(),
        spot_symbols=spot_symbols,
        start=args.start,
        end=args.end,
        catalog_root=args.catalog_root,
        prewarm_threads=args.prewarm_threads,
        prefetch_ahead_hours=args.prefetch_ahead_hours,
        prefetch_max_files_per_batch=args.prefetch_max_files_per_batch,
        load_trade_ticks=bool(args.load_trade_ticks),
        optimize_file_loading=bool(args.optimize_file_loading),
    )
    print(json.dumps(summary, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
