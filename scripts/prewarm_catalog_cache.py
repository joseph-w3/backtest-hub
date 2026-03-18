#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import re


SPOT_VENUE = "BINANCE_SPOT"
FUTURES_VENUE = "BINANCE_FUTURES"
CATALOG_DATA_DIR = "data"

METADATA_DIRS = {
    "spot": "currency_pair",
    "futures": "crypto_perpetual",
}

MARKET_DATA_DIRS = {
    "order_book": "order_book_deltas",
    "trade_tick": "trade_tick",
    "funding_rate": "funding_rate_update",
    "mark_price": "mark_price_update",
}

RUN_SPEC_TS_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})T"
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?Z$"
)

PARQUET_WINDOW_RE = re.compile(
    r"^(?P<start>\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{9}Z)"
    r"_(?P<end>\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{9}Z)\.parquet$"
)


@dataclass
class ManifestBuildResult:
    paths: list[Path]
    category_counts: dict[str, int]
    missing_dirs: list[str]


def _load_trade_ticks_enabled(run_spec: dict[str, Any]) -> bool:
    value = run_spec.get("load_trade_ticks")
    if value is None:
        return True
    return bool(value)


def _normalize_run_spec_timestamp(value: str) -> str:
    match = RUN_SPEC_TS_RE.fullmatch(value)
    if match is None:
        raise ValueError(f"Unsupported run_spec timestamp: {value}")
    fraction = (match.group("fraction") or "").ljust(9, "0")
    return (
        f"{match.group('date')}T{match.group('hour')}-{match.group('minute')}"
        f"-{match.group('second')}-{fraction}Z"
    )


def _parquet_window(path: Path) -> tuple[str, str] | None:
    match = PARQUET_WINDOW_RE.fullmatch(path.name)
    if match is None:
        return None
    return match.group("start"), match.group("end")


def _window_overlaps(
    *,
    file_start: str,
    file_end: str,
    run_start: str,
    run_end: str,
) -> bool:
    return file_end >= run_start and file_start <= run_end


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def _split_symbols(run_spec: dict[str, Any]) -> tuple[list[str], list[str]]:
    raw_symbols = run_spec.get("symbols")
    if not isinstance(raw_symbols, list):
        raise ValueError("run_spec.symbols must be a list")
    spot_symbols: list[str] = []
    futures_symbols: list[str] = []
    for raw_symbol in raw_symbols:
        if not isinstance(raw_symbol, str):
            continue
        symbol = raw_symbol.strip()
        if not symbol:
            continue
        if symbol.endswith("-PERP"):
            futures_symbols.append(symbol)
        else:
            spot_symbols.append(symbol)
    return _dedupe(spot_symbols), _dedupe(futures_symbols)


def _spot_instrument_id(symbol: str) -> str:
    return f"{symbol}.{SPOT_VENUE}"


def _futures_instrument_id(symbol: str) -> str:
    return f"{symbol}.{FUTURES_VENUE}"


def _collect_directory_files(
    directory: Path,
    *,
    include_all: bool,
    run_start: str,
    run_end: str,
) -> list[Path]:
    files: list[Path] = []
    for path in sorted(directory.glob("*.parquet")):
        if include_all:
            files.append(path)
            continue
        window = _parquet_window(path)
        if window is None:
            files.append(path)
            continue
        if _window_overlaps(
            file_start=window[0],
            file_end=window[1],
            run_start=run_start,
            run_end=run_end,
        ):
            files.append(path)
    return files


def build_manifest(
    *,
    run_spec: dict[str, Any],
    catalog_root: Path,
) -> ManifestBuildResult:
    run_start = _normalize_run_spec_timestamp(str(run_spec["start"]))
    run_end = _normalize_run_spec_timestamp(str(run_spec["end"]))
    spot_symbols, futures_symbols = _split_symbols(run_spec)
    load_trade_ticks = _load_trade_ticks_enabled(run_spec)

    seen: set[Path] = set()
    paths: list[Path] = []
    category_counts: Counter[str] = Counter()
    missing_dirs: list[str] = []

    def add_category(category: str, instrument_id: str, *, include_all: bool) -> None:
        directory = catalog_root / CATALOG_DATA_DIR / category / instrument_id
        if not directory.is_dir():
            missing_dirs.append(str(directory))
            return
        for path in _collect_directory_files(
            directory,
            include_all=include_all,
            run_start=run_start,
            run_end=run_end,
        ):
            if path in seen:
                continue
            seen.add(path)
            paths.append(path)
            category_counts[category] += 1

    for symbol in spot_symbols:
        instrument_id = _spot_instrument_id(symbol)
        add_category(METADATA_DIRS["spot"], instrument_id, include_all=True)
        add_category(MARKET_DATA_DIRS["order_book"], instrument_id, include_all=False)
        if load_trade_ticks:
            add_category(MARKET_DATA_DIRS["trade_tick"], instrument_id, include_all=False)

    for symbol in futures_symbols:
        instrument_id = _futures_instrument_id(symbol)
        add_category(METADATA_DIRS["futures"], instrument_id, include_all=True)
        add_category(MARKET_DATA_DIRS["order_book"], instrument_id, include_all=False)
        if load_trade_ticks:
            add_category(MARKET_DATA_DIRS["trade_tick"], instrument_id, include_all=False)
        add_category(MARKET_DATA_DIRS["funding_rate"], instrument_id, include_all=False)
        add_category(MARKET_DATA_DIRS["mark_price"], instrument_id, include_all=False)

    paths.sort()
    return ManifestBuildResult(
        paths=paths,
        category_counts=dict(sorted(category_counts.items())),
        missing_dirs=sorted(set(missing_dirs)),
    )


def _write_manifest(paths: list[Path], manifest_path: Path) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    content = "".join(f"{path}\n" for path in paths)
    manifest_path.write_text(content, encoding="utf-8")


def _build_summary(
    *,
    result: ManifestBuildResult,
    run_spec_path: Path,
    manifest_path: Path,
    catalog_root: Path,
) -> dict[str, Any]:
    return {
        "run_spec_path": run_spec_path.as_posix(),
        "catalog_root": catalog_root.as_posix(),
        "manifest_path": manifest_path.as_posix(),
        "file_count": len(result.paths),
        "category_counts": result.category_counts,
        "missing_dir_count": len(result.missing_dirs),
        "missing_dir_sample": result.missing_dirs[:10],
    }


def _run_warmup(
    *,
    manifest_path: Path,
    threads: int,
    background: bool,
) -> int:
    command = ["juicefs", "warmup", "-f", str(manifest_path), "-p", str(threads)]
    if background:
        command.append("--background")
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.stdout:
        print(completed.stdout.rstrip())
    if completed.stderr:
        print(completed.stderr.rstrip(), file=sys.stderr)
    return completed.returncode


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a JuiceFS warmup manifest for a backtest run_spec.json."
    )
    parser.add_argument("--run-spec", required=True, help="Path to run_spec.json")
    parser.add_argument(
        "--catalog-root",
        default="/mnt/localB2fs/backtest/catalog",
        help="Catalog root mounted on the worker host",
    )
    parser.add_argument(
        "--manifest",
        help="Output manifest path (default: <run_spec_dir>/catalog-prewarm-manifest.txt)",
    )
    parser.add_argument(
        "--warmup",
        action="store_true",
        help="Run `juicefs warmup -f <manifest>` after writing the manifest",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=50,
        help="Concurrency passed to `juicefs warmup -p`",
    )
    parser.add_argument(
        "--background",
        action="store_true",
        help="Pass `--background` to `juicefs warmup`",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    run_spec_path = Path(args.run_spec).expanduser().resolve()
    catalog_root = Path(args.catalog_root).expanduser().resolve()
    manifest_path = (
        Path(args.manifest).expanduser().resolve()
        if args.manifest
        else run_spec_path.with_name("catalog-prewarm-manifest.txt")
    )

    run_spec = json.loads(run_spec_path.read_text(encoding="utf-8"))
    if not isinstance(run_spec, dict):
        raise ValueError("run_spec.json must contain an object")

    result = build_manifest(run_spec=run_spec, catalog_root=catalog_root)
    if not result.paths:
        raise ValueError("No catalog files matched the supplied run_spec.")

    _write_manifest(result.paths, manifest_path)
    summary = _build_summary(
        result=result,
        run_spec_path=run_spec_path,
        manifest_path=manifest_path,
        catalog_root=catalog_root,
    )
    print(json.dumps(summary, ensure_ascii=True, indent=2))

    if args.warmup:
        return _run_warmup(
            manifest_path=manifest_path,
            threads=args.threads,
            background=args.background,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
