#!/usr/bin/env python3
import json
import subprocess
import sys
from pathlib import Path

OUTPUT_PATH = "/app/run_spec.json"
WHEEL_OUTPUT_DIR = "/opt/backtest/strategy_artifacts/wheels"

SCHEMA_VERSION = "1.0"
REQUESTED_BY = "researcher_001"
STRATEGY_ENTRY = "strategies.spot_futures_arb_diagnostics:SpotFuturesArbDiagnostics"
STRATEGY_CONFIG_PATH = (
    "strategies.spot_futures_arb_diagnostics:SpotFuturesArbDiagnosticsConfig"
)
STRATEGY_CONFIG = {
    "order_quantity": "1000",
    "log_interval_minutes": 10.0,
}
MARGIN_INIT = 0.05
MARGIN_MAINT = 0.025
SPOT_MAKER_FEE = 0.001
SPOT_TAKER_FEE = 0.001
FUTURES_MAKER_FEE = 0.001
FUTURES_TAKER_FEE = 0.001
SYMBOLS = ["ACTUSDT", "ACTUSDT-PERP", "DOTUSDT", "DOTUSDT-PERP"]
START_TIME = "2025-01-01T00:00:00Z"
END_TIME = "2025-01-02T00:00:00Z"
CHUNK_SIZE = 200000
SEED = 12345
TAGS = {"purpose": "example"}


def build_strategy_wheel(repo_root: Path, output_dir: Path) -> Path:
    build_script = repo_root / "scripts" / "build_strategy_wheel.sh"
    if not build_script.is_file():
        raise FileNotFoundError(f"build script not found at {build_script}")

    output_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run([str(build_script), "--output-dir", str(output_dir)], check=True)

    wheels = sorted(
        output_dir.glob("strategies-*.whl"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not wheels:
        raise RuntimeError(f"No wheel found in {output_dir}")
    return wheels[0]


def main() -> int:
    if len(sys.argv) > 1:
        print("This script uses global constants. Edit scripts/generate_run_spec.py.")
        return 1

    repo_root = Path(__file__).resolve().parent.parent
    wheel_path = build_strategy_wheel(repo_root, Path(WHEEL_OUTPUT_DIR))

    run_spec = {
        "schema_version": SCHEMA_VERSION,
        "requested_by": REQUESTED_BY,
        "strategy_artifact": str(wheel_path),
        "strategy_entry": STRATEGY_ENTRY,
        "strategy_config_path": STRATEGY_CONFIG_PATH,
        "strategy_config": STRATEGY_CONFIG,
        "margin_init": MARGIN_INIT,
        "margin_maint": MARGIN_MAINT,
        "spot_maker_fee": SPOT_MAKER_FEE,
        "spot_taker_fee": SPOT_TAKER_FEE,
        "futures_maker_fee": FUTURES_MAKER_FEE,
        "futures_taker_fee": FUTURES_TAKER_FEE,
        "symbols": SYMBOLS,
        "start": START_TIME,
        "end": END_TIME,
        "chunk_size": CHUNK_SIZE,
        "seed": SEED,
        "tags": TAGS,
    }

    output_path = Path(OUTPUT_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as handle:
        json.dump(run_spec, handle, indent=2)

    print(f"RunSpec written to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
