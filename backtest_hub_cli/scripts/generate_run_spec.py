#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

SCHEMA_VERSION = "1.0"
REQUESTED_BY = "researcher_001"
STRATEGY_FILE = "strategies/spot_futures_arb_diagnostics.py"
STRATEGY_ENTRY = "strategies.spot_futures_arb_diagnostics:SpotFuturesArbDiagnostics"
STRATEGY_CONFIG_PATH = "strategies.spot_futures_arb_diagnostics:SpotFuturesArbDiagnosticsConfig"
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
LATENCY_CONFIG = {
    "base_latency_nanos": 20_000_000,
    "insert_latency_nanos": 2_000_000,
    "update_latency_nanos": 3_000_000,
    "cancel_latency_nanos": 1_000_000,
}
SYMBOLS = ["ACTUSDT", "ACTUSDT-PERP", "DOTUSDT", "DOTUSDT-PERP"]
START_TIME = "2025-11-10T00:00:00.000Z"
END_TIME = "2025-11-11T23:59:59.999Z"
CHUNK_SIZE = 200000
SEED = 12345
TAGS = {"purpose": "example"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate run_spec.json for backtest hub.")
    parser.add_argument("--output", default="run_spec.json", help="Output path for run_spec.json")
    parser.add_argument("--requested-by", default=REQUESTED_BY)
    parser.add_argument("--strategy-file", default=STRATEGY_FILE)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    strategy_file = Path(args.strategy_file)

    run_spec = {
        "schema_version": SCHEMA_VERSION,
        "requested_by": args.requested_by,
        "strategy_file": strategy_file.name,
        "strategy_entry": STRATEGY_ENTRY,
        "strategy_config_path": STRATEGY_CONFIG_PATH,
        "strategy_config": STRATEGY_CONFIG,
        "margin_init": MARGIN_INIT,
        "margin_maint": MARGIN_MAINT,
        "spot_maker_fee": SPOT_MAKER_FEE,
        "spot_taker_fee": SPOT_TAKER_FEE,
        "futures_maker_fee": FUTURES_MAKER_FEE,
        "futures_taker_fee": FUTURES_TAKER_FEE,
        "latency_config": LATENCY_CONFIG,
        "symbols": SYMBOLS,
        "start": START_TIME,
        "end": END_TIME,
        "chunk_size": CHUNK_SIZE,
        "seed": SEED,
        "tags": TAGS,
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as handle:
        json.dump(run_spec, handle, indent=2)

    print(f"RunSpec written to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
