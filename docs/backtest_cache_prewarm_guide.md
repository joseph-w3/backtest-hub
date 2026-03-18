# Backtest Catalog Cache Prewarm Guide

## Goal

When a worker hits a cold JuiceFS local cache, the long stall happens before replay starts, inside catalog query preparation. The new `scripts/prewarm_catalog_cache.py` script generates a file manifest from a `run_spec.json` and can optionally call `juicefs warmup` for that exact run window.

This is meant for the shared worker hosts that already mount:

- catalog root: `/mnt/localB2fs/backtest/catalog`
- shared JuiceFS cache dir behind that mount: `/hdd16/jfscache`

## What The Script Includes

Given a run spec, the script resolves the same symbol universe shape the worker uses:

- spot metadata: `data/currency_pair/<instrument_id>/*.parquet`
- futures metadata: `data/crypto_perpetual/<instrument_id>/*.parquet`
- spot market data: `data/order_book_deltas/<instrument_id>/*.parquet`
- spot trade ticks: `data/trade_tick/<instrument_id>/*.parquet` when `load_trade_ticks=true` or omitted
- futures market data:
  - `data/order_book_deltas/<instrument_id>/*.parquet`
  - `data/trade_tick/<instrument_id>/*.parquet` when enabled
  - `data/funding_rate_update/<instrument_id>/*.parquet`
  - `data/mark_price_update/<instrument_id>/*.parquet`

For time-windowed market data it keeps only parquet files whose filename range overlaps the run's `start`/`end`.

## Usage

Generate a manifest only:

```bash
python scripts/prewarm_catalog_cache.py \
  --run-spec /path/to/run_spec.json
```

Write the manifest somewhere explicit:

```bash
python scripts/prewarm_catalog_cache.py \
  --run-spec /path/to/run_spec.json \
  --manifest /tmp/bt-manifest.txt
```

Generate the manifest and immediately build JuiceFS cache:

```bash
python scripts/prewarm_catalog_cache.py \
  --run-spec /path/to/run_spec.json \
  --manifest /tmp/bt-manifest.txt \
  --warmup
```

Run warmup in background with custom concurrency:

```bash
python scripts/prewarm_catalog_cache.py \
  --run-spec /path/to/run_spec.json \
  --warmup \
  --background \
  --threads 100
```

## Output

The script writes a manifest file and prints a JSON summary containing:

- `file_count`
- `category_counts`
- `missing_dir_count`
- `missing_dir_sample`
- resolved `manifest_path`

If `file_count = 0`, the script exits with an error instead of silently running a no-op warmup.

## Operational Notes

- Run this on the same worker host that will execute the backtest. Prewarming a different machine does not help the target host's local JuiceFS cache.
- `load_trade_ticks=false` will intentionally exclude `trade_tick` files from the manifest, matching the worker's reduced-memory path.
- Missing directories are reported but do not fail the script by themselves. This keeps the workflow safe across partial catalog populations.
- This is a cache accelerator only. It does not change the run spec, scheduler reservation, or replay semantics.
