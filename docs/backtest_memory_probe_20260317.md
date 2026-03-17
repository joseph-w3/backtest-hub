# Backtest Memory Probe 2026-03-17

## Scope

This note records the 38-pair / 51-day spread-arb backtest memory investigation
performed on 2026-03-17.

- Execution path: `neo-256gb -> uv-ssh-protect-v1-joe -> neo-test1 backtest-hub`
- Overlay under test:
  `configs/live/v5_runtime_universe_backtest_38pairs.yaml`
- Effective run config:
  - `chunk_size = 200000`
  - `load_trade_ticks = true` by default unless explicitly overridden

## What Was Probed

Additional progress probes were added in `scripts/run_backtest.py`:

- `node_probe`
  - `before_build`
  - `after_build`
  - `before_run_config`
  - `after_run_config`
  - `run_config_exception`
- `streaming_probe`
  - `before_prepare_queries`
  - `after_prepare_queries`
  - `before_query_result`
  - `after_materialize`
  - `after_add`
  - `after_clear`

The key goal was to determine whether RSS growth happens:

1. before streaming starts
2. while building the DataFusion/DataBackend session
3. while materializing chunks
4. inside `engine.add_data` / `engine.run` / `engine.clear_data`

## Main Finding

The large RSS growth happens before the first chunk is produced.

Observed runs stayed at:

- `streaming_probe.stage = before_prepare_queries`

while RSS kept rising rapidly.

This rules out:

- `capsule_to_list(chunk)`
- `engine.add_data(...)`
- `engine.run(streaming=True)`
- `engine.clear_data()`

as the primary cause of the initial memory explosion.

## Root-Cause Candidate

The strongest candidate is per-file query registration inside
`ParquetDataCatalog.backend_session(...)`.

Relevant behavior:

- `BacktestDataConfig.optimize_file_loading` defaults to `False`
- With `files=...` and `optimize_file_loading=False`, the catalog registers
  parquet files individually via `session.add_file(...)`
- On the 38-pair / multi-data-type run, this creates a very large pre-chunk
  DataFusion session

Relevant code paths:

- `nautilus_trader/backtest/config.py`
- `nautilus_trader/persistence/catalog/parquet.py`
- `scripts/run_backtest.py`

## Measured Runs

### Baseline: no optimized file loading

Representative runs:

- `20260317T091443Z_96617fff6aca425789037203bad9dd9f`
- `20260317T091800Z_11dae4e04a9c450688266f51df561a5d`

Key observation:

- RSS started around `~260-270 MB`
- Probe remained at `before_prepare_queries`
- RSS rose into tens of GB before any chunk-level probe fired

Examples seen during probing:

- `~52 GB`
- `~77 GB`

## Experiment: full directory-based loading

An opt-in field was added to the hub run spec:

- `optimize_file_loading: true`

This dramatically reduced memory growth, but failed on schema drift inside
futures-extra directories.

Representative run:

- `20260317T093209Z_f03bc4334da642e3bc1c1bd9dedc2b72`

Observed failure:

- `FundingRateUpdate.rate`
- schema merge conflict: `Binary` vs `Decimal128(38, 16)`

Effect:

- memory stayed low
- run completed with an effectively empty report
- therefore full directory-based loading is not safe as a blanket default

## Experiment: selective optimized file loading

The next step narrowed optimized directory registration to stable high-volume
classes only:

- `OrderBookDelta`
- `TradeTick`

and kept file-by-file loading for:

- `FundingRateUpdate`
- `MarkPriceUpdate`

Representative run:

- `20260317T093807Z_53a61b9c61f940fb815a694a898cd44b`

Observed behavior:

- no schema-drift error in stderr during the probe window
- still paused at `before_prepare_queries`
- RSS rose much more slowly than baseline

Measured RSS during the probe window:

- `~1.24 GB`
- `~2.24 GB`
- `~3.10 GB`
- `~4.76 GB`
- `~5.61 GB`
- `~6.48 GB`

This is materially better than the `50-77 GB` baseline behavior.

## Current Status

The repo now supports:

- progress probes for node/build/streaming stages
- optional run-spec field `optimize_file_loading`
- selective optimized loading for `OrderBookDelta` / `TradeTick`

This is currently recorded as an investigation and probe capability, not yet a
fully validated production default.

## Recommended Next Steps

1. Run a smaller end-to-end correctness backtest with selective optimized
   loading enabled and verify:
   - non-empty fills
   - non-empty venue reports
   - expected trade counts / pnl
2. If correctness holds, promote selective optimized loading to the default for
   long spread-arb backtests
3. Separately normalize schema drift in:
   - `FundingRateUpdate`
   - `MarkPriceUpdate`
   so full directory-based loading can be reconsidered later
