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
- automatic fallback to file-level registration when optimized directory
  registration fails for a specific data config

This is currently recorded as an investigation and probe capability, not yet a
fully validated production default.

## Correctness Validation Update

Two follow-up validation runs were launched against the same 13-day / 7-pair
medium V5 spread-arb template:

- no-opt control:
  `20260317T095432Z_275e83f5c8fd488589b64bda81b0bf27`
- optimized selective loading with fallback:
  `20260317T095930Z_8c29c0103e37454eac0ecd12b7e68d1c`

Control run status:

- stderr remained empty
- run progressed normally through chunk processing

Optimized run status after the fallback fix:

- stderr remained empty
- the run no longer died in `run_config_exception`
- stdout showed a targeted fallback on one futures order-book directory:
  - `MUBARAKUSDT-PERP.BINANCE_FUTURES`
  - reason: `Invalid Parquet file. Corrupt footer`
- after that fallback, the run continued in `engine_running`

Representative live progress snapshot for the optimized run:

- `phase = engine_running`
- `simulated_time = 2025-12-01T07:47:29.192000000Z`
- `chunks_seen = 65`
- `events_seen = 13,000,000`
- `process.rss_mb = 3470.71`

Representative live progress snapshot for the no-opt control at roughly the
same wall-clock checkpoint:

- `phase = engine_running`
- `simulated_time = 2025-12-02T04:01:54.862000000Z`
- `chunks_seen = 181`
- `events_seen = 36,200,000`
- `process.rss_mb = 8903.79`

Interpretation:

- the optimized path is now recovering correctly from bad directory reads
- correctness validation is no longer blocked by the previous early exception
- the memory benefit remains material enough to justify further guarded rollout
  work once a completed non-empty report is captured

Completed quick optimized validation:

- backtest: `20260317T100124Z_745c8aae6ae54c5193225d8e4026960d`
- template: `run_spec_spread_arb_v5_quick.json`
- duration: `3d`
- universe: `5 pairs`
- status: `success`
- report stats via `check_backtest_status.py --fast`:
  - `PnL = -4.71`
  - `Profit Factor = 2.64`
  - `Win Rate = 54%`
  - `Sharpe = 52.8`
  - `Trades = 26`
- final progress snapshot:
  - `chunks_seen = 223`
  - `events_seen = 44,444,585`
  - `reports = fills.csv, account_spot.csv, account_futures.csv, order_fills.csv, positions.csv`

This closes the earlier blocker that produced empty reports under optimized
loading.

Completed quick no-opt control:

- backtest: `20260317T101741Z_85b52852fdf14a5abf5c3678c7275bb2`
- template: `run_spec_spread_arb_v5_quick.json`
- duration: `3d`
- universe: `5 pairs`
- status: `success`
- report stats via `check_backtest_status.py --fast`:
  - `PnL = -4.71`
  - `Profit Factor = 2.64`
  - `Win Rate = 54%`
  - `Sharpe = 52.8`
  - `Trades = 26`
- final progress snapshot:
  - `chunks_seen = 223`
  - `events_seen = 44,444,585`
  - `reports = fills.csv, account_spot.csv, account_futures.csv, order_fills.csv, positions.csv`

Quick parity result:

- optimized quick and no-opt quick matched exactly on the report-level metrics
  checked here
- optimized loading therefore preserved correctness on the validated quick
  template
- however, the quick template also showed that optimized loading is not a
  universal steady-state RSS win; the main benefit remains avoiding pre-chunk
  session explosion on larger runs

## Recommended Next Steps

1. Keep selective optimized loading behind a guarded default for large V5
   spread-arb backtests only. Current auto-enable rule:
   - explicit `optimize_file_loading` in the run spec always wins
   - otherwise auto-enable only when:
     - strategy is `SpreadArbV5RuntimeUniverse`
     - and either:
       - `symbol_count >= 40`, or
       - `symbol_count >= 20` and duration `>= 30 days`
2. Do not auto-enable it for small quick/medium validations where the memory
   bottleneck is not the limiting factor and steady-state RSS is already fine.
3. Separately clean catalog corruption / schema drift in:
   - `OrderBookDelta` directories that contain corrupt parquet footers
   - `FundingRateUpdate`
   - `MarkPriceUpdate`
4. Once catalog cleanup is done, reconsider whether the fallback path is still
   needed or whether directory registration can be widened safely.
