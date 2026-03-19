# Backtest Slow-Run Mainline 20260319

## Purpose

Freeze the current mainline answer to:

- why some `51-day` backtests finish in about `1.6d`
- while other runs of similar shape can drift toward `10+d` or even `14d`

This note is intentionally short. It exists to stop future drift back into
already-ruled-out explanations.

## What Is Proven

### 1. Stable replay itself can be healthy on the current stack

The isolated `10d / 38-pair / load_trade_ticks=false / no_warmup` qualification
run reached stable replay and held roughly:

- `progress_events_per_second ~= 91k-106k`
- `progress_simulated_seconds_per_wall_second ~= 23x-40x`

That means "the replay loop is inherently stuck at ~3.6x" is not a good default
hypothesis.

### 2. Large cold reads can happen before stable replay begins

Code path:

1. the runner filters candidate parquet files
2. backend session registration calls `execute_stream()`
3. `EagerStream` starts consuming batches immediately
4. `KMerge` primes each stream with an initial element before chunk replay

Relevant code:

- [run_backtest.py](/home/joe/clawd-workspace/clawd/tbdtbd/backtest-hub/scripts/run_backtest.py#L1108)
- [run_backtest.py](/home/joe/clawd-workspace/clawd/tbdtbd/backtest-hub/scripts/run_backtest.py#L1208)
- [session.rs](/home/joe/clawd-workspace/clawd/tbdtbd/quant-trade-v1/crates/persistence/src/backend/session.rs#L180)
- [kmerge_batch.rs](/home/joe/clawd-workspace/clawd/tbdtbd/quant-trade-v1/crates/persistence/src/backend/kmerge_batch.rs#L37)
- [kmerge_batch.rs](/home/joe/clawd-workspace/clawd/tbdtbd/quant-trade-v1/crates/persistence/src/backend/kmerge_batch.rs#L87)

Practical meaning:

- a run can fill a large amount of JuiceFS cache before `chunk 0`
- replay-prefetch is therefore not the first thing to blame for very slow runs

### 3. Cache warmth remains a first-class performance variable

For the historical `51d / 38-pair` workload on `neo-256gb`, existing probes
already showed:

- warm shared cache: about `205s` to `5M`
- dropped page cache but warm shared JuiceFS cache: about `305s`
- fresh cold JuiceFS cache: still stuck in pre-replay after `360s+`

Reference:

- [backtest_cache_cold_vs_warm_20260318.md](/home/joe/clawd-workspace/clawd/tbdtbd/backtest-hub/docs/backtest_cache_cold_vs_warm_20260318.md)

### 4. `optimize_file_loading` is a regime switch, not a pure optimization

Current validated behavior on the large `51d / 38-pair` data plane:

- `optimize=false`: faster startup path, but about `183 GB` RSS
- `optimize=true`: slower startup path, but about `46 GB` stabilized RSS

Reference:

- [backtest_worker_2x2_20260318.md](/home/joe/clawd-workspace/clawd/tbdtbd/backtest-hub/docs/backtest_worker_2x2_20260318.md)

### 5. Old worker image is not the main explanation

The old cached worker image was compared against the current image on the same
historical V4 spec with the same modern probe-capable runner.

Result:

- current image was not slower

Reference:

- [backtest_worker_old_vs_current_20260318.md](/home/joe/clawd-workspace/clawd/tbdtbd/backtest-hub/docs/backtest_worker_old_vs_current_20260318.md)

## What Must Not Be Conflated

### A. The `10d` isolated harness is not the same workload as the historical `51d` full run

Current isolated qualification run:

- `10d`
- `38 pairs`
- `load_trade_ticks=false`
- `chunk_size=200000`

Historical slow/fast comparisons that motivated the investigation:

- `51d`
- `38 pairs`
- historical V4 path typically had `load_trade_ticks=true`

So:

- `30.55 GB` cache in the `10d` harness does **not** mean the full `51d` run
  only needs `30.55 GB`

### B. Stable replay speed is not the same thing as full-run wall-clock speed

A run can have:

- healthy stable replay
- but still terrible total wall time

if it spends a very long time in:

- cold cache preparation
- DataFusion/backend session build-up
- early stream priming before chunk replay starts

## Mainline Answer To "Why Did Some 51-Day Runs Take 14 Days?"

Current best answer:

- those runs did **not** simply have "slow replay"
- they likely fell into a much worse pre-replay/storage/memory regime

The most likely drivers, in order, are:

1. cold or half-cold shared JuiceFS cache
2. heavier data plane, especially `load_trade_ticks=true`
3. high-memory `optimize=false` path combined with host pressure
4. pre-replay query/session expansion cost
5. storage readability failures on the mount/object-store path

What is **not** currently a good mainline explanation:

1. strategy decision-loop logic
2. replay-prefetch being absent
3. old worker image provenance
4. chunk replay being the first dominant bottleneck

## Guardrails For Future Comparisons

Every performance comparison should record all of:

1. date window
2. symbol / pair count
3. `load_trade_ticks`
4. `chunk_size`
5. `optimize_file_loading`
6. cache state
7. worker / host identity
8. first-chunk timing
9. stable replay speed
10. host memory pressure / RSS regime

If those are not pinned, do not make strong claims from wall-clock deltas.

## Immediate Practical Rule

When a `51-day` run looks abnormally slow, ask these first:

1. was the shared JuiceFS cache cold or contaminated?
2. was `load_trade_ticks` enabled?
3. was the run on the `183 GB` regime or the `46 GB` regime?
4. did the run spend most of its time before `chunk 0`?

Only after those are answered should replay-side speculation begin.
