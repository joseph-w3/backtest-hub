# Backtest 51d Slow-Run Repro Plan 20260319

## Purpose

Define the next mainline experiment for the real question:

- why some `51-day` runs finish in about `1.6d`
- while other runs of similar shape drift toward `10+d` or `14d`

This plan intentionally avoids replay-prefetch speculation. The current
mainline is to reproduce the slow regime with the minimum number of controlled
runs.

## Fixed Facts

Already established:

1. stable replay on the current stack can be healthy
2. large cold reads can happen before `chunk 0`
3. shared JuiceFS cache warmth is a first-class variable
4. `optimize_file_loading` is a memory/speed regime switch
5. old worker image is not the main explanation

References:

- [backtest_slow_run_mainline_20260319.md](/home/joe/clawd-workspace/clawd/tbdtbd/backtest-hub/docs/backtest_slow_run_mainline_20260319.md)
- [backtest_cache_cold_vs_warm_20260318.md](/home/joe/clawd-workspace/clawd/tbdtbd/backtest-hub/docs/backtest_cache_cold_vs_warm_20260318.md)
- [backtest_worker_2x2_20260318.md](/home/joe/clawd-workspace/clawd/tbdtbd/backtest-hub/docs/backtest_worker_2x2_20260318.md)

## Experimental Goal

Reproduce the bad `51d` wall-clock regime using the fewest runs possible, while
separating these variables:

1. cache state
2. `load_trade_ticks`
3. `optimize_file_loading`
4. host memory pressure / contention

## Workload Lock

Use one fixed `51d / 38-pair` data plane first.

Default reproduction workload:

- date range: `2025-11-10` to `2025-12-31`
- universe: `38 pairs / 76 symbols`
- worker: one isolated worker, no competing backtests
- stop target: first `5,000,000` events or equivalent first stable replay slice

Do not mix:

- different universes
- different date windows
- different workers
- full-run wall time and short-controller wall time

inside the same comparison table.

## Minimal Repro Matrix

Run these in order.

### Run A: Healthy current V5 ceiling baseline

- `load_trade_ticks=false`
- `optimize_file_loading=true`
- warm cache
- isolated worker

Purpose:

- establish a known-good modern baseline for the same `51d / 38-pair` span

### Run B: Cache-only penalty

- same as Run A
- but cold cache reset before launch

Purpose:

- measure the size of the cache-state penalty without changing the data plane

### Run C: Heavy data-plane penalty

- `load_trade_ticks=true`
- `optimize_file_loading=true`
- warm cache
- isolated worker

Purpose:

- isolate how much the historical heavy data plane hurts even when cache is not
  the problem

### Run D: High-memory regime penalty

- `load_trade_ticks=true`
- `optimize_file_loading=false`
- warm cache
- isolated worker

Purpose:

- isolate the cost of the `~183 GB` regime under otherwise favorable conditions

### Run E: Pathological repro attempt

- same as Run D
- cold cache reset before launch

Purpose:

- this is the most likely candidate to reproduce the historically terrible
  startup regime

Only if A-E still do not explain the gap, add:

### Run F: Controlled contention

- same as Run D or E
- but with one known competing memory-heavy job on the same host

Purpose:

- test whether host pressure is the multiplier that turns a bad run into a
  catastrophic one

## Stop Rule

Do not start with full `51d` terminal completion.

For each run, stop after the earliest of:

1. `5,000,000` events observed
2. `20` stable replay samples collected
3. clear pathological stall:
   - still before `chunk 0` after `20m`, or
   - still pre-replay with negligible event progress after enough evidence is gathered

The objective is regime identification, not strategy PnL.

## Required Metrics

Record these for every run:

1. time to `before_query_result`
2. time to first chunk
3. time to `5M`
4. pre-replay stage at stop time
5. stable replay speed:
   - events/s
   - simulated-seconds / wall-second
6. cache state:
   - warm vs cold
   - local cache-dir growth
7. peak / stabilized RSS
8. whether the worker was isolated or contended

If any of those are missing, do not compare runs as if they were equivalent.

## Decision Logic

Interpretation order:

1. If A is already bad, fix the modern baseline first.
2. If A is healthy and B is bad, cache state is still the primary lever.
3. If A/B are healthy but C/D degrade sharply, the heavy data plane is the key multiplier.
4. If E reproduces the worst regime, the mainline answer is:
   - cold cache
   - heavy data plane
   - high-memory regime
5. If only F collapses, host contention is the missing multiplier.

## What Not To Do

1. do not branch back into replay-prefetch as the main explanation
2. do not compare `10d` harness results directly against `51d` full-run claims
3. do not change multiple variables in one run and then infer causality
4. do not use full terminal completion as the first-line diagnostic tool

## Immediate Next Step

Run A first, then Run B.

If A is healthy and B is materially worse, continue with C-D-E in that order.
