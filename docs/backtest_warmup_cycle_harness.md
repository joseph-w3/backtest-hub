# Backtest Warmup Cycle Harness

## Goal

Provide a small, reproducible runner-local harness for separating:

1. no warmup
2. launch-time prewarm completed before runner start
3. replay-time prefetch only
4. launch-time prewarm plus replay-time prefetch

This harness is intentionally smaller than the 38-pair production-style runs so
the warmup cycle can be studied without paying the full operational cost of a
large backtest.

## Design

The harness stays inside `backtest-hub` and reuses the real runner path:

- `scripts/run_backtest.py`
- `scripts/prewarm_catalog_cache.py`
- `scripts/catalog_prefetch.py`

It does **not** depend on the main `strategy` repo.

Instead, it packages a minimal existing bundle strategy already present in this
repo:

- `strategies.simple_bundle_demo:SimpleBundleDemo`

That strategy accepts the same injected config shape the runner expects:

- `spot_instrument_ids`
- `futures_instrument_ids`
- `book_type`

and otherwise does almost nothing. This keeps strategy logic out of the warmup
experiment.

## What Gets Generated

Run:

```bash
python scripts/warmup_cycle_harness.py \
  --output-dir /tmp/warmup-harness \
  --spot-symbols ACTUSDT,DOTUSDT,BCHUSDT \
  --start 2025-11-10T00:00:00.000Z \
  --end 2025-11-24T00:00:00.000Z
```

Output layout:

```text
/tmp/warmup-harness/
  README.md
  summary.json
  bundle/
    strategies-harness.zip
  no_warmup/
    env.sh
    run.sh
    run_spec.json
  prewarm_gate_only/
    env.sh
    run.sh
    run_spec.json
  replay_prefetch_only/
    env.sh
    run.sh
    run_spec.json
  prewarm_gate_plus_replay_prefetch/
    env.sh
    run.sh
    run_spec.json
```

## Mode Semantics

### `no_warmup`

- `BACKTEST_PREFETCH_BACKEND=off`
- no `prewarm_catalog_cache.py --warmup` before launch

Use this as the cold-start baseline.

### `prewarm_gate_only`

- `BACKTEST_PREFETCH_BACKEND=off`
- `run.sh` completes manifest-based prewarm before launching the runner

This isolates launch-time prewarm.

### `replay_prefetch_only`

- `BACKTEST_PREFETCH_BACKEND=local-read`
- no launch-time prewarm

This isolates replay-time prefetch.

### `prewarm_gate_plus_replay_prefetch`

- `BACKTEST_PREFETCH_BACKEND=local-read`
- prewarm completes before runner launch

This tests the combined path.

## Intended Readouts

For each mode, compare at minimum:

1. submit or launch -> `before_query_result`
2. submit or launch -> first chunk
3. first chunk -> first fixed event threshold
4. prepare-stage RSS
5. replay-stage RSS

Interpretation should stay phase-aware:

- launch-time prewarm should mainly affect the pre-replay region
- replay-time prefetch should mainly affect post-first-chunk replay

## Explicit Non-Goals

This harness does **not**:

- manage JuiceFS mounts
- clear Linux page cache
- reset or create worker-local cache directories
- choose symbols for you based on data-health truth

Those remain operator actions outside the harness.

## Recommended Use

1. Pick a small healthy 3-pair slice with known-readable data.
2. Run all four modes on the same worker shape.
3. Keep cache state controlled and explicitly recorded.
4. Only after the small harness cleanly separates the phases, scale the same
   methodology back up to larger validation runs.
