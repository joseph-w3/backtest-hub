# Backtest Warmup Cycle Harness

## Goal

Provide a small, reproducible backtest-hub harness for separating:

1. no warmup
2. launch-time prewarm completed before runner start
3. replay-time prefetch only
4. launch-time prewarm plus replay-time prefetch

This harness is intentionally smaller than the 38-pair production-style runs so
the warmup cycle can be studied without paying the full operational cost of a
large backtest.

## Safety Warning

This harness must not be used as justification to clear a shared worker cache.

In particular:

- do not clear `/hdd16/jfscache` on a shared worker
- do not run `drop_caches` on a shared worker
- do not treat a shared catalog mount as an experiment sandbox

The harness is safe only when the cache and worker state are experiment-owned or
otherwise isolated from other workloads.

See:

- `docs/backtest_warmup_cache_incident_20260319.md`
- `docs/backtest_warmup_isolated_harness_design_20260319.md`

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
    logs/                       # created after execution
  prewarm_gate_only/
    env.sh
    run.sh
    run_spec.json
    logs/                       # created after execution
  replay_prefetch_only/
    env.sh
    run.sh
    run_spec.json
    logs/                       # created after execution
  prewarm_gate_plus_replay_prefetch/
    env.sh
    run.sh
    run_spec.json
    logs/                       # created after execution
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

Mode semantics are encoded directly into each `run_spec.json` via
`catalog_controls`, so the same mode directories can be:

- submitted through the normal backtest-hub API/CLI path
- or run locally against `scripts/run_backtest.py`

When submitted through backtest-hub, the hub now ships the runner helper
modules as a formal `runner_support_bundle`, so API-native harness runs do not
depend on worker-local copies of the repo's `scripts/` tree.

Example API-style submit from the harness root:

```bash
python -m backtest_hub_cli.cli submit \
  --run-spec no_warmup/run_spec.json \
  --strategy-bundle bundle/strategies-harness.zip \
  --name warmup-no-warmup \
  --no-generate
```

This harness does **not**:

- manage JuiceFS mounts
- clear Linux page cache
- reset or create worker-local cache directories
- choose symbols for you based on data-health truth

When executed via `run.sh`, the harness exports:

- `CATALOG_PATH=<catalog_root>`
- `BACKTEST_LOGS_PATH=<mode_dir>/logs`

Those remain operator actions outside the harness.

## Recommended Use

1. Pick a small healthy 3-pair slice with known-readable data.
2. Run all four modes on the same worker shape.
3. Keep cache state controlled and explicitly recorded.
4. Only after the small harness cleanly separates the phases, scale the same
   methodology back up to larger validation runs.

## Observed Result: 2026-03-19 Isolated 3-Pair Run

Environment:

- worker host: `neo-256gb`
- isolated worker API: `127.0.0.1:11011 -> 127.0.0.1:10011`
- isolated JuiceFS cache dir: `/hdd16/jfscache_harness`
- isolated catalog mount: `/mnt/localB2fs_harness`
- harness window: `2025-11-10T00:00:00Z` -> `2025-11-20T00:00:00Z`
- symbols: `ACTUSDT`, `DOTUSDT`, `BCHUSDT` plus matching perps
- stable replay stop rule: `10` progress-derived samples after `--skip-initial-chunks 10`

Important caveats:

- `juicefs warmup` is not safe on a read-only catalog mount; it writes control
  files, so the isolated mount used for prewarm modes had to be remounted
  read-write.
- These runs cleared the isolated JuiceFS cache dir between modes but did not
  run `drop_caches`, so prepare-stage latency is directional only. Steady replay
  throughput is the more reliable comparison from this batch.

Final mode comparison:

| Mode | Total seconds | Prepare `register_session_ms` | Stable replay events/s median | Stable replay sim-sec/s median |
| --- | ---: | ---: | ---: | ---: |
| `no_warmup` | 250.376 | 5582.52 | 107227.08 | 331.97 |
| `replay_prefetch_only` | 173.160 | 43.24 | 101301.93 | 324.00 |
| `prewarm_gate_only` | 423.246 | 2319.94 | 104425.38 | 330.37 |
| `prewarm_gate_plus_replay_prefetch` | 408.985 | 1408.60 | 109750.20 | 349.33 |

Interpretation from this isolated batch:

- launch-time prewarm helped the prepare path materially versus the cold
  baseline, cutting observed `register_session_ms` from about `5583 ms` to about
  `2320 ms`
- replay-time prefetch alone did not improve steady replay in this small 3-pair
  harness; its steady replay medians were slightly below `no_warmup`
- the combined mode was the best steady replay result in this batch, but the
  gain was modest: about `+2.4%` on events/s and about `+5.2%` on simulated
  seconds per wall second versus `no_warmup`
- the strongest signal from this harness is still phase separation: prewarm
  helps pre-replay setup, while replay-prefetch does not show a large standalone
  throughput win on this small universe
