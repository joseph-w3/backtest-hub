# Backtest Worker 2x2 Probe 2026-03-18

## Scope

This note records the 2x2 worker-path probe run on `2026-03-18` to separate:

- strategy/config effects
- current worker loading-path effects
- the remaining gap between the old fast V4 run and current V5 runs

All four probes were run on the same current worker container on `neo-256gb`:

- container: `quant-trade-api-v1`
- image: current `quant-script-api-live:latest`
- runner script: current `run_backtest.py` with the 2026-03-17 probe + optimize logic

The data plane was held constant across all four runs:

- `76 symbols`
- `start = 2025-11-10T00:00:00.000Z`
- `end = 2025-12-31T00:00:00.000Z`
- `chunk_size = 200000`
- `target = 5,000,000 events`

## Why This Probe Was Needed

The historical fast V4 run:

- `20260310T165917Z_af13cdef298a4b508cb5813eed213665`

finished far faster than current long V5 runs. The first suspicion was that
current V5 overlays or chunk sizing changed the replay cost. That was already
ruled out: the old fast V4 exact spec and the current 38-pair V5 spec share the
same symbol count, date range, and chunk size.

The next question was whether the main difference is the current worker's
`optimize_file_loading` split path.

## 2x2 Matrix

The four runs were:

1. `V4 + optimize=false`
2. `V4 + optimize=true`
3. `V5 + optimize=true`
4. `V5 + optimize=false`

`optimize=true` means the current worker uses the directory-based loading path
for `OrderBookDelta` and `TradeTick`.

`optimize=false` means file-level registration.

## Results

| Strategy | optimize | First chunk | First chunk -> 5M | Total to 5M | Peak RSS |
| --- | --- | ---: | ---: | ---: | ---: |
| V4 | off | `335.093s` | `245.031s` | `580.124s` | `183716.61 MB` |
| V4 | on | `570.080s` | `200.025s` | `770.105s` | `46206.24 MB` |
| V5 | on | `510.072s` | `320.262s` | `830.334s` | `46200.79 MB` |
| V5 | off | `350.048s` | `265.034s` | `615.082s` | `183763.51 MB` |

## Main Findings

### 1. `optimize_file_loading` is the dominant RSS regime switch

The memory split is almost binary:

- `optimize=false` -> about `183 GB`
- `optimize=true` -> about `46 GB`

This held regardless of whether the strategy entry was V4 or V5.

### 2. `optimize_file_loading` also changes wall-clock behavior

Turning optimize on trades memory for slower front-loaded prepare time:

- V4: `580s -> 770s`
- V5: `615s -> 830s`

So on the current worker build it is not just a memory knob; it materially
changes elapsed time too.

### 3. The 2x2 gap is not large enough to explain `~1.6d` vs `10+d`

Even the slowest/fastest combination in this probe does not explain a 6x+ real
world slowdown:

- total wall time gap inside this 2x2 is about `1.43x`
- replay-only gap is smaller than that

This means the 2x2 probe identifies one important current-build lever, but it
does **not** fully explain the historical V4 fast run.

### 4. There is still a strategy-layer or current-build replay delta

Inside the same memory regime:

- high-memory path:
  - `V4 off`: `245.031s`
  - `V5 off`: `265.034s`
- low-memory path:
  - `V4 on`: `200.025s`
  - `V5 on`: `320.262s`

So after controlling for optimize on/off, V5 still replays somewhat slower than
V4 on the current worker build, especially in the low-memory regime.

That said, this remaining delta is still far smaller than the historical
`1.6d vs 10+d` gap.

## Historical Worker Pin Clue

The historical fast run directory still exists on the worker:

- `/app/scripts/20260310T165917Z_af13cdef298a4b508cb5813eed213665`

Its saved `run_backtest.py` blob exactly matches:

- `bd569ed` — `fix: inline build_catalog_config() in run_backtest.py`

This matters because:

- the historical fast run script predates all 2026-03-13 progress-heartbeat work
- it also predates all 2026-03-17 probe/optimize-loading changes

The current worker script is much newer and includes:

- streaming probes
- node probes
- `optimize_file_loading`
- optimize fallback logic
- auto-enable optimize for large V5 runs

So the historical fast run was definitely **not** using the current
`run_backtest.py` code path.

## What This Means

The 2x2 probe supports two conclusions at the same time:

1. On the current worker build, `optimize_file_loading` is the main control for
   `~46 GB` vs `~184 GB`.
2. The historical V4 fast run cannot be explained by this current-build 2x2
   alone; a large part of the old-vs-new gap likely lives in the older worker
   stack itself.

## Best Next Experiment

The next comparison should rebuild an old worker image pinned as close as
possible to the historical fast-run stack, then rerun the same exact V4 spec.

At minimum, pin:

- `backtest-hub/scripts/run_backtest.py` to `bd569ed`

And ideally also pin the contemporaneous worker stack that produced the old
native `quant_trade_v1` behavior, because the current worker image almost
certainly embeds a newer engine/runtime.

Only that old-worker-vs-current-worker A/B can answer whether the large
historical speed gap mainly came from:

- old worker/runtime/engine behavior
- old catalog/query behavior
- or older strategy/runtime code

