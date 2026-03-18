# Backtest Prefetch Design Backlog (2026-03-18)

## Goal

Keep backtest startup and replay prefetch fast without coupling orchestration semantics to JuiceFS.

The control plane should decide:

- what data a run needs
- where the run will execute
- whether that worker is ready to start

The storage backend should only decide:

- how those objects become locally readable

## Current State

Two different mechanisms exist today:

1. submit-time catalog warmup
   - implemented in `quant-script-api`
   - builds a full manifest for the run
   - currently actuated by `juicefs warmup --background`
2. replay-time prefetch
   - implemented in `scripts/run_backtest.py`
   - advances a moving time window during replay
   - currently uses a backend abstraction with `local-read` and `off`

This split is directionally correct, but the submit-time path is still actuator-coupled to JuiceFS.

## Required Design Boundaries

### 1. Manifest planning must stay backend-agnostic

Input:

- `run_spec`
- resolved worker-local catalog root
- feature flags such as `load_trade_ticks`

Output:

- exact file/object list for the run
- category counts
- missing-dir or missing-object diagnostics

This layer must not know about `juicefs warmup`, page cache, or B2 APIs.

### 2. Warmup actuator must be replaceable

Current implementation:

- `juicefs warmup -f <manifest>`

Future implementations:

- direct B2 download into worker-local scratch space
- object-store range prefetch
- explicit local materialization service

The scheduler/API contract should not change when the actuator changes.

### 3. Launch gating must be backend-agnostic

Do not model readiness as "JuiceFS warmup command started".

Readiness should mean one of:

- required files are locally readable
- required manifest coverage threshold is reached
- bounded spot-check reads succeed

For B2, the same gate should still work even if there is no FUSE mount.

### 4. Replay-time prefetch stays streaming

Replay-time prefetch should remain a sliding-window mechanism keyed off simulated time.

This is the right place to overlap:

- current chunk compute
- near-future data fetch/materialization

That path should keep its backend interface and gain more implementations over time.

## Immediate Backlog

1. Add a backend-neutral warmup/gate interface
   - separate `plan_manifest()`, `start_prefetch()`, `check_readiness()`
2. Add a worker-local readiness probe
   - verify a bounded sample of manifest files is actually readable before launch
3. Record warmup observability in run status
   - manifest file count
   - bytes planned
   - bytes/materialized if known
   - readiness state
   - gate wait duration
4. Keep replay-time prefetch backend-driven
   - do not introduce `juicefs` calls into runner logic
5. Add a direct-B2 actuator design
   - local target directory
   - object naming resolution
   - cleanup policy
   - concurrency and backpressure knobs
6. Define failure semantics
   - fail-open vs fail-closed
   - when unreadable objects should block launch
   - how partial warmup is surfaced to operators

## Immediate Non-Goals

- hard-coding JuiceFS assumptions into scheduler state
- requiring FUSE mounts in the long-term API contract
- changing replay semantics just to fit current storage

## Current Evidence To Carry Forward

- background submit-time warmup is not enough on its own
- some fresh-mount reads still fail with `EIO` even after `juicefs warmup`
- replay-time prefetch is already structurally closer to the future B2 shape than submit-time warmup is

So the next iteration should strengthen:

- worker-local readiness validation
- actuator abstraction
- progress observability

and should not strengthen:

- direct control-plane dependence on JuiceFS-specific commands
