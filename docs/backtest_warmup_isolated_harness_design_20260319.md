# Backtest Warmup Isolated Harness Design 20260319

## Goal

Validate the warmup cycle without polluting shared operational state:

1. `no_warmup`
2. `prewarm_gate_only`
3. `replay_prefetch_only`
4. `prewarm_gate_plus_replay_prefetch`

The design must stay API-native and repo-backed, but must not touch shared host
cache such as `/hdd16/jfscache`.

## Hard Safety Rules

These rules are not optional:

1. Never clear a shared JuiceFS cache directory.
2. Never run `drop_caches` on a shared worker.
3. Never run warmup experiments against the production/shared catalog mount when
   the cache path is shared with other workloads.
4. If cache reset is required, it must target an experiment-owned cache
   directory only.

## What We Actually Need To Measure

There are two different questions:

### A. Same-host cache reuse and B2 cost control

Question:

- if the same universe and window are run twice on the same machine, can the
  second run reuse previously fetched local data instead of re-downloading?

For this question, we do not need host-wide cold-start semantics. We need:

- an isolated local JuiceFS cache directory
- stable replay metrics
- cache size / fetch behavior before and after repeated runs

### B. Strict cold-start startup cost

Question:

- how much launch-time latency disappears when the host starts from cold local
  cache and cold page cache?

This question is more invasive. It requires a sacrificial worker or VM. It
must not be measured on a shared host.

## Recommended Two-Tier Plan

### Tier 1: Safe Same-Host Isolation

Use this first.

Properties:

- same machine is allowed
- same worker shape is allowed
- no shared cache reset
- no shared mount pollution
- good enough to answer reuse and steady replay questions

Required layout:

- isolated catalog mount, for example:
  - mount path: `/mnt/localB2fs-harness/backtest/catalog`
- isolated JuiceFS cache dir, for example:
  - cache dir: `/var/lib/backtest-harness/jfscache/<experiment_id>`
- dedicated worker process that points at the isolated mount
- all submissions go through the normal backtest API to that dedicated worker

Operational contract:

1. Start the isolated mount with the experiment-owned cache dir.
2. Start exactly one dedicated harness worker bound to that mount.
3. Submit the generated harness runs directly to that worker.
4. Between rounds, clear only the experiment-owned cache dir if a cold local
   JuiceFS state is needed.
5. Do not run `drop_caches`.

What Tier 1 can answer well:

- whether repeated runs on one machine reuse local JuiceFS cache
- whether manifest prewarm avoids later remote fetches
- whether replay-time prefetch helps steady replay once startup noise is past
- whether a 38-pair run becomes replay I/O bound enough to show benefit

What Tier 1 cannot answer cleanly:

- strict host-cold page-cache startup latency

### Tier 2: Strict Cold-Start Validation

Use this only if Tier 1 shows a meaningful effect and strict cold-start numbers
still matter.

Required environment:

- dedicated worker or VM with no shared operational load
- isolated JuiceFS cache dir
- permission to clear that isolated cache dir
- permission to run `drop_caches` because no other workload depends on it

Operational contract:

1. Reserve the worker exclusively for the experiment.
2. Confirm no other run or service depends on its local cache.
3. Clear only the experiment-owned cache dir.
4. If strict host-cold numbers are required, run `drop_caches` there only.
5. Run the four-mode matrix serially on the same dedicated worker.

## API-Native Execution Path

The harness should continue to use the formal repo path:

- generate harness with `scripts/warmup_cycle_harness.py`
- submit runs through the backtest API or CLI
- pin to the isolated worker URL

The serial orchestrator can stay repo-backed, but it must not assume that cache
reset against a worker is safe. Cache reset must be opt-in and limited to an
experiment-owned path.

## Verified Isolation Scope On neo-256gb Harness Instance

The current concrete harness instance on `neo-256gb` is:

- worker URL: `http://100.65.27.118:10011`
- container: `quant-trade-api-harness-v1`
- isolated mount: `/mnt/localB2fs_harness`
- isolated catalog bind:
  - `/mnt/localB2fs_harness/backtest/catalog => /opt/catalog`
- isolated JuiceFS cache dir:
  - `/hdd16/jfscache_harness`
- isolated arrow cache:
  - `/hdd16/tmp/arrow_cache_harness`
- isolated worker state/data dir:
  - `/trade/v1/dev-container/quant-scripts-api-harness/data`
- isolated backtest logs:
  - `/trade/v1/dev-container/quant-scripts-api-harness/backtest_logs`

What has been verified:

1. The new JuiceFS mount for `/mnt/localB2fs_harness` is a separate process with
   `--cache-dir /hdd16/jfscache_harness`.
2. The harness container binds `/opt/catalog` to the isolated mount path, not to
   the shared `/mnt/localB2fs/backtest/catalog`.
3. The harness container currently has no `B2_*` or `CATALOG_*` environment
   variables set.
4. In the main backtest code path:
   - `scripts/run_backtest.py`
   - `scripts/catalog_config.py`
   the catalog root falls back to `CATALOG_PATH` or `/opt/catalog` when no
   `B2_*` variables are present.
5. In the same code path, the catalog configuration is threaded into
   `BacktestDataConfig.catalog_path` and used for replay/catalog reads.

Operational meaning:

- for the normal warmup-cycle replay path, catalog reads are expected to go
  through the isolated `/opt/catalog` bind and therefore the isolated
  `/hdd16/jfscache_harness` cache dir

## What This Does Not Claim

This design note intentionally does **not** claim full container-level or
host-level I/O isolation.

Specifically:

1. The harness container still has a read-only bind mount for `/mnt/b2fs`.
2. The current verification shows that `/mnt/b2fs` is not referenced by the
   mainline `app.py` / `scripts/run_backtest.py` / `scripts/catalog_config.py`
   catalog path.
3. It does **not** prove that every possible auxiliary path, future code path,
   or third-party library call inside the container can never touch
   `/mnt/b2fs`.

So the correct claim is:

- catalog-cache isolation for the warmup-cycle backtest path is verified

and the incorrect stronger claim would be:

- the entire container is fully isolated from all shared mounts

## If Strict Full Isolation Is Required

If later work needs stronger guarantees than the current setup, the next step is
not another verbal assumption. It is one of these explicit changes:

1. remove the `/mnt/b2fs` bind from the harness container entirely, or
2. add file-open tracing / syscall auditing during a harness run and prove that
   only the isolated mount is touched

Until one of those is done, keep using the narrower, accurate statement above.

## Minimal Experiment Matrix

### Phase 1: 3-Pair Harness

Purpose:

- verify wiring
- verify cache reuse behavior
- verify metric collection

Workload:

- 3 healthy pairs
- 10-day window
- `load_trade_ticks=false`
- serial execution on one isolated worker

Success criteria:

- each mode produces at least 10 stable replay slope samples
- no writes occur outside the experiment-owned cache dir and artifact dir
- repeating the same mode on the same isolated cache does not trigger an
  uncontrolled refill pattern

### Phase 2: Repeat-Run Reuse Check

Purpose:

- answer the concrete cost-control question

Method:

1. Run the same spec once on the isolated worker.
2. Run the same spec again without clearing the isolated cache dir.
3. Compare:
   - startup latency
   - steady replay slope
   - isolated cache dir size growth
   - any remote-read counters available from worker or JuiceFS metrics

Interpretation:

- if the second run does not materially grow the isolated cache dir and startup
  improves, same-machine reuse is working
- if the second run still forces substantial refill, the cache keying or file
  access pattern needs a deeper audit

### Phase 3: 38-Pair Scale-Up

Purpose:

- test whether a larger, more realistic universe becomes replay I/O bound

Method:

- keep the same isolated worker and same instrumentation
- scale from 3 pairs to 38 pairs
- keep serial mode execution
- compare the same four modes

Expected outcome:

- if prefetch is going to matter materially, it is more likely to show up here
  than in the 3-pair harness

## Required Observability

For every run, record:

- backtest id
- run id
- worker URL
- isolated catalog mount path
- isolated JuiceFS cache dir
- cache dir size before run
- cache dir size after run
- prepare-stage timing
- replay progress slope after initial chunks
- whether prewarm executed
- whether prefetch executed

For Tier 1, this is enough. For Tier 2, also record whether page cache was
explicitly dropped.

## Recommended Next Execution Order

1. Do not rerun anything on `neo-256gb` shared cache.
2. Stand up an isolated harness worker with its own mount and cache dir.
3. Re-run the 3-pair harness first.
4. Run the repeat-run reuse check on the same isolated cache.
5. Only then decide whether the 38-pair scale-up is worth the cost.
