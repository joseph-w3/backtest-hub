# Backtest Warmup Cache Incident 20260319

## Summary

On 2026-03-19, a serial warmup experiment was run against the shared worker
`neo-256gb` at `http://100.65.27.118:10001`.

The experiment methodology was unsafe for a shared host:

- it cleared the host-local shared JuiceFS cache directory `/hdd16/jfscache`
- it also ran `sync` and `echo 3 > /proc/sys/vm/drop_caches`
- it did this before each of four experiment modes

This did not delete remote B2 or JuiceFS source data. It did wipe the shared
local cache on that host and invalidate host-local page cache.

This should not be repeated on a shared worker.

## What Ran

Harness:

- `/home/joe/backtest-artifacts/warmup-cycle-harness-20260319-3pairs-10d`

Result bundle:

- `/home/joe/backtest-artifacts/warmup-cycle-serial-20260319-neo256gb-3pairs-10d-v2`

Worker and shared cache:

- worker: `neo-256gb`
- worker API: `http://100.65.27.118:10001`
- shared JuiceFS cache dir: `/hdd16/jfscache`

Modes:

1. `no_warmup`
2. `prewarm_gate_only`
3. `replay_prefetch_only`
4. `prewarm_gate_plus_replay_prefetch`

The serial runner reset the shared cache before each mode and then submitted
the run directly to the pinned worker.

## Exact Runs

From `summary.json` in the result bundle:

1. `no_warmup`
   - `backtest_id=20260319T013921Z_no_warmup_44463d5ed94d3875`
   - `run_id=bdfec326-5b55-4d55-a750-59cfcef06e33`
   - `started_at=2026-03-19T01:39:25.686232+00:00`
   - `finished_at=2026-03-19T01:52:20.611050+00:00`
2. `prewarm_gate_only`
   - `backtest_id=20260319T015234Z_prewarm_gate_only_e083640ff9aac4c9`
   - `run_id=fb14bc34-12fb-4c27-bca4-0c2970df523e`
   - `started_at=2026-03-19T01:52:38.438970+00:00`
   - `finished_at=2026-03-19T01:56:31.720627+00:00`
3. `replay_prefetch_only`
   - `backtest_id=20260319T015651Z_replay_prefetch_only_6cfcec91d733b168`
   - `run_id=40272e70-1182-41f1-bb7b-295568a99933`
   - `started_at=2026-03-19T01:56:56.183861+00:00`
   - `finished_at=2026-03-19T02:01:47.973815+00:00`
4. `prewarm_gate_plus_replay_prefetch`
   - `backtest_id=20260319T020156Z_prewarm_gate_plus_replay_prefetch_92b988757fcf55fb`
   - `run_id=c0557cf6-3b15-4656-9756-7939dacb8afe`
   - `started_at=2026-03-19T02:02:01.029534+00:00`
   - `finished_at=2026-03-19T02:07:02.908710+00:00`

All four runs were manually stopped after enough stable replay samples were
collected. `status=stopped` here does not mean worker failure.

## Impact

Confirmed impact:

- the shared host-local JuiceFS cache on `neo-256gb` was cleared repeatedly
- the host page cache was also dropped repeatedly
- later reads on that host could no longer rely on the previous hot local cache
- this can increase B2/JuiceFS refill pressure and invalidate normal steady
  state expectations for other work on the same host

Confirmed non-impact:

- no remote B2 object set was deleted
- no JuiceFS metadata volume was deleted
- no backtest was left running after the experiment completed

## Current Confirmed State

Verified after the incident:

- `neo-256gb:/hdd16/jfscache` currently reports `3.5G`
- the cache directory mtime was `2026-03-19 03:02:02 CET`
- worker active run count at `http://100.65.27.118:10001/v1/runs` was `0`

These checks confirm the host cache was repopulated only partially after the
experiment and is far below the previously assumed hot-cache state.

## Experimental Result Snapshot

The small harness was:

- 3 pairs
- 10-day window
- `load_trade_ticks=false`
- `optimize_file_loading=false`

Steady-state replay medians from `summary.json`:

| mode | progress_events_per_second_median | progress_simulated_seconds_per_wall_second_median | stable samples |
| --- | ---: | ---: | ---: |
| `no_warmup` | 110433.35 | 363.16 | 44 |
| `prewarm_gate_only` | 105312.56 | 344.01 | 11 |
| `replay_prefetch_only` | 105073.67 | 341.04 | 13 |
| `prewarm_gate_plus_replay_prefetch` | 111931.17 | 372.53 | 16 |

Observed interpretation:

- launch-time prewarm materially reduced prepare-stage waiting
- replay-time prefetch did execute
- this specific small harness did not show a strong steady-state replay speedup
  from prefetch alone
- this likely means the 3-pair, 10-day harness was not strongly replay I/O
  bound after startup

This result is still useful, but the methodology was operationally unsafe.

## Code And Artifact Audit

At the time of writing, the local `backtest-hub` worktree contains only two
untracked code files from this line of work:

- `scripts/run_warmup_cycle_serial.py`
- `tests/test_run_warmup_cycle_serial.py`

No tracked repository files were modified by the serial experiment runner.

Artifacts were written outside the repo under:

- `/home/joe/backtest-artifacts/warmup-cycle-harness-20260319-3pairs-10d`
- `/home/joe/backtest-artifacts/warmup-cycle-serial-20260319-neo256gb-3pairs-10d-v2`

## Corrective Actions

Effective immediately:

1. Do not clear `/hdd16/jfscache` on any shared worker.
2. Do not run `drop_caches` on any shared worker for warmup experiments.
3. Run future warmup validation only in an isolated cache sandbox.
4. Keep experiment harnesses repo-backed and API-native, but separate worker
   state from production/shared operational state.

Follow-up design is recorded in:

- `docs/backtest_warmup_isolated_harness_design_20260319.md`
