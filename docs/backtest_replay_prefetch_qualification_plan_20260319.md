# Backtest Replay Prefetch Qualification Plan 20260319

## Purpose

Record the corrected qualification plan before running the next harness round.

The immediate goal is not to prove that replay prefetch helps in general. The
goal is to prove that the experiment reaches a replay region where cold data is
still being pulled into the experiment-owned cache, so replay-prefetch has
something real to hide.

## Current State

Already landed:

- replay `local-read` prefetch is forcibly disabled when
  `catalog_controls.prewarm_before_run=true`
- serial harness now records experiment-owned cache-dir growth during polling
- file-touch telemetry exists, but it is only a file-window proxy and is not a
  remote-read metric

Relevant references:

- [run_backtest.py](/home/joe/clawd-workspace/clawd/tbdtbd/backtest-hub/scripts/run_backtest.py)
- [run_warmup_cycle_serial.py](/home/joe/clawd-workspace/clawd/tbdtbd/backtest-hub/scripts/run_warmup_cycle_serial.py)
- [backtest_warmup_isolated_harness_design_20260319.md](/home/joe/clawd-workspace/clawd/tbdtbd/backtest-hub/docs/backtest_warmup_isolated_harness_design_20260319.md)
- maintained 38-pair source of truth:
  `/home/joe/clawd-workspace/clawd/tbdtbd/strategy/configs/live/v5_runtime_universe_backtest_38pairs.yaml`

## 2026-03-19 Qualification Notes

Observed on the first 38-pair `no_warmup` qualification run:

- `phase` stayed at `initializing`
- `init_step` stayed at `load_spot_instruments`
- `streaming_probe` and `prepare_probe` stayed `null`
- the isolated cache still grew to about `30.55 GB` before replay started

Implications:

1. cold demand is materially front-loaded before stable replay
2. replay-prefetch cannot be judged from that run alone
3. `load_spot_instruments` itself must stay cheap, because any repeated catalog
   metadata lookup there directly distorts the startup-side experiment

Mitigation landed in code:

- [run_backtest.py](/home/joe/clawd-workspace/clawd/tbdtbd/backtest-hub/scripts/run_backtest.py)
  now batches spot/futures instrument metadata lookup once per market before
  overriding fees and margins, instead of re-querying the catalog once per
  symbol

## Correct Experiment Definition

The qualification run must satisfy both:

1. Stable replay has started.
2. Stable replay is still causing new data to enter the experiment-owned cache.

If either condition is missing, the run is not a valid prefetch qualification.

## Experiment Arms

Use only these arms for the next phase:

1. `no_warmup`
2. `replay_prefetch_only`
3. optional startup control: `prewarm_gate_only`

Do not use `prewarm_gate_plus_replay_prefetch` as an experimental arm right
now. The current runner intentionally disables replay `local-read` prefetch when
launch-time prewarm is enabled.

## Environment Contract

Use the isolated harness worker only:

- worker URL: `http://100.65.27.118:10011`
- isolated catalog bind: `/opt/catalog`
- isolated JuiceFS cache dir: `/hdd16/jfscache_harness`

Hard requirements:

1. `--require-idle-worker`
2. `--reset-ssh-target` against the harness worker host only
3. `--cache-dir /hdd16/jfscache_harness`
4. `--drop-page-cache` only because this worker is already the isolated harness
   environment

## Qualification Criteria

The initial qualification run is `no_warmup` only and should not use the early
stop rule.

A run qualifies only if its `summary.json` shows all of:

1. `stable_chunk_probe_count >= 20`
2. `progress_new_files_touched_total > 0`
3. `progress_file_touch_active_ratio > 0`
4. `progress_cache_dir_bytes_growth_total > 0`
5. `progress_cache_dir_growth_active_ratio > 0`

Interpretation:

- `file_touch_*` says replay crossed into later parquet windows
- `cache_dir_*` says the experiment-owned cache still grew during replay

That combination is the minimum acceptable proxy for replay-time cold demand.

## Success Criteria For Replay Prefetch

After a workload qualifies, compare `no_warmup` vs `replay_prefetch_only`.

Replay-prefetch counts as materially helpful only if:

1. `replay_prefetch_only` shows real prefetch activity
2. replay-time `progress_cache_dir_bytes_growth_total` or active ratio drops
3. stable replay improves:
   - lower `stable_chunk_wall_ms_median`, and/or
   - higher `progress_events_per_second_median`

If replay-time cache growth stays unchanged, then prefetch is not hiding the
right cold path.

## Planned Execution Order

1. Generate a 38-pair harness from the maintained 38-pair overlay universe.
2. Run `no_warmup` alone to terminal completion on the isolated harness worker.
3. Check qualification metrics.
4. Only if qualified, run `replay_prefetch_only` against the same workload and
   same isolated reset procedure.
5. Add `prewarm_gate_only` only as a startup-stage control if needed.

## Planned Commands

Generate the 38-pair harness:

```bash
cd /home/joe/clawd-workspace/clawd/tbdtbd/backtest-hub

SPOT_SYMBOLS="$(
  awk '
    /spot_instrument_ids:/ {in_block=1; next}
    /futures_instrument_ids:/ {in_block=0}
    in_block && /BINANCE_SPOT/ {
      gsub(/^- +/, "", $0)
      gsub(/\\.BINANCE_SPOT$/, "", $0)
      printf("%s%s", sep, $0)
      sep=","
    }
  ' /home/joe/clawd-workspace/clawd/tbdtbd/strategy/configs/live/v5_runtime_universe_backtest_38pairs.yaml
)"

uv run python scripts/warmup_cycle_harness.py \
  --output-dir /home/joe/backtest-artifacts/warmup-cycle-harness-20260319-38pairs-qualification \
  --spot-symbols "${SPOT_SYMBOLS}" \
  --start 2025-11-10T00:00:00.000Z \
  --end 2025-11-20T00:00:00.000Z \
  --catalog-root /mnt/localB2fs_harness/backtest/catalog \
  --prefetch-ahead-hours 72 \
  --prefetch-max-files-per-batch 4
```

Run `no_warmup` qualification:

```bash
uv run python scripts/run_warmup_cycle_serial.py \
  --harness-dir /home/joe/backtest-artifacts/warmup-cycle-harness-20260319-38pairs-qualification \
  --worker-base-url http://100.65.27.118:10011 \
  --results-dir /home/joe/backtest-artifacts/warmup-cycle-serial-20260319-38pairs-qualification-no-warmup \
  --modes no_warmup \
  --poll-seconds 15 \
  --skip-initial-chunks 20 \
  --cache-stats-ssh-target root@neo-256gb \
  --reset-ssh-target root@neo-256gb \
  --cache-dir /hdd16/jfscache_harness \
  --drop-page-cache \
  --require-idle-worker
```

Do not set `--min-stable-progress-samples` in the qualification round.
