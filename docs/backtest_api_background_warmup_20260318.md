# API Background Warmup Probe (2026-03-18)

## Goal

Validate whether moving catalog prewarm out of `run_backtest.py` and into the API/control-plane layer is enough to fix the slow cold-cache start on `neo-256gb`.

This probe was run after:

- rejecting runner-side synchronous prewarm as the wrong placement
- landing `quant-script-api` background warmup support
- deploying that API build to `neo-256gb`

## What Was Deployed

Two changes were pushed and deployed:

- `quant-script-api` `dc8de61`
  - adds `quant_script_api.backtest_prewarm`
  - `POST /v1/scripts/run_backtest` now calls `maybe_start_catalog_warmup(...)`
  - response now includes `catalog_warmup`
- `trade-dev-container` `3d9e95e`
  - bind-mounts `/usr/local/bin/juicefs` into API containers so the helper can call `juicefs warmup`

Deployment validation on `neo-256gb`:

- API container restarted successfully
- `/usr/local/bin/juicefs` is mounted inside `quant-trade-api-v1`
- a smoke submit returned:
  - `catalog_warmup.status = "started"`
  - `reason = "auto_fs_type_fuseblk"`

## Cold-Mount Experiment

### Setup

Use the same current-build V5 probe shape as the earlier cold-cache investigation:

- source workdir: `abprobe_v5_currentbuild`
- 51-day window
- 76 symbols (38 spot + 38 perp)
- `chunk_size = 200000`

Create a fresh JuiceFS mount on `neo-256gb`:

- mount dir: `/mnt/localB2fs_probe_bgwarm_<id>`
- cache dir: `/hdd16/jfscache_probe_bgwarm_<id>`

Run a one-off container from `quant-script-api-live:latest` that:

1. calls `maybe_start_catalog_warmup(...)` against the fresh mount
2. immediately starts the existing `worker_ab_probe.py`

Important detail:

- warmup ran in `--background`
- runner launch was not delayed after warmup start

### Immediate Warmup Result

The helper started immediately and did not block the control plane:

- `catalog_warmup.status = "started"`
- `reason = "explicit"`
- `file_count = 11780`

`catalog-prewarm.log` showed:

- `warmup cache for 10240 paths in background`
- `warmup cache for 1540 paths in background`

## Observed Timeline

At these wall-clock checkpoints, the runner was still stuck before replay:

- `30s`: `stream_stage = before_prepare_queries`, `events_seen = 0`
- `60s`: `before_prepare_queries`, `events_seen = 0`
- `150s`: `before_prepare_queries`, `events_seen = 0`
- `300s`: `before_prepare_queries`, `events_seen = 0`
- `361s`: `before_prepare_queries`, `events_seen = 0`
- `571s`: `before_prepare_queries`, `events_seen = 0`

The stdout tail was still ending at:

- `DataClient-BACKTEST: READY`
- `DataEngine: Registered BACKTEST`

So the run never exited the same pre-replay region identified earlier.

## Cache / RSS Behavior

The background warmup clearly overlapped I/O and filled the fresh JuiceFS cache much faster than the old cold baseline:

- around `~150s`: fresh cache already about `14G / 7588 files`
- around `~300s`: about `27G / 11631 files`
- around `~420s`: about `45G / 16728 files`

At the same time, process RSS stayed low relative to the old cold baseline:

- `30s`: `~361 MB`
- `150s`: `~716 MB`
- `300s`: `~948 MB`
- `571s`: `~1343 MB`

Compare with the earlier cold `off` baseline from `backtest_cache_cold_vs_warm_20260318.md`:

- `360s`: still `before_prepare_queries`
- only `~2.4G / 1093 files` had reached the fresh cache dir
- RSS was already `~8240 MB`

## Conclusion

API-layer background warmup is:

- safe enough to deploy
- non-blocking
- effective at overlapping JuiceFS file warmup with runner startup

But it is **not sufficient** to solve the main slow-start problem.

Even after the fresh cache had absorbed tens of GB and most of the target file set, the runner still had not escaped:

- `stream_stage = before_prepare_queries`
- `events_seen = 0`

So the real bottleneck is not fixed by "start warmup in background at submit time" alone.

## What This Implies

The likely problem is one of these:

1. the runner begins expensive catalog/query-preparation work before enough warmup has completed
2. the slow region depends on metadata / listing / backend-session preparation more than on bulk file-content warmup
3. submission-time warmup is simply too late; prewarm has to happen before runner launch, not concurrently with it

## Next Fix Direction

The next fix should move from "background warmup at submit time" to a stronger control-plane contract:

1. assign a worker
2. prewarm on that worker
3. wait for a bounded readiness condition
4. only then launch the runner

Candidate implementations:

- worker/API-side staged launch with an explicit warmup readiness gate
- persisted file manifests / query-plan artifacts so the runner reuses them instead of rebuilding from cold state
- scheduler/control-plane prewarm job before the run enters actual execution

What should **not** be assumed anymore:

- that calling `juicefs warmup --background` right before `runner.start()` is enough on its own
