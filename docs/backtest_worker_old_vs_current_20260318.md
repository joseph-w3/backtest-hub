# Backtest Worker Old vs Current Image Probe (2026-03-18)

## Goal

Test whether the historical "fast" worker image was materially faster than the current worker image when both run the same historical V4 51-day / 38-pair data plane.

This was motivated by the gap between:

- historical full run `20260310T165917Z_af13cdef298a4b508cb5813eed213665`
- current-worker 2x2 probe results, where some runs took much longer to reach 5M events

## Image Provenance Recovered From Cached Images

On `neo-256gb`, the cached images expose the installed git revisions via `direct_url.json`.

### Historical cached API image

- image: `quant-script-api:latest`
- created: `2026-02-28T04:25:31+01:00`
- `quant-script-api` commit: `9e702d37d659ecca67e93ed29e538c262b9d4f82`
- `quant-trade-v1` / `nautilus_trader` commit: `b09916f3ab788a82835a40b3be3949ad95154d9e`

### Current live API image

- image: `quant-script-api-live:latest`
- created: `2026-03-17T07:25:47+01:00`
- image label `ai.ulysses.quant_script_api_commit`: `4a98af2aede73ee0d064fea9753654c1a247dd3b`
- image label `ai.ulysses.quant_trade_v1_commit`: `7300c3aa7651819b8197f545bf01705e9f4f6c38`

## Important Discovery About The Historical Runner

The original saved `run_backtest.py` from historical run
`20260310T165917Z_af13cdef298a4b508cb5813eed213665` predates chunk-level progress heartbeat.

It does write `status.json`, but not the later:

- `node_probe`
- `streaming_probe`
- `streaming_summary.events_seen`

That means the modern 5M-event controller cannot stop precisely at 5M against the untouched historical runner.

## Controlled Comparison Method

To isolate image differences from runner/spec drift:

1. Reused the historical saved run directory contents:
   - same `run_spec.json`
   - same `strategies.zip`
   - same `strategy_bundle/`
2. Replaced only `run_backtest.py` with the current probe-capable runner.
3. Changed only `backtest_id` per probe directory.
4. Ran the same controller (`worker_ab_probe.py`) against:
   - old image `quant-script-api:latest`
   - current image `quant-script-api-live:latest`

### Key integrity check

`diff -u` between:

- `abprobe_hist_probe_curimg/run_spec.json`
- `abprobe_v4_currentbuild/run_spec.json`

showed only the `backtest_id` changed.

`cmp -s` between:

- `abprobe_hist_probe_curimg/run_backtest.py`
- `abprobe_v4_currentbuild/run_backtest.py`

returned identical.

So any large timing gap here cannot be explained by run spec or runner drift.

## Historical Data Plane Characteristics

From the historical run spec / defaults:

- duration: `2025-11-10` to `2025-12-31`
- universe: `76` symbols (`38` spot + `38` perp)
- `chunk_size = 200000`
- strategy: `strategies.spread_arb.v4_early_exit:SpreadArbV4`
- `load_trade_ticks = true` by default
- `optimize_file_loading = false` by default

This is the high-memory path.

## Results

### Same historical V4 spec, same current probe runner

| Image | quant-script-api | quant-trade-v1 | Time to before_query_result | Time to first chunk | Time to 5M | First chunk -> 5M | Peak RSS |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| old cached image | `9e702d37` | `b09916f3` | `145.150s` | `150.156s` | `215.225s` | `65.069s` | `183541 MB` |
| current live image | `4a98af2a` | `7300c3aa` | `130.129s` | `135.135s` | `205.203s` | `70.069s` | `183640 MB` |

### Interpretation

- The current image is not slower than the old image on this historical V4 data plane.
- Current image is actually slightly faster overall:
  - about `10.0s` faster to 5M
  - about `15.0s` faster to first chunk
- Peak RSS is effectively the same: about `183.5 GB`

Conclusion: the historical "fast worker image" is **not** the explanation for the observed regression.

## Why The Earlier 2x2 Showed ~580s

Earlier current-worker probe result:

- `abprobe_v4_currentbuild`
- `time_to_5m_s = 580.124`

But the rerun above with the same run spec and same runner reached:

- `time_to_5m_s = 205.203`

Because the rerun used:

- identical `run_backtest.py`
- identical `run_spec.json` except `backtest_id`

the large delta is not explained by code/spec drift.

The strongest remaining explanation is cache state:

- cold vs warm catalog / filesystem / Arrow / OS page cache
- the first heavy scan pays the full catalog discovery + parquet metadata cost
- subsequent runs on the same data plane are much faster

## Practical Conclusion

1. Do not chase old worker image rebuilds as the main explanation for the slowdown.
2. Treat cache-warmth as a first-class variable in backtest speed experiments.
3. Any future performance comparison must record whether the run is:
   - cold cache
   - warm cache
4. The dominant remaining investigation target is replay-path behavior under cold-cache conditions, not old-vs-current worker image provenance.
