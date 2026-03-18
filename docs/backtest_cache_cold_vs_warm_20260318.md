# Backtest Cache Cold vs Warm Probe (2026-03-18)

## Goal

After proving old-vs-current worker image was not the main cause, isolate which cache layer explains the large wall-clock swing on the same historical V4 51-day / 38-pair probe.

Target spec:

- historical V4 live-candidate run shape
- `76` symbols (`38` spot + `38` perp)
- `chunk_size = 200000`
- `load_trade_ticks = true`
- `optimize_file_loading = false`

## Cache Layers Present On neo-256gb

### Shared read-only catalog mount

- mount: `/mnt/localB2fs`
- type: `fuse.juicefs`
- shared local cache dir: `/hdd16/jfscache`
- configured in `/etc/fstab`

### Kernel page cache

Before dropping caches:

- `Cached: ~68 GB`

After `sync; echo 3 > /proc/sys/vm/drop_caches`:

- `Cached: ~0.4 GB`

### Arrow cache

- `/hdd16/tmp/arrow_cache`
- effectively empty during this investigation
- not the dominant factor here

## Important Code-Level Meaning Of `before_prepare_queries`

From `scripts/run_backtest.py`, the long stall before the first chunk happens inside `_run_streaming()` between:

- `streaming_probe.stage = before_prepare_queries`
- `streaming_probe.stage = after_prepare_queries`

That region covers the per-`BacktestDataConfig` loop doing:

1. `catalog.get_file_list_from_data_cls(...)`
2. `catalog.filter_files(...)`
3. `_backend_session_with_optimize_fallback(...)`

So if a run is stuck at `before_prepare_queries`, it is not yet replaying chunks. It is still building the backend query plan over the catalog.

## Controlled Results

### A. Warm shared mount

Using the normal shared mount `/mnt/localB2fs/backtest/catalog` with the existing shared JuiceFS cache already warm:

- `time_to_before_query_result_s = 130.129`
- `time_to_first_chunk_s = 135.135`
- `time_to_5m_s = 205.203`
- `peak RSS = 183639.91 MB`

### B. Shared mount after dropping Linux page cache only

Same shared mount, same warm shared JuiceFS cache dir, but after clearing Linux page cache:

- `time_to_before_query_result_s = 230.236`
- `time_to_first_chunk_s = 235.242`
- `time_to_5m_s = 305.319`
- `peak RSS = 183628.96 MB`

Interpretation:

- dropping page cache slows the run materially
- but it still reaches 5M in about `305s`
- therefore Linux page cache is a secondary accelerator, not the main one

### C. Fresh cold JuiceFS mount with empty cache-dir

Created a temporary mount:

- mount: `/mnt/localB2fs_probe_cold_20260318`
- cache dir: `/hdd16/jfscache_probe_cold_20260318`
- initial cache dir size: `12K`

Observed behavior:

- `60s`: still `before_prepare_queries`, RSS only `265.73 MB`
- `180s`: still `before_prepare_queries`, RSS only `3735.40 MB`
- `300s`: still `before_prepare_queries`, RSS only `6442.39 MB`
- `360.4s`: still `before_prepare_queries`, `events_seen = 0`, `chunks_seen = 0`, RSS `8240.01 MB`
- by then the fresh JuiceFS cache dir had grown to `2.4G` across `1093` files

This run was stopped manually after enough evidence was gathered.

Interpretation:

- with an empty JuiceFS local cache, the run does not even finish query preparation after 6 minutes
- this is much slower than the shared-mount case, even after Linux page cache was explicitly dropped
- therefore the dominant speedup comes from the shared JuiceFS local cache dir, not from Linux page cache alone

## Conclusion

The backtest speed hierarchy for this workload is:

1. **Warm shared JuiceFS cache + warm/partial page cache**: fastest (`~205s` to 5M)
2. **Warm shared JuiceFS cache + dropped page cache**: slower but still healthy (`~305s` to 5M)
3. **Fresh empty JuiceFS cache dir**: dramatically slower; still stuck in query preparation after `360s`

So the main explanation for the earlier `~580s` / very slow cold runs is:

- cold JuiceFS local cache during catalog query preparation

Linux page cache matters, but it is not the dominant layer.

## Observability Follow-Up Implemented

To make the next cold-run diagnosis precise, `scripts/run_backtest.py` now writes a new `prepare_probe` block into `status.json`.

It reports per-`BacktestDataConfig` progress through:

- `before_file_list`
- `after_file_list`
- `after_filter_files`
- `after_register_session`

and includes:

- `config_index` / `config_count`
- `data_type`
- `instrument_id`
- `identifier_count`
- `identifier_sample`
- `optimize_file_loading`
- `cache_hit`
- `file_list_count`
- `filter_file_count`
- `file_list_ms`
- `filter_files_ms`
- `register_session_ms`
- `total_ms`
- `rss_mb`

This is meant specifically to answer future questions like:

- Which data class is cold?
- Is the stall inside file listing, file filtering, or backend session registration?
- Is the run reusing cached file lists or rebuilding them?

## Practical Next Step

If we want to reduce cold-start latency for backtests on this box, the high-value directions are:

1. prewarm / reuse JuiceFS local cache for hot catalog slices
2. reduce the number of cold catalog scans during query preparation
3. surface `prepare_probe` in progress/report tooling so slow cold starts are visible immediately
