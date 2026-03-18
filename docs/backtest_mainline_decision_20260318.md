# Backtest Mainline Decision Summary (2026-03-18)

## Purpose

This note freezes the mainline conclusions from the 2026-03-17 and
2026-03-18 backtest speed investigations so future work does not drift back
into already-ruled-out paths.

It is intentionally short and decision-oriented.

## What Is Proven

### 1. The primary slow region is pre-replay

The long stall happens while runs are still at:

- `streaming_probe.stage = before_prepare_queries`

That means the dominant delay is before the first replay chunk is produced.
The main work in that region is:

1. `catalog.get_file_list_from_data_cls(...)`
2. `catalog.filter_files(...)`
3. backend-session / query-plan registration

So this is not primarily a strategy decision-loop issue.

### 2. Cache warmth is a first-class performance variable

For the same 51-day / 38-pair historical workload on `neo-256gb`:

- warm shared JuiceFS cache + warm-ish page cache:
  about `205s` to `5M` events
- warm shared JuiceFS cache + dropped page cache:
  about `305s` to `5M`
- fresh cold JuiceFS cache:
  still stuck in `before_prepare_queries` after `360s+`

Conclusion:

- the large wall-clock swings are primarily explained by cache state
- Linux page cache matters, but shared JuiceFS local cache matters more

### 3. `optimize_file_loading` is a memory/speed tradeoff, not a free win

Current worker behavior on the same workload:

- `optimize=false`:
  about `183 GB` RSS, faster prepare
- `optimize=true`:
  about `46 GB` RSS, slower prepare

Conclusion:

- this flag is mainly a regime switch between higher-memory/faster-start and
  lower-memory/slower-start
- it should not be discussed as if it were a pure optimization

### 4. Old worker image is not the explanation

The historical fast run was compared against the current image on the same
historical V4 spec and current probe-capable runner.

Result:

- current image was not slower than the old cached image

Conclusion:

- old-vs-current image provenance is not the mainline explanation for the
  large slowdown

### 5. Submit-time background warmup is insufficient by itself

Background warmup in the API/control plane was able to:

- start safely
- overlap I/O
- grow the fresh JuiceFS cache substantially

But the runner could still remain in:

- `before_prepare_queries`
- `events_seen = 0`

for hundreds of seconds.

Conclusion:

- "start warmup in the background right before launching" is not enough
- the control plane needs a stronger readiness contract than "warmup started"

### 6. The AAVE issue is a storage/readability problem first

Current evidence points to storage-readability failure modes such as:

- host-local cache coverage differences
- mount-level `EIO` / timeout behavior
- B2 `download_cap_exceeded`

Conclusion:

- do not patch strategy or runner logic as if AAVE were a normal symbol-level
  missing-data case
- treat it as storage/data-plane recovery first

## What Is Ruled Out

The following are not good current mainline hypotheses:

1. "The strategy itself is the main reason replay start is slow"
2. "The old worker image was much faster"
3. "Chunk replay is the first dominant bottleneck"
4. "Background submit-time warmup alone solves the cold-start problem"
5. "AAVE should be handled as a normal strategy-level symbol skip"

## Mainline Direction

The current mainline should focus on cold or half-cold startup in
`before_prepare_queries`, especially:

1. reducing cold catalog/query-preparation cost
2. separating manifest planning from storage-specific warmup actuation
3. adding a worker-local readiness gate before runner launch
4. keeping replay-time prefetch backend-agnostic

## Design Boundary

The control plane should decide:

- what file set a run needs
- which worker will execute it
- whether that worker is ready to launch

The storage backend should decide:

- how those files become locally readable

This boundary must survive a future migration away from JuiceFS toward direct
B2 or another storage path.

## Immediate Guardrails

Until a better fix lands:

1. always record cache state when comparing backtest speed
2. do not restart old-image investigations as a mainline branch
3. do not treat `optimize_file_loading` as speed-only or memory-only
4. do not conflate storage-readability incidents with strategy bugs
5. prefer experiments that isolate pre-replay preparation cost rather than
   adding more replay-side speculation
