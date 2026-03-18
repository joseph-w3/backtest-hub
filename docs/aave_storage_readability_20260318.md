# AAVE Storage Readability Investigation (2026-03-18)

## Goal

Answer a specific question rigorously:

- if the current AAVE storage path is unreadable, how did the earlier completed
  51-day / 38-pair run succeed?

## Verified Facts

### 1. The historical completed run definitely included AAVE

Historical backtest:

- `20260310T165917Z_af13cdef298a4b508cb5813eed213665`

Recovered directly from the saved run directory inside the current
`neo-256gb` API container:

- path:
  `/app/scripts/20260310T165917Z_af13cdef298a4b508cb5813eed213665/run_spec.json`
- `start = 2025-11-10T00:00:00.000Z`
- `end = 2025-12-31T00:00:00.000Z`
- `strategy_entry = strategies.spread_arb.v4_early_exit:SpreadArbV4`
- `chunk_size = 200000`
- `strategy_config.spot_instrument_ids` contains:
  `AAVEUSDT.BINANCE_SPOT`
- `strategy_config.futures_instrument_ids` contains:
  `AAVEUSDT-PERP.BINANCE_FUTURES`

So the completed historical run was not an "AAVE excluded" case.

### 2. The current `neo-256gb` catalog path is unreadable for AAVE 2025-11-11

Inside the current `quant-trade-api-v1` container on `neo-256gb`, the catalog
mount is:

- `/opt/catalog`

The following file exists:

- `/opt/catalog/data/order_book_deltas/AAVEUSDT.BINANCE_SPOT/2025-11-11T00-00-00-111000000Z_2025-11-11T23-59-59-111000000Z.parquet`

Observed on `2026-03-18`:

- file size: `38,950,949 bytes`
- reading at offset `0` succeeds
- reading at offset `4,194,304` fails with:
  `Input/output error`

This matches the earlier pyarrow / metadata-read failure pattern: the file head
is present, but later chunks are unreadable.

### 3. Therefore the historical success was not caused by a graceful skip path

The historical run included AAVE and covered `2025-11-11`.

The current failure mode for that date is not a soft strategy-level issue; it
is a storage read failure before normal replay can proceed.

So the earlier completed run did **not** succeed because the backtester or
strategy gracefully ignored a broken AAVE file.

## What Is Still Not Distinguished

At least one of these must be true, but this note does not yet prove which one:

1. the relevant AAVE data was readable when the historical run executed, and
   became unreadable later
2. the historical run hit a different healthy local materialization / cache
   copy than the one current containers resolve to

## Related Raw-Data Findings

### Preprocess rebuild attempt failed before writing catalog

On `neo-test4`, submitted:

- preprocess id: `prep_20260318T192159Z_428676`

It failed during raw input loading in the orderbook builder:

- stage: `load_snapshots`
- operation: `pd.read_parquet(...)`
- error: `OSError: [Errno 5] Input/output error`

So the problem is not limited to the catalog output file alone.

### User-space JuiceFS reads fail too, with explicit B2 cap errors

To separate "FUSE mount/cache corruption" from "underlying object fetch failure",
`juicefs sync` was run on `neo-256gb` using the metadata URL directly:

- metadata:
  `postgres://postgres:****@127.0.0.1:15432/b2fs`
- source:
  `jfs://myfs/depth-delta-a/binance/spot/2025-11-11/AAVEUSDT_snapshot.parquet`

Result:

- the read did **not** succeed through the user-space path either
- the failure was explicit:
  `download_cap_exceeded`
- example slice/object failures:
  - `chunks/5/5011/5011771_0_4`
  - `chunks/5/5016/5016326_0_171675`
  - `chunks/5/5016/5016327_0_1362`

This is materially stronger than the earlier mount-level symptoms (`timeout`,
`EIO`):

- at least part of the current inability to read AAVE is caused by the backing
  B2 account/bucket refusing further downloads due to cap limits
- therefore some "corruption-like" behavior at the mount level can be a
  secondary symptom of object-fetch refusal, not only of permanently bad file
  contents

### Different hosts show different symptoms because cache coverage differs

Examples observed on `2026-03-18`:

- `neo-256gb` current container:
  - AAVE catalog file head could be read
  - later offsets failed with `EIO`
- `neo-test4` host mount:
  - the same catalog file timed out even at offset `0`
- `neo-256gb` local JuiceFS cache:
  - contained only the first catalog chunk:
    `8369253_0_4194304`
- `neo-test4` and `neo-test1` local cache probes:
  - did not contain the sampled AAVE snapshot/raw/catalog chunks

This supports the following interpretation:

- hosts are not seeing one perfectly uniform failure mode
- cached slices can make a file appear "partially readable" on one node while
  another node cannot read the same file at all
- current symptoms are therefore a mix of:
  - cached-chunk availability
  - mount behavior
  - and B2 download-cap refusal

### Raw AAVE backup files exist, but no healthy backup is confirmed yet

Example backup files present on `neo-test4`:

- `AAVEUSDT_delta.parquet.backup.1762866441281.l9qfis7jf`
- `AAVEUSDT_inline_snapshot.parquet.backup.1762866515502.ijqf44kd7`
- `AAVEUSDT_metadata.parquet.backup.1762866574558.ss0crm4dp`
- `AAVEUSDT_delta.parquet.backup.1762952798534.wxqprif16`

However, sampled `timeout 10 dd ... skip=4096` reads on both primary and backup
files did not complete within the timeout window, so there is not yet evidence
that these backups are healthy replacements.

## Operational Conclusion

Current working conclusion:

- do not patch strategy or runner logic to "handle" this as if it were a normal
  symbol-level data gap
- treat it as a storage/data readability problem first
- the next recovery step is to obtain a readable authoritative copy of the
  affected AAVE raw/catalog objects, which currently may require:
  - resolving the B2 download cap issue
  - or locating a host that still has the needed chunks hot in local cache
  - then rebuilding and revalidating
