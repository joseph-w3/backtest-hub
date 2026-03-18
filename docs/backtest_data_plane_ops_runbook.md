# Backtest Data Plane Ops Runbook

## Scope

This runbook records operational learnings for the backtest data plane:

- JuiceFS-backed catalog/raw storage
- B2 object download limits
- mount-level `EIO` / timeout symptoms
- how to distinguish storage failures from strategy or runner bugs

It is intentionally operator-focused rather than implementation-focused.

## Fast Triage Order

When a backtest or preprocess job fails while reading raw/catalog parquet:

1. confirm whether the target run/spec actually includes the symbol/date in question
2. test mount-level readability on the affected host
3. test JuiceFS user-space readability via `jfs://...` against metadata directly
4. only after storage is ruled out, inspect strategy/runner logic

Do **not** start by patching strategy code for what may be a storage-plane failure.

## Key Failure Modes Observed

### 1. Same underlying issue can look different on different hosts

Observed symptoms included:

- `dd` timeout at offset `0`
- `Input/output error` at later offsets
- parquet metadata reads failing

These differences can be caused by host-local cache coverage, not by different
logical files.

### 2. B2 cap exhaustion can masquerade as file corruption

During investigation, FUSE/mount paths showed `timeout` or `EIO`, but
user-space JuiceFS reads later revealed the deeper cause:

- `download_cap_exceeded`

Implication:

- a file that looks "corrupt" from the mount is not necessarily permanently
  damaged
- it may simply be that the required object slices cannot currently be fetched
  from B2

### 3. Host cache can create partial-read illusions

One host may read a file head successfully while another cannot read offset `0`
at all. That does not automatically mean two different files exist.

It can mean:

- one host already has some chunks hot in local JuiceFS cache
- another host must fetch those chunks remotely and currently cannot

## Minimal Verification Commands

### A. Mount-level readability check

Use this first for a fast host-local signal:

```bash
timeout 6 dd if="$FILE" of=/dev/null bs=1 count=16 skip=0 status=none
timeout 6 dd if="$FILE" of=/dev/null bs=1 count=16 skip=4096 status=none
timeout 6 dd if="$FILE" of=/dev/null bs=1 count=16 skip=4194304 status=none
```

Interpretation:

- `RC:0` means that offset was readable
- `RC:124` means timed out
- `RC:1` with `Input/output error` means mount-level read failure

### B. JuiceFS object mapping

Use this to see the backing chunk objects for a file:

```bash
juicefs info /mnt/b2fs/path/to/file.parquet
```

This gives:

- inode
- logical size
- chunk object names such as `trade-b2fs/chunks/...`

### C. User-space read check via `jfs://`

This is the most important discriminator when mount behavior is ambiguous.

Example:

```bash
export myfs='postgres://USER:PASSWORD@127.0.0.1:15432/b2fs'

juicefs sync \
  --threads 1 \
  --list-threads 1 \
  --limit 1 \
  'jfs://myfs/depth-delta-a/binance/spot/2025-11-11/BTCUSDT_snapshot.parquet' \
  /tmp/btc_snapshot_20251111.parquet
```

Interpretation:

- success here means the underlying JuiceFS/object path is readable
- failure with `download_cap_exceeded` means B2 caps are currently the blocker
- failure here and on the mount is much stronger evidence than mount-only `EIO`

Note:

- for a single file source, the destination must also be a single file path
- do not point a single-file source at an already-created directory path

## Current Known Metadata

For the investigated `trade-b2fs` volume, metadata showed:

- storage backend: `b2`
- bucket: `trade-data`

This matters because operator fixes may need to happen at the B2 account/cap
level, not inside the strategy or API repo.

## Recovery Playbook

If a symbol/date suddenly looks unreadable:

1. reproduce on mount with `dd`
2. run the same file through `juicefs sync` from `jfs://...`
3. if it reports `download_cap_exceeded`:
   - treat it as a B2 capacity/transaction issue first
   - do not conclude permanent file corruption
4. once object reads recover:
   - re-test raw files
   - re-run preprocess for the affected day/symbol
   - only then revisit catalog regeneration / backtest reruns

## Design Implications

Operators should remember:

- submit-time warmup success does not prove future reads will succeed
- mount behavior alone is not a sufficient truth source
- user-space JuiceFS reads are a better discriminator than raw FUSE symptoms
- backtest/preprocess systems should expose storage-read failure classes more
  directly, instead of collapsing them into generic `EIO`

