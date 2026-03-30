# Postgres CTID Backfill Benchmark

This repo sets up a local PostgreSQL 16 benchmark for the migration pattern you described:

- a large OLTP table with a random UUID primary key
- a new nullable `timestamptz` column added after the table is already large
- new inserts and updates populate the new column immediately
- old rows are backfilled in batches by extracting a value from a large `jsonb` payload
- the comparison is `UUID PK ordered`, `CTID ordered`, and `live CTID-cursor` batches

The benchmark is intentionally built around throughput of the backfill job while concurrent inserts and updates continue to run.

## What Is Included

- Dockerized PostgreSQL 16
- A synthetic `bench.orders` table with:
  - random UUID primary key
  - large `jsonb` payload
  - nullable `extracted_at timestamptz`
  - hot-row markers so updates concentrate on the newest `2.5%` of seeded rows
- PL/pgSQL procedures for:
  - seeding the fixture
  - resetting it
  - running the GUID-ordered backfill
  - running the CTID-ordered backfill
  - running the live CTID-cursor backfill
- `pgbench` scripts for concurrent inserts and updates
- [`scripts.ps1`](/C:/Code/20260329%20PostgresqlLoadTesting/scripts.ps1) as the primary way to run the benchmark

## Important Scaling Note

If you truly model `10,000,000` rows with `12-20 KB` payload text per row, disk usage gets very large very quickly, even before TOAST overhead and WAL are counted.

Because of that, the fixture is parameterized:

- `p_row_count` controls row count
- `p_blob_target_bytes` controls the size of the generated blob field inside the `jsonb`

The default blob target is `2500` bytes, which is usually enough to push the payload into TOAST territory without immediately making the local benchmark enormous. If you want to chase the production shape more closely, increase it gradually and watch disk growth.

## Quick Start

Start Postgres:

```powershell
.\scripts.ps1 Up
```

Seed a quick smoke-test fixture:

```powershell
.\scripts.ps1 Seed -RowCount 200000 -HotPct 2.5 -TemplateCount 64 -BlobTargetBytes 2500
```

Seed a bigger fixture:

```powershell
.\scripts.ps1 Seed -RowCount 10000000 -HotPct 2.5 -TemplateCount 128 -BlobTargetBytes 2500
```

## Inspect The Fixture

Show counts by source and hot flag:

```powershell
.\scripts.ps1 Shape
```

Sample raw payload text sizes:

```powershell
.\scripts.ps1 PayloadSize
```

Check whether the payloads are mostly toasted or compressed:

```powershell
.\scripts.ps1 ToastCheck
```

This samples rows and shows:

- `stored_bytes`: approximate on-row size of the `payload` datum
- `payload_text_bytes`: size of the rendered JSON text
- `likely_external_rows`: sampled rows where the stored datum is tiny enough to strongly suggest an out-of-line TOAST pointer
- `compressed_rows`: sampled rows where PostgreSQL reports toast compression
- `toast_relation_total_size`: total size of the table's toast relation

Show heap, index, and total relation sizes:

```powershell
.\scripts.ps1 RelationSize
```

Show the hot-row count used by the update workload:

```powershell
.\scripts.ps1 HotCount
```

## Running One Variant

The cleanest comparison is:

1. seed the fixture
2. start concurrent OLTP inserts and updates
3. run one backfill variant
4. capture the result from `bench.benchmark_runs`
5. stop the workload
6. reseed from scratch for the next variant

Reseeding matters because physical row layout is part of what you are testing.

Start the detached workload:

```powershell
.\scripts.ps1 StartWorkload -UpdateClients 4 -UpdateThreads 4 -InsertClients 2 -InsertThreads 2 -WorkloadSeconds 600
```

Tune `-UpdateClients`, `-UpdateThreads`, `-InsertClients`, `-InsertThreads`, and `-WorkloadSeconds` until the background workload looks like your environment.

This helper starts detached `docker compose exec ... pgbench` processes, writes their output under `.runtime\logs`, and records their PIDs in `.runtime\workload-state.json`.

Run one of the backfill variants:

```powershell
.\scripts.ps1 BackfillGuid -BatchSize 1000 -LogEvery 100
```

```powershell
.\scripts.ps1 BackfillCtid -BatchSize 1000 -LogEvery 100
```

```powershell
.\scripts.ps1 BackfillCtidLive -BatchSize 1000 -LogEvery 100
```

The selected variant:

- either snapshots the current backfill candidates into a temp queue or reads live batches directly from the main table
- processes rows in batches of `p_batch_size`
- commits after each batch
- records the final metrics in `bench.benchmark_runs`

For `ctid_live`, no temp queue table is built. Each batch reads the next live slice of the main table with a `ctid > last_seen_tid` cursor. In that variant, `queue_build_ms` is the cumulative time spent selecting those live batches.

If concurrent OLTP updates populate `extracted_at` before the backfill reaches a row, that row is still removed from the queue but is not rewritten again.

Read the result:

```powershell
.\scripts.ps1 Results -ResultLimit 5
```

Stop the detached workload:

```powershell
.\scripts.ps1 StopWorkload
```

## Compare Variants

Compare GUID vs full CTID on fresh reseeds:

```powershell
.\scripts.ps1 Compare -RowCount 1000000 -HotPct 2.5 -TemplateCount 128 -BlobTargetBytes 2500 -BatchSize 1000 -LogEvery 100 -WorkloadSeconds 600
```

That avoids comparing one run on a freshly loaded heap and the next run on a table that has already been fully rewritten once.

## Script Surface

Run this to see the available actions and parameters:

```powershell
.\scripts.ps1 Help
```

Useful actions:

- `Up`
- `Ps`
- `Seed`
- `Reset`
- `Shape`
- `PayloadSize`
- `ToastCheck`
- `RelationSize`
- `HotCount`
- `StartWorkload`
- `StopWorkload`
- `BackfillGuid`
- `BackfillCtid`
- `BackfillCtidLive`
- `Results`
- `Compare`
- `Down`
- `Destroy`

## Notes On The CTID Variants

The CTID queues store both the primary key and the row's `ctid` directly as PostgreSQL's native `tid` type, then process the queue ordered by that `tid`. The queue still updates rows by primary key, not by `ctid`, so concurrent row movement does not break correctness.

`backfill_ctid_order` builds one full queue up front.

`backfill_ctid_live_cursor` skips the temp queue entirely and advances directly through the main table with `ctid > last_seen_tid`. On PostgreSQL 16, that shape can use a `Tid Range Scan`, which makes it a very relevant variant to benchmark.

`backfill_ctid_live_cursor` uses `ctid` as a cross-batch cursor with `WHERE ctid > last_seen_tid`. That is only safe if your application invariant is already true: every concurrent update that changes the source data also writes `extracted_at` correctly. If that invariant is not enforced, a row can move to a different physical location between batches and be skipped by the live cursor.

For that reason:

- `backfill_ctid_order` is the safer general-purpose benchmark shape
- `backfill_ctid_live_cursor` is only appropriate when you trust that dual-write invariant, because it uses `ctid` as a cross-batch cursor against a live table

## Session Results

These are the main local benchmark results observed during this session on this machine. Treat them as directional, not universal.

### 1M Rows, GUID vs Full CTID

Observed pair:

| variant | batch_size | queue_rows | rows_updated | queue_build_ms | elapsed_ms | rows_updated_per_sec |
|---|---:|---:|---:|---:|---:|---:|
| `ctid` | 1000 | 999397 | 978467 | 1119.598 | 27780.369 | 35221.53 |
| `guid` | 1000 | 1000000 | 987885 | 1269.487 | 42360.566 | 23320.86 |

Takeaway:

- full `ctid` was materially faster than GUID order
- in that run, `ctid` completed about 34% faster and delivered about 51% higher update throughput

### 1M Rows, Live CTID Cursor

Observed run:

| variant | batch_size | queue_rows | rows_updated | queue_build_ms | elapsed_ms | rows_updated_per_sec |
|---|---:|---:|---:|---:|---:|---:|
| `ctid_live` | 1000 | 975007 | 975007 | 82628.729 | 102267.711 | 9533.87 |

Takeaways:

- `ctid_live` was much slower than the full upfront CTID queue on this 1M-row benchmark
- the main reason was repeated live batch selection cost, not the update itself
- in this variant, `queue_build_ms` means cumulative batch-selection time, and it dominated the run

Why it was slower:

- `ctid_live` does not materialize a queue once and then consume it
- instead, every batch reruns a live query of the form `WHERE extracted_at IS NULL AND ctid > last_seen_tid ORDER BY ctid LIMIT batch_size`
- on PostgreSQL 16, that shape planned as a `Tid Range Scan` plus a `Sort`
- that means the database still paid meaningful selection work for every batch
- by contrast, the full CTID queue pays candidate discovery once up front and then spends the rest of the run deleting from the temp queue and updating by primary key

## Reset And Tear Down

Reset rows without reseeding:

```powershell
.\scripts.ps1 Reset
```

Stop Postgres:

```powershell
.\scripts.ps1 Down
```

Stop and delete the database volume:

```powershell
.\scripts.ps1 Destroy
```
