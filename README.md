# Postgres CTID Backfill Benchmark

This repo sets up a local PostgreSQL 16 benchmark for the migration pattern you described:

- a large OLTP table with a random UUID primary key
- a new nullable `timestamptz` column added after the table is already large
- new inserts and updates populate the new column immediately
- old rows are backfilled in batches by extracting a value from a large `jsonb` payload
- the comparison is `UUID PK ordered`, `CTID ordered`, and `chunked CTID ordered` batches

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
  - running the chunked CTID-ordered backfill
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
.\scripts.ps1 BackfillCtidChunked -BatchSize 1000 -QueueChunkRows 100000 -LogEvery 100
```

The procedure:

- snapshots or builds the current backfill candidates into a temp queue
- processes that queue in batches of `p_batch_size`
- commits after each batch
- records the final metrics in `bench.benchmark_runs`

For `ctid_chunked`, the queue is built in repeated CTID-ordered chunks of `queue_chunk_rows` rows instead of one giant upfront queue. In that variant, `queue_build_ms` is the cumulative time spent building all queue chunks.

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

Compare only the two CTID strategies:

```powershell
.\scripts.ps1 CompareCtid -RowCount 1000000 -HotPct 2.5 -TemplateCount 128 -BlobTargetBytes 2500 -BatchSize 1000 -QueueChunkRows 100000 -LogEvery 100 -WorkloadSeconds 600
```

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
- `BackfillCtidChunked`
- `Results`
- `Compare`
- `CompareCtid`
- `Down`
- `Destroy`

## Notes On The CTID Variants

The CTID queues store both the primary key and the row's `ctid` directly as PostgreSQL's native `tid` type, then process the queue ordered by that `tid`. The queue still updates rows by primary key, not by `ctid`, so concurrent row movement does not break correctness.

`backfill_ctid_order` builds one full queue up front. `backfill_ctid_chunked_order` rebuilds smaller CTID-ordered queues repeatedly, which is useful when you want to reduce the startup scan and temp-space footprint of the full-queue approach.

`backfill_ctid_chunked_order` uses `ctid` as a cross-chunk cursor with `WHERE ctid > last_seen_tid`. That is only safe if your application invariant is already true: every concurrent update that changes the source data also writes `extracted_at` correctly. If that invariant is not enforced, a row can move to a different physical location between chunks and be skipped by the chunked queue builder.

For that reason:

- `backfill_ctid_order` is the safer general-purpose benchmark shape
- `backfill_ctid_chunked_order` is best treated as an operational tradeoff when you trust the dual-write invariant and want to reduce the impact of one giant upfront queue

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

### 1M Rows, Full CTID vs Chunked CTID

Observed pair with `queue_chunk_rows = 100000`:

| variant | batch_size | queue_chunk_rows | queue_rows | rows_updated | queue_build_ms | elapsed_ms | rows_updated_per_sec |
|---|---:|---:|---:|---:|---:|---:|---:|
| `ctid` | 1000 |  | 1000000 | 984379 | 1167.473 | 29319.984 | 33573.65 |
| `ctid_chunked` | 1000 | 100000 | 977929 | 977562 | 2508.398 | 33542.014 | 29144.40 |

Takeaway:

- chunking reduced the size of any one queue build, but it was slower overall on this 1M-row benchmark
- the full upfront CTID queue still had better throughput

### Chunk-Size Sweep For CTID Chunked

Fresh reseeds were used for each pair.

| queue_chunk_rows | ctid elapsed_ms | ctid_chunked elapsed_ms | ctid rows_updated_per_sec | ctid_chunked rows_updated_per_sec |
|---:|---:|---:|---:|---:|
| 50000 | 28871.954 | 37753.332 | 33819.36 | 25829.30 |
| 100000 | 31299.222 | 39549.028 | 31155.44 | 24653.15 |
| 250000 | 32814.201 | 35920.531 | 29713.63 | 27143.72 |
| 500000 | 33584.354 | 35134.823 | 29031.61 | 27750.30 |

Takeaways:

- larger chunks helped `ctid_chunked` considerably
- `500000` was the closest chunked run to full `ctid`
- on this 1M-row benchmark, full `ctid` still won at every tested chunk size
- the chunked variant looks more like an operational compromise for very large tables than a raw throughput winner

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
