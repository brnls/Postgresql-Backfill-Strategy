# Postgres CTID Backfill Benchmark

This repo sets up a local PostgreSQL 16 benchmark for the migration pattern you described:

- a large OLTP table with a random UUID primary key
- a new nullable `timestamptz` column added after the table is already large
- new inserts and updates populate the new column immediately
- old rows are backfilled in batches by extracting a value from a large `jsonb` payload
- the comparison is `UUID PK ordered` batches vs `CTID ordered` batches

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

## Important Scaling Note

If you truly model `10,000,000` rows with `12-20 KB` payload text per row, disk usage gets very large very quickly, even before TOAST overhead and WAL are counted.

Because of that, the fixture is parameterized:

- `p_row_count` controls row count
- `p_blob_target_bytes` controls the size of the generated blob field inside the `jsonb`

The default blob target is `2500` bytes, which is usually enough to push the payload into TOAST territory without immediately making the local benchmark enormous. If you want to chase the production shape more closely, increase it gradually and watch disk growth.

## Start Postgres

```powershell
docker compose up -d
docker compose ps
```

Or with the helper script:

```powershell
.\scripts.ps1 Up
```

## Seed A Fixture

This creates the base table state. The seeded rows all start with `extracted_at = NULL`.

For a quick smoke test:

```powershell
docker compose exec -T postgres psql -U postgres -d bench -v ON_ERROR_STOP=1 -c "CALL bench.seed_fixture(200000, 2.5, 64, 2500);"
```

For the bigger scenario:

```powershell
docker compose exec -T postgres psql -U postgres -d bench -v ON_ERROR_STOP=1 -c "CALL bench.seed_fixture(10000000, 2.5, 128, 2500);"
```

Helper script:

```powershell
.\scripts.ps1 Seed -RowCount 10000000 -HotPct 2.5 -TemplateCount 128 -BlobTargetBytes 2500
```

Parameter order:

1. `p_row_count`
2. `p_hot_pct`
3. `p_template_count`
4. `p_blob_target_bytes`
5. optional `p_history_span`

## Inspect The Fixture

Check the table shape:

```powershell
docker compose exec -T postgres psql -U postgres -d bench -c "SELECT source, is_hot, count(*) FROM bench.orders GROUP BY 1, 2 ORDER BY 1, 2;"
```

Helper script:

```powershell
.\scripts.ps1 Shape
```

Check the approximate raw payload text size:

```powershell
docker compose exec -T postgres psql -U postgres -d bench -c "SELECT avg(octet_length(payload::text))::bigint AS avg_payload_text_bytes, min(octet_length(payload::text)) AS min_payload_text_bytes, max(octet_length(payload::text)) AS max_payload_text_bytes FROM (SELECT payload FROM bench.orders TABLESAMPLE SYSTEM (1) LIMIT 1000) AS sample_rows;"
```

Helper script:

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

Check relation sizes:

```powershell
docker compose exec -T postgres psql -U postgres -d bench -c "SELECT pg_size_pretty(pg_table_size('bench.orders')) AS heap, pg_size_pretty(pg_indexes_size('bench.orders')) AS indexes, pg_size_pretty(pg_total_relation_size('bench.orders')) AS total;"
```

Helper script:

```powershell
.\scripts.ps1 RelationSize
```

## Running One Variant

The cleanest comparison is:

1. seed the fixture
2. start concurrent OLTP inserts and updates
3. run one backfill variant
4. capture the result from `bench.benchmark_runs`
5. reseed from scratch
6. run the other variant

Reseeding matters because physical row layout is part of what you are testing.

### Step 1: Get The Hot Row Count

```powershell
$hotCount = docker compose exec -T postgres psql -U postgres -d bench -At -c "SELECT count(*) FROM bench.hot_seed_ids;"
```

Helper script:

```powershell
.\scripts.ps1 HotCount
```

### Step 2: Start Concurrent Updates And Inserts

These jobs keep running while the backfill procedure runs.

```powershell
$updateJob = Start-Job -ArgumentList $hotCount -ScriptBlock {
    param($HotCount)
    docker compose exec -T postgres pgbench -U postgres -d bench -n -c 4 -j 4 -T 600 -D hot_count=$HotCount -f /pgbench/oltp_updates.sql
}

$insertJob = Start-Job -ScriptBlock {
    docker compose exec -T postgres pgbench -U postgres -d bench -n -c 2 -j 2 -T 600 -f /pgbench/oltp_inserts.sql
}
```

Tune `-c`, `-j`, and `-T` until the background workload looks like your environment.

Helper script:

```powershell
.\scripts.ps1 StartWorkload -UpdateClients 4 -UpdateThreads 4 -InsertClients 2 -InsertThreads 2 -WorkloadSeconds 600
```

This helper starts detached `docker compose exec ... pgbench` processes, writes their output under `.runtime\logs`, and records their PIDs in `.runtime\workload-state.json` so you can stop them later with:

```powershell
.\scripts.ps1 StopWorkload
```

### Step 3: Run The Backfill

GUID PK ordered:

```powershell
docker compose exec -T postgres psql -U postgres -d bench -v ON_ERROR_STOP=1 -c "CALL bench.backfill_guid_pk_order(1000, 100);"
```

Helper script:

```powershell
.\scripts.ps1 BackfillGuid -BatchSize 1000 -LogEvery 100
```

CTID ordered:

```powershell
docker compose exec -T postgres psql -U postgres -d bench -v ON_ERROR_STOP=1 -c "CALL bench.backfill_ctid_order(1000, 100);"
```

Helper script:

```powershell
.\scripts.ps1 BackfillCtid -BatchSize 1000 -LogEvery 100
```

Chunked CTID ordered:

```powershell
docker compose exec -T postgres psql -U postgres -d bench -v ON_ERROR_STOP=1 -c "CALL bench.backfill_ctid_chunked_order(1000, 100000, 100);"
```

Helper script:

```powershell
.\scripts.ps1 BackfillCtidChunked -BatchSize 1000 -QueueChunkRows 100000 -LogEvery 100
```

The procedure:

- snapshots the current backfill candidates into a temp queue
- processes that queue in batches of `p_batch_size`
- commits after each batch
- records the final metrics in `bench.benchmark_runs`

For `ctid_chunked`, the queue is built in repeated CTID-ordered chunks of `queue_chunk_rows` rows instead of one giant upfront queue. In that variant, `queue_build_ms` is the cumulative time spent building all queue chunks.

If concurrent OLTP updates populate `extracted_at` before the backfill reaches a row, that row is still removed from the queue but is not rewritten again.

### Step 4: Read The Result

```powershell
docker compose exec -T postgres psql -U postgres -d bench -c "SELECT variant, batch_size, queue_chunk_rows, queue_rows, rows_processed, rows_updated, queue_build_ms, elapsed_ms, rows_processed_per_sec, rows_updated_per_sec, started_at, finished_at FROM bench.benchmark_runs ORDER BY started_at DESC LIMIT 5;"
```

Helper script:

```powershell
.\scripts.ps1 Results -ResultLimit 5
```

### Step 5: Wait For The Workload Jobs

```powershell
Receive-Job $updateJob -Wait
Receive-Job $insertJob -Wait
Remove-Job $updateJob, $insertJob
```

## Compare Both Variants Fairly

Recommended pattern:

```powershell
docker compose exec -T postgres psql -U postgres -d bench -v ON_ERROR_STOP=1 -c "CALL bench.seed_fixture(1000000, 2.5, 128, 2500);"
# run GUID ordered variant

docker compose exec -T postgres psql -U postgres -d bench -v ON_ERROR_STOP=1 -c "CALL bench.seed_fixture(1000000, 2.5, 128, 2500);"
# run CTID ordered variant
```

That avoids comparing one run on a freshly loaded heap and the next run on a table that has already been fully rewritten once.

Helper script:

```powershell
.\scripts.ps1 Compare -RowCount 1000000 -HotPct 2.5 -TemplateCount 128 -BlobTargetBytes 2500 -BatchSize 1000 -LogEvery 100 -WorkloadSeconds 600
```

To compare only the two CTID strategies:

```powershell
.\scripts.ps1 CompareCtid -RowCount 1000000 -HotPct 2.5 -TemplateCount 128 -BlobTargetBytes 2500 -BatchSize 1000 -QueueChunkRows 100000 -LogEvery 100 -WorkloadSeconds 600
```

## Procedures And Functions

Main fixture procedures:

- `CALL bench.seed_fixture(row_count, hot_pct, template_count, blob_target_bytes);`
- `CALL bench.reset_fixture();`

Backfill procedures:

- `CALL bench.backfill_guid_pk_order(batch_size, log_every);`
- `CALL bench.backfill_ctid_order(batch_size, log_every);`
- `CALL bench.backfill_ctid_chunked_order(batch_size, queue_chunk_rows, log_every);`

Concurrent workload helpers used by `pgbench`:

- `SELECT bench.touch_hot_seed_row(slot);`
- `SELECT bench.insert_oltp_row(seed);`

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
docker compose exec -T postgres psql -U postgres -d bench -c "CALL bench.reset_fixture();"
```

Helper script:

```powershell
.\scripts.ps1 Reset
```

Stop Postgres:

```powershell
docker compose down
```

Helper script:

```powershell
.\scripts.ps1 Down
```

Stop and delete the database volume:

```powershell
docker compose down -v
```

Helper script:

```powershell
.\scripts.ps1 Destroy
```
