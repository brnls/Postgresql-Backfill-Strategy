# Experiment Summary

This document summarizes the main learnings from the local benchmark work in this repo. It is meant to capture the high-level conclusions, not the full runbook.

## Goal

The question behind this experiment was:

- for a large OLTP table with ongoing inserts and updates
- where a new nullable `timestamptz` column is being backfilled from a large toasted `jsonb` payload
- and the primary key is a non-sequential UUID

is it worthwhile to backfill existing rows in `ctid` order rather than GUID primary-key order?

## Workload Shape Modeled

The benchmark models a migration pattern with these properties:

- a large `bench.orders` table
- random UUID primary key
- a large `jsonb` payload that is materially TOAST-backed
- a new `extracted_at` column populated from `payload ->> 'eventTs'`
- new inserts and updates already dual-write `extracted_at`
- a concurrent OLTP workload that mostly updates the newest `2-3%` of seeded rows

The benchmark is focused on backfill throughput, not end-user latency.

## Main Conclusion

The full upfront CTID queue was the best-performing approach tested for the primary question in this experiment.

On the local 1M-row benchmark runs, full `ctid` ordering materially outperformed GUID-order backfill. In the representative pair recorded during the session:

- `ctid` finished in about `27.8s`
- `guid` finished in about `42.4s`

That is roughly:

- `34%` lower elapsed time for `ctid`
- `51%` higher update throughput for `ctid`

The practical takeaway is that CTID-ordered updates can still provide a meaningful win even when the source data is read from large toasted `jsonb`.

## What Appears To Be Happening

The most plausible explanation is:

- GUID order is effectively a random walk through the heap because the primary key is random
- CTID order makes the backfill behave much more like a forward pass through the heap
- the OLTP workload is concentrated on newer rows, so processing in physical order also tends to delay more of the hot region until later in the run

The toasted `jsonb` cost does not disappear, but it also did not erase the heap-locality advantage in this benchmark.

## Important PostgreSQL Planner Finding

One of the biggest learnings from this session is that PostgreSQL 16 does not treat TID range scans as ordered paths for `ORDER BY ctid`.

That matters because queries shaped like:

- `WHERE extracted_at IS NULL ORDER BY ctid LIMIT ...`
- `WHERE extracted_at IS NULL AND ctid > last_seen_tid ORDER BY ctid LIMIT ...`

can end up planning as:

- broad heap scans or partial-index scans
- plus an explicit sort

rather than as a cheap "resume walking the heap in CTID order" operation.

This explains several of the benchmark results below.

## Approaches Tested

### 1. GUID PK ordered backfill

This was the original baseline:

- build a queue of rows by logical key
- process batches ordered by random UUID primary key

Result:

- correct
- straightforward
- materially slower than CTID order on this workload

### 2. Full upfront CTID queue

This variant:

- scans the table once for rows where `extracted_at IS NULL`
- stores `id` and `ctid` in a temp queue
- indexes the temp queue by `heap_tid`
- updates rows by `id` while consuming the queue in CTID order

Result:

- best throughput of the correctness-preserving variants tested
- no repeated `ORDER BY ctid` against the live table
- one large upfront queue build, then efficient batched processing

This is the strongest option found in this session for the original CTID-vs-GUID question.

### 3. CTID chunked queue

This variant was explored as a possible way to reduce the impact of the one large upfront queue build.

The idea was:

- build a queue in chunks using live-table queries ordered by CTID
- process each chunk
- then build the next chunk

What we learned:

- the chunk builder queries still had to do expensive live-table work of the form `ORDER BY ctid LIMIT ...`
- PostgreSQL did not optimize those queries into a cheap forward CTID walk
- instead, they could plan as broad scan-and-sort operations
- that cost was paid once per chunk

Result:

- slower than full CTID queue
- operationally unattractive for large tables because the expensive discovery step repeats

Conclusion:

- this approach is not considered a reasonable option in this repo anymore
- it has been removed from the supported benchmark surface

### 4. Live CTID cursor with `ORDER BY ctid`

This variant tried to avoid a queue entirely and move through the live table using `ctid > last_seen_tid`.

What we learned:

- the `ORDER BY ctid` was the problem
- each batch paid significant selection cost
- on the 1M-row benchmark, the cumulative batch-selection time dominated the whole run

Representative result:

- `ctid_live` took about `102.3s`

Conclusion:

- correctness can be acceptable if the application invariant holds
- but performance was much worse than the full upfront CTID queue

### 5. Live CTID cursor without `ORDER BY`

This was an interesting experiment because removing `ORDER BY ctid` allowed PostgreSQL to use a very fast TID range scan.

It was fast, but it was removed.

Why it was removed:

- without `ORDER BY`, SQL does not guarantee the rows are returned in increasing CTID order
- the procedure advanced the cursor using the maximum CTID seen in each batch
- if the batch were not actually ordered, rows could be skipped

Conclusion:

- interesting as a planner/performance experiment
- not acceptable when correctness is required

## Correctness Learnings

### Using `ctid` only for ordering is fine

The full upfront CTID queue is safe because:

- the queue stores row identity by `id`
- `ctid` is only used to define processing order
- the actual update targets rows by primary key

So concurrent updates changing a row's `ctid` do not break the queue once it has been snapshotted.

### Using `ctid` as a live cross-batch cursor is different

Variants that use:

- `WHERE ctid > last_seen_tid`

against the live table depend on an application invariant:

- every concurrent update that changes the source data also writes `extracted_at` correctly

If that invariant holds, skipping a row that moved because of a concurrent update is acceptable because the row no longer needs the backfill.

If that invariant does not hold, live CTID cursor approaches can miss rows.

### Regular VACUUM is not the issue

Normal `VACUUM` does not move live rows. The main source of CTID change during the benchmark is ordinary `UPDATE`.

Rewrite operations such as:

- `VACUUM FULL`
- `CLUSTER`
- table-rewriting DDL

can change CTIDs broadly and should not be mixed casually with CTID-based migration logic.

## What This Means For Production Thinking

If the goal is maximum backfill throughput for this migration pattern, the best option found here is:

- build one full queue of candidate rows
- store `id` and `ctid`
- process that queue in CTID order
- update the base table by primary key

If the queue-build step itself is too operationally expensive for a very large production table, the next promising direction is probably not more CTID-based chunking. It is more likely:

- chunk by a stable logical key with an index, such as `id` or possibly `(created_at, id)`
- optionally store `ctid` in the per-chunk queue
- still process each chunk in CTID order once discovered

That path was discussed but not fully implemented in this round.

## High-Level Recommendation

Based on this session:

- keep full CTID queue as the primary CTID benchmark shape
- treat GUID-order backfill as the meaningful baseline
- do not rely on repeated live `ORDER BY ctid LIMIT ...` queries as a chunking strategy
- do not use the no-sort live CTID cursor if correctness is mandatory
- if a chunked production design is needed, explore logical-key chunk discovery rather than CTID-driven chunk discovery

## Limits Of These Results

These findings are directional, not universal.

They come from:

- a local Dockerized PostgreSQL 16 environment
- a synthetic workload
- a specific hot-row pattern
- a specific row shape with TOAST-heavy `jsonb`

Different conclusions are possible if:

- the table fits comfortably in cache
- the source column is much smaller
- updates are distributed differently
- physical row order has been heavily disrupted over time

Even so, the main planner and access-pattern findings from this session are strong enough to guide the next round of testing and the likely production strategy.
