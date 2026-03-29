CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS bench;

CREATE TABLE IF NOT EXISTS bench.orders (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    extracted_at timestamptz,
    source text NOT NULL CHECK (source IN ('seed', 'oltp')),
    is_hot boolean NOT NULL DEFAULT false,
    update_count integer NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS orders_created_at_idx
    ON bench.orders (created_at);

CREATE INDEX IF NOT EXISTS orders_hot_idx
    ON bench.orders (is_hot, created_at DESC);

CREATE INDEX IF NOT EXISTS orders_backfill_idx
    ON bench.orders (extracted_at)
    WHERE extracted_at IS NULL;

CREATE TABLE IF NOT EXISTS bench.payload_templates (
    template_id integer PRIMARY KEY,
    blob_text text NOT NULL
);

CREATE TABLE IF NOT EXISTS bench.hot_seed_ids (
    slot bigint PRIMARY KEY,
    id uuid NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS bench.benchmark_runs (
    run_id uuid PRIMARY KEY,
    variant text NOT NULL,
    batch_size integer NOT NULL,
    queue_chunk_rows integer,
    queue_rows bigint NOT NULL,
    rows_processed bigint NOT NULL,
    rows_updated bigint NOT NULL,
    queue_build_ms numeric(18,3) NOT NULL,
    elapsed_ms numeric(18,3) NOT NULL,
    rows_processed_per_sec numeric(18,2) NOT NULL,
    rows_updated_per_sec numeric(18,2) NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz NOT NULL
);

ALTER TABLE bench.benchmark_runs
    ADD COLUMN IF NOT EXISTS queue_chunk_rows integer;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'bench.benchmark_runs'::regclass
          AND conname = 'benchmark_runs_variant_check'
    ) THEN
        ALTER TABLE bench.benchmark_runs
            DROP CONSTRAINT benchmark_runs_variant_check;
    END IF;

    ALTER TABLE bench.benchmark_runs
        ADD CONSTRAINT benchmark_runs_variant_check
        CHECK (variant IN ('guid', 'ctid', 'ctid_chunked'));
EXCEPTION
    WHEN duplicate_object THEN
        NULL;
END;
$$;

CREATE SEQUENCE IF NOT EXISTS bench.oltp_template_seq START WITH 1 INCREMENT BY 1;

CREATE OR REPLACE FUNCTION bench.extract_payload_ts(p_payload jsonb)
RETURNS timestamptz
LANGUAGE sql
IMMUTABLE
STRICT
AS $$
    SELECT (p_payload ->> 'eventTs')::timestamptz;
$$;

CREATE OR REPLACE FUNCTION bench.make_payload(
    p_event_ts timestamptz,
    p_template_id integer,
    p_seed bigint DEFAULT 0
)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
    SELECT jsonb_build_object(
        'eventTs', to_jsonb(p_event_ts),
        'blob', t.blob_text,
        'seed', p_seed,
        'templateId', t.template_id
    )
    FROM bench.payload_templates AS t
    WHERE t.template_id = p_template_id;
$$;

CREATE OR REPLACE PROCEDURE bench.ensure_payload_templates(
    p_template_count integer DEFAULT 128,
    p_blob_target_bytes integer DEFAULT 2500
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_segments integer;
BEGIN
    IF p_template_count < 1 THEN
        RAISE EXCEPTION 'p_template_count must be at least 1';
    END IF;

    IF p_blob_target_bytes < 2000 THEN
        RAISE EXCEPTION 'p_blob_target_bytes must be at least 2000 to make TOAST likely';
    END IF;

    v_segments := CEIL(p_blob_target_bytes / 32.0)::integer + 1;

    TRUNCATE TABLE bench.payload_templates;

    INSERT INTO bench.payload_templates (template_id, blob_text)
    SELECT
        template_id,
        LEFT(
            (
                SELECT string_agg(md5(format('%s:%s', template_id, seg_id)), '')
                FROM generate_series(1, v_segments) AS seg_id
            ),
            p_blob_target_bytes
        )
    FROM generate_series(1, p_template_count) AS template_id;

    PERFORM setval('bench.oltp_template_seq', 1, false);
END;
$$;

CREATE OR REPLACE PROCEDURE bench.seed_fixture(
    p_row_count bigint DEFAULT 1000000,
    p_hot_pct numeric DEFAULT 2.5,
    p_template_count integer DEFAULT 128,
    p_blob_target_bytes integer DEFAULT 2500,
    p_history_span interval DEFAULT interval '365 days'
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_hot_start_row bigint;
    v_start_ts timestamptz;
    v_end_ts timestamptz;
BEGIN
    IF p_row_count < 1 THEN
        RAISE EXCEPTION 'p_row_count must be at least 1';
    END IF;

    IF p_hot_pct <= 0 OR p_hot_pct >= 100 THEN
        RAISE EXCEPTION 'p_hot_pct must be between 0 and 100';
    END IF;

    CALL bench.ensure_payload_templates(p_template_count, p_blob_target_bytes);

    TRUNCATE TABLE bench.hot_seed_ids;
    TRUNCATE TABLE bench.orders;

    v_start_ts := clock_timestamp() - p_history_span;
    v_end_ts := clock_timestamp() - interval '1 minute';
    v_hot_start_row := GREATEST(
        1,
        FLOOR(p_row_count::numeric * (100 - p_hot_pct) / 100.0)::bigint + 1
    );

    INSERT INTO bench.orders (
        id,
        created_at,
        updated_at,
        payload,
        extracted_at,
        source,
        is_hot,
        update_count
    )
    SELECT
        gen_random_uuid(),
        seeded.seed_ts,
        seeded.seed_ts,
        jsonb_build_object(
            'eventTs', to_jsonb(seeded.seed_ts),
            'blob', pt.blob_text,
            'seed', gs,
            'templateId', pt.template_id
        ),
        NULL,
        'seed',
        gs >= v_hot_start_row,
        0
    FROM generate_series(1, p_row_count) AS gs
    JOIN bench.payload_templates AS pt
      ON pt.template_id = ((gs - 1) % p_template_count + 1)::integer
    CROSS JOIN LATERAL (
        SELECT v_start_ts + ((v_end_ts - v_start_ts) * (gs::double precision / p_row_count::double precision)) AS seed_ts
    ) AS seeded;

    INSERT INTO bench.hot_seed_ids (slot, id)
    SELECT
        row_number() OVER (ORDER BY created_at, id),
        id
    FROM bench.orders
    WHERE source = 'seed'
      AND is_hot;

    ANALYZE bench.orders;
    ANALYZE bench.hot_seed_ids;
END;
$$;

CREATE OR REPLACE PROCEDURE bench.reset_fixture()
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM bench.orders
    WHERE source = 'oltp';

    UPDATE bench.orders
    SET extracted_at = NULL,
        updated_at = created_at,
        update_count = 0
    WHERE source = 'seed';

    ANALYZE bench.orders;
END;
$$;

CREATE OR REPLACE FUNCTION bench.touch_hot_seed_row(p_slot bigint)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows integer;
BEGIN
    UPDATE bench.orders AS o
    SET updated_at = clock_timestamp(),
        update_count = o.update_count + 1,
        extracted_at = COALESCE(o.extracted_at, bench.extract_payload_ts(o.payload))
    FROM bench.hot_seed_ids AS h
    WHERE h.slot = p_slot
      AND o.id = h.id;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows;
END;
$$;

CREATE OR REPLACE FUNCTION bench.insert_oltp_row(p_seed bigint DEFAULT 0)
RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
    v_template_count integer;
    v_template_id integer;
    v_now timestamptz;
    v_id uuid;
BEGIN
    SELECT COUNT(*) INTO v_template_count
    FROM bench.payload_templates;

    IF v_template_count = 0 THEN
        RAISE EXCEPTION 'No payload templates exist. Run bench.seed_fixture first.';
    END IF;

    v_template_id := (((nextval('bench.oltp_template_seq') - 1) % v_template_count) + 1)::integer;
    v_now := clock_timestamp();

    INSERT INTO bench.orders (
        id,
        created_at,
        updated_at,
        payload,
        extracted_at,
        source,
        is_hot,
        update_count
    )
    SELECT
        gen_random_uuid(),
        v_now,
        v_now,
        bench.make_payload(v_now, v_template_id, p_seed),
        bench.extract_payload_ts(bench.make_payload(v_now, v_template_id, p_seed)),
        'oltp',
        true,
        0
    RETURNING id INTO v_id;

    RETURN v_id;
END;
$$;

CREATE OR REPLACE PROCEDURE bench.backfill_internal(
    p_variant text,
    p_batch_size integer DEFAULT 1000,
    p_log_every integer DEFAULT 100
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_run_id uuid := gen_random_uuid();
    v_started_at timestamptz := clock_timestamp();
    v_queue_started_at timestamptz;
    v_finished_at timestamptz;
    v_queue_rows bigint := 0;
    v_rows_picked integer := 0;
    v_rows_updated integer := 0;
    v_total_processed bigint := 0;
    v_total_updated bigint := 0;
    v_batches integer := 0;
    v_queue_build_ms numeric(18,3);
    v_elapsed_ms numeric(18,3);
BEGIN
    IF p_variant NOT IN ('guid', 'ctid') THEN
        RAISE EXCEPTION 'Unsupported variant: %', p_variant;
    END IF;

    IF p_batch_size < 1 THEN
        RAISE EXCEPTION 'p_batch_size must be at least 1';
    END IF;

    DROP TABLE IF EXISTS pg_temp.backfill_queue;

    IF p_variant = 'guid' THEN
        CREATE TEMP TABLE backfill_queue (
            queue_row_id bigserial PRIMARY KEY,
            id uuid NOT NULL
        ) ON COMMIT PRESERVE ROWS;

        v_queue_started_at := clock_timestamp();

        INSERT INTO backfill_queue (id)
        SELECT id
        FROM bench.orders
        WHERE extracted_at IS NULL;

        GET DIAGNOSTICS v_queue_rows = ROW_COUNT;

        CREATE INDEX backfill_queue_order_idx
            ON backfill_queue (id, queue_row_id);
    ELSE
        CREATE TEMP TABLE backfill_queue (
            queue_row_id bigserial PRIMARY KEY,
            id uuid NOT NULL,
            heap_tid tid NOT NULL
        ) ON COMMIT PRESERVE ROWS;

        v_queue_started_at := clock_timestamp();

        INSERT INTO backfill_queue (id, heap_tid)
        SELECT
            id,
            ctid
        FROM bench.orders
        WHERE extracted_at IS NULL;

        GET DIAGNOSTICS v_queue_rows = ROW_COUNT;

        CREATE INDEX backfill_queue_order_idx
            ON backfill_queue (heap_tid, queue_row_id);
    END IF;

    ANALYZE backfill_queue;

    v_queue_build_ms := EXTRACT(epoch FROM clock_timestamp() - v_queue_started_at) * 1000.0;

    RAISE NOTICE '[%] queued % rows in % ms', p_variant, v_queue_rows, ROUND(v_queue_build_ms, 3);

    COMMIT;

    LOOP
        IF p_variant = 'guid' THEN
            WITH next_batch AS (
                DELETE FROM backfill_queue AS q
                WHERE q.queue_row_id IN (
                    SELECT queue_row_id
                    FROM backfill_queue
                    ORDER BY id, queue_row_id
                    LIMIT p_batch_size
                )
                RETURNING q.id
            ),
            updated AS (
                UPDATE bench.orders AS o
                SET extracted_at = bench.extract_payload_ts(o.payload)
                FROM next_batch AS b
                WHERE o.id = b.id
                  AND o.extracted_at IS NULL
                RETURNING o.id
            )
            SELECT
                (SELECT COUNT(*) FROM next_batch),
                (SELECT COUNT(*) FROM updated)
            INTO v_rows_picked, v_rows_updated;
        ELSE
            WITH next_batch AS (
                DELETE FROM backfill_queue AS q
                WHERE q.queue_row_id IN (
                    SELECT queue_row_id
                    FROM backfill_queue
                    ORDER BY heap_tid, queue_row_id
                    LIMIT p_batch_size
                )
                RETURNING q.id
            ),
            updated AS (
                UPDATE bench.orders AS o
                SET extracted_at = bench.extract_payload_ts(o.payload)
                FROM next_batch AS b
                WHERE o.id = b.id
                  AND o.extracted_at IS NULL
                RETURNING o.id
            )
            SELECT
                (SELECT COUNT(*) FROM next_batch),
                (SELECT COUNT(*) FROM updated)
            INTO v_rows_picked, v_rows_updated;
        END IF;

        EXIT WHEN v_rows_picked = 0;

        v_batches := v_batches + 1;
        v_total_processed := v_total_processed + v_rows_picked;
        v_total_updated := v_total_updated + v_rows_updated;

        IF p_log_every > 0 AND MOD(v_batches, p_log_every) = 0 THEN
            RAISE NOTICE '[%] batch %, processed %, updated %, elapsed_ms %',
                p_variant,
                v_batches,
                v_total_processed,
                v_total_updated,
                ROUND(EXTRACT(epoch FROM clock_timestamp() - v_started_at) * 1000.0, 3);
        END IF;

        COMMIT;
    END LOOP;

    v_finished_at := clock_timestamp();
    v_elapsed_ms := EXTRACT(epoch FROM v_finished_at - v_started_at) * 1000.0;

    INSERT INTO bench.benchmark_runs (
        run_id,
        variant,
        batch_size,
        queue_chunk_rows,
        queue_rows,
        rows_processed,
        rows_updated,
        queue_build_ms,
        elapsed_ms,
        rows_processed_per_sec,
        rows_updated_per_sec,
        started_at,
        finished_at
    )
    VALUES (
        v_run_id,
        p_variant,
        p_batch_size,
        NULL,
        v_queue_rows,
        v_total_processed,
        v_total_updated,
        v_queue_build_ms,
        v_elapsed_ms,
        CASE
            WHEN v_elapsed_ms = 0 THEN 0
            ELSE ROUND((v_total_processed::numeric * 1000.0) / v_elapsed_ms, 2)
        END,
        CASE
            WHEN v_elapsed_ms = 0 THEN 0
            ELSE ROUND((v_total_updated::numeric * 1000.0) / v_elapsed_ms, 2)
        END,
        v_started_at,
        v_finished_at
    );

    RAISE NOTICE '[%] finished: processed %, updated %, elapsed_ms %',
        p_variant,
        v_total_processed,
        v_total_updated,
        ROUND(v_elapsed_ms, 3);
END;
$$;

CREATE OR REPLACE PROCEDURE bench.backfill_ctid_chunked_order(
    p_batch_size integer DEFAULT 1000,
    p_queue_chunk_rows integer DEFAULT 100000,
    p_log_every integer DEFAULT 100
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_run_id uuid := gen_random_uuid();
    v_started_at timestamptz := clock_timestamp();
    v_chunk_started_at timestamptz;
    v_finished_at timestamptz;
    v_last_heap_tid tid;
    v_chunk_rows integer := 0;
    v_rows_picked integer := 0;
    v_rows_updated integer := 0;
    v_total_queue_rows bigint := 0;
    v_total_processed bigint := 0;
    v_total_updated bigint := 0;
    v_batches integer := 0;
    v_chunk_builds integer := 0;
    v_queue_build_ms numeric(18,3) := 0;
    v_elapsed_ms numeric(18,3);
BEGIN
    IF p_batch_size < 1 THEN
        RAISE EXCEPTION 'p_batch_size must be at least 1';
    END IF;

    IF p_queue_chunk_rows < 1 THEN
        RAISE EXCEPTION 'p_queue_chunk_rows must be at least 1';
    END IF;

    DROP TABLE IF EXISTS pg_temp.backfill_queue;

    CREATE TEMP TABLE backfill_queue (
        queue_row_id bigserial PRIMARY KEY,
        id uuid NOT NULL,
        heap_tid tid NOT NULL
    ) ON COMMIT PRESERVE ROWS;

    CREATE INDEX backfill_queue_order_idx
        ON backfill_queue (heap_tid, queue_row_id);

    LOOP
        v_chunk_started_at := clock_timestamp();

        IF v_last_heap_tid IS NULL THEN
            WITH queued AS (
                INSERT INTO backfill_queue (id, heap_tid)
                SELECT
                    id,
                    ctid
                FROM bench.orders
                WHERE extracted_at IS NULL
                ORDER BY ctid
                LIMIT p_queue_chunk_rows
                RETURNING heap_tid
            )
            SELECT COUNT(*), MAX(heap_tid)
            INTO v_chunk_rows, v_last_heap_tid
            FROM queued;
        ELSE
            WITH queued AS (
                INSERT INTO backfill_queue (id, heap_tid)
                SELECT
                    id,
                    ctid
                FROM bench.orders
                WHERE extracted_at IS NULL
                  AND ctid > v_last_heap_tid
                ORDER BY ctid
                LIMIT p_queue_chunk_rows
                RETURNING heap_tid
            )
            SELECT COUNT(*), MAX(heap_tid)
            INTO v_chunk_rows, v_last_heap_tid
            FROM queued;
        END IF;

        EXIT WHEN v_chunk_rows = 0;

        v_chunk_builds := v_chunk_builds + 1;
        v_total_queue_rows := v_total_queue_rows + v_chunk_rows;
        v_queue_build_ms := v_queue_build_ms + (EXTRACT(epoch FROM clock_timestamp() - v_chunk_started_at) * 1000.0);

        ANALYZE backfill_queue;

        RAISE NOTICE '[ctid_chunked] chunk %, queued % rows up to %, cumulative_queue_build_ms %',
            v_chunk_builds,
            v_chunk_rows,
            v_last_heap_tid,
            ROUND(v_queue_build_ms, 3);

        COMMIT;

        LOOP
            WITH next_batch AS (
                DELETE FROM backfill_queue AS q
                WHERE q.queue_row_id IN (
                    SELECT queue_row_id
                    FROM backfill_queue
                    ORDER BY heap_tid, queue_row_id
                    LIMIT p_batch_size
                )
                RETURNING q.id
            ),
            updated AS (
                UPDATE bench.orders AS o
                SET extracted_at = bench.extract_payload_ts(o.payload)
                FROM next_batch AS b
                WHERE o.id = b.id
                  AND o.extracted_at IS NULL
                RETURNING o.id
            )
            SELECT
                (SELECT COUNT(*) FROM next_batch),
                (SELECT COUNT(*) FROM updated)
            INTO v_rows_picked, v_rows_updated;

            EXIT WHEN v_rows_picked = 0;

            v_batches := v_batches + 1;
            v_total_processed := v_total_processed + v_rows_picked;
            v_total_updated := v_total_updated + v_rows_updated;

            IF p_log_every > 0 AND MOD(v_batches, p_log_every) = 0 THEN
                RAISE NOTICE '[ctid_chunked] batch %, processed %, updated %, elapsed_ms %',
                    v_batches,
                    v_total_processed,
                    v_total_updated,
                    ROUND(EXTRACT(epoch FROM clock_timestamp() - v_started_at) * 1000.0, 3);
            END IF;

            COMMIT;
        END LOOP;
    END LOOP;

    v_finished_at := clock_timestamp();
    v_elapsed_ms := EXTRACT(epoch FROM v_finished_at - v_started_at) * 1000.0;

    INSERT INTO bench.benchmark_runs (
        run_id,
        variant,
        batch_size,
        queue_chunk_rows,
        queue_rows,
        rows_processed,
        rows_updated,
        queue_build_ms,
        elapsed_ms,
        rows_processed_per_sec,
        rows_updated_per_sec,
        started_at,
        finished_at
    )
    VALUES (
        v_run_id,
        'ctid_chunked',
        p_batch_size,
        p_queue_chunk_rows,
        v_total_queue_rows,
        v_total_processed,
        v_total_updated,
        v_queue_build_ms,
        v_elapsed_ms,
        CASE
            WHEN v_elapsed_ms = 0 THEN 0
            ELSE ROUND((v_total_processed::numeric * 1000.0) / v_elapsed_ms, 2)
        END,
        CASE
            WHEN v_elapsed_ms = 0 THEN 0
            ELSE ROUND((v_total_updated::numeric * 1000.0) / v_elapsed_ms, 2)
        END,
        v_started_at,
        v_finished_at
    );

    RAISE NOTICE '[ctid_chunked] finished: chunks %, queued %, processed %, updated %, elapsed_ms %',
        v_chunk_builds,
        v_total_queue_rows,
        v_total_processed,
        v_total_updated,
        ROUND(v_elapsed_ms, 3);
END;
$$;

CREATE OR REPLACE PROCEDURE bench.backfill_guid_pk_order(
    p_batch_size integer DEFAULT 1000,
    p_log_every integer DEFAULT 100
)
LANGUAGE plpgsql
AS $$
BEGIN
    CALL bench.backfill_internal('guid', p_batch_size, p_log_every);
END;
$$;

CREATE OR REPLACE PROCEDURE bench.backfill_ctid_order(
    p_batch_size integer DEFAULT 1000,
    p_log_every integer DEFAULT 100
)
LANGUAGE plpgsql
AS $$
BEGIN
    CALL bench.backfill_internal('ctid', p_batch_size, p_log_every);
END;
$$;
