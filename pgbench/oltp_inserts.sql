\set insert_seed random(1, 2147483647)
SELECT bench.insert_oltp_row(:insert_seed);
