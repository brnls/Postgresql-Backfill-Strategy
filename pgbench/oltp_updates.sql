\set hot_slot random(1, :hot_count)
SELECT bench.touch_hot_seed_row(:hot_slot);
