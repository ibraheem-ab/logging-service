-- The tenant-leading primary key created in 0004 serves every rollup query.
-- This older non-tenant index only adds maintenance work to hot rollup writes.
DROP INDEX IF EXISTS log_second_rollups_bucket_idx;
